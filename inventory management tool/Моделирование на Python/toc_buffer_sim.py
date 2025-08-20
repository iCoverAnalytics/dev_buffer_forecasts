"""
toc_buffer_sim.py — симулятор пополнения по ТОС (Theory of Constraints) с «буфер-менеджментом».

Новые возможности:
- Порог минимального заказа: НЕ создавать поставку, пока требуемое количество меньше заданного
  (например, --min-order 6). Это удобно, если есть минимальная партия/лот.

Поддерживаемые сценарии:
  1) "Формульный ТОС-B" — ежедневный добор до вершины буфера B без недельных ±33%,
  2) "ТОС-B с недельной корректировкой" — формула + еженедельные шаги ±33% по зонам.

Вход: файл с колонками дат, дневных заказов/продаж и остатков на складе.
Выход: CSV с дневным дашбордом, сводка метрик, графики PNG.

Минимальные требования: Python 3.8+, пакеты pandas, numpy, matplotlib.

Примеры запуска:
----------------
# 1) Прогнать формульный вариант по CSV (автоопределение колонок):
python toc_buffer_sim.py --input mydata.csv --variant formal --outdir out

# 2) То же по XLSX с явным маппингом колонок и порогом минимального заказа 6 шт:
python toc_buffer_sim.py --input mydata.xlsx --sheet Лист1 \
  --date-col date --sales-col total_quantity_orders --stock-col quantity_stocks \
  --variant both --z 1.96 --h 5 --cap 0.90 --L-min 6 --L-max 10 --R 1 \
  --min-order 6

# 3) Запуск с «жёстким полом» по RT_max (L_max+R) и экспедитами в красной зоне:
python toc_buffer_sim.py --input mydata.csv --variant weekly --floor max_RT --expedite-on-red --min-order 6

Ключевые настройки:
-------------------
- z: целевой уровень сервиса (1.28≈90%, 1.65≈95%, 1.96≈97.5%, 2.33≈99%).
- h: half-life для EWMA спроса (меньше — быстрее реагируем, но нервнее).
- cap: квантильный кап (0.90=90-перцентиль, умеряет всплески).
- floor: пол буфера ("min_RT" = L_min+R; "max_RT" = L_max+R; "none").
- expedite-on-red: в красной зоне заказы идут с L=L_min (ускорение).
- min-order: минимальный размер заказа (если q < min-order — не заказываем в этот день).
- variant: "formal", "weekly" или "both".
"""

from __future__ import annotations
import argparse
import math
import os
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ============================== УТИЛИТЫ ==============================

def percentile(a: np.ndarray, p: float) -> float:
    """
    Плавный перцентиль (линейная интерполяция между соседями).
    a : массив значений (можно список/Series)
    p : доля в [0,1], например 0.90 для 90-го перцентиля
    """
    arr = np.asarray(a, dtype=float)
    if arr.size == 0:
        return 0.0
    p = max(0.0, min(1.0, float(p)))
    k = (arr.size - 1) * p
    f, c = int(np.floor(k)), int(np.ceil(k))
    s = np.sort(arr)
    if f == c:
        return float(s[f])
    return float(s[f] * (c - k) + s[c] * (k - f))


def alpha_from_half_life(h: float) -> float:
    """
    Перевод half-life -> параметр сглаживания EWMA (alpha).
    Half-life h — за сколько наблюдений вес падает в 2 раза.
    """
    h = max(1e-6, float(h))
    return 1.0 - 2.0 ** (-1.0 / h)


def ewma_level_and_var(
    last_30: List[float],
    h: float = 5.0,
    quantile_cap: float = 0.90,
    use_weighted_median: bool = True,
    change_detector_k: int = 5,
    change_detector_m: int = 15,
    change_ratio_down: float = 0.7,
) -> Tuple[float, float]:
    """
    Оценка текущего уровня спроса (lambda) и дисперсии (s2) на базе последних 30 дней.
    - EWMA по капированным данным (кап на quantile_cap, напр. 0.90 от последних 30).
    - Необязательное "подстрахованное" поведение: если последние k << предыдущих m,
      подстраиваемся вниз быстрее.
    - Можно заменить среднее на «взвешенную медиану» (robust) по времени.

    Возвращает:
      lam  — оценка уровня спроса (шт/день)
      s2   — оценка дисперсии спроса (шт^2/день)
    """
    d_raw = [max(0.0, float(x)) for x in last_30][-30:]
    if len(d_raw) == 0:
        d_raw = [0.0]

    # Кап по квантилю, чтобы промо/шумы не раздували уровень
    cap = percentile(np.array(d_raw), quantile_cap)
    d = [min(x, cap) for x in d_raw]

    alpha = alpha_from_half_life(h)
    gamma = alpha  # оцениваем дисперсию тем же темпом
    init_n = min(5, len(d))
    lam = float(np.mean(d[:init_n])) if init_n > 0 else 0.0
    s2 = float(np.var(d[:init_n])) if init_n > 0 else 0.0

    for x in d:
        e = x - lam
        s2 = gamma * (e * e) + (1.0 - gamma) * s2
        lam = alpha * x + (1.0 - alpha) * lam

    # Быстрая адаптация вниз при падении (k против предыдущих m)
    if len(d) >= change_detector_k + change_detector_m and change_detector_k > 0 and change_detector_m > 0:
        mu_r = float(np.mean(d[-change_detector_k:]))
        mu_p = float(np.mean(d[-(change_detector_k + change_detector_m):-change_detector_k]))
        if mu_p > 0 and (mu_r / mu_p) < change_ratio_down:
            lam = mu_r
            s2 = max(s2 * 0.5, 1e-6)

    # Опционально — более «робастная» замена среднего на взвешенную «медиану»
    if use_weighted_median:
        vals = np.array(d, dtype=float)
        ages = np.arange(len(vals))[::-1]  # 0 = последний день (самый свежий)
        # Веса убывают по времени, а не по значению: ближние дни дают больший вклад
        w = 0.5 ** (ages / max(1e-6, h))
        # Weighted median по значению:
        ord_idx = np.argsort(vals)
        v = vals[ord_idx]
        ww = w[ord_idx]
        cw = np.cumsum(ww)
        target = 0.5 * cw[-1]
        idx = np.searchsorted(cw, target, side="left")
        lam = float(v[idx])

    return lam, s2


# ============================== СИМУЛЯТОР ==============================

def simulate_tocB(
    df: pd.DataFrame,
    L_min: int = 6,
    L_max: int = 10,
    R: int = 1,
    h: float = 5.0,
    quantile_cap: float = 0.90,
    z: float = 1.65,
    warmup_days: int = 30,
    weekly_adjust: bool = False,
    decrease_rule_green_days: int = 5,
    expedite_on_red: bool = True,
    floor_policy: str = "min_RT",  # "min_RT" | "max_RT" | "none"
    start_mode: str = "actual",    # "actual" | "top_of_buffer" | "expected"
    min_order_qty: float = 0.0,    # <-- НОВОЕ: минимальный заказ; если q < min_order_qty, заказ не создаём
    rng_seed: int = 202,
) -> pd.DataFrame:
    """
    Симулирует политику пополнения по ТОС-B.

    df : DataFrame с колонками
        - 'date'  (datetime64) — дата
        - 'sales' (float)      — заказы/продажи за день
        - 'stock' (float)      — остаток на начало/конец дня (используем как стартовый on-hand на warmup дне)
    L_min, L_max : минимальный/максимальный срок поставки (целые дни)
    R : периодичность пересмотра (в днях). В модели — 1 (ежедневный пересмотр).
    h : half-life для EWMA спроса
    quantile_cap : квантильный кап на последние 30 дней (напр. 0.90)
    z : квантильный множитель запаса (уровень сервиса)
    warmup_days : число дней истории для старта расчёта (начинаем с day=warmup_days)
    weekly_adjust : включить/выключить недельную корректировку буфера ±33%
    decrease_rule_green_days : условие уменьшения буфера — сколько зелёных дней из 7 нужно (например, >=5)
    expedite_on_red : если день в «красной зоне», отправлять заказ с L=L_min (экспедит)
    floor_policy : «пол» буфера: по RT_min (L_min+R), по RT_max (L_max+R) или «none»
    start_mode : как задать стартовый остаток на warmup-день:
        - "actual" — взять фактический stock на warmup дне (если в df есть 'stock')
        - "top_of_buffer" — инициализировать на вершине буфера B0
        - "expected" — инициализировать на уровне lam0 * (E[L]+R)
    min_order_qty : МИН. размер заказа. Пока потребность (B - IP) меньше этого порога — не заказываем.
    rng_seed : зерно генератора для воспроизводимости случайных L в [L_min; L_max]

    Возвращает: DataFrame дневного дашборда с колонками:
        date, sales, served, lost, on_hand_end, arrivals_today, order_q, lead_time_used,
        pipeline_qty, lam, s2, sigmaH, B_base, B_floor, B_applied, B_multiplier,
        IP, penetration_pct, zone, weekly_adjust
    """
    df_sorted = df.sort_values("date").reset_index(drop=True).copy()
    dates = df_sorted["date"].tolist()
    sales = df_sorted["sales"].astype(float).tolist()
    stocks = df_sorted["stock"].astype(float).tolist() if "stock" in df_sorted.columns else [0.0] * len(df_sorted)

    n = len(dates)
    assert warmup_days < n - 1, "Недостаточно истории для warmup_days."

    rng = np.random.default_rng(rng_seed)

    L_mean = (L_min + L_max) / 2.0
    L_var = np.var(list(range(L_min, L_max + 1)), ddof=0)
    RT_mean = L_mean + R
    RT_min = L_min + R
    RT_max = L_max + R

    # История спроса для прогрева и первичной оценки
    history = sales[:warmup_days]
    lam0, s20 = ewma_level_and_var(history, h=h, quantile_cap=quantile_cap, use_weighted_median=True)
    sigmaH0 = math.sqrt(max(0.0, s20) * RT_mean + (lam0 ** 2) * max(0.0, L_var))
    B0 = lam0 * RT_mean + z * sigmaH0

    # Стартовый остаток
    if start_mode == "actual":
        on_hand = float(stocks[warmup_days])
    elif start_mode == "top_of_buffer":
        on_hand = B0
    elif start_mode == "expected":
        on_hand = lam0 * RT_mean
    else:
        raise ValueError("start_mode должен быть 'actual', 'top_of_buffer' или 'expected'")

    pipeline: List[Tuple[int, float]] = []  # список поставок: (день_прихода, количество)
    rows = []
    B_multiplier = 1.0  # для еженедельной корректировки ±33%

    for t_idx in range(warmup_days, n):
        day = dates[t_idx]

        # 1) Переоценка спроса на основе 30 дней
        lam, s2 = ewma_level_and_var(history[-30:], h=h, quantile_cap=quantile_cap, use_weighted_median=True)
        sigmaH = math.sqrt(max(0.0, s2) * RT_mean + (lam ** 2) * max(0.0, L_var))
        B_base = lam * RT_mean + z * sigmaH

        # 2) Пол буфера (floor)
        if floor_policy == "min_RT":
            sigmaH_floor = math.sqrt(max(0.0, s2) * RT_min)  # Var(L)=0 в нижнем RT
            B_floor = lam * RT_min + z * sigmaH_floor
        elif floor_policy == "max_RT":
            sigmaH_floor = math.sqrt(max(0.0, s2) * RT_max)
            B_floor = lam * RT_max + z * sigmaH_floor
        elif floor_policy == "none":
            B_floor = 0.0
        else:
            raise ValueError("floor_policy должен быть 'min_RT' | 'max_RT' | 'none'")

        B_applied = max(B_base * B_multiplier, B_floor)

        # 3) Приходы в начале дня
        arrivals_today = sum(q for (ai, q) in pipeline if ai == t_idx)
        pipeline = [(ai, q) for (ai, q) in pipeline if ai != t_idx]
        on_hand += arrivals_today

        # 4) Позиция запаса до отгрузок; решение о заказе по IP против вершины B
        on_order_within = sum(q for (ai, q) in pipeline if 0 < (ai - t_idx) <= RT_max)
        IP = on_hand + on_order_within  # backorders в этой модели не накапливаем, считаем lost
        p = (B_applied - IP) / B_applied if B_applied > 0 else 0.0
        p = max(0.0, min(1.0, p))
        zone = "GREEN" if p < 1 / 3 else ("YELLOW" if p < 2 / 3 else "RED")

        # 5) Добор до вершины буфера (pull-логика ТОС) с порогом минимального заказа
        q_need = max(0.0, B_applied - IP)  # сколько "надо" до вершины
        # НЕ создаём поставку, пока потребность меньше min_order_qty
        q = q_need if q_need >= float(min_order_qty) else 0.0

        L_used = math.nan
        if q > 0:
            if expedite_on_red and zone == "RED":
                L_used = L_min  # экспедит
            else:
                L_used = int(rng.integers(L_min, L_max + 1))
            ai = min(n - 1, t_idx + int(L_used))
            pipeline.append((ai, q))

        # 6) Сервис сегодняшнего спроса
        d = float(sales[t_idx])
        served = min(on_hand, d)
        on_hand -= served
        lost = d - served  # здесь не переносим бэкордер на завтра; можно доработать при необходимости

        # 7) Еженедельная корректировка буфера (±33%)
        adjust = ""
        if weekly_adjust and ((t_idx - warmup_days + 1) % 7 == 0):
            # Упрощённо смотрим последнюю неделю: если был RED/сток-аут — увеличиваем, если много GREEN — уменьшаем
            week = rows[-6:] if len(rows) >= 6 else []
            red = any(r.get("zone") == "RED" or r.get("lost", 0) > 0 for r in week)
            green_days = sum(1 for r in week if r.get("zone") == "GREEN")
            if red:
                B_multiplier *= 4 / 3
                adjust = "INCREASE +33%"
            elif green_days >= decrease_rule_green_days:
                B_multiplier *= 2 / 3
                adjust = "DECREASE -33%"
            else:
                adjust = "HOLD"

        rows.append({
            "date": day,
            "sales": d,
            "served": served,
            "lost": lost,
            "on_hand_end": on_hand,
            "arrivals_today": arrivals_today,
            "order_q": q,
            "lead_time_used": L_used,
            "pipeline_qty": float(sum(q for (_, q) in pipeline)),
            "lam": lam,
            "s2": s2,
            "sigmaH": sigmaH,
            "B_base": B_base,
            "B_floor": B_floor,
            "B_applied": B_applied,
            "B_multiplier": B_multiplier,
            "IP": IP,
            "penetration_pct": p * 100,
            "zone": zone,
            "weekly_adjust": adjust,
            "q_need": q_need,  # для контроля: "нужно" до вершины буфера
        })

        # 8) Обновляем историю спроса
        history.append(d)

    return pd.DataFrame(rows)


# ============================== ВХОД/ВЫХОД, АВТО-МЭППИНГ ==============================

def autodetect_columns(df: pd.DataFrame,
                       hint_date: Optional[str] = None,
                       hint_sales: Optional[str] = None,
                       hint_stock: Optional[str] = None) -> pd.DataFrame:
    """
    Автоопределение колонок: пытаемся найти дату/продажи/остаток по подстрокам.
    Можно явно подсказать имена через параметры hint_*.
    Возвращает df с колонками 'date', 'sales', 'stock' (stock может отсутствовать — тогда создадим нули).
    """
    df2 = df.copy()
    cols_low = {c: str(c).strip().lower() for c in df2.columns}

    # Если подсказано явно — используем
    date_col = hint_date or next((c for c, s in cols_low.items() if "date" in s or "дата" in s), None)
    sales_col = hint_sales or next((c for c, s in cols_low.items()
                                    if "total_quantity_orders" in s or "sales" in s or "order" in s or "заказ" in s or "продаж" in s), None)
    stock_col = hint_stock or next((c for c, s in cols_low.items()
                                    if "quantity_stocks" in s or "stock" in s or "остат" in s), None)

    rename = {}
    if date_col: rename[date_col] = "date"
    if sales_col: rename[sales_col] = "sales"
    if stock_col: rename[stock_col] = "stock"
    df2 = df2.rename(columns=rename)

    # Базовые проверки
    if "date" not in df2.columns or "sales" not in df2.columns:
        raise ValueError(f"Не удалось распознать колонки. Нашли: {df.columns.tolist()}. "
                         f"Подсказки: --date-col, --sales-col, --stock-col")

    # Типизация и сортировка
    df2["date"] = pd.to_datetime(df2["date"])
    df2 = df2.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    df2["sales"] = pd.to_numeric(df2["sales"], errors="coerce").fillna(0.0)

    if "stock" in df2.columns:
        df2["stock"] = pd.to_numeric(df2["stock"], errors="coerce").fillna(method="ffill").fillna(0.0)
    else:
        df2["stock"] = 0.0  # если нет фактических остатков, симулятор может стартовать от буфера/ожидания

    return df2


def load_table(input_path: str, sheet: Optional[str] = None) -> pd.DataFrame:
    """
    Загружает CSV или XLSX. Для XLSX можно указать лист.
    """
    ext = os.path.splitext(input_path)[1].lower()
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(input_path, sheet_name=sheet or 0)
    else:
        # пытаемся угадать разделитель
        try:
            return pd.read_csv(input_path)
        except Exception:
            return pd.read_csv(input_path, sep=";")


# ============================== ГРАФИКИ И МЕТРИКИ ==============================

def plot_buffer_ip_stock(sim: pd.DataFrame, title: str, out_png: str, base_df: pd.DataFrame | None = None) -> None:
    """
    Рисует график: буфер B, коридоры 2/3 и 1/3, позиция запаса IP, остаток (модельный) на конец дня,
    точки заказов и приходы. Если передан base_df с колонкой 'stock', дополнительно рисует
    линию «Факт остаток (из файла)» по тем же датам (для аудита).
    """
    plt.figure(figsize=(12, 6))
    Bline = sim["B_applied"] if "B_applied" in sim.columns else sim["B_base"]
    plt.plot(sim["date"], Bline, label="Буфер B")
    plt.plot(sim["date"], Bline * (2 / 3), label="2/3 B")
    plt.plot(sim["date"], Bline * (1 / 3), label="1/3 B")
    if "IP" in sim.columns:
        plt.plot(sim["date"], sim["IP"], label="Позиция запаса (IP)")
    if "on_hand_end" in sim.columns:
        plt.plot(sim["date"], sim["on_hand_end"], label="Остаток (модель, конец дня)")
    # добавляем «Факт остаток» при наличии
    if base_df is not None and "stock" in base_df.columns:
        try:
            s_fact = base_df.set_index("date").loc[sim["date"]]["stock"]
            plt.plot(sim["date"], s_fact.values, label="Факт остаток (из файла)")
        except Exception:
            pass
    if "order_q" in sim.columns:
        om = sim["order_q"] > 0
        plt.scatter(sim.loc[om, "date"], sim.loc[om, "IP"], marker="^", label="Заказ (q>0)")
    if "arrivals_today" in sim.columns:
        am = sim["arrivals_today"] > 0
        plt.scatter(sim.loc[am, "date"], sim.loc[am, "IP"], marker="o", label="Приход поставки")

    plt.title(title)
    plt.xlabel("Дата")
    plt.ylabel("Количество, шт")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_png, dpi=160)
    plt.close()


def plot_penetration(sim: pd.DataFrame, title: str, out_png: str) -> None:
    """График пенетрации буфера с отметками 33.3% и 66.7%."""
    plt.figure(figsize=(12, 4))
    if "penetration_pct" in sim.columns:
        plt.plot(sim["date"], sim["penetration_pct"], label="Пенетрация, %")
    plt.axhline(33.3333333, linestyle="--", label="33.3%")
    plt.axhline(66.6666667, linestyle="--", label="66.7%")
    plt.title(title)
    plt.xlabel("Дата")
    plt.ylabel("%")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_png, dpi=160)
    plt.close()


def plot_sales_lambda(sim: pd.DataFrame, base_df: pd.DataFrame, title: str, out_png: str) -> None:
    """Продажи/заказы и оценка уровня спроса λ_t (для контекста)."""
    plt.figure(figsize=(12, 4))
    # align по датам симуляции
    s = base_df.set_index("date").loc[sim["date"]]["sales"]
    plt.plot(sim["date"], s, label="Продажи (день)")
    if "lam" in sim.columns:
        plt.plot(sim["date"], sim["lam"], label="λ_t (уровень спроса)")
    plt.title(title)
    plt.xlabel("Дата")
    plt.ylabel("Шт/день")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_png, dpi=160)
    plt.close()


def summarize(sim: pd.DataFrame) -> pd.DataFrame:
    """
    Считает сводные метрики по симуляции.
    Возвращает небольшую таблицу (1 строка) с показателями.
    """
    overstock_days = int((sim["on_hand_end"] > sim["B_applied"]).sum())
    overstock_ratio = (sim["on_hand_end"] / sim["B_applied"]).replace([np.inf, -np.inf], np.nan)
    summary = {
        "period_start": str(sim["date"].iloc[0].date()),
        "period_end": str(sim["date"].iloc[-1].date()),
        "fill_rate_units": float(sim["served"].sum()) / max(1.0, float(sim["sales"].sum())),
        "stockout_days": int((sim["lost"] > 0).sum()),
        "lost_units": float(sim["lost"].sum()),
        "avg_on_hand": float(sim["on_hand_end"].mean()),
        "min_on_hand": float(sim["on_hand_end"].min()),
        "max_on_hand": float(sim["on_hand_end"].max()),
        "days_overstock_vs_B": overstock_days,
        "max_overstock_ratio_onhand_over_B": float(overstock_ratio.max(skipna=True)),
        "n_orders": int((sim["order_q"] > 0).sum()),
        "total_ordered": float(sim["order_q"].sum()),
        "avg_order_size": float(sim["order_q"].sum() / max(1, (sim["order_q"] > 0).sum())),
    }
    return pd.DataFrame([summary])


# ============================== CLI ==============================

def main():
    parser = argparse.ArgumentParser(description="Симулятор пополнения по ТОС-B (формульный / с недельной корректировкой).")
    parser.add_argument("--input", required=True, help="Путь к файлу CSV/XLSX с данными.")
    parser.add_argument("--sheet", default=None, help="Лист в XLSX (если не указан, берём первый).")
    parser.add_argument("--date-col", default=None, help="Имя колонки даты (если автоопределение не сработает).")
    parser.add_argument("--sales-col", default=None, help="Имя колонки продаж/заказов в день.")
    parser.add_argument("--stock-col", default=None, help="Имя колонки остатков.")
    parser.add_argument("--variant", choices=["formal", "weekly", "both"], default="both",
                        help="Какой сценарий считать: формульный, с недельной корректировкой или оба.")
    parser.add_argument("--L-min", type=int, default=6, help="Минимальный срок поставки (дни).")
    parser.add_argument("--L-max", type=int, default=10, help="Максимальный срок поставки (дни).")
    parser.add_argument("--R", type=int, default=1, help="Частота пересмотра (дни). Обычно 1.")
    parser.add_argument("--z", type=float, default=1.65, help="Квантильный множитель (SLA). Примеры: 1.65, 1.96, 2.33.")
    parser.add_argument("--h", type=float, default=5.0, help="Half-life для EWMA (дней).")
    parser.add_argument("--cap", type=float, default=0.90, help="Квантильный кап (0..1).")
    parser.add_argument("--warmup-days", type=int, default=30, help="Дней прогрева перед стартом симуляции.")
    parser.add_argument("--floor", choices=["min_RT", "max_RT", "none"], default="min_RT",
                        help="Пол буфера: min_RT=L_min+R, max_RT=L_max+R, none=без пола.")
    parser.add_argument("--expedite-on-red", action="store_true", help="Экспедит (L=L_min), если день в красной зоне.")
    parser.add_argument("--start-mode", choices=["actual", "top_of_buffer", "expected"], default="actual",
                        help="Стартовый остаток на warmup-день: из файла, на вершине буфера или ожидаемый.")
    parser.add_argument("--green-days", type=int, default=5, help="Сколько зелёных дней из 7 нужно для -33% (weekly).")
    parser.add_argument("--min-order", type=float, default=0.0, help="МИН. размер заказа. Пока q < min-order — не заказываем.")
    parser.add_argument("--outdir", default="out", help="Папка для итогов (CSV/PNG).")

    args = parser.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    # 1) Загрузка и маппинг колонок
    table = load_table(args.input, sheet=args.sheet)
    df = autodetect_columns(table, args.date_col, args.sales_col, args.stock_col)

    # 2) Сценарии
    to_run = ["formal", "weekly"] if args.variant == "both" else [args.variant]
    all_summaries = []

    for variant in to_run:
        weekly_flag = (variant == "weekly")
        sim = simulate_tocB(
            df=df,
            L_min=args.L_min,
            L_max=args.L_max,
            R=args.R,
            h=args.h,
            quantile_cap=args.cap,
            z=args.z,
            warmup_days=args.warmup_days,
            weekly_adjust=weekly_flag,
            decrease_rule_green_days=args.green_days,
            expedite_on_red=args.expedite_on_red,
            floor_policy=args.floor,
            start_mode=args.start_mode,
            min_order_qty=args.min_order,  # <-- Порог минимального заказа
        )

        # 3) Сохранение результатов
        tag = "weekly" if weekly_flag else "formal"
        csv_path = os.path.join(args.outdir, f"tocB_{tag}_daily.csv")
        sim.to_csv(csv_path, index=False, encoding="utf-8-sig")

        # 4) Графики
        plot_buffer_ip_stock(sim, f"ТОС-B ({'еженед. корректировка' if weekly_flag else 'формульный'})", 
                             os.path.join(args.outdir, f"tocB_{tag}_buffer.png"))
        plot_penetration(sim, f"ТОС-B ({'еженед. корректировка' if weekly_flag else 'формульный'}): пенетрация", 
                         os.path.join(args.outdir, f"tocB_{tag}_penetration.png"))
        plot_sales_lambda(sim, df, "Продажи и уровень спроса λ_t", 
                          os.path.join(args.outdir, f"tocB_{tag}_sales_lambda.png"))

        # 5) Метрики
        summ = summarize(sim)
        summ.insert(0, "variant", tag)
        all_summaries.append(summ)

    # 6) Сводка по всем вариантам
    summary_df = pd.concat(all_summaries, ignore_index=True)
    summary_path = os.path.join(args.outdir, "tocB_summary.csv")
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")

    print("Готово! Итоги в папке:", os.path.abspath(args.outdir))
    print("Сводка:\n", summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
