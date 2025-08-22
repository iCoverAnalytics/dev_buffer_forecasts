import pandas as pd
import math
from datetime import datetime, timedelta
import random
import logging
pd.set_option('display.max_columns', None)
pd.options.mode.chained_assignment = None
pd.options.display.float_format = '{:,.2f}'.format
import sys
sys.path.append(r'\\sigma.icover.ru\share\Аналитика\Скрипты\Сливин\plugins')
from ConnectingOperator import ClickHouseConnector
from preprocess_data_2 import preprocess_data # type: ignore
ch_object = ClickHouseConnector(task_id='my_buffers', local_run=True)

TYPES_KEYS = {
        'date': 'datetime64[ns]',
        'mp': 'string',
        'seller': 'string',
        'sku': 'string',
        'article_1c': 'string',
        'code_1c': 'string',
        'cluster_to': 'string',
        'calc_type': 'string',
        'action_suggested': 'string',
        'min_buffer': 'int64',
        'new_buffer_prev': 'int64',
        'is_first_day':'int64',
        'is_recalc_day':'int64',
        'id_ver': 'int64'
}

TYPES = {
    'id_ver': 'int64',
    'date': 'datetime64[ns]',
    'name': 'string',   
    'article_1c': 'string',
    'code_1c': 'string',
    'mp': 'string',
    'seller': 'string',
    'sku': 'string',
    'cluster_to': 'string',
    'total_quantity_orders': 'int64',
    'max_quantity_order': 'int64',
    'avg_quantity_orders': 'float64',
    'days_oos_number': 'int64',
    'days_bp_number': 'int64',    
    'avg_day_to_cluster': 'int64',
    'insurance_reserv_cluster': 'int64',
    'buffer_cluster_norm': 'int64',
    'buffer_cluster': 'int64',
    'deliveries_to_cluster': 'int64',
    'buffer_cluster_marker_current': 'float64',
    'buffer_cluster_marker': 'float64',
    'quantity_orders': 'int64',
    'quantity_stocks': 'int64',
    'quantity_supplies': 'int64',
    'quantity_supplies_within_rt': 'int64',
    'new_buffer': 'int64',
}

TYPES_SUP = {
        'id_ver': 'string',
        'supply_id': 'string',
        'shipping_date': 'datetime64[ns]',
        'closing_date': 'datetime64[ns]',
        'article_1c': 'string',
        'code_1c': 'string',
        'mp': 'string',
        'seller': 'string',
        'sku': 'string',
        'cluster_to':'string',
        'quantity_supplies': 'int32'
}

PARAMS = {
    # --- Служебные (в т.ч. для формирования первичного буфера) ---   
    'id_ver': 4,                 # версия расчёта 
    'PERIOD': 30,                 # окно истории продаж для расчётов (дни) - используется для init day и используется в т.ч. (пока нет товарной матрицы) для определения пула на расчета буфера
    'AVG_DAYS_CONST': 7,          # дефолтный RT (дней), пока нет фактического по ключу
    'OOS_RATIO': 0.02,            # порог "низкий запас" (для аналитики/отчетов, не влияет на ДУБ)
    'BAD_PRICE_LOW': -0.4,        # нижний порог "плохой" цены (для аналитики; SUSPEND пока не используем)
    'BAD_PRICE_HIGH': 0.4,        # верхний порог "плохой" цены
    'MAX_EXCLUDED_SHARE': 0.2,    # доля исключаемых дней при расчете ADU (используем позже в rebase)
    'INS_CONST': 0,               # страховой резерв (для rebase/целевого BT; не в ДУБ)
    'min_cluster_m3': 8,          # минимальный объем товаров на кластер
    'min_good': 3,                # минимальное количество товаров на поставку
    'min_cluster_limit': 6,       # добавлено потом: максимальный объем, при котором все равно грузим последнюю машину
    'RECALC_COOLDOWN_DAYS': 3,    # пересчет не чаще, чем (кроме красной кнопки)
    # --- ДУБ / ЕГОРОВ: ОКНО НАБЛЮДЕНИЯ ---
    'M_RT': 4,                    # множитель к RT: окно = RT * M_RT (на каждый ключ)
    'UP_STEP': 0.3333,            # шаг увеличения BT (+33.3%)
    'DOWN_STEP': 0.3333,          # шаг уменьшения BT (–33.3%)
    'RED_SHARE_UP': 0.3333,          # UP, если красных >= 1/3 наблюдаемых дней в окне
    'GREEN_SHARE_DOWN': 0.66666,      # DOWN, если зеленых >= 2/3 и нет красн/черн
    'REQUIRE_COVERAGE': 0.6,      # мин. доля календарных дней с валидной зоной в окне; иначе HOLD
    'FAST_RED_STREAK': 3,         # быстрый триггер UP: N подряд дней "красных" в конце окна

    # --- Прогнозирование на основе средневзешенной медианы ---
    'CAP_Q': 0.90,          # чуствительность к проколам
    'H': 5                  # чуствительность к прогнозу

    # --- Прочее ---
    #'RECEIVING_DAYS': 0,          # если приемка/разблокировка не входит в RT, поставь реальное значение (>0) (пока не используем для упрощения)
    #'INCLUDE_BOUNDARY': 1,        # включать поставки с saleable_date == t+RT в InboundWithinRT
    # Фикс проблемы с бесконечным UP
    #'BLOCK_UP_IF_OPEN_INBOUND': 1,   # блокировать апы, если есть открытая поставка
    #'PROMISED_COVER_THR': 0.8,       # порог покрытия обещанными поставками (80% от разрыва по OnHand)

    # --- SUSPEND (временно выключен) ---
    #'USE_SUSPEND': 0,             # до появления Товарной матрицы SUSPEND не применяем
    # 'SUSPEND_HARD': ['listing_block','stop_supply','hard_quarantine'],   # зарезервировано
    # 'SUSPEND_SOFT_DOWN': ['bad_price'],                                   # зарезервировано
    # 'SUSPEND_TTL_DAYS': 30,
    # 'SUSPEND_AUTORELEASE_DAYS': 3,
}

query_volumes = """
    SELECT DISTINCT article_1c, code_1c, mp, seller, volume_m3 
    FROM ref.article_mp
    WHERE volume_m3 != ''
"""
volumes = ch_object.extract_data(query=query_volumes)

volumes = volumes.astype({'volume_m3': 'float'})
volumes.info()


date_range = pd.date_range(start='2025-02-01', end='2025-05-01')

for date in date_range:
    # Преобразуем Timestamp в строку формата 'YYYY-MM-DD'
    START_DATE = date.strftime('%Y-%m-%d')
    print(START_DATE)

    print('query_keys')
    query_keys  = f'''
    /* ===================== ПАРАМЕТРЫ ===================== */
        WITH
            {PARAMS['PERIOD']}              AS PERIOD,              -- интервал для выбора ключей из заказов
            {PARAMS['AVG_DAYS_CONST']}      AS RT_DEFAULT,          -- дефолтный RT (дни) пока нет фактического
            {PARAMS['M_RT']}                AS M_RT,                -- множитель RT (окно = RT * M_RT)
            {PARAMS['RED_SHARE_UP']}        AS RED_SHARE_UP,        -- мин. доля дней с зоной в окне
            {PARAMS['GREEN_SHARE_DOWN']}    AS GREEN_SHARE_DOWN,    -- мин. доля дней с зоной в окне
            {PARAMS['REQUIRE_COVERAGE']}    AS REQUIRE_COVERAGE,    -- мин. доля дней с зоной в окне
            {PARAMS['FAST_RED_STREAK']}     AS FAST_RED_STREAK,     -- быстрый триггер: N последних дней красные
            {PARAMS['RECALC_COOLDOWN_DAYS']} AS RECALC_COOLDOWN_DAYS,
            {PARAMS['id_ver']}              AS CUR_VER

        /* ---------- 1) Кандидаты: строго по PERIOD (Далее будет по Товарной матрице) ---------- */
        , keys_from_sales AS (
            SELECT DISTINCT mp, seller, sku, article_1c, code_1c, cluster_to
            FROM kpi.all_mp_sales_wsb
            WHERE date >= toDate('{START_DATE}') - INTERVAL PERIOD DAY
            AND date <  toDate('{START_DATE}')
        )

        /* уже имеют расчитанный буфер */
        , keys_from_sim_yesterday AS (
            SELECT DISTINCT mp, seller, sku, article_1c, code_1c, cluster_to
            FROM sb.buffer_main
            WHERE date   = toDate('{START_DATE}') - INTERVAL 1 DAY
            AND id_ver = CUR_VER
        )

        /* полный набор ключей */
        , keys_today AS (
            SELECT * FROM keys_from_sales
            UNION DISTINCT
            SELECT * FROM keys_from_sim_yesterday
        )

        /* валидные артикулы только для нужных МП - базовый фильтр (Дальше будет заменен Товарной мтарицей) */
        , valid_articles AS (
            SELECT DISTINCT article_1c, code_1c, sku, mp
            FROM ref.article_mp
            WHERE discounted != 1
            AND brand != 'РЕСЕЙЛ'
            AND mp IN ('Ozon','WB','YM')
        )

        /* есть ли ХОТЬ КАКАЯ история по текущей версии ДО D (а не только на D-1) - в дальнейшем должно быть синхронизировано с SUSPEND */
        , hist_before_today AS (
            SELECT
                mp, seller, sku, article_1c, code_1c, cluster_to,
                count() AS cnt_hist
            FROM sb.buffer_main
            WHERE date < toDate('{START_DATE}')
            AND id_ver = CUR_VER
            GROUP BY mp, seller, sku, article_1c, code_1c, cluster_to
        )

        /* состояние на D-1 */
        , prev_state AS (
            SELECT
                mp, seller, sku, article_1c, code_1c, cluster_to,
                max(toInt16(new_buffer))   AS new_buffer_prev,
                max(buffer_cluster)        AS bt_prev,
                max(quantity_stocks)       AS stocks_prev
            FROM sb.buffer_main
            WHERE date = toDate('{START_DATE}') - INTERVAL 1 DAY
            AND id_ver = CUR_VER
            GROUP BY mp, seller, sku, article_1c, code_1c, cluster_to
        )

        , supplies_within_rt_keys AS (
            SELECT
                s.mp as mp, s.seller as seller, s.sku as sku, s.article_1c as article_1c, s.code_1c as code_1c, s.cluster_to as cluster_to,
                SUM(s.quantity_supplies) AS quantity_supplies_within_rt
            FROM sb.buffer_supplies s
            INNER JOIN keys_today kt
                USING (mp, seller, sku, article_1c, code_1c, cluster_to)
            WHERE s.id_ver = toString(CUR_VER)
            /* поставки, чей closing_date попадает в (D; D+RT] */
            AND s.closing_date >  toDate('{START_DATE}')
            AND s.closing_date <= addDays(toDate('{START_DATE}'), RT_DEFAULT)
            GROUP BY s.mp, s.seller, s.sku, s.article_1c, s.code_1c, s.cluster_to
        )

        /* ---------- 2) Окно ДУБ: необходимо в отдельном CTE, т.к. дальше будет разное кол-во дней окна на разные ключи ---------- */
        , win AS (
            SELECT
                mp, seller, sku, article_1c, code_1c, cluster_to,
                RT_DEFAULT                                    AS rt_days,
                toDate('{START_DATE}') - 1                    AS window_end,
                addDays(toDate('{START_DATE}') - 1, -(RT_DEFAULT * M_RT) + 1) AS window_start
            FROM keys_today kt
        )

        /* ---------- 3) День-стейт из sb.buffer_main за окно ---------- */
        , day_state AS (
        SELECT
            bm.date AS date, bm.mp as mp, bm.seller as seller, bm.sku as sku, bm.article_1c as article_1c, bm.code_1c as code_1c, bm.cluster_to as cluster_to,
            toInt64(argMax(buffer_cluster, bm.date))     AS bt,
            toInt64(argMax(quantity_stocks, bm.date))    AS available   -- только склад
        FROM sb.buffer_main bm
        INNER JOIN win w
            ON bm.mp = w.mp AND bm.seller = w.seller AND bm.sku = w.sku
        AND bm.article_1c = w.article_1c AND bm.code_1c = w.code_1c
        AND bm.cluster_to = w.cluster_to
        WHERE bm.id_ver = CUR_VER
            AND bm.date >= w.window_start
            AND bm.date <= w.window_end
        GROUP BY bm.mp, bm.seller, bm.sku, bm.article_1c, bm.code_1c, bm.cluster_to, bm.date
        )

        /* добавим проникновение и зону (по канону) */
        , day_state_zoned AS (
        SELECT
            date, mp, seller, sku, article_1c, code_1c, cluster_to,
            /* кэпим сверху и снизу */
            greatest(0, least(available, bt))           AS eff_available,
            if(bt > 0, (bt - greatest(0, least(available, bt))) / bt, NULL) AS pos,
            multiIf(
            bt <= 0 OR pos IS NULL,         NULL,
            eff_available <= 0,             'black',
            pos < 1.0/3.0,                  'green',
            pos < 2.0/3.0,                  'yellow',
                                            'red'
            ) AS zone
        FROM day_state
        )

        /* ---------- 4) Агрегация зон и покрытие по окну ---------- */
        , cov AS (
        SELECT
            w.mp as mp, w.seller as seller, w.sku as sku, w.article_1c as article_1c, w.code_1c as code_1c, w.cluster_to as cluster_to,
            w.window_start as window_start, w.window_end as window_end,
            countIf(dsz.zone IS NOT NULL)                            AS days_observed,
            sum(dsz.zone = 'black')                                  AS breach_days,
            sum(dsz.zone = 'red')                                    AS red_days,
            sum(dsz.zone = 'green')                                  AS green_days,
            sum(dsz.zone = 'yellow')                                 AS yellow_days,
            /* пороги от наблюдаемых */
            greatest(1, toUInt32(ceil(days_observed * RED_SHARE_UP)))    AS red_threshold,
            toUInt32(floor(days_observed * GREEN_SHARE_DOWN))             AS green_threshold,
            /* требование покрытия от календаря */
            (days_observed >=
            toUInt32(round((dateDiff('day', w.window_start, w.window_end) + 1) * REQUIRE_COVERAGE))) AS has_coverage
        FROM win w
        LEFT JOIN day_state_zoned dsz
            ON  dsz.mp = w.mp AND dsz.seller = w.seller AND dsz.sku = w.sku
            AND dsz.article_1c = w.article_1c AND dsz.code_1c = w.code_1c
            AND dsz.cluster_to = w.cluster_to
        WHERE (dsz.date BETWEEN w.window_start AND w.window_end) OR (dsz.date IS NULL)
        GROUP BY w.mp, w.seller, w.sku, w.article_1c, w.code_1c, w.cluster_to, w.window_start, w.window_end
        )

        /* ---------- 5) Быстрый триггер: N последних дней подряд — 'red' ---------- */
        , fast_tail AS (
            SELECT
                z.mp as mp,
                z.seller as seller,
                z.sku as sku,
                z.article_1c as article_1c,
                z.code_1c as code_1c,
                z.cluster_to as cluster_to,
                arrayMap(p -> p.2, arraySort(groupArray((z.date, z.zone)))) AS arr_zones,
                /* если наблюдений меньше N — 0; иначе проверяем, что последние N дней = 'red' */
                if(
                    length(arr_zones) < toUInt32(FAST_RED_STREAK),
                    toUInt8(0),
                    toUInt8(
                        arrayAll(x -> x = 'red',
                                arraySlice(arr_zones, -toUInt32(FAST_RED_STREAK), toUInt32(FAST_RED_STREAK)))
                    )
                ) AS fast_red_all_lastN
            FROM (
                SELECT
                    dsz.date,
                    dsz.mp, dsz.seller, dsz.sku, dsz.article_1c, dsz.code_1c, dsz.cluster_to,
                    dsz.zone
                FROM day_state_zoned AS dsz
                INNER JOIN win AS w
                    ON  dsz.mp = w.mp AND dsz.seller = w.seller AND dsz.sku = w.sku
                    AND dsz.article_1c = w.article_1c AND dsz.code_1c = w.code_1c
                    AND dsz.cluster_to = w.cluster_to
                WHERE dsz.date >= w.window_start AND dsz.date <= w.window_end
            ) AS z
            GROUP BY z.mp, z.seller, z.sku, z.article_1c, z.code_1c, z.cluster_to
        )

        /* ---------- 6) Итог: решение ДУБ и маршрутизация ---------- */
        SELECT
            toDate('{START_DATE}') AS date,
            kt.mp AS mp,
            kt.seller AS seller,
            kt.sku AS sku,
            kt.article_1c AS article_1c,
            kt.code_1c AS code_1c, 
            kt.cluster_to AS cluster_to,

            /* action_suggested без фикса поставок */
            multiIf(
                cv.has_coverage = 0,                                  'HOLD',
                ifNull(ft.fast_red_all_lastN, 0) = 1,                 'UP',
                cv.red_days >= cv.red_threshold,                      'UP',
                (cv.breach_days = 0 AND cv.red_days = 0
                    AND cv.green_days >= cv.green_threshold),         'DOWN',
                                                                    'HOLD'
            ) AS action_raw,

            /* итоговое действие с предохранителями */
            multiIf(
                -- 1) есть поставки в пределах RT → удерживаем вместо UP
                (
                action_raw = 'UP' AND ifNull(
                        least((toFloat64(ifNull(p.stocks_prev, 0)) + ifNull(swk.quantity_supplies_within_rt, 0))
                        / nullIf(toFloat64(ifNull(p.bt_prev, 0)), 0),
                        1),0) >= 0.5), 'HOLD',
                -- 2) кулдаун < RECALC_COOLDOWN_DAYS с последнего изменения буфера
                (action_raw IN ('UP','DOWN'))
                AND (ifNull(p.new_buffer_prev, 9999) < toUInt32(RECALC_COOLDOWN_DAYS))
                AND NOT (action_raw = 'UP' AND (
                            ifNull(ft.fast_red_all_lastN, 0) = 1
                            OR cv.breach_days > 0
                            OR cv.red_days >= cv.red_threshold
                        )),
                'HOLD',

                -- else
                action_raw
            ) AS action_suggested,

            /* приоритет: нет истории → first_init; есть сигнал → recalc; иначе simulate */
            multiIf(
                hist.cnt_hist IS NULL OR hist.cnt_hist = 0,           'first_init',
                (action_suggested IN ('UP','DOWN')),                  'recalc_day',
                                                                    'simulate_day'
            ) AS calc_type,
            p.new_buffer_prev AS new_buffer_prev,
            CAST(hist.cnt_hist IS NULL OR hist.cnt_hist = 0 AS UInt8) AS is_first_day,
            CAST((action_suggested IN ('UP','DOWN')) AS UInt8)         AS is_recalc_day,
            CAST(CUR_VER AS UInt8)                                     AS id_ver

        FROM keys_today kt
        INNER JOIN valid_articles va
        USING (article_1c, code_1c, sku, mp)

        LEFT JOIN hist_before_today AS hist
        USING (mp, seller, sku, article_1c, code_1c, cluster_to)

        LEFT JOIN prev_state AS p
        USING (mp, seller, sku, article_1c, code_1c, cluster_to)

        LEFT JOIN cov AS cv
        USING (mp, seller, sku, article_1c, code_1c, cluster_to)

        LEFT JOIN fast_tail AS ft
        USING (mp, seller, sku, article_1c, code_1c, cluster_to)

        LEFT JOIN supplies_within_rt_keys swk
        USING (mp, seller, sku, article_1c, code_1c, cluster_to)


        GROUP BY
            date, mp, seller, sku, article_1c, code_1c, cluster_to,
            action_suggested, calc_type, cv.has_coverage, ft.fast_red_all_lastN, cv.red_days, cv.red_threshold, cv.breach_days, cv.green_days, cv.green_threshold,
            new_buffer_prev, is_first_day, is_recalc_day, id_ver
            '''     
    df_keys = ch_object.extract_data(query_keys)
    df_keys, other_column_list = preprocess_data(df=df_keys, column_types=TYPES_KEYS, add_missed_columns=False, handle_invalid_values=False)
    ch_object.execute_query(query=f'''DELETE FROM sb.buffer_calc_keys WHERE id_ver = toInt32({PARAMS['id_ver']}) AND date = '{START_DATE}'  ''')
    ch_object.insert_data(table_name='sb.buffer_calc_keys', df=df_keys)
    # Наполняем buffer_main

    ch_object.execute_query(query=f'''DELETE FROM sb.buffer_main WHERE id_ver = {PARAMS['id_ver']} AND date = '{START_DATE}'  ''')
    # Инициация буфера (first_init)
    print('query_init')
    query_init  = f'''
    WITH
        toInt32({PARAMS['PERIOD']})                 AS PERIOD,
        toFloat64({PARAMS['OOS_RATIO']})            AS OOS_RATIO,
        toFloat64({PARAMS['BAD_PRICE_LOW']})        AS BAD_PRICE_LOW,
        toFloat64({PARAMS['BAD_PRICE_HIGH']})       AS BAD_PRICE_HIGH,
        toFloat64({PARAMS['MAX_EXCLUDED_SHARE']})   AS MAX_EXCLUDED_SHARE,
        toInt32({PARAMS['AVG_DAYS_CONST']})         AS RT_DEFAULT,
        toInt32({PARAMS['INS_CONST']})              AS INS_CONST,
        toInt32({PARAMS['AVG_DAYS_CONST']}) * toInt32({PARAMS['M_RT']})     AS BUF_NORM_CONST,
        toInt32({PARAMS['id_ver']})                 AS CUR_VER,
        toFloat64({PARAMS['CAP_Q']})                AS CAP_Q, -- например 0.90
        toFloat64({PARAMS['H']})                    AS H,     -- например 5.0
        (1 - pow(2, -1/H))                          AS ALPHA -- α для EWMA

    , key_list_prime AS (
        SELECT DISTINCT mp, seller, sku, article_1c, code_1c, cluster_to
        FROM sb.buffer_calc_keys
        WHERE date = toDate('{START_DATE}')
        AND id_ver = CUR_VER
        AND calc_type = 'first_init'
    )

    , supplies AS (
        SELECT s.mp AS mp, s.seller AS seller, s.article_1c AS article_1c, s.code_1c AS code_1c,
            cluster_to AS cluster_to, SUM(s.quantity_supplies) AS quantity_supplies
        FROM sb.buffer_supplies s
        WHERE s.shipping_date < toDate('{START_DATE}') AND s.closing_date > toDate('{START_DATE}')
        AND id_ver = toString(CUR_VER)
        GROUP BY s.mp, s.seller, s.article_1c, s.code_1c, cluster_to
    )

    , supplies_within_rt AS (
        SELECT
            mp, seller, sku, article_1c, code_1c, cluster_to,
            SUM(s.quantity_supplies) AS quantity_supplies_within_rt
        FROM sb.buffer_supplies s
        WHERE id_ver = toString(CUR_VER)
        /* включительно по RT, строго после текущей даты */
        AND closing_date > toDate('{START_DATE}')
        AND closing_date <= addDays(toDate('{START_DATE}'), RT_DEFAULT)
        GROUP BY mp, seller, sku, article_1c, code_1c, cluster_to
    )

    , stocks_mp AS (
        SELECT s.date, s.mp AS mp, s.seller AS seller, s.sku AS sku, s.article_1c AS article_1c, s.code_1c AS code_1c,
            rwh.cluster_to AS cluster_to,
            SUM(s.quantityFree) AS quantity_stocks,
            SUM(s.quantityFull) AS quantity_stocks_full
        FROM goods.stocks s
        LEFT JOIN (SELECT DISTINCT mp, warehouse_name, cluster AS cluster_to FROM ref.warehouses) rwh
        ON rwh.warehouse_name = s.warehouse_name AND rwh.mp = s.mp
        WHERE s.date >= toDate('{START_DATE}') - INTERVAL PERIOD DAY
        AND s.date <= toDate('{START_DATE}')
        AND s.mp IN ('WB','Ozon','YM')
        AND s.quantityFree > 0
        AND s.code_1c != ''
        GROUP BY s.date, s.mp, s.seller, s.sku, s.article_1c, s.code_1c, rwh.cluster_to
    )

    , stocks_1c AS (
        SELECT st.date, st.article_1c, st.code_1c,
            SUM(st.quantityFree) AS quantity_stocks_main,
            SUM(st.quantityFull) AS quantity_stocks_main_full
        FROM goods.stocks st
        WHERE st.date >= toDate('{START_DATE}') - INTERVAL PERIOD DAY
        AND st.date <= toDate('{START_DATE}')
        AND st.warehouse_name IN ('Основной','Старая Купавна ответ хранение (СТ Лоджистик)','Старая Купавна: СВХ','Старая Купавна')
        AND st.mp = '1C'
        AND st.quantityFree > 0
        AND st.code_1c != ''
        GROUP BY st.date, st.article_1c, st.code_1c
    )

    , stocks_on_cluster_to AS (
        SELECT smp.date AS date,
            smp.mp AS mp, smp.seller AS seller, smp.sku AS sku,
            smp.article_1c AS article_1c, smp.code_1c AS code_1c,
            smp.cluster_to AS cluster_to,
            SUM(smp.quantity_stocks) AS quantity_stocks
        FROM stocks_mp smp
        INNER JOIN key_list_prime
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        WHERE smp.date >= toDate('{START_DATE}') - INTERVAL PERIOD DAY
        AND smp.date < toDate('{START_DATE}')
        GROUP BY date, mp, seller, sku, article_1c, code_1c, cluster_to
    )

    , keys AS (
        SELECT DISTINCT mp, seller, sku, article_1c, code_1c, cluster_to
        FROM stocks_on_cluster_to
    )

    , calendar AS (
        SELECT
            am.date,
            k.mp, k.seller, k.sku, k.article_1c, k.code_1c, k.cluster_to,
            /* 1 = вчера, PERIOD = самый давний день в окне */
            toInt32(dateDiff('day', am.date, toDate('{START_DATE}'))) AS dfe,
        /* квинтили окна: последние ~1/5 дней получают вес 5, затем 4, 3, 2, дальние — 1 */
        multiIf(
            dfe <= intDiv(PERIOD, 5),                 5,
            dfe <= intDiv(PERIOD, 5) * 2,             4,
            dfe <= intDiv(PERIOD, 5) * 3,             3,
            dfe <= intDiv(PERIOD, 5) * 4,             2,
                                                    1
        ) AS w
        FROM ref.calendar am
        CROSS JOIN keys k
        WHERE am.date >= toDate('{START_DATE}') - INTERVAL PERIOD DAY
        AND am.date < toDate('{START_DATE}')
    )

    , max_stocks_per_cluster AS (
        SELECT mp, seller, sku, article_1c, code_1c, cluster_to,
            MAX(quantity_stocks) AS max_qs
        FROM stocks_on_cluster_to
        GROUP BY mp, seller, sku, article_1c, code_1c, cluster_to
    )

    , days_oos AS (
        SELECT cal.date AS date, cal.mp AS mp, cal.seller AS seller, cal.sku AS sku,
            cal.article_1c AS article_1c, cal.code_1c AS code_1c, cal.cluster_to AS cluster_to,
            'Низкий запас' AS out_of_stocks,
            am.quantity_stocks AS quantity_stocks,
            ms.max_qs AS max_qs
        FROM calendar cal
        LEFT JOIN stocks_on_cluster_to am
            USING (date, mp, seller, sku, article_1c, code_1c, cluster_to)
        LEFT JOIN max_stocks_per_cluster ms
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        WHERE ifNull(am.quantity_stocks, 0) <= 0
        OR (ifNull(am.quantity_stocks, 0) / nullIf(ms.max_qs, 0)) <= OOS_RATIO
    )

    , orders_per_cluster_prime AS (
        SELECT am.date AS date, am.mp AS mp, am.seller AS seller, am.sku AS sku,
            am.article_1c AS article_1c, am.code_1c AS code_1c, am.cluster_to AS cluster_to,
            SUM(am.quantity_orders) AS quantity_orders,
            SUM(am.orders_sum) AS orders_sum,
            SUM(am.cost_value_orders) AS cost_value_orders
        FROM kpi.all_mp_sales_wsb am
        INNER JOIN key_list_prime USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        WHERE am.date >= toDate('{START_DATE}') - INTERVAL PERIOD DAY
        AND am.date <= toDate('{START_DATE}')
        AND am.quantity_orders != 0
        GROUP BY am.date, am.mp, am.seller, am.sku, am.article_1c, am.code_1c, am.cluster_to
    )

    , median_price_by_key AS (
        SELECT mp, seller, sku, article_1c, code_1c, cluster_to,
            median(orders_sum/quantity_orders) AS median_price
        FROM orders_per_cluster_prime
        GROUP BY mp, seller, sku, article_1c, code_1c, cluster_to
    )

    , main AS (
        SELECT
                cal.date AS date, cal.mp AS mp, cal.seller AS seller, cal.sku AS sku,
                cal.article_1c AS article_1c, cal.code_1c AS code_1c, cal.cluster_to AS cluster_to,
                cal.w AS w,
                am.out_of_stocks AS out_of_stocks,
                ms.quantity_orders AS quantity_orders,
                ms.orders_sum AS orders_sum,
                ms.cost_value_orders AS cost_value_orders,
                (ms.orders_sum/ms.quantity_orders)        AS avg_price_per_day,
                (ms.cost_value_orders/ms.quantity_orders) AS avg_cost,
                mpk.median_price,
                (avg_price_per_day/mpk.median_price - 1) AS rate,
                IF(rate<=BAD_PRICE_LOW, 'Низкая цена', IF(rate>=BAD_PRICE_HIGH, 'Высокая цена', '')) AS bad_price
        FROM calendar cal
        LEFT JOIN orders_per_cluster_prime AS ms
            USING (date, mp, seller, sku, article_1c, code_1c, cluster_to)
        LEFT JOIN days_oos AS am
            USING (date, mp, seller, sku, article_1c, code_1c, cluster_to)
        LEFT JOIN median_price_by_key mpk
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    )

    , days_oos_number AS (
        SELECT mp, seller, sku, article_1c, code_1c, cluster_to, count(*) AS days_oos_number
        FROM main
        WHERE out_of_stocks = 'Низкий запас'
        GROUP BY mp, seller, sku, article_1c, code_1c, cluster_to
    )

    , bad_price_number AS (
        SELECT mp, seller, sku, article_1c, code_1c, cluster_to,
            SUM(quantity_orders) AS bp_quantity_orders, count(*) AS days_bp_number
        FROM main
        WHERE bad_price = 'Низкая цена'
        GROUP BY mp, seller, sku, article_1c, code_1c, cluster_to
    )

    , exclude_number AS (
        SELECT mp, seller, sku, article_1c, code_1c, cluster_to, count(*) AS exclude_number
        FROM main
        WHERE out_of_stocks = 'Низкий запас' OR bad_price = 'Низкая цена'
        GROUP BY mp, seller, sku, article_1c, code_1c, cluster_to
    )

    , import AS (
        SELECT code_1c, article_1c, SUM(quantity) AS quantity_import_orders
        FROM goods.import_supplies
        GROUP BY code_1c, article_1c
    )

    , insurance AS (
        SELECT mp, seller, sku, article_1c, code_1c, cluster_to, stddevPop(f.quantity_orders) AS sigma
        FROM main f
        GROUP BY mp, seller, sku, article_1c, code_1c, cluster_to
    )

    , final_full AS (
        SELECT
            am.mp AS mp, am.seller AS seller, am.sku AS sku, am.article_1c AS article_1c, am.code_1c AS code_1c, am.cluster_to AS cluster_to,
            don.days_oos_number, bp.days_bp_number,
            smp.quantity_stocks,
            smp.quantity_stocks_full,
            src.quantity_stocks_main,
            src.quantity_stocks_main_full,
            sup.quantity_supplies,            
            swr.quantity_supplies_within_rt,    
            bp.bp_quantity_orders,
            MAX(am.quantity_orders) AS max_quantity_order,
            MIN(am.quantity_orders) AS min_quantity_order,
            SUM(am.quantity_orders) AS total_quantity_orders_full,
            SUM(am.quantity_orders)/PERIOD AS avg_quantity_orders_full
        FROM main am
        LEFT JOIN days_oos_number don USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        LEFT JOIN bad_price_number bp USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        LEFT JOIN supplies sup     USING (mp, seller, article_1c, code_1c, cluster_to)
        LEFT JOIN supplies_within_rt swr
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        LEFT JOIN stocks_mp smp
            ON am.mp=smp.mp AND am.seller=smp.seller AND am.sku=smp.sku
            AND am.article_1c=smp.article_1c AND am.code_1c=smp.code_1c
            AND am.cluster_to=smp.cluster_to AND smp.date=toDate('{START_DATE}')
        LEFT JOIN stocks_1c src
            ON am.article_1c=src.article_1c AND am.code_1c=src.code_1c AND src.date=toDate('{START_DATE}')
        GROUP BY am.mp, am.seller, am.sku, am.article_1c, am.code_1c, am.cluster_to,
                don.days_oos_number, bp.days_bp_number,
                smp.quantity_stocks, smp.quantity_stocks_full,
                src.quantity_stocks_main, src.quantity_stocks_main_full,
                sup.quantity_supplies, swr.quantity_supplies_within_rt, bp.bp_quantity_orders
    )

    , final_exclude AS (
        /* «Правильное среднее»: P90-кап + EWMA по «хорошим» дням окна [D-PERIOD, D) */
        SELECT
            gd.mp, gd.seller, gd.sku, gd.article_1c, gd.code_1c, gd.cluster_to,

            /* заказы на START_DATE — как раньше, берём отдельным join */
            ms.quantity_orders AS quantity_orders,

            /* (для справки) невзвешенная сумма по окну */
            SUM(gd.quantity_orders) AS total_quantity_orders,

            /* массив заказов за окно (по дате), с нулями там, где их не было */
            arrayMap(x -> x.2,
                     arraySort(x -> x.1,
                               groupArray((gd.date, gd.quantity_orders)))
            ) AS arr,

            /* P90 окна и капированный массив */
            quantileExact(CAP_Q)(gd.quantity_orders)        AS p90_win,
            arrayMap(v -> least(v, p90_win), arr)         AS arr_cap,

            /* EWMA по капированному массиву (весим «свежее» сильнее) */
            length(arr_cap)                                 AS n,
            arrayReverse(arr_cap)                         AS rev,
            arrayMap(i -> pow(1 - ALPHA, i - 1), arrayEnumerate(rev)) AS geom,
            (1 - pow(1 - ALPHA, n))                         AS wnorm,
            /* итог: λ_t на конец окна = «среднее спроса» */
            arraySum(arrayMap((v, w) -> v * w, rev, geom)) * (ALPHA / nullIf(wnorm, 0)) AS avg_quantity_orders

        FROM (
            /* «хорошие» дни окна: без OOS и без плохой цены, как у тебя было */
            SELECT
                am.date,
                am.mp, am.seller, am.sku, am.article_1c, am.code_1c, am.cluster_to,
                toFloat64(ifNull(am.quantity_orders, 0)) AS quantity_orders
            FROM main am
            WHERE am.date >= toDate('{START_DATE}') - INTERVAL PERIOD DAY
            AND am.date < toDate('{START_DATE}')
            AND (am.out_of_stocks != 'Низкий запас' AND am.bad_price != 'Низкая цена')
        ) AS gd
        /* «снимок» заказов на START_DATE для этого же ключа */
        LEFT JOIN orders_per_cluster_prime ms
        ON gd.mp = ms.mp AND gd.seller = ms.seller AND gd.sku = ms.sku
        AND gd.article_1c = ms.article_1c AND gd.code_1c = ms.code_1c
        AND gd.cluster_to = ms.cluster_to AND ms.date = toDate('{START_DATE}')
        GROUP BY
            gd.mp, gd.seller, gd.sku, gd.article_1c, gd.code_1c, gd.cluster_to,
            ms.quantity_orders
    )


    SELECT
        CUR_VER                         AS id_ver,
        toDate('{START_DATE}')            AS date,
        r.name                            AS name,
        article_1c                        AS article_1c,
        code_1c                         AS code_1c,
        mp                                AS mp,
        seller                            AS seller,
        sku                             AS sku,
        cluster_to                        AS cluster_to,

        oos.total_quantity_orders         AS total_quantity_orders,
        max_quantity_order                AS max_quantity_order,
        ROUND(oos.avg_quantity_orders, 2) AS avg_quantity_orders,
        days_oos_number                 AS days_oos_number,
        days_bp_number                    AS days_bp_number,

        RT_DEFAULT                        AS avg_day_to_cluster,
        INS_CONST                         AS insurance_reserv_cluster,
        BUF_NORM_CONST                    AS buffer_cluster_norm,

        ROUND(BUF_NORM_CONST * avg_quantity_orders) AS buffer_cluster,
        /* сколько надо довезти с учётом поставок, приходящих ≤ RT */
        greatest(
            0,
            ROUND((BUF_NORM_CONST * avg_quantity_orders)
                - quantity_stocks
                - ifNull(quantity_supplies, 0))
        ) AS deliveries_to_cluster,


        -- DBM «текущий» = только OnHand / BT
        ROUND(LEAST(
            IFNULL( quantity_stocks
                    / NULLIF(BUF_NORM_CONST*avg_quantity_orders, 0), 1),
            1), 2) AS buffer_cluster_marker_current,

        -- «pipeline»-маркер оставляем как операционный (OnHand + все in-transit)
        ROUND(LEAST(
            IFNULL( (quantity_stocks + quantity_supplies)
                    / NULLIF(BUF_NORM_CONST*avg_quantity_orders, 0), 1),
            1), 2) AS buffer_cluster_marker,
        multiIf(ifNull(quantity_stocks, 0) <= 0, 0, ifNull(oos.quantity_orders, 0)) AS quantity_orders,
        quantity_stocks                 AS quantity_stocks,
        quantity_supplies                 AS quantity_supplies,
        ifNull(quantity_supplies_within_rt, 0) AS quantity_supplies_within_rt,
        0                                 AS new_buffer

    FROM final_full
    LEFT JOIN final_exclude oos USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    LEFT JOIN (
        SELECT DISTINCT name, article_1c, code_1c
        FROM ref.article_mp
        WHERE discounted != 1 AND brand != 'РЕСЕЙЛ'
    ) r USING (article_1c, code_1c)
    WHERE days_oos_number <= (PERIOD - PERIOD/6)
    AND days_bp_number <= (PERIOD - PERIOD/6)
    AND total_quantity_orders != 0

    '''
    df_init = ch_object.extract_data(query_init)
    df_init, other_column_list = preprocess_data(df=df_init, column_types=TYPES, add_missed_columns=False, handle_invalid_values=False)
    ch_object.insert_data(table_name='sb.buffer_main', df=df_init)

    # Симуляция
    print('query_sim')
    query_sim  = f'''
    /* ---- simulate_day ---- */
    WITH
        toInt32({PARAMS['AVG_DAYS_CONST']})       AS RT_DEFAULT,
        toInt32({PARAMS['id_ver']})               AS CUR_VER

    /* ключи для simulate_day */
    , key_list_second AS (
        SELECT DISTINCT mp, seller, sku, article_1c, code_1c, cluster_to
        FROM sb.buffer_calc_keys
        WHERE date   = toDate('{START_DATE}')
        AND id_ver = CUR_VER
        AND calc_type = 'simulate_day'
    )

    /* состояние на вчера: переносим метрики и увеличиваем счётчик new_buffer */
    , current_buffer AS (
        SELECT
            cb.article_1c                AS article_1c,
            cb.code_1c                   AS code_1c,
            cb.mp                        AS mp,
            cb.seller                    AS seller,
            cb.sku                       AS sku,
            cb.cluster_to                AS cluster_to,
            cb.total_quantity_orders     AS total_quantity_orders,
            cb.max_quantity_order        AS max_quantity_order,
            cb.avg_quantity_orders       AS avg_quantity_orders,
            cb.days_oos_number           AS days_oos_number,
            cb.days_bp_number            AS days_bp_number,
            cb.avg_day_to_cluster        AS avg_day_to_cluster,
            cb.insurance_reserv_cluster  AS insurance_reserv_cluster,
            cb.buffer_cluster_norm       AS buffer_cluster_norm,
            cb.buffer_cluster            AS buffer_cluster,
            cb.quantity_stocks           AS quantity_stocks_prev,
            cb.quantity_orders           AS quantity_orders_prev,
            cb.new_buffer + 1            AS new_buffer
        FROM sb.buffer_main cb
        INNER JOIN key_list_second
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        WHERE cb.date   = toDate('{START_DATE}') - INTERVAL 1 DAY
        AND cb.id_ver = CUR_VER
    )

    /* продажи на день D */
    , orders_per_cluster_second AS (
        SELECT
            am.date         AS date,
            am.mp           AS mp,
            am.seller       AS seller,
            am.sku          AS sku,
            am.article_1c   AS article_1c,
            am.code_1c      AS code_1c,
            am.cluster_to   AS cluster_to,
            SUM(am.quantity_orders) AS quantity_orders
        FROM kpi.all_mp_sales_wsb am
        INNER JOIN key_list_second
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        WHERE am.date = toDate('{START_DATE}')
        GROUP BY am.date, am.mp, am.seller, am.sku, am.article_1c, am.code_1c, am.cluster_to
    )

    /* поставки, прибывающие в день D (для эволюции запасов) */
    , supplies_arrive_today AS (
        SELECT s.mp AS mp, s.seller AS seller, s.sku AS sku, s.article_1c AS article_1c, s.code_1c AS code_1c,
            s.cluster_to AS cluster_to,
            SUM(s.quantity_supplies) AS quantity_supplies_arrive
        FROM sb.buffer_supplies s
        INNER JOIN key_list_second
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        WHERE s.id_ver = toString(CUR_VER)
        AND s.closing_date = toDate('{START_DATE}')
        AND s.shipping_date <= toDate('{START_DATE}')
        GROUP BY s.mp, s.seller, s.sku, s.article_1c, s.code_1c, s.cluster_to
    )

    /* поставки в пути на день D (для маркера/потребности) */
    , supplies_in_transit AS (
        SELECT s.mp AS mp, s.seller AS seller, s.sku AS sku, s.article_1c AS article_1c, s.code_1c AS code_1c,
            s.cluster_to AS cluster_to,
            SUM(s.quantity_supplies) AS quantity_supplies
        FROM sb.buffer_supplies s
        INNER JOIN key_list_second
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        WHERE s.id_ver = toString(CUR_VER)
        AND s.shipping_date <  toDate('{START_DATE}')
        AND s.closing_date  >  toDate('{START_DATE}')
        GROUP BY s.mp, s.seller, s.sku, s.article_1c, s.code_1c, s.cluster_to
    )

    /* поставки в пути на день D в пределах одного RT (периода доставки) */
    , supplies_within_rt AS (
    SELECT
        s.mp, s.seller, s.sku, s.article_1c, s.code_1c, s.cluster_to,
        SUM(s.quantity_supplies) AS quantity_supplies_within_rt
    FROM sb.buffer_supplies s
    INNER JOIN key_list_second
        USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    WHERE s.id_ver = toString(CUR_VER)
      AND s.closing_date >  toDate('{START_DATE}')
      AND s.closing_date <= addDays(toDate('{START_DATE}'), RT_DEFAULT)  -- включительно по RT
    GROUP BY s.mp, s.seller, s.sku, s.article_1c, s.code_1c, s.cluster_to
    )

    /* моделируем запас на день D: stock_today = max(0, stock_prev - sales_prev + supplies_arrive_today) */
    , stocks_sim_today AS (
        SELECT
            cb.mp as mp, cb.seller as seller, cb.sku as sku, cb.article_1c as article_1c, cb.code_1c as code_1c, cb.cluster_to as cluster_to,
            GREATEST(
                0,
                ROUND( cb.quantity_stocks_prev - cb.quantity_orders_prev + IFNULL(sa.quantity_supplies_arrive, 0) )
            ) AS quantity_stocks
        FROM current_buffer cb
        LEFT JOIN supplies_arrive_today sa
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    )

    /* ФИНАЛ */
    SELECT
        CUR_VER                         AS id_ver,
        toDate('{START_DATE}')          AS date,
        r.name                          AS name,
        article_1c                      AS article_1c,
        code_1c                         AS code_1c,
        mp                              AS mp,
        seller                          AS seller,
        sku                             AS sku,
        cluster_to                      AS cluster_to,

        total_quantity_orders           AS total_quantity_orders,
        max_quantity_order              AS max_quantity_order,
        ROUND(avg_quantity_orders, 2)   AS avg_quantity_orders,
        days_oos_number                 AS days_oos_number,
        days_bp_number                  AS days_bp_number,

        avg_day_to_cluster              AS avg_day_to_cluster,
        insurance_reserv_cluster        AS insurance_reserv_cluster,
        buffer_cluster_norm             AS buffer_cluster_norm,

        buffer_cluster                  AS buffer_cluster,

        GREATEST(
            0,
            ROUND(
                buffer_cluster
                - st.quantity_stocks
                - ifNull(sit.quantity_supplies, 0)   -- учитываем ВСЕ уже отправленные поставки в пути
            )
        ) AS deliveries_to_cluster,


        /* маркер ТЕКУЩИЙ по DBM: только OnHand/BT */
        ROUND(
        LEAST(IFNULL(st.quantity_stocks / NULLIF(buffer_cluster, 0), 1), 1)
        , 2) AS buffer_cluster_marker_current,

        ROUND(
        LEAST(
            IFNULL((st.quantity_stocks + IFNULL(sit.quantity_supplies, 0)) / NULLIF(buffer_cluster, 0), 1)
        , 1)
        , 2) AS buffer_cluster_marker,

        multiIf(ifNull(st.quantity_stocks, 0) <= 0, 0, ifNull(ops.quantity_orders, 0)) AS quantity_orders,
        st.quantity_stocks              AS quantity_stocks,
        IFNULL(sit.quantity_supplies, 0) AS quantity_supplies,
        ifNull(swr.quantity_supplies_within_rt, 0) AS quantity_supplies_within_rt,
        new_buffer                      AS new_buffer

    FROM current_buffer
    LEFT JOIN stocks_sim_today        st   USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    LEFT JOIN orders_per_cluster_second ops USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    LEFT JOIN supplies_in_transit     sit  USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    LEFT JOIN supplies_within_rt swr USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    LEFT JOIN (
        SELECT DISTINCT name, article_1c, code_1c
        FROM ref.article_mp
        WHERE discounted != 1 AND brand != 'РЕСЕЙЛ'
    ) r USING (article_1c, code_1c)

    '''
    df_sim = ch_object.extract_data(query_sim)
    df_sim, other_column_list = preprocess_data(df=df_sim, column_types=TYPES, add_missed_columns=True, handle_invalid_values=False)
    ch_object.insert_data(table_name='sb.buffer_main', df=df_sim)

    print('query_recalc')
    query_recalc  = f'''
    /* ---- recalc_day  ---- */
    WITH
        toInt32({PARAMS['id_ver']})        AS CUR_VER,
        toFloat64({PARAMS['UP_STEP']})     AS UP_STEP,
        toFloat64({PARAMS['DOWN_STEP']})   AS DOWN_STEP,
        toFloat64({PARAMS['REQUIRE_COVERAGE']}) AS REQUIRE_COVERAGE,
        toInt32({PARAMS['AVG_DAYS_CONST']}) AS RT_DEFAULT


    , key_list_recalc AS (
        SELECT DISTINCT
            mp, seller, sku, article_1c, code_1c, cluster_to,
            action_suggested, min_buffer,
            multiIf(
            action_suggested = 'UP',   1.0 + UP_STEP,
            action_suggested = 'DOWN', 1.0 - DOWN_STEP,
                                        1.0
            ) AS factor_final
        FROM sb.buffer_calc_keys
        WHERE date   = toDate('{START_DATE}')
        AND id_ver = CUR_VER
        AND calc_type = 'recalc_day'
    )


    , prev_state AS (
        SELECT
            cb.article_1c               AS article_1c,
            cb.code_1c                  AS code_1c,
            cb.mp                       AS mp,
            cb.seller                   AS seller,
            cb.sku                      AS sku,
            cb.cluster_to               AS cluster_to,

            cb.quantity_stocks          AS quantity_stocks_prev,
            cb.quantity_orders          AS quantity_orders_prev,

            cb.total_quantity_orders    AS total_quantity_orders_prev,
            cb.max_quantity_order       AS max_quantity_order_prev,
            cb.avg_quantity_orders      AS avg_quantity_orders_prev,
            cb.days_oos_number          AS days_oos_number_prev,
            cb.days_bp_number           AS days_bp_number_prev,
            cb.avg_day_to_cluster       AS avg_day_to_cluster_prev,
            cb.insurance_reserv_cluster AS insurance_reserv_cluster_prev,
            cb.buffer_cluster_norm      AS buffer_cluster_norm_prev,

            cb.buffer_cluster           AS buffer_cluster_prev
        FROM sb.buffer_main cb
        INNER JOIN key_list_recalc
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        WHERE cb.date   = toDate('{START_DATE}') - INTERVAL 1 DAY
        AND cb.id_ver = CUR_VER
    )

    /* поставки, прибывающие в день D (для эволюции запасов) */
    , supplies_arrive_today AS (
        SELECT
            s.mp, s.seller, s.sku, s.article_1c, s.code_1c, s.cluster_to,
            SUM(s.quantity_supplies) AS quantity_supplies_arrive
        FROM sb.buffer_supplies s
        INNER JOIN key_list_recalc
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        WHERE s.id_ver = toString(CUR_VER)
        AND s.closing_date = toDate('{START_DATE}')
        AND s.shipping_date <= toDate('{START_DATE}')
        GROUP BY s.mp, s.seller, s.sku, s.article_1c, s.code_1c, s.cluster_to
    )

    /* поставки в пути на день D (для маркера/потребности) */
    , supplies_in_transit AS (
        SELECT
            s.mp, s.seller, s.sku, s.article_1c, s.code_1c, s.cluster_to,
            SUM(s.quantity_supplies) AS quantity_supplies
        FROM sb.buffer_supplies s
        INNER JOIN key_list_recalc
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        WHERE s.id_ver = toString(CUR_VER)
        AND s.shipping_date <  toDate('{START_DATE}')
        AND s.closing_date  >  toDate('{START_DATE}')
        GROUP BY s.mp, s.seller, s.sku, s.article_1c, s.code_1c, s.cluster_to
    )

    , supplies_within_rt AS (
        SELECT
            s.mp, s.seller, s.sku, s.article_1c, s.code_1c, s.cluster_to,
            SUM(s.quantity_supplies) AS quantity_supplies_within_rt
        FROM sb.buffer_supplies s
        INNER JOIN key_list_recalc
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        WHERE s.id_ver = toString(CUR_VER)
        AND s.closing_date >  toDate('{START_DATE}')
        AND s.closing_date <= addDays(toDate('{START_DATE}'), RT_DEFAULT)
        GROUP BY s.mp, s.seller, s.sku, s.article_1c, s.code_1c, s.cluster_to
    )

    , stock_today AS (
        SELECT
            ps.mp AS mp, ps.seller AS seller, ps.sku AS sku, ps.article_1c AS article_1c, ps.code_1c AS code_1c, ps.cluster_to AS cluster_to,
            GREATEST(0, ROUND(ps.quantity_stocks_prev - ps.quantity_orders_prev + IFNULL(sa.quantity_supplies_arrive, 0))) AS quantity_stocks
        FROM prev_state ps
        LEFT JOIN supplies_arrive_today sa
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    )

    , orders_today_raw AS (
        SELECT
            am.mp AS mp, am.seller AS seller, am.sku AS sku, am.article_1c AS article_1c, am.code_1c AS code_1c, am.cluster_to AS cluster_to,
            SUM(am.quantity_orders) AS quantity_orders
        FROM kpi.all_mp_sales_wsb am
        INNER JOIN key_list_recalc
            USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        WHERE am.date = toDate('{START_DATE}')
        GROUP BY am.mp, am.seller, am.sku, am.article_1c, am.code_1c, am.cluster_to
    )

    , orders_today_sim AS (
        SELECT
            ps.mp AS mp, ps.seller AS seller, ps.sku AS sku, ps.article_1c AS article_1c, ps.code_1c AS code_1c, ps.cluster_to AS cluster_to,
            IF(st.quantity_stocks > 0, IFNULL(ot.quantity_orders, 0), 0) AS quantity_orders
        FROM prev_state ps
        LEFT JOIN stock_today      st USING (mp, seller, sku, article_1c, code_1c, cluster_to)
        LEFT JOIN orders_today_raw ot USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    )

    , buffer_new AS (
        SELECT
            ps.mp as mp, ps.seller as seller, ps.sku as sku, ps.article_1c as article_1c, ps.code_1c as code_1c, ps.cluster_to as cluster_to,
            ROUND(
                greatest(
                    toFloat64(ps.buffer_cluster_prev) * ifNull(ffa.factor_final, 1.0),
                    ffa.min_buffer
                )
            ) AS buffer_cluster_new
        FROM prev_state ps
        LEFT JOIN key_list_recalc ffa USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    )

    /* ФИНАЛ */
    SELECT
        CUR_VER                         AS id_ver,
        toDate('{START_DATE}')          AS date,
        r.name                          AS name,

        ps.article_1c                   AS article_1c,
        ps.code_1c                      AS code_1c,
        ps.mp                           AS mp,
        ps.seller                       AS seller,
        ps.sku                          AS sku,
        ps.cluster_to                   AS cluster_to,

        ps.total_quantity_orders_prev   AS total_quantity_orders,
        ps.max_quantity_order_prev      AS max_quantity_order,
        ROUND(ps.avg_quantity_orders_prev, 2) AS avg_quantity_orders,
        ps.days_oos_number_prev         AS days_oos_number,
        ps.days_bp_number_prev          AS days_bp_number,

        ps.avg_day_to_cluster_prev      AS avg_day_to_cluster,
        ps.insurance_reserv_cluster_prev AS insurance_reserv_cluster,
        ps.buffer_cluster_norm_prev     AS buffer_cluster_norm,

        bn.buffer_cluster_new           AS buffer_cluster,

        greatest(
            0,
            ROUND( bn.buffer_cluster_new
                - st.quantity_stocks
                - ifNull(sit.quantity_supplies, 0)   -- учитываем ВСЕ уже отправленные поставки в пути
            )
        ) AS deliveries_to_cluster,

        ROUND(
            LEAST(
                IFNULL(st.quantity_stocks / NULLIF(bn.buffer_cluster_new, 0), 1),
                1
            ), 2
        ) AS buffer_cluster_marker_current,

        ROUND(
        LEAST(
            IFNULL(
            (st.quantity_stocks + IFNULL(sit.quantity_supplies, 0))
            / NULLIF(bn.buffer_cluster_new, 0),
            1
            ), 1
        ), 2
        ) AS buffer_cluster_marker,

        multiIf(ifNull(st.quantity_stocks, 0) <= 0, 0, ifNull(ots.quantity_orders, 0)) AS quantity_orders,
        st.quantity_stocks              AS quantity_stocks,
        IFNULL(sit.quantity_supplies, 0) AS quantity_supplies,
        0                               AS new_buffer

    FROM prev_state ps
    LEFT  JOIN buffer_new                bn  USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    LEFT  JOIN stock_today               st  USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    LEFT JOIN supplies_in_transit  sit USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    LEFT  JOIN supplies_within_rt   swr USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    LEFT  JOIN orders_today_sim          ots USING (mp, seller, sku, article_1c, code_1c, cluster_to)
    LEFT  JOIN (
        SELECT DISTINCT name, article_1c, code_1c
        FROM ref.article_mp
        WHERE discounted != 1 AND brand != 'РЕСЕЙЛ'
    ) r USING (article_1c, code_1c)
    '''
    df_recalc = ch_object.extract_data(query_recalc)
    df_recalc, other_column_list = preprocess_data(df=df_recalc, column_types=TYPES, add_missed_columns=True, handle_invalid_values=False)
    ch_object.insert_data(table_name='sb.buffer_main', df=df_recalc)


    print('extract_by_theory - первый запрос для поставок')
    extract_by_theory = f"""
        SELECT *
        FROM sb.buffer_main
        WHERE id_ver = '{PARAMS['id_ver']}' AND date = '{START_DATE}'
    """

    # Пока в sb.buffer_main присутствуют дубли, возьмем уникальные нужные нам строки
    # extract_by_theory = f"""
    #     SELECT *
    #     FROM (SELECT *, 
    #         ROW_NUMBER()	OVER (PARTITION BY mp, seller, cluster_to, sku ORDER BY date DESC) AS rank
    #     FROM sb.buffer_main)
    #     WHERE rank = 1
    # """
    buffer_main = ch_object.extract_data(extract_by_theory)
    len(buffer_main)



    min_cluster_m3 = PARAMS['min_cluster_m3'] 
    min_good = PARAMS['min_good'] 
    min_cluster_limit = PARAMS['min_cluster_limit']
    # отсечка по количеству на один вид товара
    buffer_main = buffer_main[buffer_main['deliveries_to_cluster'] >= min_good]

    merged_df = buffer_main.merge(right=volumes, how='left', on=['article_1c', 'code_1c', 'mp', 'seller'])
    merged_df['volume_requirement'] = merged_df['deliveries_to_cluster'] * merged_df['volume_m3']

    empty_vol = merged_df[merged_df['volume_m3'].isna()]
    # print(f'Количество строк с пустым объемом: {len(empty_vol)}')
    if len(empty_vol):
        empty_vol.head()

    # будем работать только с непустым объемом
    df = merged_df[merged_df['volume_m3'].notna()]
    logging.info(f'Количество строк, по которым будут сформированны поставки: {len(df)}')

    assignments, skipped = [], []

    # находим максимально необходимое количество машин по каждой поставке 
    for wh, wh_items in df.groupby(by=['mp', 'seller', 'cluster_to']): #.query('(cluster_to == "Приволжский федеральный округ") & (seller == "Ридберг")')
        # print(wh)
        # Рандомим дату отправки и дату прибытия
        # shipping_date = datetime.today() + timedelta(days=random.randint(5, 20))
        # closing_date = wh_items['date'] + timedelta(days=random.randint(7, 10))

        total_vol = wh_items['volume_requirement'].sum() # объем поставки
        # print(f"объем поставки: {total_vol}")
        if total_vol < min_cluster_limit:
            # print(f"поставка меньше 8 м куб - не едет")
            continue # поставка меньше 8 м куб - не едет

        truck_count = int(math.floor(total_vol / min_cluster_m3)) # кол-во машин по 8 м3
        # print(f"Количество целых заполненных машин: {truck_count}")
        # if truck_count == 0:
        #     continue  # не должно случиться, но пусть будет

        # Важно ли нам знать номер машины, в которой поедет тот или иной товар? если нет, тогда:
        full_vol = truck_count * min_cluster_m3 # объем полностью загруженных машин

        # Сделаем проверку, что последняя машина не должна быть заполнениться меньше, чем 8 кубов, 
        # иначе можно грузить, не заполняя последнюю машину полностью
        if total_vol - full_vol >= PARAMS['min_cluster_limit']: # остаток свободного места меньше, чем 2 куба
            # print(f"Последняя машина заполняется на {total_vol - full_vol} что >= {PARAMS['min_cluster_limit']} остаток свободного места меньше, чем 2 куба, можно грузить, не заполняя последнюю машину полностью")
            full_vol = total_vol 

        # объём, оставшийся в текущей машине (да, я создаю 100500 объектов, и что. Потом оптимизирую)
        free_left = full_vol
        # print(f"Объем итоговый поставки: {free_left}")

        # Идём по товарам в порядке убывания приоритета (нашего светофора)
        for _, row in wh_items.sort_values('buffer_cluster_marker', ascending=False).iterrows():
            # vol = row['volume_requirement']

            # Базовый словарь параметров
            base_params = {
                            'id_ver': row['id_ver'],                    # рандомим
                            'supply_id': "_".join(str(i) for i in wh),  # рандомим
                            'shipping_date': row['date'],
                            'closing_date': row['date'] + timedelta(days=random.randint(7, 7)),               # рандомим
                            'article_1c': row['article_1c'],
                            'code_1c': row['code_1c'],
                            'mp': row['mp'], 
                            'seller': row['seller'], 
                            'sku': row['sku'],
                            'cluster_to': row['cluster_to'],           
                            'quantity_supplies': row['deliveries_to_cluster'],
                            # 'volume_m3': row['volume_m3'],                            # закомментить
                            'loaded_volume': row['volume_requirement'],               # закомментить
                            # 'buffer_cluster_marker': row['buffer_cluster_marker']     # закомментить
                        }
            # Если влазит в оставшееся место
            if row['volume_requirement'] <= free_left:
                assignments.append(base_params)
                free_left -= row['volume_requirement']
            else:
                # обработка логики, когда отправляем не полный объем товара
                half_quantity = int(math.floor(free_left / row['volume_m3'])) # целое количество на оставшееся свободное место
                if half_quantity >= min_good:
                    assignments.append({
                        **base_params,
                        'quantity_supplies': half_quantity,
                        'loaded_volume': half_quantity * row['volume_m3'],             # закомментить
                    })
                    free_left -= half_quantity * row['volume_m3']
                    
                    skipped.append({
                        **base_params,
                        'quantity_supplies': row['deliveries_to_cluster'] - half_quantity, # сколько НЕ отправили
                        'loaded_volume': (row['deliveries_to_cluster'] - half_quantity) * row['volume_m3'],             # закомментить
                    })
                else:
                    skipped.append({
                        **base_params,
                        'quantity_supplies': row['deliveries_to_cluster'], # сколько НЕ отправили
                        })
                    

    assignments_df, skipped_df = pd.DataFrame(assignments), pd.DataFrame(skipped)

    ch_object.execute_query(query=f'''DELETE FROM sb.buffer_supplies WHERE id_ver = '{PARAMS['id_ver']}' AND shipping_date = '{START_DATE}'  ''')
    assignments_df, other_column_list = preprocess_data(df=assignments_df, column_types=TYPES_SUP, add_missed_columns=True, handle_invalid_values=False)
    ch_object.insert_data(table_name='sb.buffer_supplies', df=assignments_df)

    # ch_object.execute_query(query="TRUNCATE TABLE sb.buffer_main")
    # ch_object.execute_query(query="TRUNCATE TABLE sb.buffer_supplies")

    




