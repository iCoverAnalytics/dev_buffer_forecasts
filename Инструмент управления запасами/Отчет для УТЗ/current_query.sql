WITH
30 as PERIOD,
-- БЛОК ПОСТАВКИ
supplies AS (
SELECT s.mp as mp, s.seller as seller, s.article_1c AS article_1c, s.code_1c AS code_1c, cluster,
SUM(s.quantity) as quantity_supplies
FROM goods.supplies s
LEFT JOIN (SELECT DISTINCT supply_id, fc_warehouse FROM goods.supplies_list) sl ON s.supply_id = sl.supply_id
LEFT JOIN (SELECT DISTINCT mp, warehouse_name, cluster FROM ref.warehouses) rwh ON rwh.warehouse_name=sl.fc_warehouse AND rwh.mp=s.mp
WHERE s.date = today()
AND mp IN ('WB','Ozon', 'YM')
AND s.quantity > 0
AND s.code_1c !=''
GROUP BY s.mp, s.seller, s.article_1c, s.code_1c, cluster
),
-- БЛОК ТЕКУЩИЕ ЗАПАСЫ МП
stocks_mp AS (
SELECT s.date, s.mp as mp, seller, if(s.mp='YM','',sku) as sku, s.article_1c as article_1c, s.code_1c as code_1c, cluster,
SUM(quantityFree) as quantity_stocks,
SUM(quantityFull) as quantity_stocks_full
FROM goods.stocks s
LEFT JOIN (SELECT DISTINCT mp, warehouse_name, cluster FROM ref.warehouses) rwh ON rwh.warehouse_name=s.warehouse_name AND rwh.mp=s.mp
WHERE s.date >= today() - INTERVAL PERIOD DAY AND date <= today()
AND s.sku NOT IN (SELECT DISTINCT sku FROM ref.article_mp WHERE mp = 'Ozon' AND discounted = 1)
AND mp IN ('WB','Ozon', 'YM')
AND s.quantity > 0
AND code_1c !=''
GROUP BY s.date, s.mp, seller, sku, s.article_1c, s.code_1c, cluster
),
-- БЛОК ЗАПАСЫ НА ОСНОВНОМ СКЛАДЕ
stocks_1c AS (
SELECT date, article_1c, code_1c,
SUM(quantityFree) as quantity_stocks_main,
SUM(quantityFull) as quantity_stocks_main_full
FROM goods.stocks st
WHERE st.date >= today() - INTERVAL PERIOD DAY AND date <= today()
AND warehouse_name IN ('Основной',
                     'Старая Купавна ответ хранение (СТ Лоджистик)',
                     'Старая Купавна: СВХ',
                     'Старая Купавна')
AND mp IN ('1C')
AND quantity > 0
AND code_1c !=''
GROUP BY date, article_1c, code_1c
),
-- БЛОК ЗАКАЗЫ
stocks_on_cluster_from AS (
SELECT st.date AS date, mp, seller, sku, article_1c, code_1c, cluster_to, SUM(quantity_stocks) AS quantity_stocks
FROM (
SELECT DISTINCT wsb.date AS date, wsb.mp AS mp, wsb.seller AS seller, wsb.sku AS sku, wsb.article_1c AS article_1c, wsb.code_1c AS code_1c, cluster_from, cluster_to,
if(wsb.cluster_from='Основной', src.quantity_stocks_main, smp.quantity_stocks) AS quantity_stocks
FROM kpi.all_mp_sales_wsb wsb
LEFT JOIN stocks_mp smp ON wsb.date=smp.date AND wsb.mp=smp.mp AND wsb.seller=smp.seller AND wsb.sku=smp.sku AND wsb.article_1c=smp.article_1c AND wsb.code_1c=smp.code_1c AND wsb.cluster_from=smp.cluster
LEFT JOIN stocks_1c src ON wsb.date=src.date AND wsb.article_1c=src.article_1c AND wsb.code_1c=src.code_1c AND wsb.cluster_from='Основной'
WHERE date >= today() - INTERVAL PERIOD DAY AND date < today()
AND wsb.quantity_orders != 0
) st
GROUP BY date, mp, seller, sku, article_1c, code_1c, cluster_to
),
keys AS (
SELECT DISTINCT mp, seller, sku, article_1c, code_1c, cluster_to
FROM stocks_on_cluster_from
),
calendar AS (
SELECT date, mp, seller, sku, article_1c, code_1c, cluster_to
FROM ref.calendar am
CROSS JOIN keys k
WHERE date >= today() - INTERVAL PERIOD DAY AND date < today()
),
max_stocks_per_cluster AS (
SELECT mp, seller, sku, article_1c, code_1c, cluster_to,
         MAX(quantity_stocks) AS max_qs    
FROM stocks_on_cluster_from
GROUP BY mp, seller, sku, article_1c, code_1c, cluster_to
),
days_oos AS (
SELECT cal.date as date, cal.mp as mp, cal.seller as seller, cal.sku as sku, cal.article_1c as article_1c, cal.code_1c as code_1c, cal.cluster_to as cluster_to, 'Низкий запас' as out_of_stocks,
quantity_stocks,
max_qs
FROM calendar cal
LEFT JOIN stocks_on_cluster_from am ON cal.date = am.date AND cal.mp = am.mp AND cal.seller = am.seller AND cal.sku = am.sku AND cal.article_1c = am.article_1c AND cal.code_1c = am.code_1c AND cal.cluster_to = am.cluster_to
LEFT JOIN max_stocks_per_cluster ms ON cal.mp = ms.mp AND cal.seller = ms.seller AND cal.sku = ms.sku AND cal.article_1c = ms.article_1c AND cal.code_1c = ms.code_1c AND cal.cluster_to = ms.cluster_to
WHERE
    quantity_stocks <= 0
    OR quantity_stocks / IFNULL(ms.max_qs, 1) <= 0.02
),
orders_per_cluster AS (
SELECT am.date AS date, am.mp AS mp, am.seller AS seller, am.sku AS sku, am.article_1c AS article_1c, am.code_1c AS code_1c, am.cluster_to AS cluster_to,
SUM(am.quantity_orders) AS quantity_orders, SUM(am.orders_sum) AS orders_sum, SUM(am.cost_value_orders) AS cost_value_orders
FROM kpi.all_mp_sales_wsb am
WHERE am.date >= today() - INTERVAL PERIOD DAYS AND am.date < today()
AND am.quantity_orders != 0
GROUP BY am.date, am.mp, am.seller, am.sku, am.article_1c, am.code_1c, am.cluster_to
),
main AS (
SELECT  cal.date as date, cal.mp as mp, cal.seller as seller, cal.sku as sku, cal.article_1c as article_1c,
        cal.code_1c as code_1c, cal.cluster_to as cluster_to, out_of_stocks, quantity_orders, orders_sum, cost_value_orders,
        orders_sum/quantity_orders AS avg_price_per_day,
        cost_value_orders/quantity_orders AS avg_cost,
        median(orders_sum/quantity_orders) OVER (PARTITION BY mp, seller, sku, article_1c, code_1c, cluster_to) AS median_price,
        avg_price_per_day/median_price - 1 AS rate,
        IF(rate<=-0.4, 'Низкая цена', IF(rate>=0.4, 'Высокая цена', '')) AS bad_price
FROM calendar cal
LEFT JOIN days_oos am ON cal.date = am.date AND cal.mp = am.mp AND cal.seller = am.seller AND cal.sku = am.sku AND cal.article_1c = am.article_1c AND cal.code_1c = am.code_1c AND cal.cluster_to = am.cluster_to
LEFT JOIN orders_per_cluster ms ON cal.date = ms.date AND cal.mp = ms.mp AND cal.seller = ms.seller AND cal.sku = ms.sku AND cal.article_1c = ms.article_1c AND cal.code_1c = ms.code_1c AND cal.cluster_to = ms.cluster_to
),
days_oos_number AS (
SELECT DISTINCT os.mp, os.seller, os.sku, os.article_1c, os.code_1c, os.cluster_to, count(*) AS days_oos_number
FROM main os
WHERE os.out_of_stocks = 'Низкий запас'
GROUP BY os.mp, os.seller, os.sku, os.article_1c, os.code_1c, os.cluster_to
),
bad_price_number AS (
SELECT DISTINCT os.mp, os.seller, os.sku, os.article_1c, os.code_1c, os.cluster_to, SUM(quantity_orders) as bp_quantity_orders, count(*) AS days_bp_number
FROM main os
WHERE os.bad_price = 'Низкая цена'
GROUP BY os.mp, os.seller, os.sku, os.article_1c, os.code_1c, os.cluster_to
),
exclude_number AS (
SELECT DISTINCT os.mp, os.seller, os.sku, os.article_1c, os.code_1c, os.cluster_to, count(*) AS exclude_number
FROM main os
-- WHERE os.out_of_stocks = 'Низкий запас' OR os.bad_price = 'Низкая цена'
WHERE os.out_of_stocks = 'Низкий запас'
GROUP BY os.mp, os.seller, os.sku, os.article_1c, os.code_1c, os.cluster_to
),
-- ОПРЕДЕЛЯЕМ АЛЬТЕНАТИВНЫЙ ЗАПАС (доступный кластер)
alt_stocks_source AS (
SELECT mp, seller, sku, article_1c, code_1c, cluster_to, SUM(quantity_stocks) AS alt_quantity_stocks, arrayStringConcat(groupUniqArray(cluster_from), ',') AS alt_cluster_from_list
FROM (
SELECT DISTINCT wsb.mp AS mp, wsb.seller AS seller, wsb.sku AS sku, wsb.article_1c AS article_1c, wsb.code_1c AS code_1c, cluster_from, cluster_to, smp.quantity_stocks AS quantity_stocks
FROM kpi.all_mp_sales_wsb wsb
LEFT JOIN stocks_mp smp ON wsb.mp=smp.mp AND wsb.seller=smp.seller AND wsb.sku=smp.sku AND wsb.article_1c=smp.article_1c AND wsb.code_1c=smp.code_1c AND wsb.cluster_from=smp.cluster AND smp.date=today()
WHERE date >= today() - INTERVAL PERIOD DAY AND date < today()
AND wsb.quantity_orders != 0
AND cluster_from != 'Основной'
) st
GROUP BY mp, seller, sku, article_1c, code_1c, cluster_to
),
-- БЛОК ЗАКАЗЫ НА ЦС
import AS (
SELECT code_1c, article_1c, SUM(quantity) as quantity_import_orders
FROM goods.import_supplies
GROUP BY code_1c, article_1c
),
-- БЛОК СТРАХОВОЙ РЕЗЕРВ
insurance AS (
SELECT mp, seller, sku, article_1c, code_1c, cluster_to, stddevPop(f.quantity_orders) AS sigma
FROM main f
--WHERE bad_price = '' AND out_of_stocks != 'Низкий запас'
GROUP BY mp, seller, sku, article_1c, code_1c, cluster_to
),
-- ИТОГОВЫЙ ЗАПРОС
final_full AS (
SELECT  am.mp AS mp, am.seller AS seller, am.sku AS sku, am.article_1c AS article_1c, am.code_1c AS code_1c, am.cluster_to AS cluster_to,
        days_oos_number, days_bp_number,
        smp.quantity_stocks AS quantity_stocks,
        smp.quantity_stocks_full as quantity_stocks_full,
        src.quantity_stocks_main AS quantity_stocks_main,
        src.quantity_stocks_main_full AS quantity_stocks_main_full,
        quantity_supplies,  alt_quantity_stocks, bp_quantity_orders, alt_cluster_from_list,
        MAX(am.quantity_orders) AS max_quantity_order,
        MIN(am.quantity_orders) AS min_quantity_order,
        SUM(am.quantity_orders) AS total_quantity_orders_full,
        SUM(am.quantity_orders)/(PERIOD) AS avg_quantity_orders_full
FROM main am
LEFT JOIN days_oos_number don ON am.mp=don.mp AND am.seller=don.seller AND am.sku=don.sku AND am.article_1c=don.article_1c AND am.code_1c=don.code_1c AND am.cluster_to=don.cluster_to
LEFT JOIN bad_price_number bp ON am.mp=bp.mp AND am.seller=bp.seller AND am.sku=bp.sku AND am.article_1c=bp.article_1c AND am.code_1c=bp.code_1c AND am.cluster_to=bp.cluster_to
LEFT JOIN alt_stocks_source alt ON am.mp=alt.mp AND am.seller=alt.seller AND am.sku=alt.sku AND am.article_1c=alt.article_1c AND am.code_1c=alt.code_1c AND am.cluster_to=alt.cluster_to
LEFT JOIN supplies sup ON am.mp=sup.mp AND am.seller=sup.seller AND am.article_1c=sup.article_1c AND am.code_1c=sup.code_1c AND am.cluster_to=sup.cluster
LEFT JOIN stocks_mp smp ON am.mp=smp.mp AND am.seller=smp.seller AND am.sku=smp.sku AND am.article_1c=smp.article_1c AND am.code_1c=smp.code_1c AND am.cluster_to=smp.cluster AND smp.date=today()
LEFT JOIN stocks_1c src ON am.article_1c=src.article_1c AND am.code_1c=src.code_1c AND src.date=today()
GROUP BY am.mp, am.seller, am.sku, am.article_1c, am.code_1c, am.cluster_to, days_oos_number, days_bp_number, smp.quantity_stocks, smp.quantity_stocks_full, src.quantity_stocks_main, src.quantity_stocks_main_full, quantity_supplies, alt_quantity_stocks, bp_quantity_orders, alt_cluster_from_list
),
final_exclude AS (
SELECT   am.mp AS mp, am.seller AS seller, am.sku AS sku, am.article_1c AS article_1c, am.code_1c AS code_1c, am.cluster_to AS cluster_to,
         SUM(am.quantity_orders) AS total_quantity_orders,
         SUM(am.quantity_orders)/(PERIOD-exclude_number) AS avg_quantity_orders
FROM main am
LEFT JOIN exclude_number don ON am.mp=don.mp AND am.seller=don.seller AND am.sku=don.sku AND am.article_1c=don.article_1c AND am.code_1c=don.code_1c AND am.cluster_to=don.cluster_to
WHERE am.date >= today() - INTERVAL PERIOD DAYS AND am.date < today()
AND (out_of_stocks != 'Низкий запас')
-- AND (out_of_stocks != 'Низкий запас' OR bad_price != 'Низкая цена')
GROUP BY am.mp, am.seller, am.sku, am.article_1c, am.code_1c, am.cluster_to, exclude_number
)

-- --Полная версия
-- SELECT
--     sb.name as name,
--     am.article_1c as article_1c,
--     am.code_1c as code_1c,
--     warehouse_code,
--     10000 AS min_order_size,
--     35 AS days_to_central,
--     days_to_central + 5 AS insurance_reserve_main,
--     -- 1.645 * stddevPop(avg_quantity_orders) OVER () * sqrt(days_to_central) AS insurance_reserve_main,
--     ROUND(insurance_reserve_main + days_to_central) AS buffer_main_wh_norm,    
--     ROUND(SUM(oos.avg_quantity_orders) OVER (PARTITION BY article_1c, code_1c)) AS avg_quantity_orders_total,
--     ROUND(SUM(oos.avg_quantity_orders) OVER (PARTITION BY article_1c, code_1c) * buffer_main_wh_norm) AS buffer_main_wh,
--     ROUND(
--             SUM(
--                 IF(
--                 toFloat64(am.quantity_stocks + am.quantity_supplies) >= buffer_cluster,
--                 buffer_cluster,
--                 toFloat64(am.quantity_stocks + am.quantity_supplies)
--                 )
--             ) OVER (PARTITION BY article_1c, code_1c)
--             ) AS quantity_stocks_and_supplies_total,
--     am.quantity_stocks_main as quantity_stocks_main,
--     am.quantity_stocks_main_full as quantity_stocks_main_full,
--     quantity_import_orders AS quantity_import_orders,
--     buffer_main_wh - quantity_stocks_main - quantity_import_orders - quantity_stocks_and_supplies_total AS order_to_main_wh,
--     ROUND((quantity_stocks_main + quantity_import_orders + quantity_stocks_and_supplies_total) / buffer_main_wh, 2) AS buffer_main_wh_marker,
    
--     am.mp as mp,
--     am.seller as seller,    
--     am.sku as sku,
--     am.cluster_to as cluster_to,
--     max_quantity_order,
--     min_quantity_order,
--     total_quantity_orders_full as total_quantity_orders_full,
--     ROUND(am.avg_quantity_orders_full,2) as avg_quantity_orders_full,
--     oos.total_quantity_orders as total_quantity_orders,
--     ROUND(oos.avg_quantity_orders,2) as avg_quantity_orders,
--     days_oos_number,
--     days_bp_number,
--     bp_quantity_orders,
--     7 AS avg_day_to_cluster,
--     avg_day_to_cluster + 5 AS insurance_reserv_cluster,
--     -- 1.645 * sigma * sqrt(avg_day_to_cluster) AS insurance_reserv_cluster,
--     avg_day_to_cluster + insurance_reserv_cluster AS buffer_cluster_norm,    
--     buffer_cluster_norm * avg_quantity_orders AS buffer_cluster,    
--     am.quantity_stocks as quantity_stocks,
--     am.quantity_stocks_full as quantity_stocks_full,
--     am.alt_quantity_stocks as alt_quantity_stocks,
--     alt_cluster_from_list,
--     am.quantity_supplies as quantity_supplies,
--     GREATEST(0, ROUND(buffer_cluster - quantity_stocks - quantity_supplies)) AS deliveries_to_cluster,
--     ROUND(
--         LEAST(
--         IFNULL(
--             (toFloat64(quantity_stocks) + toFloat64(quantity_supplies))
--             / NULLIF(buffer_cluster, toFloat64(0)),
--             toFloat64(1)
--         ),
--         toFloat64(1)
--         ),
--         2
--     ) AS buffer_cluster_marker
--     ,sigma
-- FROM final_full am
-- LEFT JOIN final_exclude oos ON am.mp=oos.mp AND am.seller=oos.seller AND am.sku=oos.sku AND am.article_1c=oos.article_1c AND am.code_1c=oos.code_1c AND am.cluster_to=oos.cluster_to
-- LEFT JOIN insurance AS ins ON am.mp=ins.mp AND am.seller=ins.seller AND am.sku=ins.sku AND am.article_1c=ins.article_1c AND am.code_1c=ins.code_1c AND am.cluster_to=ins.cluster_to
-- LEFT JOIN import AS im ON am.article_1c=im.article_1c AND am.code_1c=im.code_1c
-- LEFT JOIN (SELECT DISTINCT name, article_1c, code_1c, brand, discounted, warehouse_code  FROM ref.article_mp) sb ON am.article_1c=sb.article_1c AND am.code_1c=sb.code_1c
-- WHERE brand != 'РЕСЕЙЛ' AND discounted != 1
-- AND am.article_1c IN ('1178304', '1213421', '1206668', '1212270', '1208966', '1204694')
-- ORDER BY am.article_1c, am.mp, am.seller, am.cluster_to


--Усеченная версия для УТЗ
SELECT
    sb.name as name,
    am.article_1c as article_1c,
    am.code_1c as code_1c,
    warehouse_code,
    ROUND(SUM(avg_quantity_orders_full) OVER (PARTITION BY article_1c, code_1c)) AS avg_quantity_orders_full_total,
    ROUND(SUM(avg_quantity_orders) OVER (PARTITION BY article_1c, code_1c)) AS avg_quantity_orders_total,
    am.quantity_stocks_main as quantity_stocks_main,
    am.quantity_stocks_main_full as quantity_stocks_main_full,
    quantity_import_orders AS quantity_import_orders,

    am.mp as mp,
    am.seller as seller,    
    am.sku as sku,
    am.cluster_to as cluster_to,
    max_quantity_order,
    min_quantity_order,
    total_quantity_orders_full as total_quantity_orders_full,
    ROUND(am.avg_quantity_orders_full,2) as avg_quantity_orders_full,
    oos.total_quantity_orders as total_quantity_orders,
    ROUND(oos.avg_quantity_orders,2) as avg_quantity_orders,
    days_oos_number,
    days_bp_number,
    bp_quantity_orders,
    am.quantity_stocks as quantity_stocks,
    am.quantity_stocks_full as quantity_stocks_full,
    am.alt_quantity_stocks as alt_quantity_stocks,
    alt_cluster_from_list,
    am.quantity_supplies as quantity_supplies   
FROM final_full am
LEFT JOIN import AS im ON am.article_1c=im.article_1c AND am.code_1c=im.code_1c
LEFT JOIN final_exclude oos ON am.mp=oos.mp AND am.seller=oos.seller AND am.sku=oos.sku AND am.article_1c=oos.article_1c AND am.code_1c=oos.code_1c AND am.cluster_to=oos.cluster_to
LEFT JOIN (SELECT DISTINCT name, article_1c, code_1c, brand, discounted, warehouse_code  FROM ref.article_mp) sb ON am.article_1c=sb.article_1c AND am.code_1c=sb.code_1c
WHERE brand != 'РЕСЕЙЛ' AND discounted != 1
ORDER BY am.article_1c, am.mp, am.seller, am.cluster_to