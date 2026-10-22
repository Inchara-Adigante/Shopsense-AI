-- ShopSense AI - business marts (DuckDB)
-- Answers: which products will run out of stock, what is driving their demand,
-- and what should the business do?
--
-- Assumptions for the restock logic (documented so the numbers are explainable):
--   * supplier lead time      = 10 days (the simulated suppliers take 4-14 days)
--   * target cover after PO   = 21 days of demand
--   * demand estimate         = units sold in the last 28 days / days the FC had stock.
--     Dividing by in-stock days instead of 28 corrects for stockouts: a product that
--     was out of stock for 14 of 28 days really sells twice as fast as its raw average.

-- ---------------------------------------------------------------- sales
-- Grain: date x category. "orders" counts orders containing that category, so do not
-- sum it across categories (an order with two categories would be counted twice).
CREATE OR REPLACE VIEW mart.daily_sales AS
-- Cancelled orders are excluded from revenue; returns are reported separately.
SELECT d.date, p.category,
       COUNT(DISTINCT f.order_id) FILTER (WHERE f.order_status <> 'cancelled')           AS orders,
       SUM(f.quantity)            FILTER (WHERE f.order_status <> 'cancelled')           AS units,
       SUM(f.gross_amount)        FILTER (WHERE f.order_status <> 'cancelled')           AS gross_revenue,
       SUM(f.discount_amount)     FILTER (WHERE f.order_status <> 'cancelled')           AS discounts,
       SUM(f.net_amount)          FILTER (WHERE f.order_status <> 'cancelled')           AS net_revenue,
       SUM(f.net_amount - f.cost_amount) FILTER (WHERE f.order_status <> 'cancelled')    AS gross_margin,
       SUM(f.net_amount)          FILTER (WHERE f.order_status = 'returned')             AS returned_revenue,
       COUNT(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'cancelled')            AS cancelled_orders
FROM dw.fact_order_item f
JOIN dw.dim_date d    USING (date_key)
JOIN dw.dim_product p USING (product_key)
GROUP BY d.date, p.category;

-- ---------------------------------------------------------------- conversion funnel
CREATE OR REPLACE VIEW mart.daily_funnel AS
SELECT d.date,
       COUNT(*)                                              AS sessions,
       COUNT(*) FILTER (WHERE s.page_views > 0)              AS sessions_with_view,
       COUNT(*) FILTER (WHERE s.add_to_carts > 0)            AS sessions_with_cart,
       COUNT(*) FILTER (WHERE s.converted)                   AS converted_sessions,
       COUNT(*) FILTER (WHERE s.out_of_stock_views > 0)      AS sessions_hit_out_of_stock,
       ROUND(COUNT(*) FILTER (WHERE s.converted) / COUNT(*), 4)  AS conversion_rate
FROM dw.fact_session s
JOIN dw.dim_date d USING (date_key)
GROUP BY d.date;

-- ---------------------------------------------------------------- product x FC x day base
-- Units sold and stock position on the same grain - the base for demand and risk.
CREATE OR REPLACE VIEW mart.product_fc_daily AS
SELECT i.date_key, d.date, i.product_key, i.fc_key,
       i.on_hand_units, i.inbound_units, i.is_stockout,
       COALESCE(s.units, 0)       AS units_sold,
       COALESCE(s.net_revenue, 0) AS net_revenue
FROM dw.fact_inventory_daily i
JOIN dw.dim_date d USING (date_key)
LEFT JOIN (
    SELECT date_key, product_key, fc_key, SUM(quantity) AS units, SUM(net_amount) AS net_revenue
    FROM dw.fact_order_item GROUP BY ALL
) s USING (date_key, product_key, fc_key);

-- ---------------------------------------------------------------- stockout risk
-- Table macro so the same logic can be evaluated "as of" any past date (used for backtesting).
CREATE OR REPLACE MACRO mart.inventory_risk_as_of(as_of) AS TABLE
WITH window_stats AS (
    SELECT product_key, fc_key,
           SUM(units_sold)                                  AS units_28d,
           COUNT(*) FILTER (WHERE NOT is_stockout)          AS in_stock_days_28d,
           COUNT(*) FILTER (WHERE is_stockout)              AS stockout_days_28d
    FROM mart.product_fc_daily
    WHERE date > CAST(as_of AS DATE) - INTERVAL 28 DAY AND date <= CAST(as_of AS DATE)
    GROUP BY ALL
),
position AS (
    SELECT product_key, fc_key, on_hand_units, inbound_units
    FROM mart.product_fc_daily
    WHERE date = CAST(as_of AS DATE)
),
scored AS (
    SELECT p.product_key, p.fc_key, p.on_hand_units, p.inbound_units,
           w.units_28d, w.stockout_days_28d,
           w.units_28d / GREATEST(w.in_stock_days_28d, 1)              AS daily_demand,
           w.units_28d / 28.0                                          AS naive_daily_demand
    FROM position p JOIN window_stats w USING (product_key, fc_key)
)
SELECT CAST(as_of AS DATE)                                             AS as_of_date,
       s.product_key, pr.product_id, pr.product_name, pr.category, s.fc_key, fc.fc_id,
       s.on_hand_units, s.inbound_units, s.units_28d, s.stockout_days_28d,
       ROUND(s.daily_demand, 2)                                        AS daily_demand,
       ROUND(s.on_hand_units / NULLIF(s.daily_demand, 0), 1)           AS days_of_cover,
       CAST(CAST(as_of AS DATE) + CAST(FLOOR(s.on_hand_units / NULLIF(s.daily_demand, 0)) AS INTEGER) AS DATE)
                                                                       AS projected_stockout_date,
       CASE WHEN s.on_hand_units = 0                                   THEN 'Out of stock'
            WHEN s.on_hand_units / NULLIF(s.daily_demand, 0) < 7       THEN 'High'
            WHEN s.on_hand_units / NULLIF(s.daily_demand, 0) < 14      THEN 'Medium'
            ELSE 'Low' END                                             AS risk_tier,
       GREATEST(0, CEIL(s.daily_demand * (10 + 21) - s.on_hand_units - s.inbound_units))::INTEGER
                                                                       AS recommended_order_units,
       ROUND(s.daily_demand * pr.list_price, 2)                        AS revenue_at_risk_per_day,
       ROUND(s.on_hand_units / NULLIF(s.naive_daily_demand, 0), 1)     AS naive_days_of_cover
FROM scored s
JOIN dw.dim_product pr USING (product_key)
JOIN dw.dim_fulfillment_center fc USING (fc_key)
WHERE s.daily_demand > 0;

CREATE OR REPLACE VIEW mart.inventory_risk AS
SELECT r.*,
       CASE WHEN r.risk_tier IN ('Out of stock', 'High') AND r.recommended_order_units > 0 AND r.inbound_units = 0
                THEN 'Reorder ' || r.recommended_order_units || ' units now'
            WHEN r.risk_tier IN ('Out of stock', 'High') AND r.inbound_units > 0
                THEN 'Inbound ' || r.inbound_units || ' units - expedite if possible'
            WHEN r.risk_tier = 'Medium' AND r.recommended_order_units > 0
                THEN 'Plan reorder of ' || r.recommended_order_units || ' units this week'
            ELSE 'No action' END AS recommended_action
FROM mart.inventory_risk_as_of((SELECT MAX(date) FROM mart.product_fc_daily)) r;

-- ---------------------------------------------------------------- demand drivers
-- Per product: recent trend, promotion lift, weekend effect, conversion and lost demand signals.
CREATE OR REPLACE VIEW mart.demand_drivers AS
WITH latest AS (SELECT MAX(date) AS d FROM mart.product_fc_daily),
daily AS (                                              -- product x day across all FCs
    SELECT pf.date, pf.product_key, SUM(pf.units_sold) AS units,
           BOOL_OR(EXISTS (SELECT 1 FROM dw.dim_promotion pm JOIN dw.dim_product p2 ON p2.product_id = pm.product_id
                           WHERE p2.product_key = pf.product_key AND pf.date BETWEEN pm.start_date AND pm.end_date)) AS on_promo,
           ANY_VALUE(dd.is_weekend) AS is_weekend
    FROM mart.product_fc_daily pf JOIN dw.dim_date dd USING (date_key)
    GROUP BY pf.date, pf.product_key
),
agg AS (
    SELECT d.product_key,
           SUM(units) FILTER (WHERE date >  l.d - INTERVAL 28 DAY)                              AS units_last_28d,
           SUM(units) FILTER (WHERE date <= l.d - INTERVAL 28 DAY AND date > l.d - INTERVAL 56 DAY) AS units_prior_28d,
           AVG(units) FILTER (WHERE on_promo)                                                   AS avg_units_on_promo,
           AVG(units) FILTER (WHERE NOT on_promo)                                               AS avg_units_off_promo,
           AVG(units) FILTER (WHERE is_weekend)                                                 AS avg_units_weekend,
           AVG(units) FILTER (WHERE NOT is_weekend)                                             AS avg_units_weekday,
           BOOL_OR(on_promo) FILTER (WHERE date > l.d - INTERVAL 14 DAY)                        AS promo_last_14d
    FROM daily d CROSS JOIN latest l
    GROUP BY d.product_key
),
activity AS (
    SELECT a.product_key, SUM(a.page_views) AS page_views_28d, SUM(a.out_of_stock_views) AS oos_views_28d
    FROM dw.fact_product_activity a JOIN dw.dim_date dd USING (date_key) CROSS JOIN latest l
    WHERE dd.date > l.d - INTERVAL 28 DAY
    GROUP BY a.product_key
)
SELECT p.product_key, p.product_id, p.product_name, p.category,
       a.units_last_28d, a.units_prior_28d,
       ROUND(100.0 * (a.units_last_28d - a.units_prior_28d) / NULLIF(a.units_prior_28d, 0), 1) AS trend_pct,
       ROUND(a.avg_units_on_promo / NULLIF(a.avg_units_off_promo, 0), 2)                    AS promo_lift,
       ROUND(a.avg_units_weekend / NULLIF(a.avg_units_weekday, 0), 2)                       AS weekend_index,
       ROUND(a.units_last_28d / NULLIF(act.page_views_28d, 0), 4)                           AS units_per_view,
       ROUND(act.oos_views_28d / NULLIF(act.page_views_28d, 0), 4)                          AS oos_view_share,
       CASE WHEN a.promo_last_14d AND a.avg_units_on_promo / NULLIF(a.avg_units_off_promo, 0) >= 1.3
                THEN 'Promotion'
            WHEN act.oos_views_28d / NULLIF(act.page_views_28d, 0) >= 0.2 THEN 'Supply constrained'
            -- trend labels need enough volume: +/-25% on 4 units is noise, not a trend
            WHEN a.units_prior_28d >= 20 AND (a.units_last_28d - a.units_prior_28d) / a.units_prior_28d >= 0.25
                THEN 'Rising demand'
            WHEN a.units_prior_28d >= 20 AND (a.units_last_28d - a.units_prior_28d) / a.units_prior_28d <= -0.25
                THEN 'Falling demand'
            ELSE 'Stable' END                                                                AS primary_driver
FROM agg a
JOIN dw.dim_product p USING (product_key)
LEFT JOIN activity act USING (product_key);

-- ---------------------------------------------------------------- the question itself
-- "Which products need restocking this week, and why?"
CREATE OR REPLACE VIEW mart.restock_this_week AS
SELECT r.product_id, r.product_name, r.category, r.fc_id,
       r.on_hand_units, r.inbound_units, r.daily_demand, r.days_of_cover,
       r.projected_stockout_date, r.risk_tier, r.recommended_order_units, r.recommended_action,
       r.revenue_at_risk_per_day, dd.primary_driver, dd.trend_pct, dd.promo_lift
FROM mart.inventory_risk r
LEFT JOIN mart.demand_drivers dd USING (product_key)
WHERE r.risk_tier IN ('Out of stock', 'High')
ORDER BY r.revenue_at_risk_per_day DESC;
