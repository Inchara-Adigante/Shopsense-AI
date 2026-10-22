-- ShopSense AI - live (streaming) tables and views (DuckDB: warehouse/streaming.duckdb)
-- Written by shopsense/streaming/stream_job.py; read by the dashboard's Live tab.

CREATE SCHEMA IF NOT EXISTS live;

-- Deduplicated events. The primary key makes the sink idempotent: a replayed
-- micro-batch (after a crash) inserts nothing new.
CREATE TABLE IF NOT EXISTS live.click_events (
    event_id     VARCHAR PRIMARY KEY,
    session_id   VARCHAR,
    customer_id  VARCHAR,
    event_ts     TIMESTAMP NOT NULL,
    event_type   VARCHAR,
    product_id   VARCHAR,
    in_stock     BOOLEAN,
    device       VARCHAR,
    arrival_ts   TIMESTAMP,
    batch_id     BIGINT,
    ingested_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS live.order_lines (
    event_id     VARCHAR PRIMARY KEY,           -- order_id:line_number
    order_id     VARCHAR,
    line_number  INTEGER,
    product_id   VARCHAR,
    fc_id        VARCHAR,
    quantity     INTEGER,
    net_amount   DOUBLE,
    event_ts     TIMESTAMP NOT NULL,
    arrival_ts   TIMESTAMP,
    batch_id     BIGINT,
    ingested_at  TIMESTAMP
);

-- Stock and normal demand per product x FC at the start of the live day (copied from the warehouse).
CREATE TABLE IF NOT EXISTS live.start_of_day (
    live_day       DATE,
    product_id     VARCHAR,
    product_name   VARCHAR,
    category       VARCHAR,
    fc_id          VARCHAR,
    start_on_hand  INTEGER,
    inbound_units  INTEGER,
    daily_demand   DOUBLE,
    PRIMARY KEY (product_id, fc_id)
);

-- 5-minute tumbling windows of product activity.
CREATE OR REPLACE VIEW live.product_activity_5m AS
SELECT time_bucket(INTERVAL 5 MINUTE, event_ts) AS window_start, product_id,
       COUNT(*) FILTER (WHERE event_type = 'page_view')                     AS page_views,
       COUNT(*) FILTER (WHERE event_type = 'page_view' AND NOT in_stock)    AS out_of_stock_views,
       COUNT(*) FILTER (WHERE event_type = 'add_to_cart')                   AS add_to_carts
FROM live.click_events
WHERE product_id IS NOT NULL
GROUP BY ALL;

CREATE OR REPLACE VIEW live.units_5m AS
SELECT time_bucket(INTERVAL 5 MINUTE, event_ts) AS window_start, product_id, fc_id,
       SUM(quantity) AS units, SUM(net_amount) AS net_revenue
FROM live.order_lines
GROUP BY ALL;

-- Stock right now = start-of-day on hand - units sold so far today.
-- Alert when the remaining stock covers less than 12 hours of normal demand.
CREATE OR REPLACE VIEW live.stock_now AS
WITH sold AS (
    SELECT product_id, fc_id, SUM(quantity) AS units_today, MAX(event_ts) AS last_sale_ts
    FROM live.order_lines GROUP BY ALL
)
SELECT s.product_id, s.product_name, s.category, s.fc_id, s.start_on_hand, s.inbound_units,
       COALESCE(sold.units_today, 0)                                          AS units_sold_today,
       GREATEST(s.start_on_hand - COALESCE(sold.units_today, 0), 0)           AS on_hand_now,
       s.daily_demand,
       ROUND(GREATEST(s.start_on_hand - COALESCE(sold.units_today, 0), 0) / NULLIF(s.daily_demand / 24.0, 0), 1)
                                                                              AS hours_of_cover,
       CASE WHEN COALESCE(sold.units_today, 0) > s.start_on_hand THEN 'Restock arrived'   -- sold more than it opened with
            WHEN s.start_on_hand = 0 THEN 'Out of stock since open'
            WHEN s.start_on_hand - COALESCE(sold.units_today, 0) <= 0 THEN 'Sold out today'
            WHEN (s.start_on_hand - COALESCE(sold.units_today, 0)) / NULLIF(s.daily_demand / 24.0, 0) < 12
                 THEN 'Under 12h of cover'
            ELSE 'OK' END                                                     AS live_alert,
       CASE WHEN COALESCE(sold.units_today, 0) > s.start_on_hand THEN 4
            WHEN s.start_on_hand = 0 THEN 3
            WHEN s.start_on_hand - COALESCE(sold.units_today, 0) <= 0 THEN 1
            WHEN (s.start_on_hand - COALESCE(sold.units_today, 0)) / NULLIF(s.daily_demand / 24.0, 0) < 12 THEN 2
            ELSE 9 END                                                        AS alert_rank,      -- 1 = act first
       sold.last_sale_ts
FROM live.start_of_day s
LEFT JOIN sold USING (product_id, fc_id);

-- How far behind real time is the stream? (arrival - event time, per 5-minute window)
CREATE OR REPLACE VIEW live.stream_lag AS
SELECT time_bucket(INTERVAL 5 MINUTE, arrival_ts) AS window_start,
       COUNT(*)                                                         AS events,
       ROUND(AVG(date_diff('millisecond', event_ts, arrival_ts)) / 1000.0, 1)       AS avg_lag_s,
       ROUND(MAX(date_diff('millisecond', event_ts, arrival_ts)) / 1000.0, 1)       AS max_lag_s
FROM live.click_events GROUP BY ALL;
