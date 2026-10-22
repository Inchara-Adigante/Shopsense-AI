-- ShopSense AI - Amazon Redshift DDL and load commands
-- ---------------------------------------------------------------------------
-- STATUS: reference implementation, not yet executed against a Redshift
-- cluster. The tested warehouse in stage 1 is DuckDB (sql/warehouse/ddl.sql),
-- which has the same tables and columns. Running this on Redshift Serverless
-- is part of the AWS deployment work.
--
-- Physical design choices
--   * Dimensions are small (< 10k rows): DISTSTYLE ALL copies them to every
--     node, so joins to them never move data across the network.
--   * fact_order_item and fact_inventory_daily are joined on product_key in
--     the stockout and demand queries: DISTKEY(product_key) co-locates them.
--   * Almost every query filters on a date range: SORTKEY(date_key) lets
--     Redshift skip blocks outside the range (zone maps).
--   * Redshift accepts PRIMARY KEY / FOREIGN KEY but does not enforce them;
--     the pipeline's integrity checks (shopsense/warehouse/load.py) do.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS dw;

-- ---------------------------------------------------------------- dimensions
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key      INTEGER NOT NULL PRIMARY KEY,
    date          DATE    NOT NULL,
    year          SMALLINT, quarter SMALLINT, month SMALLINT,
    month_name    VARCHAR(12), iso_week SMALLINT, day_of_month SMALLINT,
    day_name      VARCHAR(12), is_weekend BOOLEAN
) DISTSTYLE ALL SORTKEY (date_key);

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key     INTEGER NOT NULL PRIMARY KEY,
    product_id      VARCHAR(16) NOT NULL,
    product_name    VARCHAR(200),
    category        VARCHAR(40),
    brand           VARCHAR(60),
    list_price      DECIMAL(10,2),
    unit_cost       DECIMAL(10,2),
    launch_date     DATE,
    version         INTEGER,
    effective_from  DATE,
    effective_to    DATE NOT NULL,
    is_current      BOOLEAN NOT NULL
) DISTSTYLE ALL SORTKEY (product_id);

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key    INTEGER NOT NULL PRIMARY KEY,
    customer_id     VARCHAR(16) NOT NULL,
    signup_date     DATE,
    region          VARCHAR(20),
    is_member       BOOLEAN,
    age_band        VARCHAR(8),
    version         INTEGER,
    effective_from  DATE,
    effective_to    DATE NOT NULL,
    is_current      BOOLEAN NOT NULL
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dw.dim_fulfillment_center (
    fc_key BIGINT NOT NULL PRIMARY KEY, fc_id VARCHAR(10) NOT NULL,
    city VARCHAR(40), state CHAR(2), region_served VARCHAR(20)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS dw.dim_promotion (
    promo_key BIGINT NOT NULL PRIMARY KEY, promo_id VARCHAR(10) NOT NULL,
    product_id VARCHAR(16) NOT NULL, start_date DATE NOT NULL, end_date DATE NOT NULL,
    discount_pct DECIMAL(4,2) NOT NULL
) DISTSTYLE ALL;

-- ---------------------------------------------------------------- facts
CREATE TABLE IF NOT EXISTS dw.fact_order_item (
    order_id VARCHAR(12) NOT NULL, line_number SMALLINT NOT NULL,
    date_key INTEGER NOT NULL REFERENCES dw.dim_date (date_key),
    order_ts TIMESTAMP NOT NULL,
    product_key BIGINT NOT NULL REFERENCES dw.dim_product (product_key),
    customer_key BIGINT NOT NULL REFERENCES dw.dim_customer (customer_key),
    fc_key BIGINT NOT NULL REFERENCES dw.dim_fulfillment_center (fc_key),
    promo_key BIGINT NOT NULL,
    channel VARCHAR(10), payment_method VARCHAR(12), order_status VARCHAR(12), status_updated_at TIMESTAMP,
    quantity SMALLINT NOT NULL, unit_price DECIMAL(10,2) NOT NULL,
    gross_amount DECIMAL(12,2) NOT NULL, discount_amount DECIMAL(12,2) NOT NULL,
    net_amount DECIMAL(12,2) NOT NULL, cost_amount DECIMAL(12,2),
    PRIMARY KEY (order_id, line_number)
) DISTKEY (product_key) SORTKEY (date_key);

CREATE TABLE IF NOT EXISTS dw.fact_inventory_daily (
    date_key INTEGER NOT NULL, product_key BIGINT NOT NULL, fc_key BIGINT NOT NULL,
    on_hand_units INTEGER NOT NULL, inbound_units INTEGER NOT NULL, reorder_point INTEGER,
    is_stockout BOOLEAN NOT NULL,
    PRIMARY KEY (date_key, product_key, fc_key)
) DISTKEY (product_key) SORTKEY (date_key, product_key);

CREATE TABLE IF NOT EXISTS dw.fact_session (
    session_id VARCHAR(12) NOT NULL PRIMARY KEY, date_key INTEGER NOT NULL,
    session_start_ts TIMESTAMP NOT NULL, customer_key BIGINT NOT NULL, is_guest BOOLEAN NOT NULL,
    device VARCHAR(10), page_views BIGINT NOT NULL, out_of_stock_views BIGINT NOT NULL,
    add_to_carts BIGINT NOT NULL, converted BOOLEAN NOT NULL, order_id VARCHAR(12)
) DISTSTYLE EVEN SORTKEY (date_key);        -- aggregated by date only; no dominant join key

CREATE TABLE IF NOT EXISTS dw.fact_product_activity (
    date_key INTEGER NOT NULL, product_key BIGINT NOT NULL,
    page_views BIGINT NOT NULL, out_of_stock_views BIGINT NOT NULL,
    add_to_carts BIGINT NOT NULL, sessions BIGINT NOT NULL,
    PRIMARY KEY (date_key, product_key)
) DISTKEY (product_key) SORTKEY (date_key);

CREATE TABLE IF NOT EXISTS dw.fact_review (
    review_id VARCHAR(12) NOT NULL PRIMARY KEY, date_key INTEGER NOT NULL,
    review_ts TIMESTAMP NOT NULL, product_key BIGINT NOT NULL, customer_key BIGINT NOT NULL,
    order_id VARCHAR(12), rating SMALLINT NOT NULL, title VARCHAR(200), body VARCHAR(2000)
) DISTKEY (product_key) SORTKEY (date_key);

-- ---------------------------------------------------------------- load from the S3 curated zone
-- Replace <bucket> and the IAM role ARN. COPY reads the Parquet files written by
-- shopsense/transform/curate.py in parallel across slices.
--
-- TRUNCATE dw.fact_order_item;
-- COPY dw.fact_order_item
-- FROM 's3://<bucket>/curated/fact_order_item/'
-- IAM_ROLE 'arn:aws:iam::<account-id>:role/<redshift-s3-read-role>'
-- FORMAT AS PARQUET;
--
-- Incremental load (same pattern as shopsense/warehouse/load.py):
--   COPY the changed partitions into a staging table, then
--   BEGIN;
--   DELETE FROM dw.fact_order_item USING stage_fact_order_item s WHERE dw.fact_order_item.date_key = s.date_key;
--   INSERT INTO dw.fact_order_item SELECT * FROM stage_fact_order_item;
--   COMMIT;
-- Dimensions: MERGE INTO dw.dim_product USING stage_dim_product ON product_key ... (upsert).
