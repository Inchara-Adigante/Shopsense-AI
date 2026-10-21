-- ShopSense AI - warehouse DDL (DuckDB, the local stand-in for Amazon Redshift)
-- The Redshift version with distribution and sort keys is in sql/redshift/create_tables.sql.
-- Primary keys are enforced here, so a duplicate grain fails the load instead of
-- silently double-counting revenue.

CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;

-- ---------------------------------------------------------------- dimensions
CREATE OR REPLACE TABLE dw.dim_date (
    date_key      INTEGER PRIMARY KEY,      -- yyyymmdd
    date          DATE    NOT NULL,
    year          INTEGER NOT NULL,
    quarter       INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    month_name    VARCHAR NOT NULL,
    iso_week      INTEGER NOT NULL,
    day_of_month  INTEGER NOT NULL,
    day_name      VARCHAR NOT NULL,
    is_weekend    BOOLEAN NOT NULL
);

CREATE OR REPLACE TABLE dw.dim_product (
    product_key     INTEGER PRIMARY KEY,
    product_id      VARCHAR NOT NULL,
    product_name    VARCHAR,
    category        VARCHAR,
    brand           VARCHAR,
    list_price      DECIMAL(10,2),
    unit_cost       DECIMAL(10,2),
    launch_date     DATE,
    effective_from  DATE,
    effective_to    DATE    NOT NULL,       -- 9999-12-31 for the current version
    is_current      BOOLEAN NOT NULL
);

CREATE OR REPLACE TABLE dw.dim_customer (
    customer_key    INTEGER PRIMARY KEY,
    customer_id     VARCHAR NOT NULL,
    signup_date     DATE,
    region          VARCHAR,
    is_member       BOOLEAN,
    age_band        VARCHAR,
    effective_from  DATE,
    effective_to    DATE    NOT NULL,
    is_current      BOOLEAN NOT NULL
);

CREATE OR REPLACE TABLE dw.dim_fulfillment_center (
    fc_key         INTEGER PRIMARY KEY,
    fc_id          VARCHAR NOT NULL,
    city           VARCHAR,
    state          VARCHAR,
    region_served  VARCHAR
);

CREATE OR REPLACE TABLE dw.dim_promotion (
    promo_key     INTEGER PRIMARY KEY,
    promo_id      VARCHAR NOT NULL,
    product_id    VARCHAR NOT NULL,
    start_date    DATE    NOT NULL,
    end_date      DATE    NOT NULL,
    discount_pct  DECIMAL(4,2) NOT NULL
);

-- ---------------------------------------------------------------- facts
CREATE OR REPLACE TABLE dw.fact_order_item (            -- grain: one order line
    order_id         VARCHAR NOT NULL,
    line_number      INTEGER NOT NULL,
    date_key         INTEGER NOT NULL,
    order_ts         TIMESTAMP NOT NULL,
    product_key      INTEGER NOT NULL,
    customer_key     INTEGER NOT NULL,
    fc_key           INTEGER NOT NULL,
    promo_key        INTEGER NOT NULL,                  -- -1 = no promotion
    channel          VARCHAR,
    payment_method   VARCHAR,
    order_status     VARCHAR,
    quantity         INTEGER NOT NULL,
    unit_price       DECIMAL(10,2) NOT NULL,
    gross_amount     DECIMAL(12,2) NOT NULL,
    discount_amount  DECIMAL(12,2) NOT NULL,
    net_amount       DECIMAL(12,2) NOT NULL,
    cost_amount      DECIMAL(12,2),
    PRIMARY KEY (order_id, line_number)
);

CREATE OR REPLACE TABLE dw.fact_inventory_daily (       -- grain: FC x product x day
    date_key       INTEGER NOT NULL,
    product_key    INTEGER NOT NULL,
    fc_key         INTEGER NOT NULL,
    on_hand_units  INTEGER NOT NULL,
    inbound_units  INTEGER NOT NULL,
    reorder_point  INTEGER,
    is_stockout    BOOLEAN NOT NULL,
    PRIMARY KEY (date_key, product_key, fc_key)
);

CREATE OR REPLACE TABLE dw.fact_session (               -- grain: one web/app session
    session_id          VARCHAR PRIMARY KEY,
    date_key            INTEGER NOT NULL,
    session_start_ts    TIMESTAMP NOT NULL,
    customer_key        INTEGER NOT NULL,
    is_guest            BOOLEAN NOT NULL,
    device              VARCHAR,
    page_views          BIGINT NOT NULL,
    out_of_stock_views  BIGINT NOT NULL,
    add_to_carts        BIGINT NOT NULL,
    converted           BOOLEAN NOT NULL,
    order_id            VARCHAR
);

CREATE OR REPLACE TABLE dw.fact_product_activity (      -- grain: product x day
    date_key            INTEGER NOT NULL,
    product_key         INTEGER NOT NULL,
    page_views          BIGINT NOT NULL,
    out_of_stock_views  BIGINT NOT NULL,
    add_to_carts        BIGINT NOT NULL,
    sessions            BIGINT NOT NULL,
    PRIMARY KEY (date_key, product_key)
);

CREATE OR REPLACE TABLE dw.fact_review (                -- grain: one review
    review_id     VARCHAR PRIMARY KEY,
    date_key      INTEGER NOT NULL,
    review_ts     TIMESTAMP NOT NULL,
    product_key   INTEGER NOT NULL,
    customer_key  INTEGER NOT NULL,
    order_id      VARCHAR,
    rating        INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    title         VARCHAR,
    body          VARCHAR
);
