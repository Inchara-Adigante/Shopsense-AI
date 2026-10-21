"""
Clean -> curated: a star schema for analytics.

Dimensions (one row per entity, surrogate integer keys)
    dim_date                 calendar attributes, date_key = yyyymmdd
    dim_product              catalog attributes; effective_from/to + is_current
                             are in place for SCD Type 2 (stage 2)
    dim_customer             region, membership, age band (same SCD columns)
    dim_fulfillment_center   which FC serves which region
    dim_promotion            promotion calendar

Facts (grain in brackets)
    fact_order_item          [order line]           units, gross/discount/net/cost amounts
    fact_inventory_daily     [FC x product x day]   on hand, inbound, reorder point, stockout flag
    fact_session             [web/app session]      page views, add-to-carts, converted flag
    fact_product_activity    [product x day]        views, out-of-stock views, add-to-carts
    fact_review              [review]               rating + text (used by the AI assistant later)

Every dimension has an "unknown" member with key -1, so a fact row whose
lookup fails is still counted instead of silently dropped by an inner join.

Usage:
    python -m shopsense.transform.curate
"""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from shopsense.config import get_paths
from shopsense.transform.spark import get_spark

UNKNOWN_KEY = -1
OPEN_END = "9999-12-31"

# Reference data owned by the warehouse team (not delivered by a source system).
FULFILLMENT_CENTERS = [
    ("FC-BOS", "Boston", "MA", "Northeast"),
    ("FC-ATL", "Atlanta", "GA", "Southeast"),
    ("FC-CHI", "Chicago", "IL", "Midwest"),
    ("FC-DAL", "Dallas", "TX", "South"),
    ("FC-LAX", "Los Angeles", "CA", "West"),
]


def date_key(col) -> F.Column:
    return F.date_format(col, "yyyyMMdd").cast("int")


def with_surrogate_key(df: DataFrame, key_name: str, natural_key: str) -> DataFrame:
    """Deterministic integer keys (1..n ordered by the natural key)."""
    return df.withColumn(key_name, F.row_number().over(Window.orderBy(natural_key)))


def add_unknown_member(df: DataFrame, key_name: str, natural_key: str) -> DataFrame:
    values = {key_name: UNKNOWN_KEY, natural_key: "UNKNOWN", "is_current": True, "effective_to": OPEN_END}
    row = df.sparkSession.range(1).select(
        *[F.lit(values.get(f.name)).cast(f.dataType).alias(f.name) for f in df.schema.fields])
    return row.unionByName(df)


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------

def build_dim_date(orders: DataFrame, inventory: DataFrame, events: DataFrame) -> DataFrame:
    bounds = (orders.select(F.col("order_date").alias("d"))
              .unionByName(inventory.select(F.col("snapshot_date").alias("d")))
              .unionByName(events.select(F.col("event_date").alias("d")))
              .agg(F.min("d").alias("lo"), F.max("d").alias("hi")))
    days = bounds.select(F.explode(F.sequence("lo", "hi")).alias("date"))
    return days.select(
        date_key("date").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.weekofyear("date").alias("iso_week"),
        F.dayofmonth("date").alias("day_of_month"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def build_dim_product(products: DataFrame) -> DataFrame:
    dim = with_surrogate_key(products, "product_key", "product_id").select(
        "product_key", "product_id", "product_name", "category", "brand", "list_price", "unit_cost",
        "launch_date",
        F.col("ingest_date").alias("effective_from"),
        F.lit(OPEN_END).cast("date").alias("effective_to"),
        F.lit(True).alias("is_current"),
    )
    return add_unknown_member(dim, "product_key", "product_id")


def build_dim_customer(customers: DataFrame) -> DataFrame:
    dim = with_surrogate_key(customers, "customer_key", "customer_id").select(
        "customer_key", "customer_id", "signup_date", "region", "is_member", "age_band",
        F.col("ingest_date").alias("effective_from"),
        F.lit(OPEN_END).cast("date").alias("effective_to"),
        F.lit(True).alias("is_current"),
    )
    return add_unknown_member(dim, "customer_key", "customer_id")


def build_dim_fc(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(FULFILLMENT_CENTERS, "fc_id string, city string, state string, region_served string")
    return add_unknown_member(with_surrogate_key(df, "fc_key", "fc_id")
                              .select("fc_key", "fc_id", "city", "state", "region_served"), "fc_key", "fc_id")


def build_dim_promotion(promotions: DataFrame) -> DataFrame:
    return with_surrogate_key(promotions, "promo_key", "promo_id").select(
        "promo_key", "promo_id", "product_id", "start_date", "end_date", "discount_pct")


def lookup(fact: DataFrame, dim: DataFrame, natural_key: str, key_name: str) -> DataFrame:
    """Swap a natural key for the dimension's surrogate key (-1 when not found)."""
    keys = dim.filter(F.col(key_name) != UNKNOWN_KEY).select(natural_key, key_name)
    return (fact.join(F.broadcast(keys), natural_key, "left")
            .withColumn(key_name, F.coalesce(F.col(key_name), F.lit(UNKNOWN_KEY))))


# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------

def build_fact_order_item(items, orders, dim_product, dim_customer, dim_fc, dim_promo) -> DataFrame:
    df = items.drop("ingest_date", "order_date").join(
        orders.select("order_id", "customer_id", "fc_id", "order_ts", "order_date", "channel",
                      "payment_method", "order_status"), "order_id")
    df = lookup(df, dim_product, "product_id", "product_key")
    df = lookup(df, dim_customer, "customer_id", "customer_key")
    df = lookup(df, dim_fc, "fc_id", "fc_key")

    # Promotion active for this product on the order date (largest discount if overlapping).
    promo = dim_promo.select("promo_key", F.col("product_id").alias("p_id"), "start_date", "end_date", "discount_pct")
    df = df.join(promo, (df.product_id == promo.p_id) & df.order_date.between(promo.start_date, promo.end_date), "left")
    w = Window.partitionBy("order_id", "line_number").orderBy(F.col("discount_pct").desc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")

    cost = dim_product.select("product_key", "unit_cost")
    df = df.join(F.broadcast(cost), "product_key", "left")
    gross = F.col("unit_price") * F.col("quantity")
    return df.select(
        "order_id", "line_number",
        date_key("order_date").alias("date_key"),
        "order_ts", "product_key", "customer_key", "fc_key",
        F.coalesce("promo_key", F.lit(UNKNOWN_KEY)).alias("promo_key"),
        "channel", "payment_method", "order_status",
        "quantity", "unit_price",
        gross.cast("decimal(12,2)").alias("gross_amount"),
        F.col("discount_amount").cast("decimal(12,2)"),
        (gross - F.col("discount_amount")).cast("decimal(12,2)").alias("net_amount"),
        (F.col("unit_cost") * F.col("quantity")).cast("decimal(12,2)").alias("cost_amount"),
    )


def build_fact_inventory(inventory, dim_product, dim_fc) -> DataFrame:
    df = lookup(lookup(inventory, dim_product, "product_id", "product_key"), dim_fc, "fc_id", "fc_key")
    return df.select(
        date_key("snapshot_date").alias("date_key"), "product_key", "fc_key",
        "on_hand_units", "inbound_units", "reorder_point",
        (F.col("on_hand_units") == 0).alias("is_stockout"),
    )


def build_fact_session(events, dim_customer) -> DataFrame:
    s = events.groupBy("session_id").agg(
        F.min("event_ts").alias("session_start_ts"),
        F.first("customer_id", ignorenulls=True).alias("customer_id"),
        F.first("device", ignorenulls=True).alias("device"),
        F.sum((F.col("event_type") == "page_view").cast("int")).alias("page_views"),
        F.sum(((F.col("event_type") == "page_view") & ~F.col("in_stock")).cast("int")).alias("out_of_stock_views"),
        F.sum((F.col("event_type") == "add_to_cart").cast("int")).alias("add_to_carts"),
        F.max("order_id").alias("order_id"),
    )
    s = lookup(s.withColumn("customer_id", F.coalesce("customer_id", F.lit("GUEST"))),
               dim_customer, "customer_id", "customer_key")
    return s.select(
        "session_id", date_key(F.to_date("session_start_ts")).alias("date_key"), "session_start_ts",
        "customer_key", (F.col("customer_id") == "GUEST").alias("is_guest"), "device",
        "page_views", "out_of_stock_views", "add_to_carts",
        F.col("order_id").isNotNull().alias("converted"), "order_id",
    )


def build_fact_product_activity(events, dim_product) -> DataFrame:
    a = (events.filter(F.col("product_id").isNotNull())
         .groupBy("event_date", "product_id").agg(
             F.sum((F.col("event_type") == "page_view").cast("int")).alias("page_views"),
             F.sum(((F.col("event_type") == "page_view") & ~F.col("in_stock")).cast("int")).alias("out_of_stock_views"),
             F.sum((F.col("event_type") == "add_to_cart").cast("int")).alias("add_to_carts"),
             F.countDistinct("session_id").alias("sessions"),
         ))
    a = lookup(a, dim_product, "product_id", "product_key")
    return a.select(date_key("event_date").alias("date_key"), "product_key",
                    "page_views", "out_of_stock_views", "add_to_carts", "sessions")


def build_fact_review(reviews, dim_product, dim_customer) -> DataFrame:
    r = lookup(lookup(reviews, dim_product, "product_id", "product_key"), dim_customer, "customer_id", "customer_key")
    return r.select("review_id", date_key("review_date").alias("date_key"), "review_ts", "product_key",
                    "customer_key", "order_id", "rating", "title", "body")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run(spark: SparkSession | None = None, lake: Path | None = None) -> dict[str, int]:
    spark = spark or get_spark("shopsense-curate")
    lake = lake or get_paths().lake
    clean, curated = lake / "clean", lake / "curated"

    def read(name: str) -> DataFrame:
        return spark.read.parquet(str(clean / name))

    orders, items, inventory, events = read("orders"), read("order_items"), read("inventory_snapshots"), read("clickstream")
    products, customers, promotions, reviews = read("products"), read("customers"), read("promotions"), read("reviews")

    dims = {
        "dim_date": build_dim_date(orders, inventory, events),
        "dim_product": build_dim_product(products),
        "dim_customer": build_dim_customer(customers),
        "dim_fulfillment_center": build_dim_fc(spark),
        "dim_promotion": build_dim_promotion(promotions),
    }
    for name, df in dims.items():
        df.coalesce(1).write.mode("overwrite").parquet(str(curated / name))
    d = {name: spark.read.parquet(str(curated / name)) for name in dims}

    facts = {
        "fact_order_item": build_fact_order_item(items, orders, d["dim_product"], d["dim_customer"],
                                                 d["dim_fulfillment_center"], d["dim_promotion"]),
        "fact_inventory_daily": build_fact_inventory(inventory, d["dim_product"], d["dim_fulfillment_center"]),
        "fact_session": build_fact_session(events, d["dim_customer"]),
        "fact_product_activity": build_fact_product_activity(events, d["dim_product"]),
        "fact_review": build_fact_review(reviews, d["dim_product"], d["dim_customer"]),
    }
    for name, df in facts.items():
        df.coalesce(2).write.mode("overwrite").parquet(str(curated / name))

    return {name: spark.read.parquet(str(curated / name)).count() for name in list(dims) + list(facts)}


def main() -> None:
    counts = run()
    for name, n in counts.items():
        print(f"{name:<26}{n:>11,} rows")
    print(f"\ncurated zone -> {get_paths().curated}")


if __name__ == "__main__":
    main()
