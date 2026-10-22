"""
Clean -> curated: a star schema for analytics.

Dimensions
    dim_date                 calendar attributes, date_key = yyyymmdd
    dim_product              SCD Type 2: a new version whenever name, category, brand,
                             price or cost changes (built from weekly catalog snapshots)
    dim_customer             SCD Type 2: a new version when region, membership or age
                             band changes (built from the daily customer CDC feed)
    dim_fulfillment_center   which FC serves which region
    dim_promotion            promotion calendar

Facts (grain in brackets)
    fact_order_item          [order line]           units, gross/discount/net/cost amounts
    fact_inventory_daily     [FC x product x day]   on hand, inbound, reorder point, stockout flag
    fact_session             [web/app session]      page views, add-to-carts, converted flag
    fact_product_activity    [product x day]        views, out-of-stock views, add-to-carts
    fact_review              [review]               rating + text (used by the AI assistant later)

Keys
    * Surrogate keys are a 64-bit hash of (natural key, effective_from). They are
      stable: adding a new version never renumbers existing rows, which is what
      lets the warehouse load incrementally without rewriting old facts.
    * Facts pick the dimension version that was valid on the fact's own date
      (point-in-time lookup), so a January order keeps January's price.
    * Every dimension has an "unknown" member with key -1, so a fact row whose
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
OPEN_START = "1900-01-01"   # version 1 is valid "since forever": activity before the first extract still resolves

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


def hash_key(*cols) -> F.Column:
    """Stable 64-bit surrogate key from natural key column(s); never collides with UNKNOWN_KEY."""
    h = F.xxhash64(*cols)
    return F.when(h == UNKNOWN_KEY, F.lit(UNKNOWN_KEY - 1)).otherwise(h)


def scd2_versions(df: DataFrame, natural_key: str, as_of_col: str, tracked: list[str]) -> DataFrame:
    """
    Collapse a history of snapshots / change records into SCD Type 2 versions.

    A row starts a new version only if a tracked attribute differs from the previous
    row for the same key; repeated identical snapshots are not new versions.
    """
    w = Window.partitionBy(natural_key).orderBy(as_of_col)
    attr_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in tracked]), 256)
    changes = (df.withColumn("_h", attr_hash)
               .withColumn("_prev_h", F.lag("_h").over(w))
               .filter(F.col("_prev_h").isNull() | (F.col("_h") != F.col("_prev_h"))))
    w2 = Window.partitionBy(natural_key).orderBy(as_of_col)
    next_from = F.lead(as_of_col).over(w2)
    first = F.lag(as_of_col).over(w2).isNull()
    return (changes
            .withColumn("effective_from", F.when(first, F.lit(OPEN_START).cast("date")).otherwise(F.col(as_of_col)))
            .withColumn("effective_to", F.coalesce(F.date_sub(next_from, 1), F.lit(OPEN_END).cast("date")))
            .withColumn("is_current", next_from.isNull())
            .withColumn("version", F.row_number().over(w2))
            .drop("_h", "_prev_h"))


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


PRODUCT_TRACKED = ["product_name", "category", "brand", "list_price", "unit_cost"]
CUSTOMER_TRACKED = ["region", "is_member", "age_band"]


def build_dim_product(snapshots: DataFrame) -> DataFrame:
    v = scd2_versions(snapshots, "product_id", "snapshot_date", PRODUCT_TRACKED)
    dim = v.select(
        hash_key("product_id", "effective_from").alias("product_key"),
        "product_id", "product_name", "category", "brand", "list_price", "unit_cost", "launch_date",
        "version", "effective_from", "effective_to", "is_current",
    )
    return add_unknown_member(dim, "product_key", "product_id")


def build_dim_customer(changes: DataFrame) -> DataFrame:
    v = scd2_versions(changes, "customer_id", "updated_at", CUSTOMER_TRACKED)
    dim = v.select(
        hash_key("customer_id", "effective_from").alias("customer_key"),
        "customer_id", "signup_date", "region", "is_member", "age_band",
        "version", "effective_from", "effective_to", "is_current",
    )
    return add_unknown_member(dim, "customer_key", "customer_id")


def build_dim_fc(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(FULFILLMENT_CENTERS, "fc_id string, city string, state string, region_served string")
    return add_unknown_member(df.select(hash_key("fc_id").alias("fc_key"), "fc_id", "city", "state",
                                        "region_served"), "fc_key", "fc_id")


def build_dim_promotion(promotions: DataFrame) -> DataFrame:
    return promotions.select(hash_key("promo_id").alias("promo_key"),
                             "promo_id", "product_id", "start_date", "end_date", "discount_pct")


def lookup(fact: DataFrame, dim: DataFrame, natural_key: str, key_name: str) -> DataFrame:
    """Swap a natural key for the surrogate key of a non-versioned dimension (-1 when not found)."""
    keys = dim.filter(F.col(key_name) != UNKNOWN_KEY).select(natural_key, key_name)
    return (fact.join(F.broadcast(keys), natural_key, "left")
            .withColumn(key_name, F.coalesce(F.col(key_name), F.lit(UNKNOWN_KEY))))


def lookup_as_of(fact: DataFrame, dim: DataFrame, natural_key: str, key_name: str, date_col: str,
                 extra: list[str] | None = None) -> DataFrame:
    """Point-in-time lookup: the SCD2 version whose validity window contains the fact's date."""
    cols = [natural_key, key_name, "effective_from", "effective_to"] + (extra or [])
    versions = dim.filter(F.col(key_name) != UNKNOWN_KEY).select(*cols).withColumnRenamed(natural_key, "_nk")
    cond = (F.col(natural_key) == F.col("_nk")) & F.col(date_col).between(F.col("effective_from"), F.col("effective_to"))
    return (fact.join(F.broadcast(versions), cond, "left")
            .withColumn(key_name, F.coalesce(F.col(key_name), F.lit(UNKNOWN_KEY)))
            .drop("_nk", "effective_from", "effective_to"))


# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------

def current_order_status(orders: DataFrame, status_updates: DataFrame) -> DataFrame:
    """Latest status per order from the status-update feed ('placed' if no update yet)."""
    w = Window.partitionBy("order_id").orderBy(F.col("status_ts").desc())
    latest = (status_updates.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
              .select("order_id", F.col("order_status").alias("_latest"), F.col("status_ts").alias("status_updated_at")))
    return (orders.join(latest, "order_id", "left")
            .withColumn("order_status", F.coalesce("_latest", "order_status"))
            .withColumn("status_updated_at", F.coalesce("status_updated_at", "order_ts"))
            .drop("_latest"))


def build_fact_order_item(items, orders, dim_product, dim_customer, dim_fc, dim_promo) -> DataFrame:
    df = items.drop("ingest_date", "order_date").join(
        orders.select("order_id", "customer_id", "fc_id", "order_ts", "order_date", "channel",
                      "payment_method", "order_status", "status_updated_at"), "order_id")
    df = lookup_as_of(df, dim_product, "product_id", "product_key", "order_date", extra=["unit_cost"])
    df = lookup_as_of(df, dim_customer, "customer_id", "customer_key", "order_date")
    df = lookup(df, dim_fc, "fc_id", "fc_key")

    # Promotion active for this product on the order date (largest discount if overlapping).
    promo = dim_promo.select("promo_key", F.col("product_id").alias("p_id"), "start_date", "end_date", "discount_pct")
    df = df.join(promo, (df.product_id == promo.p_id) & df.order_date.between(promo.start_date, promo.end_date), "left")
    w = Window.partitionBy("order_id", "line_number").orderBy(F.col("discount_pct").desc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")

    gross = F.col("unit_price") * F.col("quantity")
    return df.select(
        "order_id", "line_number",
        date_key("order_date").alias("date_key"),
        "order_ts", "product_key", "customer_key", "fc_key",
        F.coalesce("promo_key", F.lit(UNKNOWN_KEY)).alias("promo_key"),
        "channel", "payment_method", "order_status", "status_updated_at",
        "quantity", "unit_price",
        gross.cast("decimal(12,2)").alias("gross_amount"),
        F.col("discount_amount").cast("decimal(12,2)"),
        (gross - F.col("discount_amount")).cast("decimal(12,2)").alias("net_amount"),
        (F.col("unit_cost") * F.col("quantity")).cast("decimal(12,2)").alias("cost_amount"),
    )


def build_fact_inventory(inventory, dim_product, dim_fc) -> DataFrame:
    df = lookup(lookup_as_of(inventory, dim_product, "product_id", "product_key", "snapshot_date"),
                dim_fc, "fc_id", "fc_key")
    return df.select(
        date_key("snapshot_date").alias("date_key"), "product_key", "fc_key",
        "on_hand_units", "inbound_units", "reorder_point",
        (F.col("on_hand_units") == 0).alias("is_stockout"),
    )


def build_fact_session(events, dim_customer) -> DataFrame:
    s = events.groupBy("session_id").agg(
        F.min("event_ts").alias("session_start_ts"),
        # min_by = value on the session's earliest event: deterministic, unlike first().
        F.min_by("customer_id", "event_ts").alias("customer_id"),
        F.min_by("device", "event_ts").alias("device"),
        F.sum((F.col("event_type") == "page_view").cast("int")).alias("page_views"),
        F.sum(((F.col("event_type") == "page_view") & ~F.col("in_stock")).cast("int")).alias("out_of_stock_views"),
        F.sum((F.col("event_type") == "add_to_cart").cast("int")).alias("add_to_carts"),
        F.max("order_id").alias("order_id"),
    )
    s = lookup_as_of(s.withColumn("customer_id", F.coalesce("customer_id", F.lit("GUEST")))
                      .withColumn("_d", F.to_date("session_start_ts")),
                     dim_customer, "customer_id", "customer_key", "_d")
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
    a = lookup_as_of(a, dim_product, "product_id", "product_key", "event_date")
    # Aggregate on the surrogate key so unresolved rows (-1) can never collide on the grain.
    return (a.groupBy(date_key("event_date").alias("date_key"), "product_key")
            .agg(*[F.sum(c).alias(c) for c in ("page_views", "out_of_stock_views", "add_to_carts", "sessions")]))


def build_fact_review(reviews, dim_product, dim_customer) -> DataFrame:
    r = lookup_as_of(lookup_as_of(reviews, dim_product, "product_id", "product_key", "review_date"),
                     dim_customer, "customer_id", "customer_key", "review_date")
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
    orders = current_order_status(orders, read("order_status_updates"))

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
