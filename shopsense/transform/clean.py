"""
Raw -> clean: typed, standardized, deduplicated Parquet.

For every source table:
    1. read every raw delivery (the ingest_date folder becomes a column)
    2. cast to proper types and standardize text (casing, whitespace, enums)
    3. apply validation rules; failing rows go to lake/quarantine/<table>/
       with every reason that applied
    4. deduplicate on the business key; when the same key arrives in several
       deliveries, the latest delivery wins (events: the first one wins, since
       a repeated event_id is a retry of the same event)
    5. write Parquet partitioned by the *business* date (order_date,
       event_date, snapshot_date), so a late event lands in the partition of
       the day it happened, not the day it arrived

Usage:
    python -m shopsense.transform.clean
"""
import json
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from shopsense.config import get_paths
from shopsense.transform.spark import get_spark

CATEGORIES = ["Electronics", "Home & Kitchen", "Beauty", "Sports & Outdoors",
              "Toys & Games", "Books", "Grocery", "Apparel"]
EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]
ORDER_STATUSES = ["placed", "shipped", "delivered", "cancelled", "returned"]


@dataclass
class CleanResult:
    clean: DataFrame
    rejected: DataFrame
    key: list[str]
    partition_by: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_raw(spark: SparkSession, raw: Path, source: str, fmt: str) -> DataFrame:
    path = str(raw / source)
    if fmt == "csv":
        # Read everything as strings: type problems become quarantine reasons, not crashes.
        df = spark.read.option("header", True).option("inferSchema", False).csv(path)
    else:
        df = spark.read.json(path)
    return df.withColumn("ingest_date", F.col("ingest_date").cast("date")) \
             .withColumn("_source_file", F.input_file_name())


def parse_ts(col: Column) -> Column:
    """Accept ISO-8601 UTC ('2026-01-05T13:45:10Z') and the legacy US format ('01/05/2026 13:45:10')."""
    return F.coalesce(F.try_to_timestamp(col, F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'")),
                      F.try_to_timestamp(col, F.lit("MM/dd/yyyy HH:mm:ss")))


def canonical(col: Column, allowed: list[str]) -> Column:
    """Map a messy categorical value to its canonical spelling (case/whitespace-insensitive), else NULL."""
    mapping = F.create_map(*[x for a in allowed for x in (F.lit(a.lower()), F.lit(a))])
    return mapping[F.lower(F.trim(col))]


def split_valid(df: DataFrame, rules: list[tuple[str, Column]]) -> tuple[DataFrame, DataFrame]:
    """rules = [(reason, condition_that_marks_row_invalid), ...]"""
    reasons = F.concat_ws("; ", *[F.when(cond, F.lit(reason)) for reason, cond in rules])
    df = df.withColumn("_reject_reason", reasons)
    valid = df.filter(F.col("_reject_reason") == "").drop("_reject_reason")
    rejected = df.filter(F.col("_reject_reason") != "")
    return valid, rejected


def dedupe(df: DataFrame, key: list[str], latest_first: bool = True) -> DataFrame:
    """Keep one row per key: from the latest delivery (or the earliest, for event retries)."""
    order = F.col("ingest_date").desc() if latest_first else F.col("ingest_date").asc()
    w = Window.partitionBy(*key).orderBy(order, F.col("_source_file"))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def as_quarantine(rejected: DataFrame, table: str) -> DataFrame:
    """Keep the rejected row as JSON plus why it was rejected."""
    payload = [c for c in rejected.columns if c not in ("_reject_reason", "_source_file", "ingest_date")]
    return rejected.select(
        F.lit(table).alias("table_name"),
        F.col("ingest_date"),
        F.col("_source_file").alias("source_file"),
        F.col("_reject_reason").alias("reason"),
        F.to_json(F.struct(*payload)).alias("record"),
    )


# ---------------------------------------------------------------------------
# Table rules
# ---------------------------------------------------------------------------

def clean_orders(raw: DataFrame) -> CleanResult:
    df = raw.select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        parse_ts(F.col("order_ts")).alias("order_ts"),
        F.lower(F.trim("channel")).alias("channel"),
        F.lower(F.trim("payment_method")).alias("payment_method"),
        F.upper(F.trim("fc_id")).alias("fc_id"),
        canonical(F.col("order_status"), ORDER_STATUSES).alias("order_status"),
        "ingest_date", "_source_file",
    )
    valid, rejected = split_valid(df, [
        ("null order_id", F.col("order_id").isNull()),
        ("null customer_id", F.col("customer_id").isNull()),
        ("unparseable order_ts", F.col("order_ts").isNull()),
        ("unknown order_status", F.col("order_status").isNull()),
    ])
    clean = dedupe(valid, ["order_id"]).withColumn("order_date", F.to_date("order_ts"))
    return CleanResult(clean, rejected, ["order_id"], "order_date")


def clean_order_items(raw: DataFrame, orders: DataFrame) -> CleanResult:
    df = raw.select(
        F.trim("order_id").alias("order_id"),
        F.col("line_number").try_cast("int").alias("line_number"),
        F.trim("product_id").alias("product_id"),
        F.col("quantity").try_cast("int").alias("quantity"),
        F.col("unit_price").try_cast("decimal(10,2)").alias("unit_price"),
        F.col("discount_amount").try_cast("decimal(10,2)").alias("discount_amount"),
        "ingest_date", "_source_file",
    )
    known = orders.select("order_id", "order_date").withColumn("_known", F.lit(True))
    df = df.join(known, "order_id", "left")
    valid, rejected = split_valid(df, [
        ("null product_id", F.col("product_id").isNull()),
        ("quantity must be > 0", F.coalesce(F.col("quantity") <= 0, F.lit(True))),
        ("invalid unit_price", F.coalesce(F.col("unit_price") <= 0, F.lit(True))),
        ("discount exceeds line value", F.col("discount_amount") > F.col("unit_price") * F.col("quantity")),
        ("unknown order_id", F.col("_known").isNull()),
    ])
    clean = dedupe(valid.drop("_known"), ["order_id", "line_number"])
    return CleanResult(clean, rejected.drop("_known", "order_date"), ["order_id", "line_number"], "order_date")


def clean_products(raw: DataFrame) -> CleanResult:
    df = raw.select(
        F.trim("product_id").alias("product_id"),
        F.trim("product_name").alias("product_name"),
        canonical(F.col("category"), CATEGORIES).alias("category"),
        F.trim("brand").alias("brand"),
        F.col("list_price").try_cast("decimal(10,2)").alias("list_price"),
        F.col("unit_cost").try_cast("decimal(10,2)").alias("unit_cost"),
        F.col("launch_date").try_cast("date").alias("launch_date"),
        "ingest_date", "_source_file",
    )
    valid, rejected = split_valid(df, [
        ("null product_id", F.col("product_id").isNull()),
        ("unknown category", F.col("category").isNull()),
        ("invalid list_price", F.coalesce(F.col("list_price") <= 0, F.lit(True))),
    ])
    return CleanResult(dedupe(valid, ["product_id"]), rejected, ["product_id"])


def clean_customers(raw: DataFrame) -> CleanResult:
    df = raw.select(
        F.trim("customer_id").alias("customer_id"),
        F.col("signup_date").try_cast("date").alias("signup_date"),
        F.initcap(F.trim("region")).alias("region"),
        F.lower(F.trim("is_member")).isin("true", "1").alias("is_member"),
        F.trim("age_band").alias("age_band"),
        "ingest_date", "_source_file",
    )
    valid, rejected = split_valid(df, [
        ("null customer_id", F.col("customer_id").isNull()),
        ("invalid signup_date", F.col("signup_date").isNull()),
    ])
    return CleanResult(dedupe(valid, ["customer_id"]), rejected, ["customer_id"])


def clean_promotions(raw: DataFrame) -> CleanResult:
    df = raw.select(
        F.trim("promo_id").alias("promo_id"),
        F.trim("product_id").alias("product_id"),
        F.col("start_date").try_cast("date").alias("start_date"),
        F.col("end_date").try_cast("date").alias("end_date"),
        F.col("discount_pct").try_cast("decimal(4,2)").alias("discount_pct"),
        "ingest_date", "_source_file",
    )
    valid, rejected = split_valid(df, [
        ("invalid dates", F.coalesce(F.col("end_date") < F.col("start_date"), F.lit(True))),
        ("discount out of range", F.coalesce(~F.col("discount_pct").between(0.01, 0.9), F.lit(True))),
    ])
    return CleanResult(dedupe(valid, ["promo_id"]), rejected, ["promo_id"])


def clean_inventory(raw: DataFrame) -> CleanResult:
    df = raw.select(
        F.col("snapshot_date").try_cast("date").alias("snapshot_date"),
        F.upper(F.trim("fc_id")).alias("fc_id"),
        F.trim("product_id").alias("product_id"),
        F.col("on_hand_units").try_cast("int").alias("on_hand_units"),
        F.col("inbound_units").try_cast("int").alias("inbound_units"),
        F.col("reorder_point").try_cast("int").alias("reorder_point"),
        "ingest_date", "_source_file",
    )
    valid, rejected = split_valid(df, [
        ("null key", F.col("snapshot_date").isNull() | F.col("fc_id").isNull() | F.col("product_id").isNull()),
        ("negative stock", F.coalesce((F.col("on_hand_units") < 0) | (F.col("inbound_units") < 0), F.lit(True))),
    ])
    key = ["snapshot_date", "fc_id", "product_id"]
    return CleanResult(dedupe(valid, key), rejected, key, "snapshot_date")


def clean_clickstream(raw: DataFrame) -> CleanResult:
    df = raw.select(
        F.trim("event_id").alias("event_id"),
        F.trim("session_id").alias("session_id"),
        F.trim("customer_id").alias("customer_id"),          # NULL = guest
        parse_ts(F.col("event_ts")).alias("event_ts"),
        canonical(F.col("event_type"), EVENT_TYPES).alias("event_type"),
        F.trim("product_id").alias("product_id"),
        F.trim("order_id").alias("order_id"),
        F.col("in_stock").cast("boolean").alias("in_stock"),
        F.lower(F.trim("device")).alias("device"),
        "ingest_date", "_source_file",
    )
    needs_product = F.col("event_type").isin("page_view", "add_to_cart")
    valid, rejected = split_valid(df, [
        ("null event_id", F.col("event_id").isNull()),
        ("unparseable event_ts", F.col("event_ts").isNull()),
        ("unknown event_type", F.col("event_type").isNull()),
        ("missing product_id", needs_product & F.col("product_id").isNull()),
        ("purchase without order_id", (F.col("event_type") == "purchase") & F.col("order_id").isNull()),
    ])
    clean = (dedupe(valid, ["event_id"], latest_first=False)
             .withColumn("event_date", F.to_date("event_ts"))
             .withColumn("arrival_lag_days", F.datediff("ingest_date", F.to_date("event_ts"))))
    return CleanResult(clean, rejected, ["event_id"], "event_date")


def clean_reviews(raw: DataFrame) -> CleanResult:
    df = raw.select(
        F.trim("review_id").alias("review_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("customer_id").alias("customer_id"),
        F.trim("order_id").alias("order_id"),
        parse_ts(F.col("review_ts")).alias("review_ts"),
        F.col("rating").try_cast("int").alias("rating"),
        F.trim("title").alias("title"),
        F.trim("body").alias("body"),
        "ingest_date", "_source_file",
    )
    valid, rejected = split_valid(df, [
        ("rating out of range", F.coalesce(~F.col("rating").between(1, 5), F.lit(True))),
        ("unparseable review_ts", F.col("review_ts").isNull()),
    ])
    clean = dedupe(valid, ["review_id"]).withColumn("review_date", F.to_date("review_ts"))
    return CleanResult(clean, rejected, ["review_id"])


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def write_result(name: str, result: CleanResult, clean_root: Path, quarantine_root: Path) -> dict:
    clean = result.clean.drop("_source_file")
    if result.partition_by:
        # One file per date partition: avoids thousands of tiny files (slow S3 listing / scans).
        writer = clean.repartition(result.partition_by).write.mode("overwrite").partitionBy(result.partition_by)
    else:
        writer = clean.coalesce(1).write.mode("overwrite")
    writer.parquet(str(clean_root / name))
    as_quarantine(result.rejected, name).coalesce(1).write.mode("overwrite").parquet(str(quarantine_root / name))

    spark = clean.sparkSession
    written = spark.read.parquet(str(clean_root / name))
    return {"clean_rows": written.count(),
            "rejected_rows": spark.read.parquet(str(quarantine_root / name)).count()}


def run(spark: SparkSession | None = None, lake: Path | None = None) -> dict[str, dict]:
    spark = spark or get_spark("shopsense-clean")
    lake = lake or get_paths().lake
    raw, clean_root, quarantine_root = lake / "raw", lake / "clean", lake / "quarantine"

    def source(name, fmt="csv"):
        return read_raw(spark, raw, name, fmt)

    stats = {}
    orders = clean_orders(source("orders"))
    stats["orders"] = write_result("orders", orders, clean_root, quarantine_root)
    clean_orders_df = spark.read.parquet(str(clean_root / "orders"))

    steps = {
        "order_items": lambda: clean_order_items(source("order_items"), clean_orders_df),
        "products": lambda: clean_products(source("products")),
        "customers": lambda: clean_customers(source("customers")),
        "promotions": lambda: clean_promotions(source("promotions")),
        "inventory_snapshots": lambda: clean_inventory(source("inventory_snapshots")),
        "clickstream": lambda: clean_clickstream(source("clickstream", "json")),
        "reviews": lambda: clean_reviews(source("reviews", "json")),
    }
    for name, build in steps.items():
        stats[name] = write_result(name, build(), clean_root, quarantine_root)

    # Input row counts, for the run summary (raw rows = clean + rejected + duplicates).
    for name, fmt in [("orders", "csv"), ("order_items", "csv"), ("products", "csv"), ("customers", "csv"),
                      ("promotions", "csv"), ("inventory_snapshots", "csv"), ("clickstream", "json"),
                      ("reviews", "json")]:
        n_raw = source(name, fmt).count()
        s = stats[name]
        s["raw_rows"] = n_raw
        s["duplicates_removed"] = n_raw - s["clean_rows"] - s["rejected_rows"]

    audit = lake / "_audit" / "clean_run.json"
    audit.parent.mkdir(parents=True, exist_ok=True)
    audit.write_text(json.dumps(stats, indent=2))
    return stats


def main() -> None:
    stats = run()
    print(f"{'table':<22}{'raw':>11}{'clean':>11}{'rejected':>10}{'dupes':>9}")
    for name, s in stats.items():
        print(f"{name:<22}{s['raw_rows']:>11,}{s['clean_rows']:>11,}{s['rejected_rows']:>10,}{s['duplicates_removed']:>9,}")
    print(f"\nclean zone -> {get_paths().clean}")


if __name__ == "__main__":
    main()
