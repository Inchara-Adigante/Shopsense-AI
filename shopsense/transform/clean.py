"""
Raw -> clean: typed, standardized, deduplicated Parquet - processed incrementally.

Each run handles only raw deliveries (ingest_date folders) it has not processed
yet; the processed set is kept in lake/_state/clean_state.json. A backfill
re-runs a chosen date range, and because every write is a merge + dedupe on the
business key, re-processing a date never creates duplicates (idempotent).

For every source table in a run:
    1. read the new raw deliveries (the ingest_date folder becomes a column)
    2. cast to proper types and standardize text (casing, whitespace, enums)
    3. apply validation rules; failing rows go to lake/quarantine/<table>/
       with every reason that applied
    4. deduplicate on the business key; when the same key arrives in several
       deliveries, the latest delivery wins (events: the first one wins, since
       a repeated event_id is a retry of the same event)
    5. merge into the clean table: only the business-date partitions touched
       by the new rows are read, re-deduplicated with the new rows, written to
       a staging area, audited, and then swapped in (write-audit-publish).
       A late event therefore lands in the partition of the day it happened.

Usage:
    python -m shopsense.transform.clean                        # new deliveries only
    python -m shopsense.transform.clean --from 2026-03-01 --to 2026-03-07   # backfill
    python -m shopsense.transform.clean --full                 # rebuild from all raw
"""
import argparse
import json
import shutil
from collections.abc import Callable
from dataclasses import dataclass, field
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
    latest_first: bool = True          # on a key collision: latest delivery wins (False: earliest wins)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def raw_dates(raw: Path, source: str) -> list[str]:
    """ingest_date partitions present in the raw zone for a source."""
    src = raw / source
    return sorted(d.name.split("=", 1)[1] for d in src.glob("ingest_date=*")) if src.exists() else []


def read_raw(spark: SparkSession, raw: Path, source: str, fmt: str, dates: list[str] | None = None) -> DataFrame:
    base = raw / source
    paths = [str(base / f"ingest_date={d}") for d in dates] if dates is not None else [str(base)]
    reader = spark.read.option("basePath", str(base))      # keep ingest_date as a column
    if fmt == "csv":
        # Read everything as strings: type problems become quarantine reasons, not crashes.
        df = reader.option("header", True).option("inferSchema", False).csv(paths)
    else:
        df = reader.json(paths)
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
    # Each delivery is a full catalog snapshot; keep every snapshot (SCD Type 2 is built from them).
    clean = dedupe(valid.withColumn("snapshot_date", F.col("ingest_date")), ["product_id", "snapshot_date"])
    return CleanResult(clean, rejected, ["product_id", "snapshot_date"])


def clean_customers(raw: DataFrame) -> CleanResult:
    df = raw.select(
        F.trim("customer_id").alias("customer_id"),
        F.col("signup_date").try_cast("date").alias("signup_date"),
        F.initcap(F.trim("region")).alias("region"),
        F.lower(F.trim("is_member")).isin("true", "1").alias("is_member"),
        F.trim("age_band").alias("age_band"),
        F.col("updated_at").try_cast("date").alias("updated_at"),
        "ingest_date", "_source_file",
    )
    valid, rejected = split_valid(df, [
        ("null customer_id", F.col("customer_id").isNull()),
        ("invalid signup_date", F.col("signup_date").isNull()),
        ("invalid updated_at", F.col("updated_at").isNull()),
    ])
    # CDC feed: one row per customer per change date (SCD Type 2 is built from them).
    key = ["customer_id", "updated_at"]
    return CleanResult(dedupe(valid, key), rejected, key)


def clean_order_status(raw: DataFrame) -> CleanResult:
    df = raw.select(
        F.trim("order_id").alias("order_id"),
        canonical(F.col("order_status"), ORDER_STATUSES).alias("order_status"),
        parse_ts(F.col("status_ts")).alias("status_ts"),
        "ingest_date", "_source_file",
    )
    valid, rejected = split_valid(df, [
        ("null order_id", F.col("order_id").isNull()),
        ("unknown order_status", F.col("order_status").isNull()),
        ("unparseable status_ts", F.col("status_ts").isNull()),
    ])
    key = ["order_id", "order_status"]
    clean = dedupe(valid, key).withColumn("status_date", F.to_date("status_ts"))
    return CleanResult(clean, rejected, key, "status_date")


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
    return CleanResult(clean, rejected, ["event_id"], "event_date", latest_first=False)


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
# Incremental runner
# ---------------------------------------------------------------------------

# Processing order matters: order_items validates against clean orders.
TABLES = {
    "orders": "csv",
    "order_status_updates": "csv",
    "order_items": "csv",
    "products": "csv",
    "customers": "csv",
    "promotions": "csv",
    "inventory_snapshots": "csv",
    "clickstream": "json",
    "reviews": "json",
}

Audit = Callable[[str, DataFrame], None]


@dataclass
class RunReport:
    dates: dict[str, list[str]] = field(default_factory=dict)      # ingest dates processed per table
    tables: dict[str, dict] = field(default_factory=dict)          # stats per table

    def affected(self, table: str) -> list[str]:
        return self.tables.get(table, {}).get("affected_partitions", [])


def _state_path(lake: Path) -> Path:
    return lake / "_state" / "clean_state.json"


def load_state(lake: Path) -> dict[str, list[str]]:
    path = _state_path(lake)
    return json.loads(path.read_text()) if path.exists() else {}


def save_state(lake: Path, state: dict[str, list[str]]) -> None:
    path = _state_path(lake)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(path)                                  # atomic: never a half-written state file


def publish(name: str, result: CleanResult, lake: Path, audit: Audit | None = None) -> dict:
    """Merge new clean rows into lake/clean/<name> through a staging area (write-audit-publish)."""
    spark = result.clean.sparkSession
    target, staging = lake / "clean" / name, lake / "_staging" / name
    new = result.clean
    pcol = result.partition_by
    affected = sorted(str(r[0]) for r in new.select(pcol).distinct().collect()) if pcol else []

    if (target / "_SUCCESS").exists() or any(target.glob("*.parquet")) or any(target.glob(f"{pcol}=*")):
        existing = spark.read.parquet(str(target))
        if pcol:
            existing = existing.filter(F.col(pcol).isin(affected))
        merged = dedupe(new.unionByName(existing.withColumn("_source_file", F.lit("")), allowMissingColumns=True),
                        result.key, result.latest_first)
    else:
        merged = new
    merged = merged.drop("_source_file")

    # 1. write: stage the merged rows (never read and overwrite the same files in one job).
    if pcol:
        merged.repartition(pcol).write.mode("overwrite").partitionBy(pcol).parquet(str(staging))
    else:
        merged.coalesce(1).write.mode("overwrite").parquet(str(staging))
    staged = spark.read.parquet(str(staging))

    # 2. audit: data-quality checks run on the staged data; an exception stops the publish.
    if audit:
        audit(name, staged)

    # 3. publish: replace only the affected partitions (dynamic partition overwrite).
    if pcol:
        staged.repartition(pcol).write.mode("overwrite").partitionBy(pcol).parquet(str(target))
    else:
        staged.coalesce(1).write.mode("overwrite").parquet(str(target))
    shutil.rmtree(staging, ignore_errors=True)
    return {"affected_partitions": affected, "table_rows": spark.read.parquet(str(target)).count()}


def write_quarantine(name: str, rejected: DataFrame, lake: Path) -> int:
    q = as_quarantine(rejected, name)
    q.coalesce(1).write.mode("overwrite").partitionBy("ingest_date").parquet(str(lake / "quarantine" / name))
    return q.count()


def build(name: str, raw: DataFrame, lake: Path) -> CleanResult:
    spark = raw.sparkSession
    if name == "order_items":
        return clean_order_items(raw, spark.read.parquet(str(lake / "clean" / "orders")))
    return {
        "orders": clean_orders, "order_status_updates": clean_order_status, "products": clean_products,
        "customers": clean_customers, "promotions": clean_promotions, "inventory_snapshots": clean_inventory,
        "clickstream": clean_clickstream, "reviews": clean_reviews,
    }[name](raw)


def run(spark: SparkSession | None = None, lake: Path | None = None, dates: list[str] | None = None,
        full: bool = False, audit: Audit | None = None) -> RunReport:
    """
    dates=None, full=False : process every raw delivery not processed yet (normal daily run)
    dates=[...]            : (re)process exactly these ingest dates (backfill)
    full=True              : drop the clean zone and rebuild from all raw deliveries
    """
    spark = spark or get_spark("shopsense-clean")
    lake = lake or get_paths().lake
    if full:
        for zone in ("clean", "quarantine", "_staging"):
            shutil.rmtree(lake / zone, ignore_errors=True)
        _state_path(lake).unlink(missing_ok=True)
    state, report = load_state(lake), RunReport()

    for name, fmt in TABLES.items():
        available = raw_dates(lake / "raw", name)
        todo = sorted(set(available) & set(dates)) if dates is not None \
            else [d for d in available if d not in set(state.get(name, []))]
        if not todo:
            continue
        raw = read_raw(spark, lake / "raw", name, fmt, todo)
        n_raw = raw.count()
        result = build(name, raw, lake)
        n_valid = result.clean.count()
        stats = {"ingest_dates": len(todo), "raw_rows": n_raw, "valid_rows": n_valid,
                 "rejected_rows": write_quarantine(name, result.rejected, lake)}
        stats["duplicates_in_batch"] = n_raw - n_valid - stats["rejected_rows"]
        stats.update(publish(name, result, lake, audit))
        report.dates[name], report.tables[name] = todo, stats
        state[name] = sorted(set(state.get(name, [])) | set(todo))
        save_state(lake, state)                        # per table: a crash later re-runs only what's left

    # Orders whose status changed live in older order_date partitions - the warehouse must refresh them too.
    if "order_status_updates" in report.dates:
        updated = spark.read.parquet(str(lake / "clean" / "order_status_updates")) \
            .filter(F.col("ingest_date").isin(report.dates["order_status_updates"])).select("order_id")
        days = spark.read.parquet(str(lake / "clean" / "orders")).join(updated, "order_id") \
            .select("order_date").distinct().collect()
        report.tables["order_status_updates"]["affected_order_dates"] = sorted(str(r[0]) for r in days)

    audit_file = lake / "_audit" / "clean_runs.jsonl"
    audit_file.parent.mkdir(parents=True, exist_ok=True)
    with audit_file.open("a") as fh:
        fh.write(json.dumps({"tables": {k: {kk: vv for kk, vv in v.items() if kk != "affected_partitions"}
                                        for k, v in report.tables.items()}}) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean raw deliveries into the clean zone (incremental).")
    parser.add_argument("--from", dest="date_from", help="backfill start ingest date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="date_to", help="backfill end ingest date (inclusive)")
    parser.add_argument("--full", action="store_true", help="rebuild the clean zone from all raw data")
    args = parser.parse_args()

    dates = None
    if args.date_from:
        import pandas as pd
        dates = [d.strftime("%Y-%m-%d") for d in pd.date_range(args.date_from, args.date_to or args.date_from)]
    report = run(dates=dates, full=args.full)
    if not report.tables:
        print("nothing new to process")
    print(f"{'table':<22}{'dates':>6}{'raw':>11}{'valid':>11}{'rejected':>9}{'dupes':>8}{'parts':>7}{'rows now':>11}")
    for name, s in report.tables.items():
        print(f"{name:<22}{s['ingest_dates']:>6}{s['raw_rows']:>11,}{s['valid_rows']:>11,}{s['rejected_rows']:>9,}"
              f"{s['duplicates_in_batch']:>8,}{len(s['affected_partitions']):>7}{s['table_rows']:>11,}")


if __name__ == "__main__":
    main()
