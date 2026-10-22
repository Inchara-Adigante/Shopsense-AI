"""
Data-quality checks.

Where they run
    * lake stage      - inside clean.publish(), on the *staged* data before it
                        replaces the published partitions (write-audit-publish)
    * warehouse stage - inside the warehouse load transaction, before COMMIT

Severity
    error -> the step fails: the lake partitions are not swapped in / the warehouse
             transaction rolls back. Airflow marks the task failed and retries.
    warn  -> recorded and shown on the pipeline-health page; the run continues.

Every result (pass or fail) is appended to lake/_audit/dq_results.jsonl and
synced into the warehouse table ops.dq_results for monitoring.
"""
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ERROR, WARN = "error", "warn"


@dataclass
class CheckResult:
    stage: str
    table_name: str
    check_name: str
    severity: str
    passed: bool
    observed: float | None = None
    threshold: str | None = None
    message: str = ""
    run_id: str = ""
    checked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))


class DataQualityError(Exception):
    def __init__(self, failures: list[CheckResult]):
        self.failures = failures
        super().__init__("; ".join(f"{f.table_name}.{f.check_name}: {f.message}" for f in failures))


def results_path(lake: Path) -> Path:
    return lake / "_audit" / "dq_results.jsonl"


def record(results: list[CheckResult], lake: Path) -> None:
    path = results_path(lake)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        for r in results:
            fh.write(json.dumps(asdict(r)) + "\n")


def raise_on_errors(results: list[CheckResult]) -> None:
    failures = [r for r in results if not r.passed and r.severity == ERROR]
    if failures:
        raise DataQualityError(failures)


# ---------------------------------------------------------------------------
# Lake stage (Spark)
# ---------------------------------------------------------------------------

NOT_NULL = {
    "orders": ["order_id", "customer_id", "order_ts", "order_date", "order_status"],
    "order_items": ["order_id", "line_number", "product_id", "quantity", "unit_price"],
    "order_status_updates": ["order_id", "order_status", "status_ts"],
    "products": ["product_id", "category", "list_price", "snapshot_date"],
    "customers": ["customer_id", "region", "updated_at"],
    "inventory_snapshots": ["snapshot_date", "fc_id", "product_id", "on_hand_units"],
    "clickstream": ["event_id", "session_id", "event_ts", "event_type", "event_date"],
    "reviews": ["review_id", "product_id", "rating"],
}
ACCEPTED = {
    ("orders", "order_status"): ["placed", "shipped", "delivered", "cancelled", "returned"],
    ("order_status_updates", "order_status"): ["placed", "shipped", "delivered", "cancelled", "returned"],
    ("clickstream", "event_type"): ["page_view", "add_to_cart", "purchase"],
    ("products", "category"): ["Electronics", "Home & Kitchen", "Beauty", "Sports & Outdoors",
                               "Toys & Games", "Books", "Grocery", "Apparel"],
    ("inventory_snapshots", "fc_id"): ["FC-BOS", "FC-ATL", "FC-CHI", "FC-DAL", "FC-LAX"],
}
# Daily volume vs the median of recent days: warn outside [0.5x, 2x], error below 0.2x.
VOLUME = {"orders": "order_date", "order_items": "order_date", "clickstream": "event_date",
          "inventory_snapshots": "snapshot_date"}


def lake_checks(table: str, staged: DataFrame, key: list[str], target: Path, run_id: str) -> list[CheckResult]:
    res = []

    def add(check, severity, passed, observed=None, threshold=None, message=""):
        res.append(CheckResult("lake", table, check, severity, bool(passed), observed, threshold, message, run_id))

    dupes = staged.groupBy(*key).count().filter("count > 1").count()
    add("unique_key", ERROR, dupes == 0, dupes, "0", f"{dupes} duplicate {'+'.join(key)} values")

    for col in NOT_NULL.get(table, []):
        nulls = staged.filter(F.col(col).isNull()).count()
        add(f"not_null:{col}", ERROR, nulls == 0, nulls, "0", f"{nulls} null {col}")

    for (t, col), allowed in ACCEPTED.items():
        if t == table:
            bad = staged.filter(~F.col(col).isin(allowed)).count()
            add(f"accepted_values:{col}", ERROR, bad == 0, bad, "0", f"{bad} rows outside {allowed}")

    pcol = VOLUME.get(table)
    if pcol and (target.exists() and any(target.glob(f"{pcol}=*"))):
        new_counts = {str(r[0]): r[1] for r in staged.groupBy(pcol).count().collect()}
        history = staged.sparkSession.read.parquet(str(target)).filter(~F.col(pcol).isin(list(new_counts))) \
            .groupBy(pcol).count().orderBy(F.col(pcol).desc()).limit(14).collect()
        if len(history) >= 7:
            counts = sorted(r[1] for r in history)
            median = counts[len(counts) // 2]
            for day, n in sorted(new_counts.items()):
                if day < str(history[-1][0]):
                    continue                        # an old partition re-merged by a late row: not a daily volume
                ratio = n / median
                sev = ERROR if ratio < 0.2 else WARN
                add(f"volume:{day}", sev, 0.5 <= ratio <= 2.0, round(ratio, 2), "0.5-2.0x of 14-day median",
                    f"{n:,} rows vs median {median:,}")
    return res


def quarantine_rate_checks(stats: dict[str, dict], run_id: str) -> list[CheckResult]:
    """Share of raw rows rejected by cleaning rules: warn above 2%, error above 10%."""
    res = []
    for table, s in stats.items():
        if not s.get("raw_rows"):
            continue
        rate = s["rejected_rows"] / s["raw_rows"]
        sev = ERROR if rate > 0.10 else WARN
        res.append(CheckResult("lake", table, "quarantine_rate", sev, rate <= 0.02, round(rate, 4), "<= 2%",
                               f"{s['rejected_rows']:,} of {s['raw_rows']:,} rows quarantined", run_id))
    return res


# ---------------------------------------------------------------------------
# Warehouse stage (SQL, inside the load transaction)
# ---------------------------------------------------------------------------

WAREHOUSE_CHECKS = [
    # (check name, table, severity, SQL returning one number, pass condition, threshold text)
    ("freshness_days", "fact_inventory_daily", ERROR,
     "SELECT date_diff('day', MAX(d.date), (SELECT MAX(date) FROM dw.dim_date)) FROM dw.fact_inventory_daily "
     "JOIN dw.dim_date d USING (date_key)", lambda v: v <= 1, "<= 1 day behind the calendar"),
    ("orders_vs_inventory_lag_days", "fact_order_item", WARN,
     "SELECT abs(date_diff('day', (SELECT MAX(d.date) FROM dw.fact_order_item JOIN dw.dim_date d USING (date_key)), "
     "(SELECT MAX(d.date) FROM dw.fact_inventory_daily JOIN dw.dim_date d USING (date_key))))",
     lambda v: v <= 1, "<= 1 day"),
    ("inventory_completeness", "fact_inventory_daily", ERROR,
     "SELECT COUNT(*) FROM (SELECT date_key, COUNT(*) n FROM dw.fact_inventory_daily GROUP BY 1) "
     "WHERE n <> (SELECT COUNT(DISTINCT (product_key, fc_key)) FROM dw.fact_inventory_daily "
     "            WHERE date_key = (SELECT MAX(date_key) FROM dw.fact_inventory_daily))",
     lambda v: v == 0, "every day has every product x FC"),
    ("scd2_one_current_version", "dim_product", ERROR,
     "SELECT COUNT(*) FROM (SELECT product_id FROM dw.dim_product WHERE product_key <> -1 "
     "GROUP BY 1 HAVING SUM(is_current::INT) <> 1)", lambda v: v == 0, "0 products"),
    ("scd2_one_current_version", "dim_customer", ERROR,
     "SELECT COUNT(*) FROM (SELECT customer_id FROM dw.dim_customer WHERE customer_key <> -1 "
     "GROUP BY 1 HAVING SUM(is_current::INT) <> 1)", lambda v: v == 0, "0 customers"),
    ("scd2_no_overlapping_versions", "dim_product", ERROR,
     "SELECT COUNT(*) FROM dw.dim_product a JOIN dw.dim_product b USING (product_id) "
     "WHERE a.version < b.version AND a.effective_to >= b.effective_from", lambda v: v == 0, "0 overlaps"),
    ("unknown_product_keys", "fact_order_item", ERROR,
     "SELECT COUNT(*) FROM dw.fact_order_item WHERE product_key = -1", lambda v: v == 0, "0 rows"),
    ("unknown_customer_keys_pct", "fact_session", WARN,
     "SELECT 100.0 * COUNT(*) FILTER (WHERE customer_key = -1 AND NOT is_guest) / COUNT(*) FROM dw.fact_session",
     lambda v: v <= 0.5, "<= 0.5% of sessions"),
    ("net_amount_non_negative", "fact_order_item", ERROR,
     "SELECT COUNT(*) FROM dw.fact_order_item WHERE net_amount < 0", lambda v: v == 0, "0 rows"),
    ("price_matches_product_version", "fact_order_item", WARN,
     "SELECT 100.0 * AVG((f.unit_price <> p.list_price)::INT) FROM dw.fact_order_item f JOIN dw.dim_product p "
     "USING (product_key)", lambda v: v <= 1.0, "<= 1% of lines"),
    ("converted_sessions_vs_orders_pct", "fact_session", WARN,
     "SELECT 100.0 * abs((SELECT COUNT(*) FROM dw.fact_session WHERE converted) - "
     "(SELECT COUNT(DISTINCT order_id) FROM dw.fact_order_item)) / (SELECT COUNT(DISTINCT order_id) FROM dw.fact_order_item)",
     lambda v: v <= 1.0, "<= 1% difference"),
    ("stockout_share_pct", "fact_inventory_daily", WARN,
     "SELECT 100.0 * AVG(is_stockout::INT) FROM dw.fact_inventory_daily "
     "WHERE date_key = (SELECT MAX(date_key) FROM dw.fact_inventory_daily)", lambda v: v <= 10.0,
     "<= 10% of product x FC out of stock today"),
]


def warehouse_checks(con, run_id: str) -> list[CheckResult]:
    res = []
    for name, table, severity, sql, ok, threshold in WAREHOUSE_CHECKS:
        value = con.execute(sql).fetchone()[0]
        value = float(value) if value is not None else None
        passed = value is not None and ok(value)
        res.append(CheckResult("warehouse", table, name, severity, passed, value, threshold,
                               "" if passed else f"observed {value}", run_id))
    return res


def sync_to_warehouse(con, lake: Path) -> None:
    """Mirror the results log into ops.dq_results (rebuilt from the log each load)."""
    path = results_path(lake)
    if path.exists():
        con.execute(f"CREATE OR REPLACE TABLE ops.dq_results AS "
                    f"SELECT * FROM read_json_auto('{path}', format='newline_delimited')")
