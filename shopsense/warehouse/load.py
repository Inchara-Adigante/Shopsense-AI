"""
Load the curated star schema into the warehouse and build the business marts.

The local warehouse is DuckDB, a columnar SQL engine that plays the role
Amazon Redshift plays in production (see sql/redshift/ for the Redshift DDL
and COPY commands).

Two modes
    full         drop and rebuild every table from curated Parquet
                 (first load, or `--full` after a schema change)
    incremental  apply only what the clean step reported as changed
                 (lake/_state/pending_warehouse_changes.json):
                   * dimensions  -> upsert by primary key (INSERT OR REPLACE).
                     Surrogate keys are stable hashes, so a new SCD2 version adds a
                     row and closes the previous one without touching any fact.
                   * facts       -> partition replace: DELETE the changed date_keys,
                     then INSERT them again from curated. Idempotent - running the
                     same load twice gives the same result.
                   * order status changes reach back to old order dates; the clean
                     step reports those dates so the affected rows are refreshed.

Both modes run in one transaction with integrity checks (no orphan foreign keys,
row counts equal to the lake) and the warehouse data-quality checks
(shopsense/quality/checks.py); on an error-level failure everything rolls back and
the pending-changes file is kept for the retry. Each table load is logged to ops.load_log.

Usage:
    python -m shopsense.warehouse.load            # incremental (full if the warehouse is empty)
    python -m shopsense.warehouse.load --full
"""
import argparse
import json
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from shopsense.config import ROOT, get_paths
from shopsense import storage
from shopsense.quality import checks

DDL = ROOT / "sql" / "warehouse" / "ddl.sql"
MARTS = ROOT / "sql" / "warehouse" / "marts.sql"

DIMENSIONS = ["dim_date", "dim_product", "dim_customer", "dim_fulfillment_center", "dim_promotion"]
FACTS = ["fact_order_item", "fact_inventory_daily", "fact_session", "fact_product_activity", "fact_review"]
TABLES = DIMENSIONS + FACTS

# Which clean tables' changed dates drive each fact's partition replace.
FACT_SOURCES = {
    "fact_order_item": ["orders", "order_items", "order_status_updates"],
    "fact_inventory_daily": ["inventory_snapshots"],
    "fact_session": ["clickstream"],
    "fact_product_activity": ["clickstream"],
}

# (fact table, column, dimension table) - every fact key must exist in its dimension.
FOREIGN_KEYS = [
    ("fact_order_item", "date_key", "dim_date"),
    ("fact_order_item", "product_key", "dim_product"),
    ("fact_order_item", "customer_key", "dim_customer"),
    ("fact_order_item", "fc_key", "dim_fulfillment_center"),
    ("fact_inventory_daily", "date_key", "dim_date"),
    ("fact_inventory_daily", "product_key", "dim_product"),
    ("fact_inventory_daily", "fc_key", "dim_fulfillment_center"),
    ("fact_session", "date_key", "dim_date"),
    ("fact_session", "customer_key", "dim_customer"),
    ("fact_product_activity", "product_key", "dim_product"),
    ("fact_review", "product_key", "dim_product"),
]


class IntegrityError(Exception):
    pass


def connect(db_path: Path | None = None, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    db_path = db_path or get_paths().warehouse_db
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path), read_only=read_only)


def pending_path(lake: Path) -> Path:
    return lake / "_state" / "pending_warehouse_changes.json"


def date_keys(dates: list[str], pad_days: int = 0) -> list[int]:
    """'2026-03-01' -> 20260301; pad_days also includes the preceding days (sessions crossing midnight)."""
    keys = set()
    for d in dates:
        day = date.fromisoformat(d)
        for i in range(pad_days + 1):
            keys.add(int((day - timedelta(days=i)).strftime("%Y%m%d")))
    return sorted(keys)


def check_integrity(con: duckdb.DuckDBPyConnection, expected: dict[str, int]) -> None:
    problems = []
    for fact, col, dim in FOREIGN_KEYS:
        orphans = con.execute(f"SELECT COUNT(*) FROM dw.{fact} f ANTI JOIN dw.{dim} d USING ({col})").fetchone()[0]
        if orphans:
            problems.append(f"{fact}.{col}: {orphans} rows with no matching {dim}")
    for table, n in expected.items():
        loaded = con.execute(f"SELECT COUNT(*) FROM dw.{table}").fetchone()[0]
        if loaded != n:
            problems.append(f"{table}: warehouse has {loaded} rows, lake has {n}")
    if problems:
        raise IntegrityError("; ".join(problems))


def _source(curated: Path, table: str) -> str:
    return f"read_parquet('{curated / table}/*.parquet')"


def _log(con, run_id, table, mode, partitions, deleted, inserted):
    con.execute("INSERT INTO ops.load_log VALUES (?, ?, ?, ?, ?, ?, ?)",
                [run_id, datetime.now(timezone.utc).replace(tzinfo=None), table, mode, partitions, deleted, inserted])


def load(lake: Path | None = None, db_path: Path | None = None, full: bool = False, quality: bool = True) -> dict:
    lake = lake or get_paths().lake
    curated = lake / "curated"
    pend_file = pending_path(lake)
    pending = json.loads(pend_file.read_text()) if pend_file.exists() else {}
    run_id = uuid.uuid4().hex[:12]

    con = connect(db_path)
    storage.register_duckdb(con, curated)              # s3:// lakes are read through fsspec
    try:
        empty = not con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'dw'").fetchone()[0]
        mode = "full" if (full or empty) else "incremental"
        con.execute("BEGIN")
        if mode == "full":
            con.execute("DROP SCHEMA IF EXISTS mart CASCADE")
            con.execute("DROP SCHEMA IF EXISTS dw CASCADE")
        con.execute(DDL.read_text())

        stats = {}
        for table in TABLES:
            src = _source(curated, table)
            if mode == "full":
                con.execute(f"INSERT INTO dw.{table} BY NAME SELECT * FROM {src}")
                n = con.execute(f"SELECT COUNT(*) FROM dw.{table}").fetchone()[0]
                stats[table] = {"mode": "full", "inserted": n}
                _log(con, run_id, table, "full", None, 0, n)
            elif table in DIMENSIONS or table == "fact_review":
                # Small tables: upsert by primary key. Rows that disappeared from the lake are removed too.
                before = con.execute(f"SELECT COUNT(*) FROM dw.{table}").fetchone()[0]
                con.execute(f"INSERT OR REPLACE INTO dw.{table} BY NAME SELECT * FROM {src}")
                pk = con.execute(f"""
                    SELECT string_agg(kcu.column_name, ', ') FROM information_schema.key_column_usage kcu
                    JOIN information_schema.table_constraints tc USING (constraint_name)
                    WHERE tc.table_schema = 'dw' AND tc.table_name = '{table}'
                      AND tc.constraint_type = 'PRIMARY KEY'""").fetchone()[0]
                removed = con.execute(f"""DELETE FROM dw.{table} WHERE ({pk}) NOT IN (SELECT ({pk}) FROM {src})""").fetchone()[0]
                after = con.execute(f"SELECT COUNT(*) FROM dw.{table}").fetchone()[0]
                stats[table] = {"mode": "upsert", "net_new": after - before, "removed": removed}
                _log(con, run_id, table, "upsert", None, removed, after - before + removed)
            else:
                dates = sorted({d for s in FACT_SOURCES[table] for d in pending.get(s, [])})
                keys = date_keys(dates, pad_days=1 if table == "fact_session" else 0)
                if not keys:
                    stats[table] = {"mode": "unchanged"}
                    continue
                con.execute("CREATE OR REPLACE TEMP TABLE _keys AS SELECT UNNEST(?::INTEGER[]) AS date_key", [keys])
                deleted = con.execute(f"DELETE FROM dw.{table} WHERE date_key IN (SELECT date_key FROM _keys)").fetchone()[0]
                con.execute(f"INSERT INTO dw.{table} BY NAME SELECT * FROM {src} "
                            f"WHERE date_key IN (SELECT date_key FROM _keys)")
                inserted = con.execute(f"SELECT COUNT(*) FROM dw.{table} WHERE date_key IN (SELECT date_key FROM _keys)").fetchone()[0]
                stats[table] = {"mode": "partition_replace", "partitions": len(keys), "deleted": deleted, "inserted": inserted}
                _log(con, run_id, table, "partition_replace", len(keys), deleted, inserted)

        expected = {t: con.execute(f"SELECT COUNT(*) FROM {_source(curated, t)}").fetchone()[0] for t in TABLES}
        check_integrity(con, expected)
        if quality:
            dq = checks.warehouse_checks(con, run_id)
            checks.record(dq, lake)
            checks.raise_on_errors(dq)                 # error-level failure -> ROLLBACK below
        checks.sync_to_warehouse(con, lake)
        con.execute(MARTS.read_text())
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()

    pend_file.unlink(missing_ok=True)                  # consumed only after a successful commit
    return {"run_id": run_id, "mode": mode, "tables": stats, "rows": expected}


def main() -> None:
    parser = argparse.ArgumentParser(description="Load the curated zone into the warehouse.")
    parser.add_argument("--full", action="store_true", help="drop and rebuild every table")
    args = parser.parse_args()
    result = load(full=args.full)
    print(f"run {result['run_id']} - {result['mode']} load")
    for table, s in result["tables"].items():
        detail = ", ".join(f"{k}={v:,}" if isinstance(v, int) else f"{k}={v}" for k, v in s.items())
        print(f"dw.{table:<26} {result['rows'][table]:>9,} rows  ({detail})")
    print(f"\nwarehouse -> {get_paths().warehouse_db}")


if __name__ == "__main__":
    main()
