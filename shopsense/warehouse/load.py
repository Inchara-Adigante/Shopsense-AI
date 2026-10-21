"""
Load the curated star schema into the warehouse and build the business marts.

The local warehouse is DuckDB, a columnar SQL engine that plays the role
Amazon Redshift plays in production (see sql/redshift/ for the Redshift DDL
and COPY commands).

Steps
    1. (re)create tables from sql/warehouse/ddl.sql - primary keys enforced
    2. full refresh of every table from curated Parquet, in one transaction
    3. integrity checks: no orphan foreign keys, row counts match the lake
    4. (re)create the marts from sql/warehouse/marts.sql

Usage:
    python -m shopsense.warehouse.load
"""
from pathlib import Path

import duckdb

from shopsense.config import ROOT, get_paths

DDL = ROOT / "sql" / "warehouse" / "ddl.sql"
MARTS = ROOT / "sql" / "warehouse" / "marts.sql"

TABLES = ["dim_date", "dim_product", "dim_customer", "dim_fulfillment_center", "dim_promotion",
          "fact_order_item", "fact_inventory_daily", "fact_session", "fact_product_activity", "fact_review"]

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


def connect(db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    db_path = db_path or get_paths().warehouse_db
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def check_integrity(con: duckdb.DuckDBPyConnection, expected: dict[str, int]) -> None:
    problems = []
    for fact, col, dim in FOREIGN_KEYS:
        orphans = con.execute(f"SELECT COUNT(*) FROM dw.{fact} f ANTI JOIN dw.{dim} d USING ({col})").fetchone()[0]
        if orphans:
            problems.append(f"{fact}.{col}: {orphans} rows with no matching {dim}")
    for table, n in expected.items():
        loaded = con.execute(f"SELECT COUNT(*) FROM dw.{table}").fetchone()[0]
        if loaded != n:
            problems.append(f"{table}: loaded {loaded} rows, lake has {n}")
    if problems:
        raise IntegrityError("; ".join(problems))


def load(lake: Path | None = None, db_path: Path | None = None) -> dict[str, int]:
    curated = (lake or get_paths().lake) / "curated"
    con = connect(db_path)
    try:
        con.execute("BEGIN")
        con.execute(DDL.read_text())
        expected = {}
        for table in TABLES:
            source = f"'{curated / table}/*.parquet'"
            expected[table] = con.execute(f"SELECT COUNT(*) FROM read_parquet({source})").fetchone()[0]
            con.execute(f"INSERT INTO dw.{table} BY NAME SELECT * FROM read_parquet({source})")
        check_integrity(con, expected)
        con.execute(MARTS.read_text())
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    return expected


def main() -> None:
    counts = load()
    for table, n in counts.items():
        print(f"dw.{table:<26}{n:>11,} rows")
    print(f"\nwarehouse -> {get_paths().warehouse_db}")


if __name__ == "__main__":
    main()
