"""
Performance experiments on the lake and warehouse (DuckDB as the query engine).

Each experiment runs the same question two ways and reports the median of
several runs plus the bytes of files the engine had to open.

    1. File format       raw gzip JSON  vs  clean Parquet (columnar, typed)
    2. Partition pruning  one week of clickstream: filter on the partition column
                          (event_date=... folders) vs a column that is not partitioned
    3. Small files        the same data as many tiny Parquet files vs one file per day
    4. Materialization    the stockout backtest against a view vs a materialized table

Usage:
    python -m shopsense.warehouse.benchmark            # writes docs/performance_results.json
"""
import json
import shutil
import statistics
import time
from pathlib import Path

import duckdb

from shopsense.config import ROOT, get_paths

RUNS = 5


def timed(con, sql: str, runs: int = RUNS) -> float:
    times = []
    for _ in range(runs):
        t = time.perf_counter()
        con.execute(sql).fetchall()
        times.append(time.perf_counter() - t)
    return statistics.median(times)


def size_mb(paths) -> float:
    return round(sum(p.stat().st_size for p in paths) / 1e6, 1)


def experiment_format(con, lake: Path) -> dict:
    """Daily page views per product, from raw JSON vs clean Parquet."""
    raw_files = list((lake / "raw" / "clickstream").glob("*/*.json.gz"))
    pq_files = list((lake / "clean" / "clickstream").glob("*/*.parquet"))
    q_raw = (f"SELECT CAST(event_ts AS DATE), product_id, COUNT(*) FROM read_json_auto('{lake}/raw/clickstream/*/*.json.gz') "
             f"WHERE lower(event_type) = 'page_view' GROUP BY ALL")
    q_pq = (f"SELECT event_date, product_id, COUNT(*) FROM read_parquet('{lake}/clean/clickstream/*/*.parquet', "
            f"hive_partitioning = true) WHERE event_type = 'page_view' GROUP BY ALL")
    return {"question": "daily page views per product (1M events)",
            "raw_json_gz": {"seconds": round(timed(con, q_raw, 3), 3), "files": len(raw_files), "mb": size_mb(raw_files)},
            "clean_parquet": {"seconds": round(timed(con, q_pq), 3), "files": len(pq_files), "mb": size_mb(pq_files)}}


def experiment_pruning(con, lake: Path) -> dict:
    """One week of sessions: partition filter (prunes folders) vs filter on a non-partition column."""
    path = f"{lake}/clean/clickstream/*/*.parquet"
    week = "event_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-07'"
    pruned = f"SELECT COUNT(DISTINCT session_id) FROM read_parquet('{path}', hive_partitioning = true) WHERE {week}"
    full = (f"SELECT COUNT(DISTINCT session_id) FROM read_parquet('{path}', hive_partitioning = true) "
            f"WHERE CAST(event_ts AS DATE) BETWEEN DATE '2026-06-01' AND DATE '2026-06-07'")
    week_files = [p for p in (lake / "clean" / "clickstream").glob("event_date=2026-06-0[1-7]/*.parquet")]
    all_files = list((lake / "clean" / "clickstream").glob("*/*.parquet"))
    return {"question": "distinct sessions in one week (Jun 1-7)",
            "partition_filter": {"seconds": round(timed(con, pruned), 3), "files_read": len(week_files), "mb": size_mb(week_files)},
            "non_partition_filter": {"seconds": round(timed(con, full), 3), "files_read": len(all_files), "mb": size_mb(all_files)}}


def experiment_small_files(con, lake: Path, scratch: Path) -> dict:
    """Same rows split into several small files per day (what an un-tuned Spark write produces) vs one file per day."""
    src = f"read_parquet('{lake}/clean/clickstream/*/*.parquet', hive_partitioning = true)"
    tiny = scratch / "clickstream_tiny"
    shutil.rmtree(tiny, ignore_errors=True)
    con.execute(f"SET threads = 8; SET preserve_insertion_order = false")
    # Tiny row groups force DuckDB to split each day into several small files,
    # like many small Spark tasks each writing their own file.
    con.execute(f"COPY (SELECT * FROM {src}) TO '{tiny}' (FORMAT parquet, PARTITION_BY (event_date), "
                f"ROW_GROUP_SIZE 100, FILE_SIZE_BYTES '1KB')")
    tiny_files = list(tiny.glob("*/*.parquet"))
    compact_files = list((lake / "clean" / "clickstream").glob("*/*.parquet"))
    q = "SELECT event_type, COUNT(*) FROM read_parquet('{}/*/*.parquet', hive_partitioning = true) GROUP BY 1"
    result = {"question": "event counts by type over all 1M events",
              "many_small_files": {"seconds": round(timed(con, q.format(tiny)), 3), "files": len(tiny_files),
                                   "mb": size_mb(tiny_files)},
              "one_file_per_day": {"seconds": round(timed(con, q.format(lake / "clean" / "clickstream")), 3),
                                   "files": len(compact_files), "mb": size_mb(compact_files)}}
    shutil.rmtree(tiny, ignore_errors=True)
    return result


def experiment_materialize(db_path: Path) -> dict:
    """
    The backtest evaluates inventory_risk_as_of() 21 times over mart.product_fc_daily.
    Compare the materialized table (what marts.sql builds) with the same SQL as a view.
    """
    from shopsense.warehouse import backtest
    t = time.perf_counter()
    backtest.run(db_path)
    table_s = time.perf_counter() - t

    marts = (ROOT / "sql" / "warehouse" / "marts.sql").read_text()
    body = marts.split("CREATE OR REPLACE TABLE mart.product_fc_daily AS", 1)[1].split(";", 1)[0]
    macro = marts.split("CREATE OR REPLACE MACRO mart.inventory_risk_as_of", 1)[1].split(";", 1)[0]
    copy = db_path.with_name("benchmark_copy.duckdb")
    shutil.copy(db_path, copy)
    con = duckdb.connect(str(copy))
    con.execute("DROP TABLE mart.product_fc_daily CASCADE")
    con.execute(f"CREATE VIEW mart.product_fc_daily AS {body}")
    con.execute(f"CREATE OR REPLACE MACRO mart.inventory_risk_as_of{macro}")
    con.close()
    t = time.perf_counter()
    backtest.run(copy)
    view_s = time.perf_counter() - t
    copy.unlink()
    return {"question": "stockout backtest (21 as-of dates)",
            "view": {"seconds": round(view_s, 3)}, "materialized_table": {"seconds": round(table_s, 3)}}


def run() -> dict:
    paths = get_paths()
    scratch = paths.lake / "_benchmark"
    scratch.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    results = {
        "file_format": experiment_format(con, paths.lake),
        "partition_pruning": experiment_pruning(con, paths.lake),
        "small_files": experiment_small_files(con, paths.lake, scratch),
        "materialization": experiment_materialize(paths.warehouse_db),
    }
    con.close()
    shutil.rmtree(scratch, ignore_errors=True)
    out = ROOT / "docs" / "performance_results.json"
    out.write_text(json.dumps(results, indent=2))
    return results


def main() -> None:
    results = run()
    for name, r in results.items():
        print(f"\n{name}: {r['question']}")
        for variant, m in r.items():
            if variant != "question":
                extra = "  ".join(f"{k}={v}" for k, v in m.items() if k != "seconds")
                print(f"  {variant:<22}{m['seconds']:>8.3f}s  {extra}")
    print("\nresults -> docs/performance_results.json")


if __name__ == "__main__":
    main()
