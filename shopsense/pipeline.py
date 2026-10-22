"""
Run the full stage 1 pipeline end to end:

    generate -> ingest (landing -> raw) -> clean (Spark) -> curate (Spark) -> warehouse load
             -> review summaries -> backtest

Each step is timed and the run summary is written to lake/_audit/pipeline_run.json.
Without --skip-generate the lake and warehouse are rebuilt from scratch; with it,
clean and load are incremental (only new deliveries). For scheduled daily runs with
retries and backfills, use the Airflow DAG in airflow/dags/shopsense_daily.py.

Usage:
    python -m shopsense.pipeline                  # full run, regenerating data
    python -m shopsense.pipeline --skip-generate  # reprocess existing landing files
"""
import argparse
import json
import time
from datetime import datetime, timezone

from shopsense.ai import reviews as review_summaries
from shopsense import storage
from shopsense.config import get_paths
from shopsense.generate.retail_data import generate
from shopsense.ingest.land_raw import ingest
from shopsense.transform import clean, curate
from shopsense.transform.spark import get_spark
from shopsense.warehouse import backtest
from shopsense.warehouse.load import load


def run(skip_generate: bool = False, days: int | None = None) -> dict:
    paths = get_paths()
    summary = {"started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "steps": {}}

    def step(name, fn):
        t0 = time.perf_counter()
        result = fn()
        summary["steps"][name] = {"seconds": round(time.perf_counter() - t0, 1), "result": result}
        print(f"[{name:<9}] {summary['steps'][name]['seconds']:>6.1f}s")
        return result

    if not skip_generate:
        # New simulated history -> start from an empty lake and warehouse.
        storage.rmtree(paths.lake)
        paths.warehouse_db.unlink(missing_ok=True)
        step("generate", lambda: generate(**({"n_days": days} if days else {})))
    step("ingest", lambda: {"batches": len(ingest())})
    spark = get_spark("shopsense-pipeline")
    try:
        step("clean", lambda: clean.run(spark).tables)
        step("curate", lambda: curate.run(spark))
    finally:
        spark.stop()
    step("load", lambda: {k: v for k, v in load().items() if k in ("run_id", "mode")})
    step("reviews", lambda: review_summaries.build())
    bt = step("backtest", lambda: backtest.run().round(3).reset_index().to_dict(orient="records"))

    summary["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    out = paths.lake / "_audit" / "pipeline_run.json"
    out.write_text(json.dumps(summary, indent=2, default=str))
    print(f"\nstockout backtest (7-day horizon): {bt}")
    print(f"run summary -> {out}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the ShopSense stage 1 pipeline.")
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--days", type=int, help="days of history to simulate (default 180)")
    args = parser.parse_args()
    run(args.skip_generate, args.days)


if __name__ == "__main__":
    main()
