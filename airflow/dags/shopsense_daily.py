"""
ShopSense daily pipeline (Airflow 3).

    wait_for_landing -> ingest -> clean -> curate -> load_warehouse

* One DAG run per landing date ({{ ds }}). catchup=True turns the DAG into a
  backfill engine: un-run dates are processed oldest first, one at a time
  (max_active_runs=1, because each run builds on the previous warehouse state).
* Every step is idempotent (checksummed ingest, merge-dedupe cleaning, partition
  replace loads), so a retry or a re-run of the same date is always safe.
* Data-quality errors make `clean` or `load_warehouse` fail. Nothing is
  published, the run is marked failed, and the next attempt retries the same
  work (see docs/runbook.md).
* Steps run as separate processes in the project's own virtualenv
  (SHOPSENSE_PYTHON), so Airflow's dependencies never mix with Spark's - the same
  separation you get with Glue jobs or KubernetesPodOperator in production.

Environment
    SHOPSENSE_HOME    repo root (default: two levels above this file)
    SHOPSENSE_PYTHON  interpreter of the project venv (default: <home>/.venv/bin/python)
    SHOPSENSE_LAKE / SHOPSENSE_LANDING / SHOPSENSE_WAREHOUSE  passed through to the jobs
"""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

HOME = Path(os.getenv("SHOPSENSE_HOME", Path(__file__).resolve().parents[2]))
PYTHON = os.getenv("SHOPSENSE_PYTHON", str(HOME / ".venv" / "bin" / "python"))
LANDING = Path(os.getenv("SHOPSENSE_LANDING", HOME / "data" / "landing"))
LAKE = Path(os.getenv("SHOPSENSE_LAKE", HOME / "lake"))

# Sources that must have landed before a day can be processed.
REQUIRED_SOURCES = ["orders", "order_items", "inventory_snapshots", "clickstream"]

JOB_ENV = {k: v for k, v in os.environ.items() if k.startswith(("SHOPSENSE_", "SPARK_", "JAVA", "PATH"))}


def record_run(context, state: str) -> None:
    """Append the DAG run outcome to lake/_audit/pipeline_runs.jsonl (read by the pipeline-health page)."""
    dag_run = context["dag_run"]                       # Airflow 3: a lightweight run object, not the ORM model
    ti = context.get("ti") or context.get("task_instance")
    entry = {
        "dag_id": dag_run.dag_id, "run_id": dag_run.run_id,
        "logical_date": str(context.get("ds")), "state": state,
        "failed_task": getattr(ti, "task_id", None) if state == "failed" else None,
        "reason": context.get("reason"),
        "start_date": str(dag_run.start_date), "end_date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    path = LAKE / "_audit" / "pipeline_runs.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write(json.dumps(entry) + "\n")


def job(task_id: str, module: str, args: str = "", timeout_min: int = 30) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {HOME} && {PYTHON} -m {module} {args}",
        env=JOB_ENV, append_env=True,
        execution_timeout=timedelta(minutes=timeout_min),
    )


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}


@dag(
    dag_id="shopsense_daily",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["shopsense", "retail"],
    on_success_callback=lambda ctx: record_run(ctx, "success"),
    on_failure_callback=lambda ctx: record_run(ctx, "failed"),
    doc_md=__doc__,
)
def shopsense_daily():
    @task.sensor(poke_interval=300, timeout=6 * 3600, mode="reschedule")
    def wait_for_landing(ds: str | None = None) -> bool:
        """Wait until every required source has delivered its file for the day."""
        missing = [s for s in REQUIRED_SOURCES if not any((LANDING / s / ds).glob("*"))]
        if missing:
            print(f"waiting for {ds}: {missing}")
        return not missing

    ingest = job("ingest", "shopsense.ingest.land_raw", "--date {{ ds }}", timeout_min=10)
    clean = job("clean", "shopsense.transform.clean", timeout_min=60)
    curate = job("curate", "shopsense.transform.curate", timeout_min=60)
    load = job("load_warehouse", "shopsense.warehouse.load", timeout_min=20)

    wait_for_landing() >> ingest >> clean >> curate >> load


shopsense_daily()


@dag(
    dag_id="shopsense_reprocess",
    schedule=None,                                     # manual: after an upstream correction
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    params={"date_from": "2026-01-01", "date_to": "2026-01-07"},
    tags=["shopsense", "backfill"],
    doc_md="Re-clean a range of ingest dates (after a corrected re-delivery was ingested with --force), "
           "then rebuild curated and load the warehouse. Safe to run repeatedly.",
)
def shopsense_reprocess():
    clean = job("reclean_range", "shopsense.transform.clean",
                "--from {{ params.date_from }} --to {{ params.date_to }}", timeout_min=120)
    curate = job("curate", "shopsense.transform.curate", timeout_min=60)
    load = job("load_warehouse", "shopsense.warehouse.load", timeout_min=20)
    clean >> curate >> load


shopsense_reprocess()
