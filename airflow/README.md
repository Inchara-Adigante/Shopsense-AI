# Orchestration (Airflow 3)

Two DAGs in [`dags/shopsense_daily.py`](dags/shopsense_daily.py):

| DAG | Schedule | Tasks |
|---|---|---|
| `shopsense_daily` | `@daily`, `catchup=True`, one run at a time | `wait_for_landing` → `ingest` → `clean` → `curate` → `load_warehouse` → `review_summaries` |
| `shopsense_reprocess` | manual, with `date_from` / `date_to` params | `reclean_range` → `curate` → `load_warehouse` |

- **Retries:** 2 retries with exponential backoff (2 → 20 minutes). A retry is always safe, because every step is idempotent.
- **Backfill:** `catchup=True` means unprocessed dates run oldest first. To force a range, use `airflow backfill create --dag-id shopsense_daily --from-date 2026-03-01 --to-date 2026-03-07`.
- **Corrections:** re-ingest the fixed file (`python -m shopsense.ingest.land_raw --date D --force`), then trigger `shopsense_reprocess` for that range.
- **Monitoring:** every DAG run's outcome is appended to `lake/_audit/pipeline_runs.jsonl`, which the dashboard's Pipeline health tab reads.

The steps run as **subprocesses in the project's own virtualenv**, so Airflow and Spark never share dependencies. That's the same separation you'd get with Glue jobs or `KubernetesPodOperator` in production.

## Run locally (tested with Airflow 3.3.2, Python 3.12)

```bash
# separate virtualenv just for Airflow, installed with the official constraints file
python3.12 -m venv ~/airflow-venv
~/airflow-venv/bin/pip install "apache-airflow==3.3.2" \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt

export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export SHOPSENSE_HOME=$PWD                      # repo root; jobs use $PWD/.venv/bin/python

~/airflow-venv/bin/airflow db migrate
~/airflow-venv/bin/airflow dags test shopsense_daily 2026-06-29   # one run, in-process
~/airflow-venv/bin/airflow standalone                             # web UI on http://localhost:8080
```

`airflow dags test` was run against a lake processed through 2026-06-27 for both 2026-06-28 and 2026-06-29. All 5 tasks succeeded each time, the warehouse advanced incrementally (1–3 date partitions replaced per fact), and each run was recorded in `pipeline_runs.jsonl`.
