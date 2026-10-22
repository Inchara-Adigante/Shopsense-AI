# Runbook: when the pipeline fails

Every step is idempotent, so **retrying is always safe**: the same input produces the same output, with no duplicates. Most incidents come down to fixing the input, then re-running.

Where to look:

| What | Where |
|---|---|
| DAG run outcomes | `lake/_audit/pipeline_runs.jsonl`, Airflow UI, dashboard → 🩺 Pipeline health |
| Data-quality results | `lake/_audit/dq_results.jsonl` and `ops.dq_results` |
| What each clean run processed | `lake/_audit/clean_runs.jsonl` |
| What each warehouse load changed | `ops.load_log` |
| Rejected rows and why | `lake/quarantine/<table>/ingest_date=…/` |
| Which deliveries are processed | `lake/_state/clean_state.json` |
| Changes waiting for the warehouse | `lake/_state/pending_warehouse_changes.json` |

---

## `volume:<date>` failed (error): a delivery is much smaller than usual

**Symptom:** the `clean` task fails with `orders.volume:2026-06-27: 11 rows vs median 186`.
**What protected you:** the staged data was not published, and the date was **not** marked as processed.

1. Check the landing file: `wc -l data/landing/orders/2026-06-27/orders.csv`.
2. If the source sent a partial file, ask for a corrected re-delivery. Then replace the raw batch:
   `python -m shopsense.ingest.land_raw --date 2026-06-27 --force`
3. Re-run the DAG run, or just `python -m shopsense.transform.clean`. The date is still unprocessed, so it's picked up automatically.
4. If the volume is genuinely low (a holiday, an outage), record that and re-run with quality checks disabled for that table, *after review*, or adjust the threshold in `shopsense/quality/checks.py`.

## `unique_key` / `not_null` / `accepted_values` failed

A source changed its format or started sending bad keys.

1. Look at the quarantine and staged data to find the offending values:
   `duckdb -c "SELECT reason, count(*) FROM 'lake/quarantine/orders/*/*.parquet' GROUP BY 1"`
2. If a **new valid value** appeared (for example, a new order status), add it to `ORDER_STATUSES` in `clean.py` and `ACCEPTED` in `quality/checks.py`, then deploy and re-run.
3. If the data is wrong, get a corrected file and follow steps 2–3 of the volume case.

## `quarantine_rate` warning or error

More than 2% (warning) or 10% (error) of a delivery's rows failed the cleaning rules.

1. `SELECT reason, count(*) … FROM 'lake/quarantine/<table>/ingest_date=<date>/*.parquet' GROUP BY 1`
2. One dominant reason usually means one upstream bug. Fix it upstream, re-deliver with `--force`, and re-run.

## `ChecksumMismatch` in `ingest`

A file for an already-ingested date arrived with **different content**. Ingest refuses to overwrite history silently.

1. Compare the two versions (raw vs landing). If the new one is the correction:
   `python -m shopsense.ingest.land_raw --date <date> --force`
2. Reprocess that date and everything that depends on it: trigger `shopsense_reprocess` with `date_from = date_to = <date>`, or run `python -m shopsense.transform.clean --from <date> --to <date>`, then curate and load.

## Warehouse `IntegrityError` or a warehouse-stage DQ error

**Symptom:** `load_warehouse` fails, for example with `fact_order_item.product_key: 12 rows with no matching dim_product`.
**What protected you:** the whole load transaction rolled back. The warehouse still holds the previous good state, and the pending-changes file is kept.

1. Read the message: orphan keys usually mean a dimension source is late (for example, a new product isn't in the catalog snapshot yet).
2. Wait for or fix the dimension feed, run `curate`, then `load_warehouse` again. It retries the same pending changes.
3. If the warehouse schema changed, rebuild: `python -m shopsense.warehouse.load --full`.

## `freshness_days` error

Inventory in the warehouse is more than one day behind the calendar.

1. Check `wait_for_landing` in Airflow. Is it still waiting for a source?
2. Check `clean_state.json`. Was the date processed?
3. Late data needs no special handling: once it lands, the next run processes it.

## Spark runs out of memory or is slow

Increase `SPARK_DRIVER_MEMORY` (default `4g`) or `SPARK_SHUFFLE_PARTITIONS`. Incremental runs only read the affected partitions; a `--full` rebuild reads everything.

## Rebuild from scratch (last resort)

Raw is immutable and complete, so everything downstream can be rebuilt:

```bash
python -m shopsense.transform.clean --full
python -m shopsense.transform.curate
python -m shopsense.warehouse.load --full
```
