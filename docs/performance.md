# Performance Evidence

Measured with `python -m shopsense.warehouse.benchmark`, using DuckDB as the query engine on the stage 2 lake (1.0M clickstream events, 180 days). Times are the median of 5 runs, except raw JSON (3 runs). Raw numbers are in [`performance_results.json`](performance_results.json).

| Experiment | Variant | Time | Files opened | Data on disk |
|---|---|---:|---:|---:|
| **1. File format**: daily page views per product | raw gzip JSON | 0.488 s | 181 | 13.4 MB |
| | clean Parquet | **0.078 s** (6.3× faster) | 181 | 22.0 MB |
| **2. Partition pruning**: distinct sessions, Jun 1–7 | filter on a non-partition column | 0.034 s | 181 | 22.0 MB |
| | filter on `event_date` (partition) | **0.008 s** (4× faster) | **7** | **0.8 MB** |
| **3. Small files**: event counts over everything | 655 small files | 0.054 s | 655 | 23.6 MB |
| | one file per day (what `clean.py` writes) | **0.022 s** (2.5× faster) | 181 | 22.0 MB |
| **4. Materialization**: 21-date stockout backtest | `mart.product_fc_daily` as a view | 0.556 s | n/a | n/a |
| | as a table, rebuilt each load | **0.342 s** (1.6× faster) | n/a | n/a |

## What the numbers mean

1. **Parquet wins on time, even though it's bigger here.** Parquet stores types and columns, so the query reads only `event_type`, `event_date` and `product_id`, and skips JSON parsing. Gzip JSON is smaller on disk (13.4 vs 22.0 MB) because gzip compresses repetitive text well and the Parquet files use Snappy, which is optimized for speed. Switching the clean zone to `zstd` would trade some write CPU for smaller files.
2. **Partitioning by business date is what makes "last week" cheap.** With the `event_date=…` folder filter, the engine never opens 174 of the 181 daily files: 0.8 MB is read instead of 22 MB. On S3 with Athena or Redshift Spectrum, which charge by data scanned, that's the cost difference too. A filter on `event_ts`, which isn't the partition column, has to look at every file.
3. **Small files cost time even locally, and S3 makes it much worse.** Every file has open and metadata overhead. Locally, 3.6× more files made the query 2.5× slower. On S3 each file is also an HTTP request with tens of milliseconds of latency. That's why `clean.py` writes one file per date partition. The same change also shrank the clean zone from 51 MB to 33 MB in stage 1.
4. **Materialize what's read repeatedly.** The backtest evaluates the risk macro 21 times over the same product × FC × day base. As a view, that base is recomputed each time; as a table, built once per load, it's read directly. The mart now builds it as a table.

## Caveats
- These are small datasets on one machine, so absolute times are tiny. The *ratios* and *bytes read* are what carry over to bigger data and to S3.
- DuckDB already prunes using Parquet row-group statistics. On Spark or Athena, a filter on a non-partition column would usually read even more.
