"""
Spark Structured Streaming job: live clicks and orders -> live DuckDB tables.

    stream/clicks, stream/orders  (JSON-lines files, like Firehose output from Kinesis)
        -> withWatermark(event_ts, 10 minutes)
        -> dropDuplicatesWithinWatermark(event_id)     retries removed, too-late events dropped
        -> foreachBatch: INSERT OR IGNORE into warehouse/streaming.duckdb (primary key = event_id)

Guarantees (and their limits)
    * No event is lost: the checkpoint records which files each micro-batch read, so
      after a crash or restart the job resumes where it stopped.
    * No event is counted twice: retries inside the watermark are removed by
      dropDuplicatesWithinWatermark, and a replayed micro-batch (crash mid-write, or a
      re-sent file) is a no-op because of INSERT OR IGNORE on the event_id primary key.
    * Too-late events: Spark guarantees it never drops data that is *less* late than
      the watermark delay. Data later than that is dropped in almost all cases, but one
      that shares a micro-batch with the events that advance the watermark can slip
      through - so "beyond the watermark" means "may be dropped", not "always dropped".
      (Measured: 29 of 29 dropped in one run; 28 of 29 after a mid-day restart.)

Live views (sql/live/live.sql) turn those events into 5-minute product activity,
units sold so far today, and a live stock position with stockout alerts.

Usage:
    python -m shopsense.streaming.stream_job --day 2026-06-29                # process what has arrived, then stop
    python -m shopsense.streaming.stream_job --day 2026-06-29 --continuous   # keep running (5 s micro-batches)
"""
import argparse
import json
import os
import shutil
import threading
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from shopsense.config import ROOT, get_paths
from shopsense.streaming.producer import stream_root
from shopsense.transform.spark import get_spark

WATERMARK = "10 minutes"
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"
LIVE_SQL = ROOT / "sql" / "live" / "live.sql"

SCHEMAS = {
    "clicks": "event_id string, session_id string, customer_id string, event_ts timestamp, event_type string, "
              "product_id string, in_stock boolean, device string, arrival_ts timestamp",
    "orders": "event_id string, order_id string, line_number int, product_id string, fc_id string, "
              "quantity int, net_amount double, event_ts timestamp, arrival_ts timestamp",
}
TABLES = {"clicks": "live.click_events", "orders": "live.order_lines"}

_db_lock = threading.Lock()                      # both queries' batches write from driver threads


def live_db_path() -> Path:
    return Path(os.getenv("SHOPSENSE_LIVE_DB", get_paths().warehouse_db.with_name("streaming.duckdb")))


def connect_with_retry(db: Path, read_only: bool = False, attempts: int = 50, wait_s: float = 0.1):
    """DuckDB allows one writer process or many readers; wait briefly for the other side to finish."""
    for i in range(attempts):
        try:
            return duckdb.connect(str(db), read_only=read_only)
        except duckdb.IOException:
            if i == attempts - 1:
                raise
            time.sleep(wait_s)


def init_live_db(day: str, db: Path, warehouse: Path) -> None:
    """Create live tables and snapshot start-of-day stock + demand from the warehouse."""
    con = duckdb.connect(str(db))
    con.execute(LIVE_SQL.read_text())
    prev = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
    wh = duckdb.connect(str(warehouse), read_only=True)
    start_stock = wh.execute(f"""
        SELECT DATE '{day}' AS live_day, product_id, product_name, category, fc_id,
               on_hand_units AS start_on_hand, inbound_units, daily_demand
        FROM mart.inventory_risk_as_of(DATE '{prev}')""").df()
    wh.close()
    con.execute("DELETE FROM live.start_of_day")
    con.register("start_stock", start_stock)
    con.execute("INSERT INTO live.start_of_day BY NAME SELECT * FROM start_stock")
    con.close()


def make_sink(topic: str, db: Path):
    table = TABLES[topic]

    def sink(batch: DataFrame, batch_id: int) -> None:
        rows = batch.withColumn("batch_id", F.lit(batch_id)).toPandas()
        if rows.empty:
            return
        with _db_lock:
            con = connect_with_retry(db)              # the dashboard may be reading the file right now
            try:
                con.register("incoming", rows)
                con.execute(f"INSERT OR IGNORE INTO {table} BY NAME SELECT *, now() AS ingested_at FROM incoming")
            finally:
                con.close()
    return sink


def log_progress(queries, path: Path) -> list[dict]:
    """Append each micro-batch's progress (rows, late drops, durations, watermark) to a JSON-lines log."""
    path.parent.mkdir(parents=True, exist_ok=True)
    entries = []
    for q in queries:
        for p in q.recentProgress:
            state = (p.get("stateOperators") or [{}])[0]
            entries.append({
                "query": p["name"], "batch_id": p["batchId"], "timestamp": p["timestamp"],
                "input_rows": p["numInputRows"],
                "rows_dropped_by_watermark": state.get("numRowsDroppedByWatermark", 0),
                "state_rows": state.get("numRowsTotal", 0),
                "duration_ms": p["durationMs"].get("triggerExecution"),
                "watermark": p.get("eventTime", {}).get("watermark"),
                "max_event_time": p.get("eventTime", {}).get("max"),
            })
    with path.open("a") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")
    return entries


def start(spark: SparkSession, topic: str, root: Path, db: Path, checkpoints: Path, continuous: bool):
    (root / topic).mkdir(parents=True, exist_ok=True)        # the topic may not have received a file yet
    events = (spark.readStream.schema(SCHEMAS[topic])
              .option("timestampFormat", TS_FORMAT)
              .option("maxFilesPerTrigger", int(os.getenv("SHOPSENSE_STREAM_MAX_FILES", "5")))  # ~5 min per batch
              .json(str(root / topic)))
    deduped = events.withWatermark("event_ts", WATERMARK).dropDuplicatesWithinWatermark(["event_id"])
    writer = (deduped.writeStream.queryName(topic).outputMode("append")
              .foreachBatch(make_sink(topic, db))
              .option("checkpointLocation", str(checkpoints / topic)))
    writer = writer.trigger(processingTime="5 seconds") if continuous else writer.trigger(availableNow=True)
    return writer.start()


def run(day: str, continuous: bool = False, reset: bool = False, spark: SparkSession | None = None,
        root: Path | None = None, db: Path | None = None, warehouse: Path | None = None) -> dict:
    root, db = root or stream_root(), db or live_db_path()
    warehouse = warehouse or get_paths().warehouse_db
    checkpoints = root / "_checkpoints"
    if reset:
        shutil.rmtree(checkpoints, ignore_errors=True)
        (root / "_progress").exists() and shutil.rmtree(root / "_progress")
        db.unlink(missing_ok=True)
    if not db.exists():
        init_live_db(day, db, warehouse)

    spark = spark or get_spark("shopsense-stream")
    spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "100000")   # keep every batch's progress
    queries = [start(spark, t, root, db, checkpoints, continuous) for t in ("clicks", "orders")]
    try:
        for q in queries:
            q.awaitTermination()
    finally:
        progress = log_progress(queries, root / "_progress" / "progress.jsonl")
    con = duckdb.connect(str(db), read_only=True)
    counts = {t: con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0] for t, tbl in TABLES.items()}
    con.close()
    return {"events": counts, "batches": len(progress),
            "dropped_by_watermark": sum(p["rows_dropped_by_watermark"] for p in progress),
            "input_rows": sum(p["input_rows"] for p in progress)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream live clicks and orders into the live warehouse.")
    parser.add_argument("--day", default="2026-06-29")
    parser.add_argument("--continuous", action="store_true", help="keep running with 5-second micro-batches")
    parser.add_argument("--reset", action="store_true", help="drop checkpoints and the live database first")
    args = parser.parse_args()
    result = run(args.day, args.continuous, args.reset)
    for topic, n in result["events"].items():
        print(f"{TABLES[topic]:<20}{n:>8,} events")
    print(f"{result['batches']} micro-batches, {result['input_rows']:,} deliveries read, "
          f"{result['dropped_by_watermark']:,} dropped as too late")
    print(f"\nlive warehouse -> {live_db_path()}")


if __name__ == "__main__":
    main()
