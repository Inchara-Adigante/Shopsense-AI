"""
Simulated live event stream (stands in for Kinesis Data Streams / Kafka).

Replays one day of orders and clicks from the clean zone as a stream of small
JSON-lines files - one file per simulated minute of *arrival* time - into
stream/<topic>/. Delivery is deliberately messy, the way real producers behave:

    * most events arrive a few seconds after they happen (exponential delay)
    * 2%   arrive 2-8 minutes late      -> out of order, but inside the watermark
    * 0.5% arrive 20-60 minutes late    -> beyond the 10-minute watermark: must be dropped
    * 3%   are delivered twice          -> at-least-once retries: must be deduplicated

Files are written to a temporary name and renamed, so the consumer never sees
a half-written file, and each file's modification time is set to its arrival
minute so the consumer reads them in arrival order. A truth manifest (stream/_truth/<topic>.json) records which
events were sent too late, so tests can check the stream's output exactly.

Usage:
    python -m shopsense.streaming.producer --day 2026-06-29            # write the whole day at once
    python -m shopsense.streaming.producer --day 2026-06-29 --speed 360   # live: 1 simulated hour per 10 s
"""
import argparse
import json
import os
import shutil
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from shopsense.config import ROOT, get_paths

STREAM_ROOT = ROOT / "stream"
SEED = 11
LATE_IN_WATERMARK = (0.02, 120, 480)        # share, min delay s, max delay s
TOO_LATE = (0.005, 1200, 3600)
DUPLICATE_SHARE = 0.03


def stream_root() -> Path:
    import os
    return Path(os.getenv("SHOPSENSE_STREAM", STREAM_ROOT))


def load_day(day: str, lake: Path) -> dict[str, pd.DataFrame]:
    """The day's clean, deduplicated events - the 'truth' that the stream replays."""
    con = duckdb.connect()
    clicks = con.execute(f"""
        SELECT event_id, session_id, customer_id, event_ts, event_type, product_id, in_stock, device
        FROM read_parquet('{lake}/clean/clickstream/event_date={day}/*.parquet')""").df()
    orders = con.execute(f"""
        SELECT i.order_id || ':' || i.line_number AS event_id, i.order_id, i.line_number, i.product_id,
               o.fc_id, i.quantity, i.unit_price * i.quantity - i.discount_amount AS net_amount, o.order_ts AS event_ts
        FROM read_parquet('{lake}/clean/order_items/order_date={day}/*.parquet') i
        JOIN read_parquet('{lake}/clean/orders/order_date={day}/*.parquet') o USING (order_id)""").df()
    con.close()
    return {"clicks": clicks, "orders": orders}


def schedule(events: pd.DataFrame, rng: np.random.Generator) -> tuple[pd.DataFrame, list[str]]:
    """Assign each event an arrival time; add late arrivals and duplicate deliveries."""
    n = len(events)
    delay = rng.exponential(8.0, n)                                  # seconds
    u = rng.random(n)
    late = u < LATE_IN_WATERMARK[0]
    too_late = (u >= LATE_IN_WATERMARK[0]) & (u < LATE_IN_WATERMARK[0] + TOO_LATE[0])
    delay[late] = rng.uniform(LATE_IN_WATERMARK[1], LATE_IN_WATERMARK[2], late.sum())
    delay[too_late] = rng.uniform(TOO_LATE[1], TOO_LATE[2], too_late.sum())
    out = events.copy()
    out["arrival_ts"] = out["event_ts"] + pd.to_timedelta(delay, unit="s")

    dup = out[rng.random(n) < DUPLICATE_SHARE].copy()                # retry 5-60 s after the first delivery
    dup["arrival_ts"] = dup["arrival_ts"] + pd.to_timedelta(rng.uniform(5, 60, len(dup)), unit="s")
    sent = pd.concat([out, dup], ignore_index=True).sort_values("arrival_ts")
    return sent, events.loc[too_late, "event_id"].tolist()


def write_files(topics: dict[str, pd.DataFrame], root: Path, speed: float | None,
                file_minutes: int = 1) -> dict[str, int]:
    """
    One JSON-lines file per topic per minute of arrival time, written atomically (tmp file + rename).
    All topics are interleaved by arrival time, so in live mode they stream side by side.
    """
    batches = []
    for topic, sent in topics.items():
        (root / topic).mkdir(parents=True, exist_ok=True)
        bucket = sent["arrival_ts"].dt.floor(f"{file_minutes}min")
        for minute, batch in sent.assign(minute=bucket).groupby("minute"):
            batches.append((minute, topic, batch.drop(columns="minute")))
    batches.sort(key=lambda b: (b[0], b[1]))
    n_files = {t: 0 for t in topics}
    start_wall, start_sim = time.time(), batches[0][0] if batches else None
    for minute, topic, batch in batches:
        if speed:                                                    # live mode: wait for the simulated clock
            due = start_wall + (minute - start_sim).total_seconds() / speed
            time.sleep(max(0.0, due - time.time()))
        body = batch.assign(event_ts=batch["event_ts"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                            arrival_ts=batch["arrival_ts"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
        name = f"part-{minute:%Y%m%d%H%M}"
        tmp = root / topic / f".{name}.tmp"                          # Spark ignores dot-files
        body.to_json(tmp, orient="records", lines=True)
        final = root / topic / f"{name}.json"
        tmp.rename(final)
        # Spark's file source orders new files by modification time (ms precision). Stamp each file with
        # its arrival minute so files are strictly ordered even when written in a burst - the ordering
        # guarantee Kafka/Kinesis give through offsets.
        epoch = minute.tz_localize("UTC").timestamp() if minute.tzinfo is None else minute.timestamp()
        os.utime(final, (epoch, epoch))
        n_files[topic] += 1
    return n_files


def produce(day: str, lake: Path | None = None, root: Path | None = None, speed: float | None = None,
            seed: int = SEED, file_minutes: int = 1) -> dict:
    lake, root = lake or get_paths().lake, root or stream_root()
    for sub in ("clicks", "orders", "_truth"):
        shutil.rmtree(root / sub, ignore_errors=True)
    rng = np.random.default_rng(seed)
    data = load_day(day, lake)
    summary, sent_by_topic, truths = {"day": day}, {}, {}
    for topic, events in data.items():
        sent, too_late = schedule(events, rng)
        sent_by_topic[topic] = sent
        truths[topic] = {"unique_events": len(events), "sent": len(sent), "duplicates_sent": len(sent) - len(events),
                         "too_late_event_ids": too_late}
    # Truth first: a test (or the dashboard) can compare against it while a live replay is still running.
    (root / "_truth").mkdir(parents=True, exist_ok=True)
    for topic, truth in truths.items():
        (root / "_truth" / f"{topic}.json").write_text(json.dumps(truth))
    files = write_files(sent_by_topic, root, speed, file_minutes)
    for topic, truth in truths.items():
        summary[topic] = {k: v for k, v in truth.items() if k != "too_late_event_ids"} | {
            "too_late": len(truth["too_late_event_ids"]), "files": files[topic]}
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay a day of events as a messy live stream.")
    parser.add_argument("--day", default="2026-06-29")
    parser.add_argument("--speed", type=float, help="simulated seconds per real second (omit: write instantly)")
    args = parser.parse_args()
    summary = produce(args.day, speed=args.speed)
    for topic in ("clicks", "orders"):
        s = summary[topic]
        print(f"{topic:<7} {s['unique_events']:>7,} events -> {s['sent']:>7,} deliveries "
              f"({s['duplicates_sent']:,} duplicates, {s['too_late']:,} too late) in {s['files']:,} files")
    print(f"\nstream -> {stream_root()}")


if __name__ == "__main__":
    main()
