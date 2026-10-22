"""
Streaming correctness: the live tables must contain exactly the day's events,
once each, minus events sent beyond the watermark - also across a restart and
when files are re-sent.
"""
import json
import shutil

import duckdb
import pytest

from shopsense.streaming import producer, stream_job


def _day(pipeline):
    return sorted(p.name.split("=")[1] for p in (pipeline["lake"] / "clean" / "clickstream").glob("event_date=*"))[-2]


def _truth(pipeline, root, day):
    events = {e for (e,) in duckdb.sql(
        f"SELECT event_id FROM read_parquet('{pipeline['lake']}/clean/clickstream/event_date={day}/*.parquet')").fetchall()}
    late = set(json.loads((root / "_truth" / "clicks.json").read_text())["too_late_event_ids"])
    return events, late


def _stored(db, table="click_events"):
    con = duckdb.connect(str(db), read_only=True)
    ids = [e for (e,) in con.execute(f"SELECT event_id FROM live.{table}").fetchall()]
    con.close()
    return ids


@pytest.fixture(scope="module")
def stream(pipeline, spark, tmp_path_factory, monkeypatch_module):
    monkeypatch_module.setenv("SHOPSENSE_STREAM_MAX_FILES", "2")
    root = tmp_path_factory.mktemp("stream")
    day = _day(pipeline)
    summary = producer.produce(day, lake=pipeline["lake"], root=root, file_minutes=5)
    db = root / "rt.duckdb"
    result = stream_job.run(day, spark=spark, root=root, db=db, warehouse=pipeline["db"])
    return {"root": root, "day": day, "db": db, "summary": summary, "result": result}


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


def test_stream_has_every_on_time_event_exactly_once(pipeline, stream):
    events, late = _truth(pipeline, stream["root"], stream["day"])
    stored = _stored(stream["db"])
    assert len(stored) == len(set(stored))                       # no duplicates (primary key)
    assert (events - late) <= set(stored)                        # nothing inside the watermark is lost
    assert set(stored) <= events                                 # nothing that was never sent
    assert stream["summary"]["clicks"]["duplicates_sent"] > 0    # retries really were sent


def test_too_late_events_are_mostly_dropped(pipeline, stream):
    _, late = _truth(pipeline, stream["root"], stream["day"])
    admitted = late & set(_stored(stream["db"]))
    assert len(late) > 0
    assert len(admitted) <= 0.2 * len(late)                      # "may be dropped": borderline ones can slip in
    assert stream["result"]["dropped_by_watermark"] >= len(late) - len(admitted)


def test_restart_and_resent_files_are_exactly_once(pipeline, spark, stream, tmp_path):
    src, root, db = stream["root"], tmp_path / "s", tmp_path / "restart.duckdb"
    files = {t: sorted((src / t).glob("part-*.json")) for t in ("clicks", "orders")}
    for t, fs in files.items():
        (root / t).mkdir(parents=True)
        for f in fs[: len(fs) // 2]:
            shutil.copy2(f, root / t / f.name)                   # keep arrival-time mtimes
    stream_job.run(stream["day"], spark=spark, root=root, db=db, warehouse=pipeline["db"])
    for t, fs in files.items():                                  # the rest arrives; resume from the checkpoint
        for f in fs[len(fs) // 2:]:
            shutil.copy2(f, root / t / f.name)
    stream_job.run(stream["day"], spark=spark, root=root, db=db, warehouse=pipeline["db"])
    for f in files["clicks"][-2:]:                               # a producer re-sends files
        shutil.copy2(f, root / "clicks" / f"resent-{f.name}")
    stream_job.run(stream["day"], spark=spark, root=root, db=db, warehouse=pipeline["db"])

    events, late = _truth(pipeline, src, stream["day"])
    stored = _stored(db)
    assert len(stored) == len(set(stored))
    assert (events - late) <= set(stored) <= events
    lines = {e for (e,) in duckdb.sql(
        f"SELECT order_id || ':' || line_number FROM read_parquet("
        f"'{pipeline['lake']}/clean/order_items/order_date={stream['day']}/*.parquet')").fetchall()}
    lines_late = set(json.loads((src / "_truth" / "orders.json").read_text())["too_late_event_ids"])
    stored_lines = _stored(db, "order_lines")
    assert len(stored_lines) == len(set(stored_lines))
    assert (lines - lines_late) <= set(stored_lines) <= lines


def test_live_views_are_consistent(stream):
    con = duckdb.connect(str(stream["db"]), read_only=True)
    [(views_5m,)] = con.execute("SELECT SUM(page_views) FROM live.product_activity_5m").fetchall()
    [(views,)] = con.execute("SELECT COUNT(*) FROM live.click_events WHERE event_type = 'page_view' "
                             "AND product_id IS NOT NULL").fetchall()
    assert views_5m == views
    alerts = con.execute("SELECT live_alert, on_hand_now, units_sold_today, start_on_hand FROM live.stock_now").df()
    con.close()
    assert set(alerts["live_alert"]) <= {"OK", "Sold out today", "Under 12h of cover", "Out of stock since open",
                                         "Restock arrived"}
    assert (alerts["on_hand_now"] >= 0).all()
    sold_out = alerts[alerts["live_alert"] == "Sold out today"]
    assert (sold_out["start_on_hand"] > 0).all() and (sold_out["on_hand_now"] == 0).all()
