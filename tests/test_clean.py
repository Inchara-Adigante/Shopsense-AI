import datetime as dt

from pyspark.sql import functions as F

from shopsense.transform.clean import (CATEGORIES, canonical, clean_clickstream, clean_order_items, dedupe,
                                       parse_ts)


def test_parse_ts_accepts_iso_and_legacy_formats(spark):
    df = spark.createDataFrame([("2026-01-05T13:45:10Z",), ("01/05/2026 13:45:10",), ("garbage",)], ["ts"])
    out = [r.t for r in df.select(parse_ts(F.col("ts")).alias("t")).collect()]
    assert out[0] == out[1] == dt.datetime(2026, 1, 5, 13, 45, 10)
    assert out[2] is None


def test_canonical_fixes_casing_and_whitespace(spark):
    df = spark.createDataFrame([("electronics",), (" Home & Kitchen ",), ("BEAUTY",), ("Gadgets",)], ["c"])
    out = [r.c for r in df.select(canonical(F.col("c"), CATEGORIES).alias("c")).collect()]
    assert out == ["Electronics", "Home & Kitchen", "Beauty", None]


def test_dedupe_keeps_latest_delivery(spark):
    df = spark.createDataFrame(
        [("O1", "old", dt.date(2026, 1, 1), "a"), ("O1", "new", dt.date(2026, 1, 2), "b")],
        ["order_id", "status", "ingest_date", "_source_file"])
    [row] = dedupe(df, ["order_id"]).collect()
    assert row.status == "new"


def _events(spark, rows):
    schema = ("event_id string, session_id string, customer_id string, event_ts string, event_type string, "
              "product_id string, order_id string, in_stock boolean, device string, ingest_date date, "
              "_source_file string")
    return spark.createDataFrame(rows, schema)


def test_late_and_duplicate_events(spark):
    d1, d2 = dt.date(2026, 1, 1), dt.date(2026, 1, 2)
    raw = _events(spark, [
        ("E1", "S1", "C1", "2026-01-01T23:59:00Z", "PAGE_VIEW", "P1", None, True, "mobile", d1, "f1"),
        ("E1", "S1", "C1", "2026-01-01T23:59:00Z", "page_view", "P1", None, True, "mobile", d2, "f2"),  # retry
        ("E2", "S1", "C1", "2026-01-01T23:59:30Z", "add_to_cart", "P1", None, True, "mobile", d2, "f2"),  # late
        ("E3", "S2", None, "2026-01-02T10:00:00Z", "page_view", None, None, True, "web", d2, "f2"),      # invalid
    ])
    result = clean_clickstream(raw)
    rows = {r.event_id: r for r in result.clean.collect()}
    assert set(rows) == {"E1", "E2"}
    assert rows["E1"].ingest_date == d1                     # first delivery of a retried event wins
    assert rows["E2"].event_date == d1 and rows["E2"].arrival_lag_days == 1
    assert rows["E1"].event_type == "page_view"
    [bad] = result.rejected.collect()
    assert bad.event_id == "E3" and "missing product_id" in bad._reject_reason


def test_order_items_rules(spark):
    orders = spark.createDataFrame([("O1", dt.date(2026, 1, 1))], ["order_id", "order_date"])
    d = dt.date(2026, 1, 1)
    raw = spark.createDataFrame([
        ("O1", "1", "P1", "2", "10.00", "0.00", d, "f"),
        ("O1", "2", "P2", "0", "10.00", "0.00", d, "f"),     # zero quantity
        ("O9", "1", "P1", "1", "10.00", "0.00", d, "f"),     # unknown order
    ], ["order_id", "line_number", "product_id", "quantity", "unit_price", "discount_amount", "ingest_date",
        "_source_file"])
    result = clean_order_items(raw, orders)
    assert [(r.order_id, r.line_number) for r in result.clean.collect()] == [("O1", 1)]
    reasons = {r.order_id + str(r.line_number): r._reject_reason for r in result.rejected.collect()}
    assert "quantity must be > 0" in reasons["O12"]
    assert "unknown order_id" in reasons["O91"]
