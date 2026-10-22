"""Incremental processing must give exactly the same warehouse as a full rebuild."""
import duckdb
import pytest

from shopsense.ingest.land_raw import ingest_batch
from shopsense.transform import clean, curate
from shopsense.warehouse.load import TABLES, load


@pytest.fixture(scope="module")
def incremental(pipeline, spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("incremental")
    lake, db = root / "lake", root / "wh.duckdb"
    landing = pipeline["landing"]
    days = sorted({b.name for src in landing.iterdir() for b in src.iterdir()})
    cut = days[-4]

    def ingest(pred):
        for src in landing.iterdir():
            for b in sorted(src.iterdir()):
                if pred(b.name):
                    ingest_batch(src.name, b, lake / "raw")

    ingest(lambda d: d <= cut)
    clean.run(spark, lake)
    curate.run(spark, lake)
    load(lake, db)
    reports = []
    for day in days[-3:]:                            # then one day at a time, incrementally
        ingest(lambda d, day=day: d == day)
        reports.append(clean.run(spark, lake))
        curate.run(spark, lake)
        reports.append(load(lake, db))
    return {"lake": lake, "db": db, "reports": reports}


def test_incremental_equals_full_rebuild(pipeline, incremental):
    con = duckdb.connect(str(incremental["db"]), read_only=True)
    con.execute(f"ATTACH '{pipeline['db']}' AS full_wh (READ_ONLY)")
    for t in TABLES:
        diff = con.execute(f"SELECT (SELECT COUNT(*) FROM (SELECT * FROM dw.{t} EXCEPT ALL SELECT * FROM full_wh.dw.{t})) "
                           f"+ (SELECT COUNT(*) FROM (SELECT * FROM full_wh.dw.{t} EXCEPT ALL SELECT * FROM dw.{t}))").fetchone()[0]
        assert diff == 0, f"{t}: {diff} rows differ"


def test_incremental_loads_replace_only_changed_partitions(incremental):
    loads = [r for r in incremental["reports"] if isinstance(r, dict) and r.get("mode") == "incremental"]
    assert loads
    for r in loads:
        inv = r["tables"]["fact_inventory_daily"]
        if inv["mode"] == "partition_replace":
            assert inv["partitions"] <= 2


def test_rerun_processes_nothing_and_backfill_is_idempotent(incremental, spark):
    lake = incremental["lake"]
    assert clean.run(spark, lake).tables == {}
    before = {t: spark.read.parquet(str(lake / "clean" / t)).count() for t in ("orders", "clickstream")}
    last_two = sorted(clean.raw_dates(lake / "raw", "orders"))[-2:]
    report = clean.run(spark, lake, dates=last_two)
    assert report.tables["orders"]["ingest_dates"] == 2
    after = {t: spark.read.parquet(str(lake / "clean" / t)).count() for t in ("orders", "clickstream")}
    assert after == before
