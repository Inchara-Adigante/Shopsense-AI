import json

import duckdb
import pytest

from shopsense.quality import checks
from shopsense.warehouse import load as wh_load


def test_lake_checks_catch_duplicates_nulls_and_bad_values(spark, tmp_path):
    df = spark.createDataFrame([("O1", "placed"), ("O1", "placed"), (None, "teleported")],
                               "order_id string, order_status string")
    results = checks.lake_checks("orders", df, ["order_id"], tmp_path / "none", "t")
    failed = {r.check_name for r in results if not r.passed}
    assert {"unique_key", "not_null:order_id", "accepted_values:order_status"} <= failed
    with pytest.raises(checks.DataQualityError):
        checks.raise_on_errors(results)


def test_quarantine_rate_thresholds():
    res = checks.quarantine_rate_checks({"a": {"raw_rows": 100, "rejected_rows": 1},
                                         "b": {"raw_rows": 100, "rejected_rows": 5},
                                         "c": {"raw_rows": 100, "rejected_rows": 20}}, "t")
    by = {r.table_name: r for r in res}
    assert by["a"].passed
    assert not by["b"].passed and by["b"].severity == checks.WARN
    assert not by["c"].passed and by["c"].severity == checks.ERROR


def test_warehouse_checks_pass_on_pipeline_output(pipeline):
    con = duckdb.connect(str(pipeline["db"]), read_only=True)
    results = checks.warehouse_checks(con, "t")
    con.close()
    errors = [r for r in results if not r.passed and r.severity == checks.ERROR]
    assert not errors, errors


def test_load_rolls_back_on_quality_error(pipeline, tmp_path, monkeypatch):
    """A failing error-level check must leave the warehouse untouched and keep pending changes."""
    db = tmp_path / "wh.duckdb"
    db.write_bytes(pipeline["db"].read_bytes())
    pending = wh_load.pending_path(pipeline["lake"])
    pending.write_text(json.dumps({"orders": ["2026-02-01"], "_changed_tables": ["orders"]}))
    before = duckdb.connect(str(db), read_only=True).execute("SELECT COUNT(*) FROM ops.load_log").fetchone()[0]

    failing = ("always_fails", "fact_order_item", checks.ERROR, "SELECT 1", lambda v: False, "never")
    monkeypatch.setattr(checks, "WAREHOUSE_CHECKS", checks.WAREHOUSE_CHECKS + [failing])
    with pytest.raises(checks.DataQualityError):
        wh_load.load(lake=pipeline["lake"], db_path=db)

    after = duckdb.connect(str(db), read_only=True).execute("SELECT COUNT(*) FROM ops.load_log").fetchone()[0]
    assert after == before                       # the whole load rolled back
    assert pending.exists()                      # changes kept for the retry
    pending.unlink()
