import pytest

from shopsense.ingest.land_raw import ChecksumMismatch, ingest


def _landing(tmp_path):
    batch = tmp_path / "landing" / "orders" / "2026-01-01"
    batch.mkdir(parents=True)
    (batch / "orders.csv").write_text("order_id\nO1\n")
    return tmp_path / "landing", tmp_path / "lake"


def test_ingest_copies_into_partitioned_raw(tmp_path):
    landing, lake = _landing(tmp_path)
    [record] = ingest(landing=landing, lake=lake)
    assert record["status"] == "ingested"
    target = lake / "raw" / "orders" / "ingest_date=2026-01-01"
    assert (target / "orders.csv").read_text() == "order_id\nO1\n"
    assert (target / "_manifest.json").exists()
    assert (lake / "_audit" / "ingest_log.jsonl").read_text().count("\n") == 1


def test_reingesting_same_batch_is_skipped(tmp_path):
    landing, lake = _landing(tmp_path)
    ingest(landing=landing, lake=lake)
    [record] = ingest(landing=landing, lake=lake)
    assert record["status"] == "skipped_duplicate"


def test_changed_redelivery_is_refused_unless_forced(tmp_path):
    landing, lake = _landing(tmp_path)
    ingest(landing=landing, lake=lake)
    (landing / "orders" / "2026-01-01" / "orders.csv").write_text("order_id\nO1\nO2\n")
    with pytest.raises(ChecksumMismatch):
        ingest(landing=landing, lake=lake)
    [record] = ingest(landing=landing, lake=lake, force=True)
    assert record["status"] == "ingested"
    raw = lake / "raw" / "orders" / "ingest_date=2026-01-01" / "orders.csv"
    assert raw.read_text().endswith("O2\n")
