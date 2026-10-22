import datetime as dt

from shopsense.transform.curate import OPEN_END, OPEN_START, hash_key, lookup_as_of, scd2_versions

D = dt.date


def _snapshots(spark):
    rows = [  # product P1: price changes once, repeated identical snapshots in between
        ("P1", 10.0, D(2026, 1, 1)), ("P1", 10.0, D(2026, 1, 5)), ("P1", 12.0, D(2026, 1, 12)),
        ("P1", 12.0, D(2026, 1, 19)), ("P2", 5.0, D(2026, 1, 1)),
    ]
    return spark.createDataFrame(rows, "product_id string, list_price double, snapshot_date date")


def test_identical_snapshots_do_not_create_versions(spark):
    v = scd2_versions(_snapshots(spark), "product_id", "snapshot_date", ["list_price"]).orderBy("product_id", "version")
    rows = [(r.product_id, r.version, r.list_price, r.effective_from, r.effective_to, r.is_current) for r in v.collect()]
    assert rows == [
        ("P1", 1, 10.0, D.fromisoformat(OPEN_START), D(2026, 1, 11), False),
        ("P1", 2, 12.0, D(2026, 1, 12), D.fromisoformat(OPEN_END), True),
        ("P2", 1, 5.0, D.fromisoformat(OPEN_START), D.fromisoformat(OPEN_END), True),
    ]


def test_point_in_time_lookup_picks_the_valid_version(spark):
    v = scd2_versions(_snapshots(spark), "product_id", "snapshot_date", ["list_price"]) \
        .withColumn("product_key", hash_key("product_id", "effective_from"))
    facts = spark.createDataFrame([("P1", D(2026, 1, 11)), ("P1", D(2026, 1, 12)), ("P9", D(2026, 1, 12))],
                                  "product_id string, order_date date")
    out = lookup_as_of(facts, v, "product_id", "product_key", "order_date", extra=["list_price"]).collect()
    by_date = {(r.product_id, r.order_date): r for r in out}
    assert by_date[("P1", D(2026, 1, 11))].list_price == 10.0
    assert by_date[("P1", D(2026, 1, 12))].list_price == 12.0
    assert by_date[("P9", D(2026, 1, 12))].product_key == -1          # unknown member


def test_surrogate_keys_are_stable_when_a_version_is_added(spark):
    before = scd2_versions(_snapshots(spark).filter("snapshot_date < '2026-01-12'"),
                           "product_id", "snapshot_date", ["list_price"]) \
        .select("product_id", "effective_from", hash_key("product_id", "effective_from").alias("k"))
    after = scd2_versions(_snapshots(spark), "product_id", "snapshot_date", ["list_price"]) \
        .select("product_id", "effective_from", hash_key("product_id", "effective_from").alias("k"))
    assert set(before.collect()) <= set(after.collect())               # old keys unchanged, one new key added
