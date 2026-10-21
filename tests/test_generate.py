import glob

import pandas as pd

from shopsense.generate.retail_data import generate


def _read(landing, source, name):
    return pd.concat([pd.read_csv(p) for p in sorted(glob.glob(str(landing / source / "*" / name)))])


def test_generation_is_reproducible(tmp_path):
    a = generate(n_days=5, seed=3, landing=tmp_path / "a")
    b = generate(n_days=5, seed=3, landing=tmp_path / "b")
    assert a == b
    pd.testing.assert_frame_equal(_read(tmp_path / "a", "orders", "orders.csv"),
                                  _read(tmp_path / "b", "orders", "orders.csv"))


def test_simulated_inventory_never_negative(pipeline):
    inv = _read(pipeline["landing"], "inventory_snapshots", "inventory.csv")
    assert (inv["on_hand_units"] >= 0).all()
    assert (inv["on_hand_units"] == 0).any(), "simulation should produce some stockouts"


def test_order_items_reference_orders(pipeline):
    orders = _read(pipeline["landing"], "orders", "orders.csv")
    items = _read(pipeline["landing"], "order_items", "order_items.csv")
    assert set(items["order_id"]) <= set(orders["order_id"])


def test_dirty_data_is_injected(pipeline):
    orders = _read(pipeline["landing"], "orders", "orders.csv")
    assert orders["order_id"].duplicated().any()                    # resent rows
    assert orders["order_ts"].str.contains("/").any()               # legacy timestamp format
    assert (orders["order_status"] != "placed").any()               # messy casing
    items = _read(pipeline["landing"], "order_items", "order_items.csv")
    assert (items["quantity"] <= 0).any()                           # invalid quantities
