"""End-to-end checks on a 45-day run of the real pipeline."""
import duckdb
import pytest

from shopsense.warehouse import backtest


@pytest.fixture(scope="module")
def con(pipeline):
    c = duckdb.connect(str(pipeline["db"]), read_only=True)
    yield c
    c.close()


def q(con, sql):
    return con.execute(sql).fetchall()


def test_clean_row_counts_reconcile(pipeline):
    for table, s in pipeline["clean"].items():
        assert s["raw_rows"] == s["valid_rows"] + s["rejected_rows"] + s["duplicates_in_batch"], table
    assert pipeline["clean"]["orders"]["duplicates_in_batch"] > 0
    assert pipeline["clean"]["order_items"]["rejected_rows"] > 0


def test_every_clean_order_line_reaches_the_fact(pipeline):
    assert pipeline["curated"]["fact_order_item"] == pipeline["clean"]["order_items"]["table_rows"]


def test_revenue_reconciles_between_lake_and_warehouse(pipeline, con):
    lake_total = duckdb.sql(
        f"SELECT SUM(unit_price * quantity - discount_amount) FROM "
        f"read_parquet('{pipeline['lake']}/clean/order_items/*/*.parquet')").fetchall()[0][0]
    [(wh_total,)] = q(con, "SELECT SUM(net_amount) FROM dw.fact_order_item")
    assert wh_total == lake_total


def test_no_unknown_keys_and_unique_grain(con):
    assert q(con, "SELECT COUNT(*) FROM dw.fact_order_item WHERE product_key = -1 OR customer_key = -1") == [(0,)]
    assert q(con, "SELECT COUNT(*) - COUNT(DISTINCT (order_id, line_number)) FROM dw.fact_order_item") == [(0,)]


def test_converted_sessions_match_orders(pipeline, con):
    # Every clean order has exactly one purchase session. (Compare with clean orders, not
    # fact_order_item: an order whose only line was quarantined has no fact rows.)
    [(converted,)] = q(con, "SELECT SUM(converted::INT) FROM dw.fact_session")
    assert converted == pipeline["clean"]["orders"]["table_rows"]


def test_inventory_risk_is_consistent(con):
    rows = con.execute("SELECT risk_tier, on_hand_units, days_of_cover, recommended_order_units "
                       "FROM mart.inventory_risk").df()
    assert len(rows) > 0
    assert set(rows["risk_tier"]) <= {"Out of stock", "High", "Medium", "Low"}
    assert (rows["recommended_order_units"] >= 0).all()
    assert (rows.loc[rows["risk_tier"] == "Out of stock", "on_hand_units"] == 0).all()
    assert (rows.loc[rows["risk_tier"] == "High", "days_of_cover"] < 7).all()


def test_restock_view_only_lists_urgent_items(con):
    tiers = {t for (t,) in q(con, "SELECT DISTINCT risk_tier FROM mart.restock_this_week")}
    assert tiers <= {"Out of stock", "High"}


def test_backtest_runs(pipeline):
    result = backtest.run(pipeline["db"])
    assert set(result.index) == {"adjusted", "naive"}
    assert result.attrs["observations"] > 0
    assert result["precision"].between(0, 1).all()
