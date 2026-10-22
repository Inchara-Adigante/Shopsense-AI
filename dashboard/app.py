"""
ShopSense AI dashboard (Streamlit), reading the warehouse marts.

Run:
    streamlit run dashboard/app.py
"""
import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shopsense.config import get_paths  # noqa: E402

st.set_page_config(page_title="ShopSense AI", page_icon="🛒", layout="wide")


@st.cache_resource
def connection() -> duckdb.DuckDBPyConnection:
    db = get_paths().warehouse_db
    if not db.exists():
        st.error(f"Warehouse not found at {db}. Run `python -m shopsense.pipeline` first.")
        st.stop()
    return duckdb.connect(str(db), read_only=True)


@st.cache_data
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    return connection().execute(sql, list(params)).df()


as_of = query("SELECT MAX(date) AS d FROM mart.product_fc_daily")["d"].iloc[0]
st.title("🛒 ShopSense AI: Retail Intelligence")
st.caption(f"Data as of **{pd.Timestamp(as_of):%b %d, %Y}** · Which products will run out of stock, "
           "what is driving demand, and what should we do?")

# ---------------------------------------------------------------- KPIs
# Orders are counted from the fact table: summing mart.daily_sales.orders would double-count
# orders that contain items from more than one category.
kpi = query("""
    WITH f AS (SELECT d.date, f.order_id, f.net_amount FROM dw.fact_order_item f JOIN dw.dim_date d USING (date_key)
               WHERE f.order_status <> 'cancelled'),
         l AS (SELECT MAX(date) d FROM f)
    SELECT SUM(net_amount) FILTER (WHERE date >  l.d - INTERVAL 28 DAY) AS rev_28,
           SUM(net_amount) FILTER (WHERE date <= l.d - INTERVAL 28 DAY AND date > l.d - INTERVAL 56 DAY) AS rev_prev,
           COUNT(DISTINCT order_id) FILTER (WHERE date > l.d - INTERVAL 28 DAY) AS orders_28
    FROM f, l""").iloc[0]
conv = query("""
    SELECT SUM(converted_sessions) / SUM(sessions) AS conv FROM mart.daily_funnel
    WHERE date > (SELECT MAX(date) FROM mart.daily_funnel) - INTERVAL 28 DAY""")["conv"].iloc[0]
tiers = query("SELECT risk_tier, COUNT(*) n FROM mart.inventory_risk GROUP BY 1").set_index("risk_tier")["n"]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Net revenue (28d)", f"${kpi.rev_28:,.0f}", f"{(kpi.rev_28 / kpi.rev_prev - 1):+.1%} vs prior 28d")
c2.metric("Orders (28d)", f"{int(kpi.orders_28):,}")
c3.metric("Session conversion (28d)", f"{conv:.1%}")
c4.metric("Out of stock now", int(tiers.get("Out of stock", 0)), help="product x fulfillment center")
c5.metric("High stockout risk", int(tiers.get("High", 0)), help="under 7 days of cover")

tab_restock, tab_drivers, tab_sales = st.tabs(["📦 Restock this week", "📈 Demand drivers", "💰 Sales & funnel"])

# ---------------------------------------------------------------- restock
with tab_restock:
    st.subheader("Which products need restocking this week?")
    st.caption("Demand = units sold in the last 28 days ÷ days in stock (corrects for stockouts). "
               "Recommended order covers a 10-day lead time + 21 days of demand.")
    restock = query("SELECT * FROM mart.restock_this_week")
    fcs = st.multiselect("Fulfillment center", sorted(restock["fc_id"].unique()))
    cats = st.multiselect("Category", sorted(restock["category"].unique()))
    mask = pd.Series(True, index=restock.index)
    if fcs:
        mask &= restock["fc_id"].isin(fcs)
    if cats:
        mask &= restock["category"].isin(cats)
    view = restock[mask]
    st.dataframe(
        view[["risk_tier", "product_id", "product_name", "category", "fc_id", "on_hand_units", "inbound_units",
              "daily_demand", "days_of_cover", "projected_stockout_date", "recommended_action",
              "primary_driver", "revenue_at_risk_per_day"]],
        hide_index=True, width="stretch",
        column_config={"revenue_at_risk_per_day": st.column_config.NumberColumn("revenue at risk / day", format="$%.2f"),
                       "daily_demand": st.column_config.NumberColumn(format="%.2f"),
                       "projected_stockout_date": st.column_config.DateColumn("projected stockout", format="YYYY-MM-DD"),
                       "recommended_action": st.column_config.TextColumn(width="large")},
    )
    st.caption(f"{len(view)} product × FC combinations at risk · "
               f"${view['revenue_at_risk_per_day'].sum():,.0f} of daily revenue at risk")

# ---------------------------------------------------------------- drivers
with tab_drivers:
    st.subheader("What is driving demand?")
    drivers = query("SELECT * FROM mart.demand_drivers ORDER BY units_last_28d DESC")
    left, right = st.columns([1, 2])
    with left:
        st.dataframe(drivers.groupby("primary_driver").size().rename("products").reset_index(), hide_index=True)
        options = drivers["product_id"] + " · " + drivers["product_name"]
        pick = st.selectbox("Product", options)
    pid = pick.split(" · ")[0]
    row = drivers[drivers["product_id"] == pid].iloc[0]
    with right:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Primary driver", row.primary_driver)
        m2.metric("28-day trend", "n/a" if pd.isna(row.trend_pct) else f"{row.trend_pct:+.0f}%")
        m3.metric("Promotion lift", "n/a" if pd.isna(row.promo_lift) else f"×{row.promo_lift:.2f}")
        m4.metric("Out-of-stock views", "n/a" if pd.isna(row.oos_view_share) else f"{row.oos_view_share:.0%}")
        weekly = query("""
            SELECT date_trunc('week', pf.date) AS week, SUM(pf.units_sold) AS units_sold,
                   SUM(pf.is_stockout::INT) AS stockout_fc_days
            FROM mart.product_fc_daily pf JOIN dw.dim_product p USING (product_key)
            WHERE p.product_id = ? GROUP BY 1 ORDER BY 1""", (pid,))
        st.bar_chart(weekly, x="week", y="units_sold", height=260)
        st.caption("Stockout FC-days per week")
        st.line_chart(weekly, x="week", y="stockout_fc_days", height=160)

# ---------------------------------------------------------------- sales
with tab_sales:
    st.subheader("Revenue by category (weekly)")
    weekly_rev = query("""
        SELECT date_trunc('week', date) AS week, category, SUM(net_revenue) AS net_revenue
        FROM mart.daily_sales GROUP BY 1, 2 ORDER BY 1""")
    st.area_chart(weekly_rev.pivot(index="week", columns="category", values="net_revenue"), height=320)
    st.subheader("Conversion funnel (last 28 days)")
    funnel = query("""
        SELECT SUM(sessions) sessions, SUM(sessions_with_cart) with_cart, SUM(converted_sessions) purchased,
               SUM(sessions_hit_out_of_stock) hit_out_of_stock
        FROM mart.daily_funnel WHERE date > (SELECT MAX(date) FROM mart.daily_funnel) - INTERVAL 28 DAY""").iloc[0]
    f1, f2, f3, f4 = st.columns(4)
    f1.metric("Sessions", f"{int(funnel.sessions):,}")
    f2.metric("Added to cart", f"{int(funnel.with_cart):,}", f"{funnel.with_cart / funnel.sessions:.1%} of sessions")
    f3.metric("Purchased", f"{int(funnel.purchased):,}", f"{funnel.purchased / funnel.sessions:.1%} of sessions")
    f4.metric("Saw an out-of-stock page", f"{int(funnel.hit_out_of_stock):,}",
              f"{funnel.hit_out_of_stock / funnel.sessions:.1%} of sessions", delta_color="inverse")
