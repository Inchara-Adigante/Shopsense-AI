"""
Backtest the stockout-risk logic against what actually happened.

For every Monday in the history, evaluate mart.inventory_risk_as_of(that date)
and compare its "High" flag (in stock now, under 7 days of cover) with whether
that product x FC really ran out of stock in the following 7 days.

Two demand estimates are compared:
    adjusted - units sold / days the FC had stock (what the mart uses)
    naive    - units sold / 28 days (ignores stockouts, so under-estimates
               demand for products that were recently out of stock)

Usage:
    python -m shopsense.warehouse.backtest
"""
import duckdb
import pandas as pd

from shopsense.config import get_paths

HORIZON_DAYS = 7

QUERY = """
WITH risk AS (
    SELECT * FROM mart.inventory_risk_as_of(CAST($as_of AS DATE)) WHERE on_hand_units > 0
),
actual AS (
    SELECT product_key, fc_key, BOOL_OR(is_stockout) AS stocked_out
    FROM mart.product_fc_daily
    WHERE date > CAST($as_of AS DATE) AND date <= CAST($as_of AS DATE) + INTERVAL {h} DAY
    GROUP BY ALL
)
SELECT r.days_of_cover < {h}        AS flag_adjusted,
       r.naive_days_of_cover < {h}  AS flag_naive,
       a.stocked_out
FROM risk r JOIN actual a USING (product_key, fc_key)
""".format(h=HORIZON_DAYS)


def scores(flag: pd.Series, actual: pd.Series) -> dict:
    tp = int((flag & actual).sum())
    fp = int((flag & ~actual).sum())
    fn = int((~flag & actual).sum())
    return {"flagged": tp + fp, "caught": tp, "missed": fn,
            "precision": tp / max(tp + fp, 1), "recall": tp / max(tp + fn, 1)}


def run(db_path=None) -> pd.DataFrame:
    con = duckdb.connect(str(db_path or get_paths().warehouse_db), read_only=True)
    lo, hi = con.execute("SELECT MIN(date), MAX(date) FROM mart.product_fc_daily").fetchone()
    # Need 28 days of history before each date and HORIZON_DAYS after it.
    dates = pd.date_range(pd.Timestamp(lo) + pd.Timedelta(days=28),
                          pd.Timestamp(hi) - pd.Timedelta(days=HORIZON_DAYS), freq="W-MON")
    frames = [con.execute(QUERY, {"as_of": d.date()}).df().assign(as_of=d) for d in dates]
    con.close()
    if not frames:                                     # history shorter than 28 + HORIZON_DAYS days
        empty = pd.DataFrame([{"method": m, "flagged": 0, "caught": 0, "missed": 0, "precision": None,
                               "recall": None} for m in ("adjusted", "naive")]).set_index("method")
        empty.attrs.update(weeks=0, observations=0, stockouts=0,
                           note=f"needs at least {28 + HORIZON_DAYS} days of history")
        return empty
    df = pd.concat(frames, ignore_index=True)
    rows = [{"method": m, **scores(df[f"flag_{m}"].astype(bool), df["stocked_out"].astype(bool))}
            for m in ("adjusted", "naive")]
    result = pd.DataFrame(rows).set_index("method")
    result.attrs.update(weeks=len(dates), observations=len(df), stockouts=int(df["stocked_out"].sum()))
    return result


def main() -> None:
    result = run()
    a = result.attrs
    print(f"{a['weeks']} weekly as-of dates, {a['observations']:,} product x FC observations, "
          f"{a['stockouts']:,} real stockouts within {HORIZON_DAYS} days\n")
    with pd.option_context("display.float_format", "{:.1%}".format):
        print(result)


if __name__ == "__main__":
    main()
