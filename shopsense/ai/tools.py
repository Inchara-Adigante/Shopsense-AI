"""
Read-only tools over the warehouse marts - the only way the assistant can see data.

Every tool returns rows, and every row carries a `ref` (for example
`restock:P00381@FC-LAX`). Answers must cite refs, and every number in an
answer must come from a cited row (checked by shopsense.ai.grounding).

Tools
    find_products(query)                         product ids matching a name / brand / category
    get_restock_list(fc_id, category, limit)     what needs restocking this week, most urgent first
    get_product_status(product_id)               stock risk per FC + demand drivers
    get_review_summary(product_id)               what customers say (from ai.review_summaries)
    get_sales_summary(days, category)            revenue / orders / units vs the previous period
"""
import json
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb

from shopsense.config import get_paths

FC_CITIES = {"boston": "FC-BOS", "atlanta": "FC-ATL", "chicago": "FC-CHI", "dallas": "FC-DAL",
             "los angeles": "FC-LAX", "la": "FC-LAX"}


@dataclass
class ToolResult:
    tool: str
    args: dict
    as_of: str
    rows: list[dict] = field(default_factory=list)
    note: str = ""

    def to_json(self) -> str:
        return json.dumps({"as_of": self.as_of, "rows": self.rows, "note": self.note}, default=str)


def _clean(v):
    if isinstance(v, (float, Decimal)):
        return round(float(v), 2)
    if isinstance(v, date):
        return v.isoformat()
    return v


class Warehouse:
    """Thin read-only access to the marts. One short-lived connection per call."""

    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or get_paths().warehouse_db

    def query(self, sql: str, params: list | None = None) -> list[dict]:
        con = duckdb.connect(str(self.db_path), read_only=True)
        try:
            cur = con.execute(sql, params or [])
            cols = [d[0] for d in cur.description]
            return [{c: _clean(v) for c, v in zip(cols, row)} for row in cur.fetchall()]
        finally:
            con.close()

    def as_of(self) -> str:
        return str(self.query("SELECT MAX(date) AS d FROM mart.product_fc_daily")[0]["d"])

    def has_table(self, schema: str, table: str) -> bool:
        return bool(self.query("SELECT COUNT(*) AS n FROM information_schema.tables "
                               "WHERE table_schema = ? AND table_name = ?", [schema, table])[0]["n"])


class RetailTools:
    def __init__(self, warehouse: Warehouse | None = None):
        self.wh = warehouse or Warehouse()

    # ------------------------------------------------------------------ tools
    def find_products(self, query: str, limit: int = 5) -> ToolResult:
        words = [w for w in query.lower().replace("-", " ").split() if len(w) > 2]
        if not words:
            return ToolResult("find_products", {"query": query}, self.wh.as_of(), note="empty query")
        score = " + ".join("(CASE WHEN lower(product_name || ' ' || category || ' ' || product_id) LIKE ? "
                           "THEN 1 ELSE 0 END)" for _ in words)
        rows = self.wh.query(f"""
            SELECT * FROM (
                SELECT product_id, product_name, category, list_price, {score} AS match_score
                FROM dw.dim_product WHERE is_current AND product_key <> -1)
            WHERE match_score > 0 ORDER BY match_score DESC, product_id LIMIT ?""",
                             [f"%{w}%" for w in words] + [limit])
        for r in rows:
            r["ref"] = f"product:{r['product_id']}"
        return ToolResult("find_products", {"query": query}, self.wh.as_of(), rows)

    def get_restock_list(self, fc_id: str | None = None, category: str | None = None, limit: int = 10) -> ToolResult:
        sql = ["SELECT risk_tier, product_id, product_name, category, fc_id, on_hand_units, inbound_units,",
               "       daily_demand, days_of_cover, projected_stockout_date, recommended_order_units,",
               "       recommended_action, revenue_at_risk_per_day, primary_driver",
               "FROM mart.restock_this_week WHERE 1 = 1"]
        params: list = []
        if fc_id:
            sql.append("AND fc_id = ?")
            params.append(fc_id.upper())
        if category:
            sql.append("AND lower(category) = lower(?)")
            params.append(category)
        sql.append("ORDER BY revenue_at_risk_per_day DESC LIMIT ?")
        rows = self.wh.query("\n".join(sql), params + [limit])
        total = self.wh.query("SELECT COUNT(*) AS n, ROUND(SUM(revenue_at_risk_per_day), 2) AS revenue_at_risk "
                              "FROM mart.restock_this_week" + (" WHERE fc_id = ?" if fc_id else ""),
                              [fc_id.upper()] if fc_id else [])[0]
        for r in rows:
            r["ref"] = f"restock:{r['product_id']}@{r['fc_id']}"
        note = (f"{total['n']} product x FC combinations need restocking (risk tier High or Out of stock); "
                f"total revenue at risk per day {total['revenue_at_risk']}. Showing the top {len(rows)} by revenue at risk.")
        return ToolResult("get_restock_list", {"fc_id": fc_id, "category": category, "limit": limit},
                          self.wh.as_of(), rows, note)

    def get_product_status(self, product_id: str) -> ToolResult:
        pid = product_id.upper()
        risk = self.wh.query("""
            SELECT product_id, product_name, fc_id, risk_tier, on_hand_units, inbound_units, daily_demand,
                   days_of_cover, projected_stockout_date, recommended_order_units, recommended_action
            FROM mart.inventory_risk WHERE product_id = ? ORDER BY days_of_cover NULLS FIRST""", [pid])
        drivers = self.wh.query("""
            SELECT product_id, product_name, category, primary_driver, 28 AS window_days, units_last_28d,
                   units_prior_28d, trend_pct,
                   promo_lift, weekend_index, oos_view_share
            FROM mart.demand_drivers WHERE product_id = ?""", [pid])
        for r in risk:
            r["ref"] = f"risk:{pid}@{r['fc_id']}"
        for r in drivers:
            r["ref"] = f"drivers:{pid}"
        rows = drivers + risk
        note = "" if rows else f"no product {pid} in the warehouse"
        return ToolResult("get_product_status", {"product_id": pid}, self.wh.as_of(), rows, note)

    def get_review_summary(self, product_id: str) -> ToolResult:
        pid = product_id.upper()
        if not self.wh.has_table("ai", "review_summaries"):
            return ToolResult("get_review_summary", {"product_id": pid}, self.wh.as_of(),
                              note="review summaries not built yet (python -m shopsense.ai.reviews)")
        rows = self.wh.query("""
            SELECT product_id, product_name, n_reviews, avg_rating, pct_positive, pct_negative,
                   avg_rating_28d, avg_rating_prior, praise, complaints, summary, method
            FROM ai.review_summaries WHERE product_id = ?""", [pid])
        for r in rows:
            r["ref"] = f"reviews:{pid}"
        return ToolResult("get_review_summary", {"product_id": pid}, self.wh.as_of(), rows,
                          "" if rows else f"no reviews summarized for {pid}")

    def get_sales_summary(self, days: int = 28, category: str | None = None) -> ToolResult:
        days = max(1, min(int(days), 90))
        where, params = ("AND lower(category) = lower(?)", [category]) if category else ("", [])
        rows = self.wh.query(f"""
            WITH l AS (SELECT MAX(date) AS d FROM mart.daily_sales)
            SELECT category,
                   ROUND(SUM(net_revenue) FILTER (WHERE date > l.d - {days} * INTERVAL 1 DAY), 2)            AS net_revenue,
                   ROUND(SUM(net_revenue) FILTER (WHERE date <= l.d - {days} * INTERVAL 1 DAY
                                                  AND date > l.d - {2 * days} * INTERVAL 1 DAY), 2)            AS prior_net_revenue,
                   SUM(units) FILTER (WHERE date > l.d - {days} * INTERVAL 1 DAY)                              AS units,
                   ROUND(SUM(returned_revenue) FILTER (WHERE date > l.d - {days} * INTERVAL 1 DAY), 2)        AS returned_revenue
            FROM mart.daily_sales, l WHERE 1 = 1 {where}
            GROUP BY category ORDER BY net_revenue DESC""", params)
        for r in rows:
            prev = r.get("prior_net_revenue") or 0
            r["change_pct"] = round(100 * (r["net_revenue"] - prev) / prev, 1) if prev else None
            r["ref"] = f"sales:{r['category']}:{days}d"
        total = {k: round(sum((r[k] or 0) for r in rows), 2) for k in ("net_revenue", "prior_net_revenue", "units")}
        orders = self.wh.query(f"""
            SELECT COUNT(DISTINCT f.order_id) AS orders FROM dw.fact_order_item f JOIN dw.dim_date d USING (date_key)
            {"JOIN dw.dim_product p USING (product_key)" if category else ""}
            WHERE f.order_status <> 'cancelled' {"AND lower(p.category) = lower(?)" if category else ""}
              AND d.date > (SELECT MAX(date) FROM mart.daily_sales) - {days} * INTERVAL 1 DAY""", params)[0]["orders"]
        if len(rows) > 1 or not category:
            rows.insert(0, {"category": "All categories", **total, "orders": orders,
                            "change_pct": round(100 * (total["net_revenue"] - total["prior_net_revenue"])
                                                / total["prior_net_revenue"], 1) if total["prior_net_revenue"] else None,
                            "ref": f"sales:all:{days}d"})
        elif rows:
            rows[0]["orders"] = orders
        return ToolResult("get_sales_summary", {"days": days, "category": category}, self.wh.as_of(), rows,
                          f"last {days} days vs the {days} days before; cancelled orders excluded")

    # ------------------------------------------------------------------ dispatch
    def call(self, name: str, args: dict) -> ToolResult:
        fn = {"find_products": self.find_products, "get_restock_list": self.get_restock_list,
              "get_product_status": self.get_product_status, "get_review_summary": self.get_review_summary,
              "get_sales_summary": self.get_sales_summary}.get(name)
        if fn is None:
            raise ValueError(f"unknown tool {name}")
        return fn(**{k: v for k, v in args.items() if v is not None})


def _nullable(t: str) -> dict:
    return {"type": [t, "null"]}


# JSON schemas for Claude tool use. strict: true needs every property required +
# additionalProperties false; optional filters are expressed as nullable.
TOOL_SCHEMAS = [
    {"name": "find_products", "strict": True,
     "description": "Find product ids by name, brand or category words, e.g. 'yoga mat' or 'Voltix headphones'. "
                    "Use this first whenever the user names a product instead of giving a P-number id.",
     "input_schema": {"type": "object", "properties": {"query": {"type": "string"}},
                      "required": ["query"], "additionalProperties": False}},
    {"name": "get_restock_list", "strict": True,
     "description": "Products that need restocking this week (risk tier High = under 7 days of cover, or Out of "
                    "stock), most revenue at risk first, with projected stockout date, recommended order and the "
                    "primary demand driver. Optional filters: fulfillment center id (FC-BOS, FC-ATL, FC-CHI, "
                    "FC-DAL, FC-LAX) and category.",
     "input_schema": {"type": "object",
                      "properties": {"fc_id": _nullable("string"), "category": _nullable("string"),
                                     "limit": {"type": "integer"}},
                      "required": ["fc_id", "category", "limit"], "additionalProperties": False}},
    {"name": "get_product_status", "strict": True,
     "description": "Stock risk per fulfillment center (on hand, inbound, daily demand, days of cover, projected "
                    "stockout, recommended action) and demand drivers (28-day trend, promotion lift, weekend index, "
                    "out-of-stock view share, primary driver) for one product id like P00121.",
     "input_schema": {"type": "object", "properties": {"product_id": {"type": "string"}},
                      "required": ["product_id"], "additionalProperties": False}},
    {"name": "get_review_summary", "strict": True,
     "description": "What customers say about a product: review count, average rating (all time, last 28 days, "
                    "before), share of positive and negative reviews, most praised and most criticised aspects.",
     "input_schema": {"type": "object", "properties": {"product_id": {"type": "string"}},
                      "required": ["product_id"], "additionalProperties": False}},
    {"name": "get_sales_summary", "strict": True,
     "description": "Net revenue, orders, units and returns for the last N days (default 28, max 90) compared with "
                    "the N days before, overall and by category. Optional category filter.",
     "input_schema": {"type": "object",
                      "properties": {"days": {"type": "integer"}, "category": _nullable("string")},
                      "required": ["days", "category"], "additionalProperties": False}},
]
