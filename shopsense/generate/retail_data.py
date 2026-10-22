"""
Synthetic retail data generator.

Simulates an online retailer day by day and drops files into the landing area
the way upstream systems would:

    landing/products/<date>/products.csv             full catalog snapshot (day 1, then every Monday)
    landing/customers/<date>/customers.csv           customer changes that day (CDC: new + changed rows)
    landing/promotions/<date>/promotions.csv         promotion calendar (day 1)
    landing/orders/<date>/orders.csv                 orders placed that day
    landing/order_items/<date>/order_items.csv       their line items
    landing/order_status_updates/<date>/...csv       shipped / delivered / cancelled / returned events
    landing/inventory_snapshots/<date>/inventory.csv end-of-day stock per FC
    landing/clickstream/<date>/events.json.gz        web/app events (JSON lines)
    landing/reviews/<date>/reviews.json              reviews posted that day

How the simulation works
    * Demand: each product has a popularity weight, its category has a monthly
      seasonality curve, and active promotions lift demand (bigger discount ->
      bigger lift). Order volume also varies by weekday and trends upward.
    * Fulfillment: each customer region is served by one fulfillment center (FC).
      Items are only sold if the FC has stock; otherwise the sale is lost and the
      shopper sees an out-of-stock page.
    * Replenishment: a reorder-point policy per product x FC with a supplier lead
      time. Some products get deliberately thin safety stock, so stockouts occur.
    * Change over time: ~6% of products change price every Monday, customers
      move region or change membership, and orders progress through
      placed -> shipped -> delivered (some cancelled, some returned). These
      drive incremental loads and SCD Type 2 dimensions downstream.
    * Dirty data: duplicate rows and events, late-arriving events, mixed
      timestamp formats, inconsistent casing, and a few invalid values.

Usage:
    python -m shopsense.generate.retail_data [--days 180] [--seed 42]
"""
import argparse
import gzip
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

from shopsense.config import get_paths

SEED = 42
START_DATE = pd.Timestamp("2026-01-01")
N_DAYS = 180
N_PRODUCTS = 400
N_CUSTOMERS = 6000
BASE_ORDERS_PER_DAY = 170

# Category -> (typical price, monthly seasonality Jan..Dec, brands, product nouns)
CATEGORIES = {
    "Electronics": (120, [0.9, 0.8, 0.85, 0.9, 0.95, 1.0, 1.0, 1.05, 1.1, 1.1, 1.4, 1.6],
                    ["Voltix", "Nimbus", "Auralab"], ["Headphones", "Charger", "Smart Speaker", "Webcam", "Power Bank"]),
    "Home & Kitchen": (45, [1.0, 0.9, 1.0, 1.05, 1.1, 1.05, 1.0, 1.0, 1.0, 1.05, 1.2, 1.35],
                       ["Hearth", "Copperline", "Nestwell"], ["Blender", "Knife Set", "Air Fryer", "Storage Jars", "Kettle"]),
    "Beauty": (25, [1.0, 1.15, 1.0, 1.0, 1.1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.15, 1.3],
               ["Glowe", "Purely", "Aster"], ["Face Serum", "Moisturizer", "Shampoo", "Lip Balm", "Sunscreen"]),
    "Sports & Outdoors": (50, [0.8, 0.8, 1.0, 1.2, 1.35, 1.4, 1.35, 1.2, 1.0, 0.9, 0.95, 1.05],
                          ["Trailpeak", "Stride", "Kinetic"], ["Yoga Mat", "Water Bottle", "Running Shoes", "Tent", "Dumbbells"]),
    "Toys & Games": (30, [0.7, 0.7, 0.8, 0.85, 0.9, 0.95, 0.95, 1.0, 1.0, 1.1, 1.5, 2.0],
                     ["Playforge", "Brightbox", "Tinker"], ["Puzzle", "Board Game", "Building Set", "Plush Toy", "RC Car"]),
    "Books": (18, [1.1, 1.0, 1.0, 1.0, 1.0, 1.1, 1.15, 1.1, 1.05, 1.0, 1.05, 1.2],
              ["Northleaf Press", "Quill & Co", "Lantern Books"], ["Novel", "Cookbook", "Workbook", "Biography", "Guide"]),
    "Grocery": (12, [1.05, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.1, 1.2],
                ["Fieldfresh", "Harvest Co", "Daily Pantry"], ["Coffee Beans", "Granola", "Olive Oil", "Green Tea", "Protein Bars"]),
    "Apparel": (40, [0.85, 0.9, 1.05, 1.15, 1.2, 1.15, 1.05, 1.1, 1.1, 1.0, 1.1, 1.25],
                ["Threadly", "Northloom", "Urbanfit"], ["T-Shirt", "Hoodie", "Jeans", "Rain Jacket", "Socks Pack"]),
}
ADJECTIVES = ["Classic", "Pro", "Essential", "Ultra", "Eco", "Compact", "Premium", "Everyday"]

# Customer region -> fulfillment center that serves it.
REGION_FC = {"Northeast": "FC-BOS", "Southeast": "FC-ATL", "Midwest": "FC-CHI", "South": "FC-DAL", "West": "FC-LAX"}
REGION_WEIGHTS = [0.24, 0.2, 0.18, 0.16, 0.22]
FCS = list(REGION_FC.values())

DOW_FACTOR = [0.95, 0.9, 0.92, 0.98, 1.08, 1.15, 1.02]   # Mon..Sun
BROWSE_SESSIONS_PER_ORDER = 9                            # ~10% session conversion
PROMO_ELASTICITY = 2.5                                   # 20% off -> demand x1.5
WEEKLY_PRICE_CHANGE_SHARE = 0.06                         # products repriced each Monday
DAILY_MOVE_PROB = 0.0003                                 # customer moves to another region
DAILY_MEMBERSHIP_FLIP_PROB = 0.0005                      # joins / cancels membership
CANCEL_RATE, RETURN_RATE = 0.02, 0.05

POSITIVE = ["Exactly what I needed.", "Great quality for the price.", "Would buy again.", "Arrived quickly and works well."]
NEUTRAL = ["Does the job.", "Okay overall, nothing special.", "Decent, but a bit overpriced."]
NEGATIVE = ["Stopped working after a week.", "Not as described.", "Poor quality, returning it.", "Disappointed with this one."]
ASPECTS = {
    "Electronics": ["battery life", "sound quality", "build quality"],
    "Home & Kitchen": ["durability", "ease of cleaning", "design"],
    "Beauty": ["scent", "texture", "results on my skin"],
    "Sports & Outdoors": ["comfort", "durability", "grip"],
    "Toys & Games": ["how much my kids love it", "durability", "instructions"],
    "Books": ["writing", "pacing", "print quality"],
    "Grocery": ["taste", "freshness", "packaging"],
    "Apparel": ["fit", "fabric", "sizing"],
}


# ---------------------------------------------------------------------------
# Master data
# ---------------------------------------------------------------------------

def make_products(rng: np.random.Generator) -> pd.DataFrame:
    cats = rng.choice(list(CATEGORIES), size=N_PRODUCTS)
    rows = []
    for i, cat in enumerate(cats, 1):
        base_price, _, brands, nouns = CATEGORIES[cat]
        price = round(float(base_price * rng.lognormal(0, 0.45)), 2)
        rows.append({
            "product_id": f"P{i:05d}",
            "product_name": f"{rng.choice(brands)} {rng.choice(ADJECTIVES)} {rng.choice(nouns)}",
            "category": cat,
            "brand": None,  # filled below from the name
            "list_price": max(price, 2.99),
            "unit_cost": round(max(price, 2.99) * float(rng.uniform(0.45, 0.7)), 2),
            # 5% of products launch during the simulated period.
            "launch_date": (START_DATE + pd.Timedelta(days=int(rng.integers(20, N_DAYS - 30))))
            if rng.random() < 0.05 else (START_DATE - pd.Timedelta(days=int(rng.integers(30, 900)))),
        })
    products = pd.DataFrame(rows)
    products["brand"] = [next(b for b in CATEGORIES[c][2] if n.startswith(b))
                         for c, n in zip(products["category"], products["product_name"])]
    # Hidden simulation attributes (not exported): popularity and true quality.
    products["_popularity"] = rng.lognormal(0, 1.0, N_PRODUCTS)
    products["_quality"] = np.clip(rng.normal(4.0, 0.6, N_PRODUCTS), 1.5, 5.0)
    return products


def make_customers(rng: np.random.Generator) -> pd.DataFrame:
    signup_offset = np.where(rng.random(N_CUSTOMERS) < 0.8,
                             -rng.integers(1, 1100, N_CUSTOMERS),     # existing customers
                             rng.integers(0, N_DAYS, N_CUSTOMERS))    # sign up during the period
    return pd.DataFrame({
        "customer_id": [f"C{i:06d}" for i in range(1, N_CUSTOMERS + 1)],
        "signup_date": START_DATE + pd.to_timedelta(signup_offset, unit="D"),
        "region": rng.choice(list(REGION_FC), size=N_CUSTOMERS, p=REGION_WEIGHTS),
        "is_member": rng.random(N_CUSTOMERS) < 0.45,
        "age_band": rng.choice(["18-24", "25-34", "35-44", "45-54", "55+"], size=N_CUSTOMERS,
                               p=[0.14, 0.3, 0.25, 0.17, 0.14]),
        "_activity": rng.lognormal(0, 0.8, N_CUSTOMERS),  # heavy vs light shoppers
    })


def make_promotions(products: pd.DataFrame, rng: np.random.Generator, n: int = 70) -> pd.DataFrame:
    weights = products["_popularity"] / products["_popularity"].sum()
    picked = rng.choice(products["product_id"], size=n, p=weights)
    start = START_DATE + pd.to_timedelta(rng.integers(0, N_DAYS - 10, n), unit="D")
    length = rng.integers(3, 15, n)
    return pd.DataFrame({
        "promo_id": [f"PR{i:04d}" for i in range(1, n + 1)],
        "product_id": picked,
        "start_date": start,
        "end_date": start + pd.to_timedelta(length - 1, unit="D"),
        "discount_pct": rng.choice([0.10, 0.15, 0.20, 0.25, 0.30, 0.40], size=n),
    })


# ---------------------------------------------------------------------------
# Daily simulation
# ---------------------------------------------------------------------------

class RetailSimulator:
    def __init__(self, n_days: int, rng: np.random.Generator):
        self.rng = rng
        self.n_days = n_days
        self.products = make_products(rng)
        self.customers = make_customers(rng)
        self.promotions = make_promotions(self.products, rng)
        self.cat_of = self.products["category"].to_numpy()
        self.n_p = len(self.products)
        self.fc_index = {fc: i for i, fc in enumerate(FCS)}
        self.cust_fc = self.customers["region"].map(REGION_FC).map(self.fc_index).to_numpy()
        self._init_inventory()
        self.counters = {"order": 0, "event": 0, "session": 0, "review": 0}
        self.status_due: dict[int, list[tuple]] = {}               # day index -> status updates
        self.changed_customers: set[int] = set()

    def _init_inventory(self):
        """Reorder-point policy per product x FC, sized from expected demand."""
        rng = self.rng
        items_per_order = 1.75
        share_p = (self.products["_popularity"] / self.products["_popularity"].sum()).to_numpy()
        share_fc = np.array(REGION_WEIGHTS)
        expected = BASE_ORDERS_PER_DAY * items_per_order * np.outer(share_fc, share_p)  # [fc, product] units/day
        self.lead_time = rng.integers(4, 15, size=expected.shape)
        safety = rng.uniform(0.5, 1.8, size=expected.shape)       # < 1 means thin buffer -> stockouts
        self.reorder_point = np.ceil(expected * self.lead_time * safety + 1).astype(int)
        self.order_qty = np.ceil(expected * rng.uniform(14, 35, size=expected.shape) + 2).astype(int)
        self.on_hand = self.order_qty + self.reorder_point
        self.inbound = np.zeros_like(self.on_hand)
        self.arrivals: dict[int, np.ndarray] = {}                  # day index -> units arriving

    def _next(self, kind: str, n: int) -> np.ndarray:
        start = self.counters[kind] + 1
        self.counters[kind] += n
        return np.arange(start, start + n)

    def _demand_weights(self, day: pd.Timestamp) -> np.ndarray:
        p = self.products
        season = np.array([CATEGORIES[c][1][day.month - 1] for c in self.cat_of])
        promo = self.promotions[(self.promotions["start_date"] <= day) & (self.promotions["end_date"] >= day)]
        lift = np.ones(self.n_p)
        discount = np.zeros(self.n_p)
        idx = p.index[p["product_id"].isin(promo["product_id"])]
        disc_map = promo.groupby("product_id")["discount_pct"].max()
        discount[idx] = p.loc[idx, "product_id"].map(disc_map).to_numpy()
        lift[idx] = 1 + PROMO_ELASTICITY * discount[idx]
        launched = (p["launch_date"] <= day).to_numpy()
        self.today_discount = discount
        return p["_popularity"].to_numpy() * season * lift * launched

    def simulate_day(self, d: int) -> dict[str, pd.DataFrame]:
        rng, day = self.rng, START_DATE + pd.Timedelta(days=d)

        # 0. Master data changes: Monday repricing, customer moves and membership changes.
        if day.dayofweek == 0 and d > 0:
            repriced = rng.random(self.n_p) < WEEKLY_PRICE_CHANGE_SHARE
            factor = rng.uniform(0.85, 1.15, repriced.sum())
            new_price = np.maximum(np.round(self.products.loc[repriced, "list_price"].to_numpy() * factor, 2), 2.99)
            self.products.loc[repriced, "list_price"] = new_price
        signed_up = (self.customers["signup_date"] <= day).to_numpy()
        moves = np.flatnonzero(signed_up & (rng.random(len(self.customers)) < DAILY_MOVE_PROB))
        for i in moves:
            others = [r for r in REGION_FC if r != self.customers.at[i, "region"]]
            self.customers.at[i, "region"] = rng.choice(others)
            self.cust_fc[i] = self.fc_index[REGION_FC[self.customers.at[i, "region"]]]
        flips = np.flatnonzero(signed_up & (rng.random(len(self.customers)) < DAILY_MEMBERSHIP_FLIP_PROB))
        self.customers.loc[flips, "is_member"] = ~self.customers.loc[flips, "is_member"]
        self.changed_customers = set(moves) | set(flips)

        # 1. Supplier deliveries arrive.
        if d in self.arrivals:
            arriving = self.arrivals.pop(d)
            self.on_hand += arriving
            self.inbound -= arriving

        # 2. Shoppers place orders.
        weights = self._demand_weights(day)
        probs = weights / weights.sum()
        trend = 1 + 0.15 * d / self.n_days
        n_orders = rng.poisson(BASE_ORDERS_PER_DAY * DOW_FACTOR[day.dayofweek] * trend)
        eligible = np.flatnonzero((self.customers["signup_date"] <= day).to_numpy())
        act = self.customers["_activity"].to_numpy()[eligible]
        cust_idx = rng.choice(eligible, size=n_orders, p=act / act.sum())
        order_seq = self._next("order", n_orders)
        order_ts = day + pd.to_timedelta(rng.integers(0, 86400, n_orders), unit="s")

        n_items = np.minimum(1 + rng.poisson(0.75, n_orders), 4)
        item_order = np.repeat(np.arange(n_orders), n_items)
        items = pd.DataFrame({
            "o": item_order,
            "p": rng.choice(self.n_p, size=len(item_order), p=probs),
            "qty": rng.choice([1, 2, 3], size=len(item_order), p=[0.85, 0.12, 0.03]),
        })
        items = items.drop_duplicates(["o", "p"])            # one line per product per order
        items["fc"] = self.cust_fc[cust_idx[items["o"]]]

        # 3. Fulfillment: sell only what the FC has; the rest is a lost sale.
        items = items.sample(frac=1, random_state=int(rng.integers(1 << 31)))
        items["cum"] = items.groupby(["fc", "p"])["qty"].cumsum()
        items["available"] = self.on_hand[items["fc"], items["p"]]
        sold = items[items["cum"] <= items["available"]].sort_values(["o", "p"])
        lost = items[items["cum"] > items["available"]]
        np.subtract.at(self.on_hand, (sold["fc"].to_numpy(), sold["p"].to_numpy()), sold["qty"].to_numpy())

        # 4. Replenishment: reorder when stock position drops to the reorder point.
        position = self.on_hand + self.inbound
        need = (position <= self.reorder_point) & (self.inbound == 0)
        for fc, p in zip(*np.nonzero(need)):
            arrive = d + int(self.lead_time[fc, p])
            self.arrivals.setdefault(arrive, np.zeros_like(self.on_hand))[fc, p] += self.order_qty[fc, p]
            self.inbound[fc, p] += self.order_qty[fc, p]

        orders, order_items = self._order_tables(day, sold, cust_idx, order_seq, order_ts)
        self._schedule_status_updates(d, orders)
        events = self._events(day, items, sold, lost, cust_idx, order_seq, order_ts, probs)
        inventory = self._inventory_snapshot(day)
        updates = pd.DataFrame(self.status_due.pop(d, []), columns=["order_id", "order_status", "status_ts"])
        return {"orders": orders, "order_items": order_items, "events": events, "inventory": inventory,
                "status_updates": updates.sort_values("status_ts")}

    def _schedule_status_updates(self, d: int, orders: pd.DataFrame) -> None:
        """Order lifecycle: placed -> shipped -> delivered, with cancellations and returns."""
        rng = self.rng
        for oid, ts in zip(orders["order_id"], orders["order_ts"]):
            def add(days_after, status, base=ts):
                when = base + pd.Timedelta(seconds=int(days_after * 86400))
                self.status_due.setdefault((when.normalize() - START_DATE).days, []).append((oid, status, when))
                return when
            if rng.random() < CANCEL_RATE:
                add(rng.uniform(0.05, 1.0), "cancelled")
                continue
            shipped = add(rng.uniform(0.5, 2.0), "shipped")
            delivered = add(rng.uniform(2.0, 5.0), "delivered", shipped)
            if rng.random() < RETURN_RATE:
                add(rng.uniform(5.0, 20.0), "returned", delivered)

    def customer_changes(self, d: int) -> pd.DataFrame:
        """CDC feed for day d: customers who signed up that day plus customers whose attributes changed."""
        day = START_DATE + pd.Timedelta(days=d)
        new = (self.customers["signup_date"] <= day) if d == 0 else (self.customers["signup_date"] == day)
        rows = self.customers[new.to_numpy() | self.customers.index.isin(self.changed_customers)]
        return rows.drop(columns=["_activity"]).assign(updated_at=day)

    def _order_tables(self, day, sold, cust_idx, order_seq, order_ts):
        p = self.products
        placed = np.unique(sold["o"])                          # orders with at least one item sold
        orders = pd.DataFrame({
            "order_id": [f"O{order_seq[o]:08d}" for o in placed],
            "customer_id": self.customers["customer_id"].to_numpy()[cust_idx[placed]],
            "order_ts": order_ts[placed],
            "channel": self.rng.choice(["web", "app"], size=len(placed), p=[0.45, 0.55]),
            "payment_method": self.rng.choice(["card", "gift_card", "wallet"], size=len(placed), p=[0.75, 0.05, 0.2]),
            "fc_id": [FCS[f] for f in self.cust_fc[cust_idx[placed]]],
            "order_status": "placed",
        })
        unit_price = p["list_price"].to_numpy()[sold["p"]]
        discount = self.today_discount[sold["p"]]
        order_items = pd.DataFrame({
            "order_id": [f"O{order_seq[o]:08d}" for o in sold["o"]],
            "product_id": p["product_id"].to_numpy()[sold["p"]],
            "quantity": sold["qty"].to_numpy(),
            "unit_price": unit_price,
            "discount_amount": np.round(unit_price * sold["qty"].to_numpy() * discount, 2),
        })
        order_items.insert(1, "line_number", order_items.groupby("order_id").cumcount() + 1)
        return orders, order_items

    def _events(self, day, items, sold, lost, cust_idx, order_seq, order_ts, probs):
        rng, p = self.rng, self.products
        rows = []
        sold_orders = set(sold["o"])
        # Sessions that came from an order attempt (purchase or out-of-stock bounce).
        for o, grp in items.groupby("o"):
            sid = self._next("session", 1)[0]
            cust = self.customers["customer_id"].iat[cust_idx[o]]
            base_ts = order_ts[o] - pd.Timedelta(minutes=int(rng.integers(3, 40)))
            extra = rng.choice(self.n_p, size=int(rng.integers(0, 4)), p=probs)
            in_stock_now = {pp: True for pp in grp["p"]}
            for pp in lost[lost["o"] == o]["p"]:
                in_stock_now[pp] = False
            for k, pp in enumerate(list(grp["p"]) + list(extra)):
                fc = self.cust_fc[cust_idx[o]]
                stock = in_stock_now.get(pp, self.on_hand[fc, pp] > 0)
                rows.append((sid, cust, base_ts + pd.Timedelta(seconds=30 * k), "page_view", p["product_id"].iat[pp], None, bool(stock)))
                if pp in in_stock_now and stock:
                    rows.append((sid, cust, base_ts + pd.Timedelta(seconds=30 * k + 10), "add_to_cart", p["product_id"].iat[pp], None, True))
            if o in sold_orders:
                rows.append((sid, cust, order_ts[o], "purchase", None, f"O{order_seq[o]:08d}", True))

        # Browse-only sessions (no order), some with abandoned carts.
        n_browse = len(items["o"].unique()) * BROWSE_SESSIONS_PER_ORDER
        signed_up = np.flatnonzero((self.customers["signup_date"] <= day).to_numpy())
        browse_cust = rng.choice(signed_up, size=n_browse)       # only existing customers can log in
        guest = rng.random(n_browse) < 0.2
        for i in range(n_browse):
            sid = self._next("session", 1)[0]
            cust = None if guest[i] else self.customers["customer_id"].iat[browse_cust[i]]
            fc = self.cust_fc[browse_cust[i]]
            ts = day + pd.Timedelta(seconds=int(rng.integers(0, 86000)))
            viewed = rng.choice(self.n_p, size=int(rng.integers(1, 5)), p=probs)
            for k, pp in enumerate(viewed):
                stock = bool(self.on_hand[fc, pp] > 0)
                rows.append((sid, cust, ts + pd.Timedelta(seconds=25 * k), "page_view", p["product_id"].iat[pp], None, stock))
            if rng.random() < 0.25 and stock:
                rows.append((sid, cust, ts + pd.Timedelta(seconds=25 * len(viewed)), "add_to_cart",
                             p["product_id"].iat[viewed[-1]], None, True))

        ev = pd.DataFrame(rows, columns=["session_id", "customer_id", "event_ts", "event_type",
                                         "product_id", "order_id", "in_stock"])
        ev["session_id"] = [f"S{s:09d}" for s in ev["session_id"]]
        ev.insert(0, "event_id", [f"E{e:010d}" for e in self._next("event", len(ev))])
        ev["device"] = rng.choice(["desktop", "mobile", "tablet"], size=len(ev), p=[0.35, 0.55, 0.1])
        return ev

    def _inventory_snapshot(self, day):
        fc, prod = np.meshgrid(np.arange(len(FCS)), np.arange(self.n_p), indexing="ij")
        return pd.DataFrame({
            "snapshot_date": day,
            "fc_id": np.array(FCS)[fc.ravel()],
            "product_id": self.products["product_id"].to_numpy()[prod.ravel()],
            "on_hand_units": self.on_hand.ravel(),
            "inbound_units": self.inbound.ravel(),
            "reorder_point": self.reorder_point.ravel(),
        })

    def make_reviews(self, order_items: pd.DataFrame, orders: pd.DataFrame, end: pd.Timestamp) -> pd.DataFrame:
        """~7% of purchased items get a review 5-25 days after the order."""
        rng = self.rng
        it = order_items.merge(orders[["order_id", "customer_id", "order_ts"]], on="order_id")
        it = it[rng.random(len(it)) < 0.07].copy()
        it["review_ts"] = it["order_ts"] + pd.to_timedelta(rng.integers(5 * 86400, 25 * 86400, len(it)), unit="s")
        it = it[it["review_ts"] <= end].reset_index(drop=True)
        prod = self.products.set_index("product_id")
        quality = it["product_id"].map(prod["_quality"]).to_numpy()
        rating = np.clip(np.round(quality + rng.normal(0, 1.0, len(it))), 1, 5).astype(int)
        cats = it["product_id"].map(prod["category"]).to_numpy()
        titles, bodies = [], []
        for r, c in zip(rating, cats):
            pool = POSITIVE if r >= 4 else NEUTRAL if r == 3 else NEGATIVE
            aspect = rng.choice(ASPECTS[c])
            comment = (f"I love the {aspect}." if r >= 4
                       else f"The {aspect} is okay." if r == 3
                       else f"Unhappy with the {aspect}.")
            titles.append(rng.choice(pool))
            bodies.append(f"{rng.choice(pool)} {comment}")
        return pd.DataFrame({
            "review_id": [f"R{i:07d}" for i in self._next("review", len(it))],
            "product_id": it["product_id"], "customer_id": it["customer_id"], "order_id": it["order_id"],
            "review_ts": it["review_ts"], "rating": rating, "title": titles, "body": bodies,
        })


# ---------------------------------------------------------------------------
# Dirty data and export
# ---------------------------------------------------------------------------

def _fmt_ts(ts: pd.Series) -> pd.Series:
    return ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def inject_dirty(day_tables: dict[str, pd.DataFrame], rng: np.random.Generator) -> dict[str, pd.DataFrame]:
    t = {k: v.copy() for k, v in day_tables.items()}
    o, oi, ev = t["orders"], t["order_items"], t["events"]

    o["order_ts"] = _fmt_ts(o["order_ts"])
    legacy = rng.random(len(o)) < 0.02          # older checkout service writes US-style timestamps
    o.loc[legacy, "order_ts"] = pd.to_datetime(o.loc[legacy, "order_ts"], format="%Y-%m-%dT%H:%M:%SZ").dt.strftime("%m/%d/%Y %H:%M:%S")
    messy = rng.random(len(o)) < 0.03
    o.loc[messy, "order_status"] = rng.choice(["PLACED", " placed", "Placed "], size=messy.sum())
    t["orders"] = pd.concat([o, o[rng.random(len(o)) < 0.01]], ignore_index=True)      # resent rows

    bad = rng.random(len(oi)) < 0.002
    oi.loc[bad, "quantity"] = rng.choice([0, -1], size=bad.sum())
    t["order_items"] = pd.concat([oi, oi[rng.random(len(oi)) < 0.01]], ignore_index=True)

    su = t["status_updates"]
    su["status_ts"] = _fmt_ts(su["status_ts"]) if len(su) else su["status_ts"]
    loud = rng.random(len(su)) < 0.02
    su.loc[loud, "order_status"] = su.loc[loud, "order_status"].str.upper()
    t["status_updates"] = pd.concat([su, su[rng.random(len(su)) < 0.01]], ignore_index=True)

    ev["event_ts"] = _fmt_ts(ev["event_ts"])
    shout = rng.random(len(ev)) < 0.01
    ev.loc[shout, "event_type"] = ev.loc[shout, "event_type"].str.upper()
    ev.loc[(ev["event_type"] == "page_view") & (rng.random(len(ev)) < 0.001), "product_id"] = None
    t["events"] = pd.concat([ev, ev[rng.random(len(ev)) < 0.02]], ignore_index=True)   # at-least-once delivery
    return t


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, date_format="%Y-%m-%d")


def generate(n_days: int = N_DAYS, seed: int = SEED, landing: Path | None = None) -> dict[str, int]:
    """Run the simulation and write every file to the landing area. Returns row counts."""
    rng = np.random.default_rng(seed)
    landing = landing or get_paths().landing
    if landing.exists():
        shutil.rmtree(landing)
    sim = RetailSimulator(n_days, rng)
    first = START_DATE.strftime("%Y-%m-%d")
    counts = {}

    def write_product_snapshot(date_dir: str) -> int:
        products = sim.products.drop(columns=["_popularity", "_quality"]).copy()
        messy = rng.random(len(products)) < 0.03
        products.loc[messy, "category"] = [rng.choice([c.lower(), c.upper(), f" {c} "])
                                           for c in products.loc[messy, "category"]]
        _write_csv(products, landing / "products" / date_dir / "products.csv")
        return len(products)

    _write_csv(sim.promotions, landing / "promotions" / first / "promotions.csv")
    counts.update(products=0, customers=0, promotions=len(sim.promotions), order_status_updates=0)

    all_orders, all_items, late = [], [], None
    for d in range(n_days):
        day = START_DATE + pd.Timedelta(days=d)
        tables = sim.simulate_day(d)
        all_orders.append(tables["orders"])
        all_items.append(tables["order_items"])
        dirty = inject_dirty(tables, rng)
        date_dir = day.strftime("%Y-%m-%d")

        # Catalog snapshot on day 1 and every Monday (after that day's repricing).
        if d == 0 or day.dayofweek == 0:
            counts["products"] += write_product_snapshot(date_dir)
        changes = sim.customer_changes(d)
        if len(changes):
            _write_csv(changes, landing / "customers" / date_dir / "customers.csv")
            counts["customers"] += len(changes)
        upd = dirty["status_updates"]
        if len(upd):
            _write_csv(upd, landing / "order_status_updates" / date_dir / "order_status_updates.csv")
            counts["order_status_updates"] += len(upd)

        # ~1% of events reach the pipeline a day late (mobile apps offline, retries).
        ev = dirty["events"]
        is_late = rng.random(len(ev)) < 0.01
        on_time = pd.concat([late, ev[~is_late]]) if late is not None else ev[~is_late]
        late = ev[is_late]

        _write_csv(dirty["orders"], landing / "orders" / date_dir / "orders.csv")
        _write_csv(dirty["order_items"], landing / "order_items" / date_dir / "order_items.csv")
        _write_csv(tables["inventory"], landing / "inventory_snapshots" / date_dir / "inventory.csv")
        out = landing / "clickstream" / date_dir / "events.json.gz"
        out.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(out, "wt") as fh:
            on_time.to_json(fh, orient="records", lines=True)
        for k in ("orders", "order_items", "inventory_snapshots", "clickstream"):
            src = {"inventory_snapshots": tables["inventory"], "clickstream": on_time}.get(k, dirty.get(k))
            counts[k] = counts.get(k, 0) + len(src)

    # Events still in flight at the end of the simulation arrive the next day.
    if late is not None and len(late):
        out = landing / "clickstream" / (START_DATE + pd.Timedelta(days=n_days)).strftime("%Y-%m-%d") / "events.json.gz"
        out.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(out, "wt") as fh:
            late.to_json(fh, orient="records", lines=True)
        counts["clickstream"] += len(late)

    # Reviews, delivered on the day they were posted.
    end = START_DATE + pd.Timedelta(days=n_days) - pd.Timedelta(seconds=1)
    reviews = sim.make_reviews(pd.concat(all_items), pd.concat(all_orders), end)
    bad = rng.random(len(reviews)) < 0.003
    reviews.loc[bad, "rating"] = rng.choice([0, 6], size=bad.sum())
    reviews["review_ts"] = _fmt_ts(reviews["review_ts"])
    for date_dir, grp in reviews.groupby(reviews["review_ts"].str[:10]):
        path = landing / "reviews" / date_dir / "reviews.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        grp.to_json(path, orient="records", lines=True)
    counts["reviews"] = len(reviews)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic retail data into the landing area.")
    parser.add_argument("--days", type=int, default=N_DAYS)
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()
    counts = generate(args.days, args.seed)
    for name, n in counts.items():
        print(f"{name:<22}{n:>10,} rows")
    print(f"\nlanding area -> {get_paths().landing}")


if __name__ == "__main__":
    main()
