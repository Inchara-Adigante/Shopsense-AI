"""
Per-product review summaries -> warehouse table ai.review_summaries.

Numbers (review count, average rating, % positive / negative, 28-day vs prior
rating) are always computed in SQL. The *words* come from one of two methods:

    extractive  (default without Claude credentials, deterministic, free)
                aspects = phrases that follow "the" in review text ("the battery life"),
                ranked by how much more often they appear in positive (4-5 star) than in
                negative (1-2 star) reviews, and vice versa
    claude      one Message Batches request per product (50% cheaper, asynchronous) with a
                structured-output schema: a 2-sentence summary + praised / criticised aspects.
                Claude only writes prose; it never produces the numbers.

Usage:
    python -m shopsense.ai.reviews                    # claude if credentials are configured, else extractive
    python -m shopsense.ai.reviews --method extractive
"""
import argparse
import json
import os
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from shopsense.config import get_paths

MODEL = "claude-opus-5-5"
MIN_REVIEWS = 3
STOP = {"is", "was", "are", "were", "and", "but", "a", "an", "it", "to", "of", "for", "my", "this", "that", "very"}

SUMMARY_SCHEMA = {
    "type": "object",
    "properties": {
        "summary": {"type": "string", "description": "Two sentences on what customers say. No numbers."},
        "praise": {"type": "array", "items": {"type": "string"}, "description": "Up to 3 aspects customers like"},
        "complaints": {"type": "array", "items": {"type": "string"}, "description": "Up to 3 aspects customers dislike"},
        "overall_sentiment": {"type": "string", "enum": ["positive", "mixed", "negative"]},
    },
    "required": ["summary", "praise", "complaints", "overall_sentiment"],
    "additionalProperties": False,
}


def claude_available() -> bool:
    """Credentials the Anthropic SDK can resolve (env key/token, profile, or WIF)."""
    if any(os.getenv(v) for v in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_PROFILE",
                                  "ANTHROPIC_IDENTITY_TOKEN_FILE", "ANTHROPIC_IDENTITY_TOKEN")):
        return True
    return (Path.home() / ".config" / "anthropic").exists()


def load_reviews(con) -> pd.DataFrame:
    return con.execute("""
        SELECT p.product_id, p.product_name, p.category, r.review_id, r.rating, r.title, r.body, d.date AS review_date
        FROM dw.fact_review r
        JOIN dw.dim_product p USING (product_key)
        JOIN dw.dim_date d USING (date_key)""").df()


def review_stats(reviews: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    recent = pd.to_datetime(reviews["review_date"]) > as_of - pd.Timedelta(days=28)
    r = reviews.assign(recent=recent, pos=reviews["rating"] >= 4, neg=reviews["rating"] <= 2)
    g = r.groupby(["product_id", "product_name", "category"])
    stats = g.agg(n_reviews=("review_id", "size"), avg_rating=("rating", "mean"),
                  pct_positive=("pos", "mean"), pct_negative=("neg", "mean")).reset_index()
    stats["avg_rating_28d"] = stats["product_id"].map(r[r["recent"]].groupby("product_id")["rating"].mean())
    stats["avg_rating_prior"] = stats["product_id"].map(r[~r["recent"]].groupby("product_id")["rating"].mean())
    for c in ("avg_rating", "avg_rating_28d", "avg_rating_prior"):
        stats[c] = stats[c].round(2)
    for c in ("pct_positive", "pct_negative"):
        stats[c] = (100 * stats[c]).round(1)
    return stats[stats["n_reviews"] >= MIN_REVIEWS]


def aspects(texts: list[str]) -> Counter:
    c = Counter()
    for t in texts:
        for m in re.finditer(r"\bthe ([a-z']+(?: [a-z']+)?)", t.lower()):
            words = [w for w in m.group(1).split() if w not in STOP]
            if words:
                c[" ".join(words)] += 1
    return c


def extractive_summary(product_reviews: pd.DataFrame, stats: pd.Series) -> dict:
    pos = aspects((product_reviews.loc[product_reviews["rating"] >= 4, "body"]).tolist())
    neg = aspects((product_reviews.loc[product_reviews["rating"] <= 2, "body"]).tolist())
    praise = [a for a, _ in sorted(pos.items(), key=lambda kv: -(kv[1] - neg.get(kv[0], 0))) if pos[a] > neg.get(a, 0)][:3]
    complaints = [a for a, _ in sorted(neg.items(), key=lambda kv: -(kv[1] - pos.get(kv[0], 0))) if neg[a] >= pos.get(a, 0)][:3]
    parts = [f"{int(stats.n_reviews)} reviews, average {stats.avg_rating}/5 "
             f"({stats.pct_positive}% positive, {stats.pct_negative}% negative)."]
    if praise:
        parts.append("Customers praise the " + ", ".join(praise) + ".")
    if complaints:
        parts.append("Complaints mention the " + ", ".join(complaints) + ".")
    if pd.notna(stats.avg_rating_28d) and pd.notna(stats.avg_rating_prior):
        parts.append(f"Average rating in the last 28 days: {stats.avg_rating_28d} vs {stats.avg_rating_prior} before.")
    return {"summary": " ".join(parts), "praise": praise, "complaints": complaints}


def claude_summaries(reviews: pd.DataFrame, product_ids: list[str], poll_s: int = 30) -> dict[str, dict]:
    """One Message Batches request per product; returns {product_id: parsed JSON}."""
    import anthropic
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request

    client = anthropic.Anthropic()
    requests = []
    for pid in product_ids:
        rv = reviews[reviews["product_id"] == pid].sort_values("review_date", ascending=False).head(60)
        lines = "\n".join(f"- {r.rating}/5 | {r.title} | {r.body}" for r in rv.itertuples())
        requests.append(Request(custom_id=pid, params=MessageCreateParamsNonStreaming(
            model=MODEL, max_tokens=2000,
            output_config={"effort": "low", "format": {"type": "json_schema", "schema": SUMMARY_SCHEMA}},
            system="You summarize product reviews for a retail operations team. Use only what the reviews say. "
                   "Do not include numbers or ratings; those are reported separately.",
            messages=[{"role": "user", "content": f"Product: {rv['product_name'].iloc[0]}\n\nReviews:\n{lines}"}],
        )))
    batch = client.messages.batches.create(requests=requests)
    while client.messages.batches.retrieve(batch.id).processing_status != "ended":
        time.sleep(poll_s)
    out = {}
    for result in client.messages.batches.results(batch.id):          # results arrive in any order
        if result.result.type != "succeeded":
            continue
        msg = result.result.message
        if msg.stop_reason == "refusal":
            continue
        text = next((b.text for b in msg.content if b.type == "text"), "")
        try:
            out[result.custom_id] = json.loads(text)
        except json.JSONDecodeError:
            continue
    return out


def build(db_path: Path | None = None, method: str | None = None) -> dict:
    db_path = db_path or get_paths().warehouse_db
    method = method or ("claude" if claude_available() else "extractive")
    con = duckdb.connect(str(db_path))
    try:
        reviews = load_reviews(con)
        as_of = pd.Timestamp(con.execute("SELECT MAX(date) FROM mart.product_fc_daily").fetchone()[0])
        stats = review_stats(reviews, as_of)
        llm = claude_summaries(reviews, stats["product_id"].tolist()) if method == "claude" else {}
        rows = []
        for s in stats.itertuples():
            ext = extractive_summary(reviews[reviews["product_id"] == s.product_id], s)
            gen = llm.get(s.product_id)
            text = ext if gen is None else {
                "summary": ext["summary"].split(".")[0] + ". " + gen["summary"],   # numbers from SQL, words from Claude
                "praise": gen["praise"][:3], "complaints": gen["complaints"][:3]}
            rows.append({**s._asdict(), "praise": json.dumps(text["praise"]), "complaints": json.dumps(text["complaints"]),
                         "summary": text["summary"], "method": "claude" if gen else "extractive",
                         "generated_at": datetime.now(timezone.utc).replace(tzinfo=None)})
        df = pd.DataFrame(rows).drop(columns=["Index"], errors="ignore")
        con.execute("CREATE SCHEMA IF NOT EXISTS ai")
        con.register("summaries", df)
        con.execute("CREATE OR REPLACE TABLE ai.review_summaries AS SELECT * FROM summaries")
    finally:
        con.close()
    return {"products": len(df), "method": method, "claude_summaries": int((df["method"] == "claude").sum())}


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize product reviews into ai.review_summaries.")
    parser.add_argument("--method", choices=["extractive", "claude"])
    args = parser.parse_args()
    result = build(method=args.method)
    print(f"{result['products']} products summarized ({result['method']}; "
          f"{result['claude_summaries']} written by Claude)")


if __name__ == "__main__":
    main()
