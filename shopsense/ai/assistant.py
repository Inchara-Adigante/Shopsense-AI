"""
ShopSense business assistant: answers questions from the warehouse, with citations.

    answer = Assistant().ask("Which products need restocking this week in Boston?")
    answer.text        # the answer, citing rows like [restock:P00170@FC-BOS]
    answer.citations   # the cited rows, with the values behind every number
    answer.grounding   # every number in the text checked against the tool results

Two modes, same tools (shopsense/ai/tools.py) and the same grounding check:
    claude   - Claude (claude-opus-5-5) decides which tools to call in a tool-use loop and
               writes the answer. Used when Anthropic credentials are configured.
    offline  - a keyword router picks the tool and fills an answer template. No LLM,
               deterministic, free; used when no credentials are configured and in tests.

Usage:
    python -m shopsense.ai.assistant "Why is P00121 selling so fast?"
    python -m shopsense.ai.assistant --mode offline "Which products need restocking this week?"
"""
import argparse
import json
import re
from dataclasses import dataclass, field

from shopsense.ai import grounding
from shopsense.ai.reviews import claude_available
from shopsense.ai.tools import FC_CITIES, TOOL_SCHEMAS, RetailTools, ToolResult

MODEL = "claude-opus-5-5"
MAX_TOOL_ROUNDS = 8
CATEGORIES = ["Electronics", "Home & Kitchen", "Beauty", "Sports & Outdoors", "Toys & Games", "Books", "Grocery",
              "Apparel"]

SYSTEM_PROMPT = """You are ShopSense, an analyst for an online retailer's operations team. You answer questions \
about stock, demand, sales and customer reviews using only the tools provided, which read the company's data \
warehouse.

How to answer:
- Look the data up with the tools before answering. If the user names a product rather than giving an id \
like P00121, call find_products first.
- Every number you state must come from a tool result in this conversation. Do not estimate, extrapolate or \
do arithmetic beyond simple comparisons the user asked for; if the tools don't have a figure, say so.
- After each claim that uses data, cite the row it came from with its ref in square brackets, for example \
[restock:P00170@FC-BOS] or [drivers:P00121].
- Lead with the direct answer and the recommended action, then the supporting numbers. Keep it short: a few \
sentences or a compact list.
- Data is as of the as_of date in the tool results; mention it when timing matters."""


@dataclass
class Answer:
    question: str
    text: str
    mode: str
    tool_calls: list[dict] = field(default_factory=list)
    citations: list[dict] = field(default_factory=list)
    grounding: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"question": self.question, "text": self.text, "mode": self.mode, "tool_calls": self.tool_calls,
                "citations": self.citations, "grounding": self.grounding}


def cited_rows(text: str, results: list[ToolResult]) -> list[dict]:
    refs = set(re.findall(r"\[([^\]]+)\]", text))
    refs = {r.strip() for group in refs for r in group.split(",")}
    rows = [row for res in results for row in res.rows if row.get("ref") in refs]
    seen, out = set(), []
    for row in rows:
        if row["ref"] not in seen:
            seen.add(row["ref"])
            out.append(row)
    return out


# ---------------------------------------------------------------------------
# Claude mode: manual tool-use loop
# ---------------------------------------------------------------------------

class ClaudeBackend:
    def __init__(self, tools: RetailTools, client=None):
        self.tools = tools
        if client is None:
            import anthropic
            client = anthropic.Anthropic()
        self.client = client

    def _create(self, messages: list):
        # Refusal fallbacks: if a safety classifier declines, the API re-runs the request on
        # Anthropic's recommended fallback model instead of returning a refusal.
        return self.client.beta.messages.create(
            model=MODEL,
            max_tokens=16000,
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            output_config={"effort": "medium"},
            system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
            tools=TOOL_SCHEMAS,
            messages=messages,
        )

    def ask(self, question: str) -> tuple[str, list[ToolResult], list[dict]]:
        messages = [{"role": "user", "content": question}]
        results, calls = [], []
        for _ in range(MAX_TOOL_ROUNDS):
            response = self._create(messages)
            if response.stop_reason == "refusal":
                return "I can't help with that request.", results, calls
            tool_uses = [b for b in response.content if b.type == "tool_use"]
            if response.stop_reason != "tool_use" or not tool_uses:
                text = "".join(b.text for b in response.content if b.type == "text").strip()
                return text, results, calls
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in tool_uses:                       # all results go back in one user message
                calls.append({"tool": block.name, "args": block.input})
                try:
                    res = self.tools.call(block.name, dict(block.input))
                    results.append(res)
                    tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": res.to_json()})
                except Exception as exc:                   # report the failure to Claude instead of crashing
                    tool_results.append({"type": "tool_result", "tool_use_id": block.id,
                                         "content": f"Error: {exc}", "is_error": True})
            messages.append({"role": "user", "content": tool_results})
        return "I couldn't finish looking that up - please ask a narrower question.", results, calls


# ---------------------------------------------------------------------------
# Offline mode: keyword router + templates
# ---------------------------------------------------------------------------

class OfflineBackend:
    def __init__(self, tools: RetailTools):
        self.tools = tools

    def _fc(self, q: str) -> str | None:
        m = re.search(r"\bFC-?([A-Z]{3})\b", q.upper())
        if m:
            return f"FC-{m.group(1)}"
        ql = q.lower()
        for city, fc in sorted(FC_CITIES.items(), key=lambda kv: -len(kv[0])):
            if re.search(rf"\b{city}\b", ql):
                return fc
        return None

    def _category(self, q: str) -> str | None:
        ql = q.lower()
        for c in CATEGORIES:
            if c.lower() in ql or c.split()[0].lower() in ql.split():
                return c
        return None

    def _product(self, q: str, results: list, calls: list) -> str | None:
        m = re.search(r"\bP\d{5}\b", q.upper())
        if m:
            return m.group()
        stop = {"which", "what", "why", "how", "does", "need", "this", "week", "about", "selling", "sell", "fast",
                "customers", "say", "reviews", "review", "stock", "restock", "product", "products", "the", "are",
                "is", "of", "for", "in", "on", "so", "much", "many", "doing", "think", "going", "run", "out",
                "will", "when", "next", "there", "any", "all", "our", "we", "hello", "help", "can", "you"}
        category_words = {w for c in CATEGORIES for w in re.findall(r"[a-z]+", c.lower())}
        words = [w for w in re.findall(r"[a-z]+", q.lower())
                 if w not in stop and w not in category_words and len(w) > 2]
        if len(words) < 2:                                # one stray word is not a product name
            return None
        res = self.tools.find_products(" ".join(words))
        calls.append({"tool": "find_products", "args": res.args})
        results.append(res)
        if not res.rows or res.rows[0]["match_score"] < 2:
            return None
        best = [r for r in res.rows if r["match_score"] == res.rows[0]["match_score"]]
        if len(best) == 1:
            return best[0]["product_id"]
        # Several equally good name matches: answer for the one most at risk of running out, mention the rest.
        status = {r["product_id"]: self.tools.get_product_status(r["product_id"]) for r in best}
        results.extend(status.values())
        cover = {pid: min((x["days_of_cover"] for x in st.rows if x["ref"].startswith("risk:")
                           and x["days_of_cover"] is not None), default=float("inf")) for pid, st in status.items()}
        chosen = min(cover, key=cover.get)
        self.other_matches = [f"{r['product_name']} ({r['product_id']})" for r in best if r["product_id"] != chosen]
        return chosen

    def ask(self, question: str) -> tuple[str, list[ToolResult], list[dict]]:
        q = question.lower()
        results, calls = [], []
        self.other_matches = []

        def run(name, **args):
            res = self.tools.call(name, args)
            calls.append({"tool": name, "args": args})
            results.append(res)
            return res

        wants_reviews = any(w in q for w in ("review", "customers say", "complain", "rating", "feedback"))
        wants_sales = any(w in q for w in ("revenue", "sales", "how much did we", "orders", "selling overall"))
        wants_restock = any(w in q for w in ("restock", "run out", "running out", "stock out", "stockout",
                                             "reorder", "out of stock", "sell out"))
        # A named product (id, or name words matching a catalog entry) takes priority over the general list.
        product = None if wants_sales and "P0" not in question.upper() else self._product(question, results, calls)

        if product and wants_reviews:
            res = run("get_review_summary", product_id=product)
            if not res.rows:
                return f"I have no review summary for {product}: {res.note}.", results, calls
            r = res.rows[0]
            return (f"{r['product_name']} ({product}): {r['summary']} [{r['ref']}]", results, calls)

        if product:
            res = run("get_product_status", product_id=product)
            drivers = [r for r in res.rows if r["ref"].startswith("drivers:")]
            risk = [r for r in res.rows if r["ref"].startswith("risk:")]
            if not res.rows:
                return f"I couldn't find {product} in the warehouse.", results, calls
            lines = []
            if drivers:
                d = drivers[0]
                why = {"Promotion": f"an active promotion lifting sales x{d['promo_lift']}",
                       "Rising demand": f"rising demand ({d['trend_pct']}% vs the prior 28 days)",
                       "Falling demand": f"falling demand ({d['trend_pct']}% vs the prior 28 days)",
                       "Supply constrained": f"stockouts - {round(100 * (d['oos_view_share'] or 0), 1)}% of page "
                                             f"views saw it out of stock",
                       "Stable": "steady demand"}[d["primary_driver"]]
                lines.append(f"{d['product_name']} ({product}) - primary driver: {d['primary_driver']}, i.e. {why}. "
                             f"It sold {d['units_last_28d']} units in the last {d['window_days']} days vs "
                             f"{d['units_prior_28d']} before [{d['ref']}].")
            urgent = [r for r in risk if r["risk_tier"] in ("Out of stock", "High")]
            for r in urgent or risk[:1]:
                lines.append(f"- {r['fc_id']}: {r['risk_tier']} - {r['on_hand_units']} on hand, "
                             f"{r['days_of_cover']} days of cover at {r['daily_demand']} units/day; "
                             f"{r['recommended_action']} [{r['ref']}].")
            if not urgent and risk:
                lines.append(f"No fulfillment center is at high risk ({len(risk)} checked).")
            if self.other_matches:
                lines.append("Other products matching that name: " + ", ".join(self.other_matches) + ".")
            return "\n".join(lines), results, calls

        if wants_sales and not wants_restock:
            days = int(m.group(1)) if (m := re.search(r"(\d+)\s*days?", q)) else 28
            res = run("get_sales_summary", days=days, category=self._category(question))
            if not res.rows:
                return "I found no sales for that period.", results, calls
            top = res.rows[0]
            text = (f"{top['category']}: ${top['net_revenue']:,.2f} net revenue from {top['orders']:,} orders in the "
                    f"last {days} days, {top['change_pct']:+}% vs the {days} days before [{top['ref']}].")
            others = [r for r in res.rows[1:4]]
            if others:
                text += "\nTop categories: " + "; ".join(
                    f"{r['category']} ${r['net_revenue']:,.2f} ({r['change_pct']:+}%) [{r['ref']}]" for r in others) + "."
            return text, results, calls

        if wants_restock or "need" in q:
            fc, cat = self._fc(question), self._category(question)
            res = run("get_restock_list", fc_id=fc, category=cat, limit=5)
            if not res.rows:
                return "Nothing needs restocking this week for that filter.", results, calls
            where = f" at {fc}" if fc else ""
            lines = [f"These need restocking this week{where} (most revenue at risk first, as of {res.as_of}):"]
            for r in res.rows:
                lines.append(f"- {r['product_name']} ({r['product_id']}, {r['fc_id']}): {r['risk_tier']}, "
                             f"{r['on_hand_units']} on hand, {r['days_of_cover']} days of cover, stockout "
                             f"{r['projected_stockout_date']} - {r['recommended_action']}; driver: "
                             f"{r['primary_driver']} [{r['ref']}]")
            return "\n".join(lines), results, calls

        return ("I can answer questions about what needs restocking (optionally by fulfillment center or category), "
                "why a product is selling the way it is, what customers say in reviews, and recent sales. "
                "Try: \"Which products need restocking this week in Boston?\" or \"Why is P00121 selling so fast?\"",
                results, calls)


# ---------------------------------------------------------------------------

class Assistant:
    def __init__(self, mode: str | None = None, tools: RetailTools | None = None, client=None):
        self.tools = tools or RetailTools()
        self.mode = mode or ("claude" if (client is not None or claude_available()) else "offline")
        self.backend = ClaudeBackend(self.tools, client) if self.mode == "claude" else OfflineBackend(self.tools)

    def ask(self, question: str) -> Answer:
        text, results, calls = self.backend.ask(question)
        payloads = [json.loads(r.to_json()) for r in results]
        return Answer(question=question, text=text, mode=self.mode, tool_calls=calls,
                      citations=cited_rows(text, results),
                      grounding=grounding.check(text, payloads, question))


def main() -> None:
    parser = argparse.ArgumentParser(description="Ask the ShopSense assistant a question.")
    parser.add_argument("question")
    parser.add_argument("--mode", choices=["claude", "offline"])
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    answer = Assistant(args.mode).ask(args.question)
    if args.json:
        print(json.dumps(answer.to_dict(), indent=2, default=str))
        return
    print(f"[{answer.mode} mode]\n\n{answer.text}\n")
    g = answer.grounding
    print(f"grounding: {'OK' if g['grounded'] else 'UNSUPPORTED ' + str(g['unsupported'])} "
          f"({g['checked']} numbers checked, {len(answer.citations)} rows cited, "
          f"tools: {', '.join(c['tool'] for c in answer.tool_calls) or 'none'})")


if __name__ == "__main__":
    main()
