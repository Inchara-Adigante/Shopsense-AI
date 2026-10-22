"""AI layer: tools, review summaries, grounding, the offline router, and the Claude tool loop (mocked)."""
import json
from types import SimpleNamespace as NS

import duckdb
import pytest

from shopsense.ai import grounding
from shopsense.ai.assistant import MODEL, Assistant
from shopsense.ai.reviews import build as build_reviews
from shopsense.ai.tools import TOOL_SCHEMAS, RetailTools, Warehouse


@pytest.fixture(scope="module")
def tools(pipeline):
    build_reviews(pipeline["db"], method="extractive")
    return RetailTools(Warehouse(pipeline["db"]))


def test_restock_tool_matches_the_mart(pipeline, tools):
    res = tools.get_restock_list(limit=100)
    con = duckdb.connect(str(pipeline["db"]), read_only=True)
    [(n,)] = con.execute("SELECT COUNT(*) FROM mart.restock_this_week").fetchall()
    con.close()
    assert len(res.rows) == min(n, 100)
    assert all(r["ref"] == f"restock:{r['product_id']}@{r['fc_id']}" for r in res.rows)
    assert all(r["risk_tier"] in ("High", "Out of stock") for r in res.rows)


def test_review_summaries_use_sql_numbers(pipeline, tools):
    con = duckdb.connect(str(pipeline["db"]), read_only=True)
    pid, n, avg = con.execute("""SELECT p.product_id, COUNT(*), ROUND(AVG(rating), 2) FROM dw.fact_review r
                                 JOIN dw.dim_product p USING (product_key) GROUP BY 1 ORDER BY 2 DESC LIMIT 1""").fetchone()
    con.close()
    row = tools.get_review_summary(pid).rows[0]
    assert row["n_reviews"] == n and row["avg_rating"] == avg
    assert f"{n} reviews" in row["summary"]


def test_grounding_flags_numbers_that_no_tool_returned():
    payloads = [{"rows": [{"on_hand_units": 14, "days_of_cover": 6.4, "revenue": 1234.5, "share": 0.152}]}]
    ok = grounding.check("14 on hand, 6.4 days of cover, $1,234.50 revenue, 15.2% share [restock:P00121@FC-LAX]",
                         payloads)
    assert ok["grounded"], ok
    bad = grounding.check("14 on hand, about 3 weeks of cover, 9,999 units next month", payloads)
    assert not bad["grounded"] and set(bad["unsupported"]) == {"3", "9,999"}
    # numbers inside refs, ids and dates are not claims
    assert grounding.check("P00121 at FC-LAX stocks out on 2026-07-02 [risk:P00121@FC-LAX]", payloads)["grounded"]


@pytest.mark.parametrize("question, tool", [
    ("Which products need restocking this week in Boston?", "get_restock_list"),
    ("Why is P00121 selling so fast?", "get_product_status"),
    ("How were sales in the last 14 days?", "get_sales_summary"),
])
def test_offline_answers_are_routed_cited_and_grounded(tools, question, tool):
    answer = Assistant(mode="offline", tools=tools).ask(question)
    assert tool in [c["tool"] for c in answer.tool_calls]
    assert answer.grounding["grounded"], (answer.text, answer.grounding)
    assert answer.citations, answer.text


def test_offline_review_question(pipeline, tools):
    con = duckdb.connect(str(pipeline["db"]), read_only=True)
    [(pid,)] = con.execute("SELECT product_id FROM ai.review_summaries ORDER BY n_reviews DESC LIMIT 1").fetchall()
    con.close()
    answer = Assistant(mode="offline", tools=tools).ask(f"What do customers say about {pid}?")
    assert [c["tool"] for c in answer.tool_calls] == ["get_review_summary"]
    assert answer.citations[0]["ref"] == f"reviews:{pid}" and answer.grounding["grounded"]


def test_offline_restock_filter_by_city(tools):
    answer = Assistant(mode="offline", tools=tools).ask("What needs restocking in Boston?")
    call = next(c for c in answer.tool_calls if c["tool"] == "get_restock_list")
    assert call["args"]["fc_id"] == "FC-BOS"
    assert all(c["fc_id"] == "FC-BOS" for c in answer.citations)


def test_tool_schemas_are_strict():
    for t in TOOL_SCHEMAS:
        assert t["strict"] is True
        schema = t["input_schema"]
        assert schema["additionalProperties"] is False
        assert set(schema["required"]) == set(schema["properties"])


# ---------------------------------------------------------------- Claude loop with a scripted fake client

class FakeClaude:
    """Plays back scripted responses and records every request (no network)."""

    def __init__(self, script):
        self.script, self.requests = list(script), []
        self.beta = NS(messages=NS(create=self._create))

    def _create(self, **kwargs):
        self.requests.append(json.loads(json.dumps(kwargs, default=lambda o: getattr(o, "__dict__", str(o)))))
        return self.script.pop(0)(kwargs)


def tool_use(name, args):
    return lambda _: NS(stop_reason="tool_use", content=[
        NS(type="text", text="Let me check."), NS(type="tool_use", id="tu_1", name=name, input=args)])


def final_from_tool_result(fmt):
    """Final answer built from the tool_result the loop sent back - like a model citing what it read."""
    def respond(kwargs):
        result = kwargs["messages"][-1]["content"][0]
        rows = json.loads(result["content"])["rows"]
        return NS(stop_reason="end_turn", content=[NS(type="text", text=fmt(rows))])
    return respond


def test_claude_loop_runs_tools_and_returns_a_grounded_cited_answer(tools):
    fake = FakeClaude([
        tool_use("get_restock_list", {"fc_id": "FC-BOS", "category": None, "limit": 3}),
        final_from_tool_result(lambda rows: "Restock first: " + "; ".join(
            f"{r['product_name']} has {r['on_hand_units']} on hand and {r['days_of_cover']} days of cover [{r['ref']}]"
            for r in rows)),
    ])
    answer = Assistant(tools=tools, client=fake).ask("What should we restock in Boston?")
    assert answer.mode == "claude"
    assert answer.tool_calls == [{"tool": "get_restock_list", "args": {"fc_id": "FC-BOS", "category": None, "limit": 3}}]
    assert answer.grounding["grounded"] and answer.citations
    first, second = fake.requests
    assert first["model"] == MODEL and first["fallbacks"] == "default"
    assert "server-side-fallback-2026-07-01" in first["betas"]
    assert first["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert all(t["strict"] for t in first["tools"])
    assert second["messages"][-1]["content"][0]["type"] == "tool_result"   # results went back to Claude


def test_claude_answer_with_an_invented_number_is_flagged(tools):
    fake = FakeClaude([
        tool_use("get_sales_summary", {"days": 28, "category": None}),
        lambda _: NS(stop_reason="end_turn", content=[NS(type="text", text="Revenue will reach $9,876,543 next month.")]),
    ])
    answer = Assistant(tools=tools, client=fake).ask("Forecast next month's revenue")
    assert not answer.grounding["grounded"]
    assert "$9,876,543" in answer.grounding["unsupported"]


def test_claude_tool_errors_are_reported_back_not_raised(tools):
    fake = FakeClaude([
        tool_use("no_such_tool", {}),
        lambda kwargs: NS(stop_reason="end_turn", content=[NS(type="text", text="That data isn't available.")]),
    ])
    answer = Assistant(tools=tools, client=fake).ask("anything")
    sent = fake.requests[1]["messages"][-1]["content"][0]
    assert sent["is_error"] is True and "unknown tool" in sent["content"]
    assert answer.text == "That data isn't available."


def test_claude_refusal_is_handled(tools):
    fake = FakeClaude([lambda _: NS(stop_reason="refusal", content=[])])
    answer = Assistant(tools=tools, client=fake).ask("...")
    assert answer.text == "I can't help with that request."
