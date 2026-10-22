"""
Grounding check: every number in an answer must come from a tool result.

Numbers that appear in the question, in references like [restock:P00381@FC-LAX],
or as calendar dates are ignored. Everything else must match a value in a
tool result returned during the same turn - allowing for formatting (1,234 /
$1,234.50 / 12.5%) and rounding to whole numbers or one decimal. An answer
with an unsupported number is flagged, so a fabricated figure can't reach the
user unmarked.
"""
import re
from collections.abc import Iterable

NUMBER = re.compile(r"(?<![\w.])[-+]?\$?\d[\d,]*(?:\.\d+)?%?")
REF = re.compile(r"\[[^\]]*\]")
DATE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
ID = re.compile(r"\b(?:P|FC-?|C|O|R|S|E)\d+\b|\bFC-[A-Z]{3}\b")


def _to_float(token: str) -> float | None:
    t = token.replace("$", "").replace(",", "").rstrip("%")
    try:
        return float(t)
    except ValueError:
        return None


def numbers_in(text: str) -> list[tuple[str, float]]:
    text = DATE.sub(" ", ID.sub(" ", REF.sub(" ", text)))
    out = []
    for m in NUMBER.finditer(text):
        v = _to_float(m.group())
        if v is not None:
            out.append((m.group(), v))
    return out


def _walk(obj) -> Iterable[float]:
    if isinstance(obj, bool):
        return
    if isinstance(obj, (int, float)):
        yield float(obj)
    elif isinstance(obj, str):
        for _, v in numbers_in(obj):
            yield v
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            yield from _walk(v)


def allowed_values(tool_payloads: list) -> set[float]:
    allowed = set()
    for v in _walk(tool_payloads):
        for x in (v, abs(v)):
            allowed.update({round(x, 2), round(x, 1), float(round(x)), round(100 * x, 1), float(round(100 * x))})
    return allowed


def check(answer: str, tool_payloads: list, question: str = "") -> dict:
    """Return {grounded: bool, unsupported: [...], checked: n}."""
    allowed = allowed_values(tool_payloads)
    in_question = {v for _, v in numbers_in(question)}
    unsupported, checked = [], 0
    for token, v in numbers_in(answer):
        if v in in_question:
            continue
        checked += 1
        candidates = {round(v, 2), round(v, 1), float(round(v))}
        if not candidates & allowed:
            unsupported.append(token)
    return {"grounded": not unsupported, "unsupported": unsupported, "checked": checked}
