from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"


def test_dashboard_renders_without_errors(pipeline, monkeypatch):
    monkeypatch.setenv("SHOPSENSE_WAREHOUSE", str(pipeline["db"]))
    at = AppTest.from_file(str(APP), default_timeout=120).run()
    assert not at.exception
    labels = [m.label for m in at.metric]
    assert "Net revenue (28d)" in labels and "High stockout risk" in labels
    at.selectbox[0].select_index(1).run()
    assert not at.exception
