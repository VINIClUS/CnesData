"""Smoke tests for dashboard_components — no Streamlit server needed."""
import importlib
import sys
from unittest.mock import MagicMock


def test_module_exports_five_functions():
    st_mock = MagicMock()
    sys.modules.setdefault("streamlit", st_mock)
    sys.modules.setdefault("st_aggrid", MagicMock())
    sys.modules.setdefault("st_aggrid.shared", MagicMock())

    if "dashboard_components" in sys.modules:
        del sys.modules["dashboard_components"]

    sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent.parent / "scripts"))
    mod = importlib.import_module("dashboard_components")

    for name in ("inject_css", "setup_sidebar", "render_status_banner",
                 "render_kpi_card", "render_aggrid_table"):
        assert callable(getattr(mod, name, None)), f"missing: {name}"
