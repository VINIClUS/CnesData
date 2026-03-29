"""CnesData Analytics — entry point do dashboard Streamlit."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import streamlit as st

import config
from storage.historico_reader import HistoricoReader

st.set_page_config(
    page_title="CnesData Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

_CSS = """
<style>
@media (prefers-color-scheme: light) {
    .stApp { background-color: #ffffff !important; color: #1a1a2e !important; }
    section[data-testid="stSidebar"] { background-color: #f0f2f6 !important; }
    .stDataFrame { color: #1a1a2e !important; }
}
.stDataFrame { overflow-x: auto; }
</style>
"""
st.markdown(_CSS, unsafe_allow_html=True)


@st.cache_resource
def _get_reader() -> HistoricoReader:
    return HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)


if "reader" not in st.session_state:
    st.session_state["reader"] = _get_reader()

st.sidebar.title("CnesData Analytics")
st.sidebar.caption("Presidente Epitácio/SP")
