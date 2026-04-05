"""Shared UI primitives for all CnesData dashboard pages."""
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, ColumnsAutoSizeMode, GridOptionsBuilder

from storage.historico_reader import HistoricoReader

_CSS = """
<style>
[data-testid="stMetric"] {
    background-color: rgba(28, 131, 225, 0.05);
    border: 1px solid rgba(28, 131, 225, 0.15);
    border-radius: 8px;
    padding: 1rem;
}
[data-testid="stMetricLabel"] { font-size: 0.85rem; }
[data-testid="stMetricValue"] { font-size: 1.4rem; font-weight: 600; }
@media (prefers-color-scheme: light) {
    .stApp { background-color: #ffffff !important; color: #1a1a2e !important; }
    section[data-testid="stSidebar"] { background-color: #f0f2f6 !important; }
}
.stDataFrame { overflow-x: auto; }
</style>
"""


def inject_css() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


def setup_sidebar(reader: HistoricoReader) -> str:
    st.sidebar.title("CnesData Analytics")
    st.sidebar.caption("Presidente Epitácio/SP")
    competencias = reader.listar_competencias()[::-1]
    return st.sidebar.selectbox("Competência", options=competencias, index=0)


def render_status_banner(message: str, kind: str = "info") -> None:
    _KINDS = {"info": st.info, "warning": st.warning, "error": st.error}
    _KINDS.get(kind, st.info)(message)


def render_kpi_card(
    col,
    label: str,
    value: int | str | None,
    delta: int | str | None = None,
) -> None:
    if value is None or value == "—":
        col.metric(label=label, value="—")
        return
    col.metric(
        label=label,
        value=value,
        delta=str(delta) if delta is not None else None,
        delta_color="inverse",
    )


def render_aggrid_table(
    df: pd.DataFrame,
    sortable: bool = True,
    height: int | None = None,
) -> None:
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(resizable=True, sortable=sortable, filter=True)
    layout = "normal" if height else "autoHeight"
    gb.configure_grid_options(domLayout=layout)
    kwargs: dict = dict(
        gridOptions=gb.build(),
        use_container_width=True,
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
        theme="streamlit",
    )
    if height:
        kwargs["height"] = height
    AgGrid(df, **kwargs)
