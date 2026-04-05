"""Página 4 — Glosas: drill-down individual por profissional/competência."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

import config
from glosas_helpers import _filtrar_glosas, _mascarar_pii_glosas
from storage.historico_reader import REGRAS_AUDITORIA, HistoricoReader

_TODAS_REGRAS = list(REGRAS_AUDITORIA)

st.title("Glosas por Profissional")

if "reader" not in st.session_state:
    st.session_state["reader"] = HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

competencia = st.sidebar.selectbox("Competência", options=competencias[::-1], index=0)
regras_sel = st.sidebar.multiselect("Regra", options=_TODAS_REGRAS, default=[])
busca = st.sidebar.text_input("Buscar por nome ou CPF")


@st.cache_data(ttl=300, show_spinner=False)
def _carregar(comp: str) -> pd.DataFrame:
    return reader.carregar_glosas_historicas(competencia_inicio=comp)


df_raw = _carregar(competencia)
df_raw = df_raw[df_raw["competencia"] == competencia] if not df_raw.empty else df_raw
df_filtrado = _filtrar_glosas(df_raw, regras_sel, busca)

st.metric("Glosas encontradas", len(df_filtrado))

if df_filtrado.empty:
    st.info("Sem glosas para os filtros selecionados. Ajuste os filtros ou execute o pipeline.")
    st.stop()

mostrar_completo = st.checkbox("Mostrar dados completos (CPF/CNS sem máscara)")
df_display = _mascarar_pii_glosas(df_filtrado, mostrar_completo)

gb = GridOptionsBuilder.from_dataframe(df_display)
gb.configure_default_column(resizable=True, sortable=True, filter=True)
gb.configure_grid_options(domLayout="autoHeight")
AgGrid(
    df_display,
    gridOptions=gb.build(),
    use_container_width=True,
    fit_columns_on_grid_load=False,
    theme="streamlit",
    key="grid_glosas",
)

st.download_button(
    "Baixar CSV",
    data=df_filtrado.to_csv(index=False).encode("utf-8-sig"),
    file_name=f"glosas_{competencia}.csv",
    mime="text/csv",
)
