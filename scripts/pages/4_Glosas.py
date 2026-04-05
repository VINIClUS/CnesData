"""Página 4 — Glosas: drill-down individual por profissional/competência."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st

import config
from dashboard_components import inject_css, render_aggrid_table, setup_sidebar
from glosas_helpers import _filtrar_glosas, _mascarar_pii_glosas
from storage.historico_reader import REGRAS_AUDITORIA, HistoricoReader

_TODAS_REGRAS = list(REGRAS_AUDITORIA)

inject_css()
st.title("Glosas por Profissional")

if "reader" not in st.session_state:
    st.session_state["reader"] = HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)

reader: HistoricoReader = st.session_state["reader"]

if not reader.listar_competencias():
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

competencia = setup_sidebar(reader)
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

render_aggrid_table(df_display)

st.download_button(
    "Baixar CSV",
    data=df_filtrado.to_csv(index=False).encode("utf-8-sig"),
    file_name=f"glosas_{competencia}.csv",
    mime="text/csv",
)
