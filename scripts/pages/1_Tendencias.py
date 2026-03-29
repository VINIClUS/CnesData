"""Página 2 — Tendências: gráfico de linhas Plotly multi-regra com filtros."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import plotly.express as px
import streamlit as st

from storage.historico_reader import CSV_MAP, HistoricoReader

_TODAS_REGRAS = list(CSV_MAP.keys())

_CORES: dict[str, str] = {
    "RQ008": "#e74c3c", "GHOST":     "#c0392b",
    "RQ006": "#e67e22", "RQ007":     "#d35400",
    "RQ009": "#f39c12", "MISSING":   "#e74c3c",
    "RQ003B": "#f1c40f", "RQ005_ACS": "#f1c40f", "RQ005_ACE": "#e6b800",
    "RQ010": "#f39c12", "RQ011": "#2ecc71",
}

st.title("📈 Tendências")

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

regras_sel = st.sidebar.multiselect(
    "Regras",
    options=_TODAS_REGRAS,
    default=_TODAS_REGRAS,
)
comp_ini = st.sidebar.selectbox("De", options=competencias, index=0)
comp_fim = st.sidebar.selectbox("Até", options=competencias, index=len(competencias) - 1)

if not regras_sel:
    st.info("Selecione ao menos uma regra na sidebar.")
    st.stop()

df = reader.carregar_tendencias(regras_sel, comp_ini, comp_fim)

if df.empty:
    st.info("Sem dados para o período e regras selecionados.")
    st.stop()

if df["data_competencia"].nunique() < 2:
    st.info("Selecione ao menos 2 competências para visualizar a tendência.")

color_map = {r: _CORES.get(r, "#95a5a6") for r in regras_sel}
fig = px.line(
    df,
    x="data_competencia",
    y="total_anomalias",
    color="regra",
    markers=True,
    color_discrete_map=color_map,
    labels={
        "data_competencia": "Competência",
        "total_anomalias":  "Anomalias",
        "regra":            "Regra",
    },
    template="plotly_dark",
)
fig.update_layout(hovermode="x unified", legend_title_text="Regra")
fig.update_xaxes(type="category", tickangle=-45)
st.plotly_chart(fig, use_container_width=True)

if st.checkbox("Mostrar dados brutos"):
    st.dataframe(df, use_container_width=True, hide_index=True)

st.download_button(
    "⬇ Exportar tabela",
    data=df.to_csv(index=False).encode("utf-8-sig"),
    file_name="tendencias_cnes.csv",
    mime="text/csv",
)
