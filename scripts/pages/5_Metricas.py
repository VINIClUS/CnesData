"""Página 5 — Métricas Avançadas: KPIs estatísticos e charts Plotly."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import plotly.express as px
import streamlit as st

import config
from metricas_helpers import _parsear_metricas
from storage.historico_reader import HistoricoReader


def _pct(v) -> str:
    return f"{v:.1%}" if v is not None else "—"


def _num(v, decimais: int = 1) -> str:
    return f"{v:.{decimais}f}" if v is not None else "—"


st.title("Métricas Avançadas")

if "reader" not in st.session_state:
    st.session_state["reader"] = HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

competencia = st.sidebar.selectbox("Competência", options=competencias[::-1], index=0)
raw = reader.carregar_metricas_avancadas(competencia)

if raw is None:
    st.info(
        f"Sem métricas avançadas para {competencia}. "
        "Execute o pipeline para esta competência e tente novamente."
    )
    st.stop()

m = _parsear_metricas(raw)

st.subheader("KPIs")
col1, col2, col3 = st.columns(3)
col1.metric("Taxa de Anomalia Geral",       _pct(m.get("taxa_anomalia_geral")))
col2.metric("P90 Carga Horária (h/semana)", _num(m.get("p90_ch_total")))
col3.metric("Proporção Feminina Geral",     _pct(m.get("proporcao_feminina_geral")))

col4, col5, col6 = st.columns(3)
col4.metric("Reincidentes",                 str(m.get("n_reincidentes", "—")))
col5.metric("Taxa de Resolução",            _pct(m.get("taxa_resolucao")))
col6.metric("Velocidade Regularização (d)", _num(m.get("velocidade_regularizacao_media")))

if m["ranking_cnes"]:
    st.subheader("Ranking CNES por Anomalias")
    df_rank = pd.DataFrame(m["ranking_cnes"])
    st.plotly_chart(
        px.bar(df_rank, x="cnes", y="total_anomalias", labels={"cnes": "CNES", "total_anomalias": "Anomalias"}),
        use_container_width=True,
    )

if m["anomalias_por_cbo"]:
    st.subheader("Top 15 CBOs com Anomalias")
    df_cbo = (
        pd.DataFrame(m["anomalias_por_cbo"])[["cbo", "total"]]
        .rename(columns={"total": "n"})
        .sort_values("n", ascending=False)
        .head(15)
    )
    st.plotly_chart(
        px.bar(df_cbo, x="cbo", y="n", labels={"cbo": "CBO", "n": "Anomalias"}),
        use_container_width=True,
    )

if m["top_glosas"]:
    st.subheader("Top Glosas por Regra")
    st.dataframe(pd.DataFrame(m["top_glosas"]), use_container_width=True)
