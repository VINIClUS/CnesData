"""Página 6 — Delta Snapshot: drift entre rodadas do pipeline por competência."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st

import config
from storage.historico_reader import HistoricoReader

st.title("Delta — Drift do CNES Local")

if "reader" not in st.session_state:
    st.session_state["reader"] = HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

competencia = st.sidebar.selectbox("Competência", options=competencias[::-1], index=0)
raw = reader.carregar_delta_snapshot(competencia)

if raw is None:
    st.info(
        f"Sem delta para {competencia}. "
        "Execute o pipeline com `--force-reingestao` para calcular o drift entre rodadas."
    )
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("Novos vínculos", int(raw.get("n_novos", 0)))
col2.metric("Desligamentos", int(raw.get("n_removidos", 0)))
col3.metric("Alterações de atributo", int(raw.get("n_alterados", 0)))


def _parse(val) -> list:
    if not val:
        return []
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return []


novos = _parse(raw.get("novos_json"))
removidos = _parse(raw.get("removidos_json"))
alterados = _parse(raw.get("alterados_json"))

if novos:
    st.subheader("Novos Vínculos")
    st.dataframe(pd.DataFrame(novos), use_container_width=True, hide_index=True)

if removidos:
    st.subheader("Desligamentos")
    st.dataframe(pd.DataFrame(removidos), use_container_width=True, hide_index=True)

if alterados:
    st.subheader("Alterações")
    st.dataframe(pd.DataFrame(alterados), use_container_width=True, hide_index=True)

if not novos and not removidos and not alterados:
    st.success("Nenhuma diferença encontrada entre esta rodada e a anterior.")
