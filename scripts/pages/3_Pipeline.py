"""Página 3 — Executar Pipeline: trigger, log streaming e stop."""
import queue
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

import os

import streamlit as st

import config
from dashboard_components import inject_css, setup_sidebar
from pipeline_runner import competencia_atual, iniciar_leitor, iniciar_pipeline
from storage.historico_reader import HistoricoReader

_ANO_MIN = 2000
_ANO_MAX = 2099

inject_css()

if "reader" not in st.session_state:
    st.session_state["reader"] = HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)

reader: HistoricoReader = st.session_state["reader"]
_ = setup_sidebar(reader)

st.title("Executar Pipeline")

# ── Inicializar session_state ──────────────────────────────────────────────
for key, default in [
    ("pipeline_proc", None),
    ("pipeline_log_queue", None),
    ("pipeline_logs", []),
    ("pipeline_status", "idle"),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Opções avançadas ──────────────────────────────────────────────────────
ano_default, mes_default = competencia_atual()
hr_disponivel = os.getenv("FOLHA_HR_PATH") is not None

with st.expander("Opções avançadas"):
    col1, col2 = st.columns(2)
    with col1:
        ano = st.number_input("Ano", min_value=_ANO_MIN, max_value=_ANO_MAX, value=ano_default)
    with col2:
        mes = st.number_input("Mês", min_value=1, max_value=12, value=mes_default)
    skip_nacional = not st.toggle("Nacional (BigQuery)", value=True)
    skip_hr = not st.toggle(
        "RH (Folha de Pagamento)",
        value=hr_disponivel,
        disabled=not hr_disponivel,
        help=None if hr_disponivel else "FOLHA_HR_PATH não configurado no .env",
    )

competencia = f"{int(ano)}-{int(mes):02d}"

# ── Botão de execução ─────────────────────────────────────────────────────
em_execucao = st.session_state["pipeline_status"] == "running"

if st.button("Executar pipeline agora", disabled=em_execucao, type="primary"):
    proc = iniciar_pipeline(competencia, skip_nacional=skip_nacional, skip_hr=skip_hr)
    q = iniciar_leitor(proc)
    st.session_state["pipeline_proc"] = proc
    st.session_state["pipeline_log_queue"] = q
    st.session_state["pipeline_logs"] = []
    st.session_state["pipeline_status"] = "running"
    st.rerun()

# ── Banner de status ──────────────────────────────────────────────────────
status = st.session_state["pipeline_status"]
if status == "running":
    st.info(f"Pipeline em execução — competência {competencia}…")
elif status == "done":
    st.success("Pipeline concluído com sucesso.")
elif status == "error":
    st.error("Pipeline encerrado com erro ou interrompido. Verifique os logs abaixo.")

# ── Fragment de streaming ─────────────────────────────────────────────────
@st.fragment(run_every=1)
def _streaming() -> None:
    q: queue.Queue = st.session_state["pipeline_log_queue"]
    proc = st.session_state["pipeline_proc"]

    for _ in range(50):
        try:
            linha = q.get_nowait()
            st.session_state["pipeline_logs"].append(linha)
        except queue.Empty:
            break

    with st.expander("Logs", expanded=True):
        st.code("".join(st.session_state["pipeline_logs"]), language=None)

    if st.button("Parar execução", type="secondary"):
        proc.terminate()
        st.session_state["pipeline_status"] = "error"
        st.rerun()

    returncode = proc.poll()
    if returncode is not None:
        st.session_state["pipeline_status"] = "done" if returncode == 0 else "error"
        st.rerun()


if st.session_state["pipeline_status"] == "running":
    _streaming()

elif st.session_state["pipeline_logs"]:
    with st.expander("Logs da última execução", expanded=False):
        st.code("".join(st.session_state["pipeline_logs"]), language=None)
