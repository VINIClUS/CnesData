"""CnesData Analytics — home page (Visão Geral) com container de status."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

import config
from dashboard_status import (
    carregar_status,
    renderizar_container_status,
    renderizar_container_diretorios,
    REGRAS_FONTE,
)
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
}
.stDataFrame { overflow-x: auto; }
</style>
"""
st.markdown(_CSS, unsafe_allow_html=True)

_REGRAS_META: dict[str, tuple[str, str]] = {
    "RQ008":     ("Prof Fantasma (CNS)",   "CRÍTICA"),
    "GHOST":     ("Ghost Payroll",          "CRÍTICA"),
    "RQ006":     ("Estab Fantasma",         "ALTA"),
    "RQ007":     ("Estab Ausente Local",    "ALTA"),
    "RQ009":     ("Prof Ausente Local",     "ALTA"),
    "MISSING":   ("Missing Registration",  "ALTA"),
    "RQ005_ACS": ("ACS/TACS Incorretos",   "ALTA"),
    "RQ005_ACE": ("ACE/TACE Incorretos",   "ALTA"),
    "RQ003B":    ("Múltiplas Unidades",    "MÉDIA"),
    "RQ010":     ("Divergência CBO",       "MÉDIA"),
    "RQ011":     ("Divergência CH",        "BAIXA"),
}
_SEV_ORDER = {"CRÍTICA": 0, "ALTA": 1, "MÉDIA": 2, "BAIXA": 3}
_SEV_ICON  = {"CRÍTICA": "🔴", "ALTA": "🟠", "MÉDIA": "🟡", "BAIXA": "🟢"}
_KPI_DESTAQUE = ["RQ008", "RQ006", "RQ009", "GHOST", "MISSING"]


@st.cache_resource
def _get_reader() -> HistoricoReader:
    return HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)


@st.cache_data(ttl=300)
def _get_status() -> dict:
    return carregar_status(config.LAST_RUN_PATH, config.DUCKDB_PATH)


if "reader" not in st.session_state:
    st.session_state["reader"] = _get_reader()

st.sidebar.title("CnesData Analytics")
st.sidebar.caption("Presidente Epitácio/SP")

st.title("📊 Visão Geral")

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias_validas()
cobertura = reader.contar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    status = _get_status()
    renderizar_container_status(status, [], (0, 0), config.DUCKDB_PATH)
    renderizar_container_diretorios(config.OUTPUT_PATH, config.HISTORICO_DIR, config.DUCKDB_PATH)
    st.stop()

status = _get_status()
renderizar_container_status(status, competencias, cobertura, config.DUCKDB_PATH)
renderizar_container_diretorios(config.OUTPUT_PATH, config.HISTORICO_DIR, config.DUCKDB_PATH)

competencia = st.sidebar.selectbox("Competência", options=competencias[::-1], index=0)

kpis           = reader.carregar_kpis(competencia)
deltas         = reader.carregar_delta(competencia)
total_vinculos = reader.carregar_total_vinculos(competencia)

col_vinculos, *_ = st.columns([1, 1, 1, 1, 1, 1])
with col_vinculos:
    st.metric(
        label="Vínculos processados",
        value=total_vinculos,
        help="Total de vínculos profissionais na competência selecionada",
    )

st.divider()

cols = st.columns(len(_KPI_DESTAQUE))
for i, regra in enumerate(_KPI_DESTAQUE):
    desc, sev = _REGRAS_META[regra]
    fonte_ok = status[REGRAS_FONTE[regra]].ok is True
    delta = deltas.get(regra, 0)
    with cols[i]:
        if fonte_ok:
            st.metric(
                label=f"{_SEV_ICON[sev]} {desc}",
                value=kpis.get(regra, 0),
                delta=f"+{delta}" if delta > 0 else str(delta),
                delta_color="inverse",
                help=f"Regra {regra} — Severidade: {sev}",
            )
        else:
            fonte_nome = REGRAS_FONTE[regra].capitalize()
            st.metric(
                label=f"{_SEV_ICON[sev]} {desc}",
                value="—",
                help=f"Regra {regra} — {fonte_nome} não configurado",
            )

st.divider()

if total_vinculos == 0:
    st.warning("Pipeline rodou mas não processou vínculos. Verifique os logs.")
elif not kpis:
    st.warning("Dados de auditoria não encontrados para esta competência.")
elif all(
    kpis.get(r, 0) == 0
    for r, _ in _REGRAS_META.items()
    if status[REGRAS_FONTE[r]].ok is True
):
    st.info(
        "Nenhuma anomalia detectada nas fontes configuradas. "
        "Se esperava resultados, verifique os logs do pipeline."
    )

rows = []
for regra, (desc, sev) in sorted(_REGRAS_META.items(), key=lambda x: _SEV_ORDER[x[1][1]]):
    fonte_ok = status[REGRAS_FONTE[regra]].ok is True
    rows.append({
        "Regra":      regra,
        "Descrição":  desc,
        "Anomalias":  kpis.get(regra, 0) if fonte_ok else "—",
        "Δ mês":      (f"+{deltas.get(regra,0)}" if deltas.get(regra,0) > 0
                       else str(deltas.get(regra,0))) if fonte_ok else "—",
        "Severidade": f"{_SEV_ICON[sev]} {sev}",
    })

df_resumo = pd.DataFrame(rows)
gb = GridOptionsBuilder.from_dataframe(df_resumo)
gb.configure_default_column(resizable=True, sortable=True, filter=True)
gb.configure_grid_options(domLayout="autoHeight")
AgGrid(df_resumo, gridOptions=gb.build(), use_container_width=True,
       fit_columns_on_grid_load=True, theme="streamlit")
