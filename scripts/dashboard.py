"""CnesData Analytics — home page (Visão Geral) com container de status."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import streamlit as st

import config
from dashboard_components import (
    inject_css,
    render_aggrid_table,
    render_kpi_card,
    render_status_banner,
    setup_sidebar,
)
from dashboard_status import (
    REGRAS_FONTE,
    carregar_status,
    renderizar_container_diretorios,
    renderizar_container_status,
)
from storage.historico_reader import HistoricoReader

st.set_page_config(
    page_title="CnesData Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

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
_FONTE_BIGQUERY = "bigquery"
_FONTE_HR = "hr"


def _fonte_ok(regra: str, status: dict, pipeline_run: dict | None) -> bool:
    if pipeline_run is None:
        return status[REGRAS_FONTE[regra]].ok is True
    if not pipeline_run.get("local_disponivel"):
        return False
    fonte = REGRAS_FONTE[regra]
    if fonte == _FONTE_BIGQUERY and not pipeline_run.get("nacional_disponivel"):
        return False
    if fonte == _FONTE_HR and not pipeline_run.get("hr_disponivel"):
        return False
    return True


@st.cache_resource
def _get_reader() -> HistoricoReader:
    return HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)


@st.cache_data(ttl=300)
def _get_status() -> dict:
    return carregar_status(config.LAST_RUN_PATH, config.DUCKDB_PATH)


if "reader" not in st.session_state:
    st.session_state["reader"] = _get_reader()

st.title("📊 Visão Geral")

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias()
cobertura = reader.contar_competencias()

if not competencias:
    render_status_banner(
        "Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.",
        "warning",
    )
    status = _get_status()
    renderizar_container_status(status, [], (0, 0), config.DUCKDB_PATH)
    renderizar_container_diretorios(config.OUTPUT_PATH, config.HISTORICO_DIR, config.DUCKDB_PATH)
    st.stop()

status = _get_status()
renderizar_container_status(status, competencias, cobertura, config.DUCKDB_PATH)
renderizar_container_diretorios(config.OUTPUT_PATH, config.HISTORICO_DIR, config.DUCKDB_PATH)

competencia = setup_sidebar(reader)

pipeline_run   = reader.carregar_pipeline_run(competencia)
kpis           = reader.carregar_kpis(competencia)
deltas         = reader.carregar_delta(competencia)
total_vinculos = reader.carregar_total_vinculos(competencia)

if pipeline_run and not pipeline_run.get("local_disponivel"):
    render_status_banner(
        "Competência processada sem dados locais (CNES Firebird indisponível). "
        "Auditorias requerem dados locais — KPIs exibidos como —.",
        "info",
    )

col_vinculos, *_ = st.columns([1, 1, 1, 1, 1, 1])
render_kpi_card(col_vinculos, label="Vínculos processados", value=total_vinculos)

st.divider()

cols = st.columns(len(_KPI_DESTAQUE))
for i, regra in enumerate(_KPI_DESTAQUE):
    desc, sev = _REGRAS_META[regra]
    fonte_ok = _fonte_ok(regra, status, pipeline_run)
    delta = deltas.get(regra, 0)
    if fonte_ok:
        render_kpi_card(
            cols[i],
            label=f"{_SEV_ICON[sev]} {desc}",
            value=kpis.get(regra),
            delta=f"+{delta}" if delta > 0 else str(delta),
        )
    else:
        render_kpi_card(cols[i], label=f"{_SEV_ICON[sev]} {desc}", value="—")

st.divider()

if total_vinculos == 0 and pipeline_run and pipeline_run.get("local_disponivel"):
    render_status_banner("Pipeline rodou mas não processou vínculos. Verifique os logs.", "warning")
elif not kpis and pipeline_run and pipeline_run.get("local_disponivel"):
    render_status_banner("Dados de auditoria não encontrados para esta competência.", "warning")
elif kpis and all(
    (kpis.get(r) or 0) == 0
    for r, _ in _REGRAS_META.items()
    if _fonte_ok(r, status, pipeline_run)
):
    render_status_banner(
        "Nenhuma anomalia detectada nas fontes configuradas. "
        "Se esperava resultados, verifique os logs do pipeline.",
        "info",
    )

rows = []
for regra, (desc, sev) in sorted(_REGRAS_META.items(), key=lambda x: _SEV_ORDER[x[1][1]]):
    fonte_ok = _fonte_ok(regra, status, pipeline_run)
    rows.append({
        "Regra":      regra,
        "Descrição":  desc,
        "Anomalias":  kpis.get(regra) if fonte_ok else "—",
        "Δ mês":      (f"+{deltas.get(regra,0)}" if deltas.get(regra,0) > 0
                       else str(deltas.get(regra,0))) if fonte_ok else "—",
        "Severidade": f"{_SEV_ICON[sev]} {sev}",
    })

df_resumo = pd.DataFrame(rows)
render_aggrid_table(df_resumo)
