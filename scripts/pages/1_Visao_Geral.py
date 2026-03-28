"""Página 1 — Visão Geral: KPI cards e tabela resumo por severidade."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import pandas as pd
import streamlit as st

from storage.historico_reader import HistoricoReader

_REGRAS_META: dict[str, tuple[str, str]] = {
    "RQ008":     ("Prof Fantasma (CNS)",      "CRÍTICA"),
    "GHOST":     ("Ghost Payroll",             "CRÍTICA"),
    "RQ006":     ("Estab Fantasma",            "ALTA"),
    "RQ007":     ("Estab Ausente Local",       "ALTA"),
    "RQ009":     ("Prof Ausente Local",        "ALTA"),
    "MISSING":   ("Missing Registration",      "ALTA"),
    "RQ005_ACS": ("ACS/TACS Incorretos",       "ALTA"),
    "RQ005_ACE": ("ACE/TACE Incorretos",       "ALTA"),
    "RQ003B":    ("Múltiplas Unidades",        "MÉDIA"),
    "RQ010":     ("Divergência CBO",           "MÉDIA"),
    "RQ011":     ("Divergência CH",            "BAIXA"),
}
_SEV_ORDER = {"CRÍTICA": 0, "ALTA": 1, "MÉDIA": 2, "BAIXA": 3}
_SEV_ICON  = {"CRÍTICA": "🔴", "ALTA": "🟠", "MÉDIA": "🟡", "BAIXA": "🟢"}
_KPI_DESTAQUE = ["RQ008", "RQ006", "RQ009", "GHOST", "MISSING"]

st.title("📊 Visão Geral")

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

competencia = st.sidebar.selectbox(
    "Competência",
    options=competencias[::-1],
    index=0,
)

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
    delta = deltas.get(regra, 0)
    with cols[i]:
        st.metric(
            label=f"{_SEV_ICON[sev]} {desc}",
            value=kpis.get(regra, 0),
            delta=f"+{delta}" if delta > 0 else str(delta),
            delta_color="inverse",
            help=f"Regra {regra} — Severidade: {sev}",
        )

st.divider()

if total_vinculos == 0:
    st.warning(
        "Pipeline rodou mas não processou vínculos. Verifique os logs."
    )
elif not kpis:
    st.warning(
        "Dados de auditoria não encontrados para esta competência. Reexecute o pipeline."
    )
elif sum(kpis.values()) == 0:
    st.info(
        "Nenhuma anomalia detectada nesta competência. "
        "Se esperava resultados, verifique se o pipeline rodou com dados nacionais (BigQuery habilitado)."
    )

st.divider()

rows = [
    {
        "Regra":      regra,
        "Descrição":  desc,
        "Anomalias":  kpis.get(regra, 0),
        "Δ mês":      f"+{deltas.get(regra,0)}" if deltas.get(regra,0) > 0 else str(deltas.get(regra,0)),
        "Severidade": sev,
    }
    for regra, (desc, sev) in sorted(
        _REGRAS_META.items(), key=lambda x: _SEV_ORDER[x[1][1]]
    )
]

st.dataframe(
    pd.DataFrame(rows),
    use_container_width=True,
    hide_index=True,
)
