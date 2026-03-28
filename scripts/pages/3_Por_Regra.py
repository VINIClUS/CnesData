"""Página 3 — Por Regra: drill-down de registros individuais com máscara CPF/CNS."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st

from storage.historico_reader import HistoricoReader

_REGRA_DESC: dict[str, str] = {
    "RQ003B":    "RQ-003-B — Múltiplas Unidades",
    "RQ005_ACS": "RQ-005 ACS/TACS — Lotação Incorreta",
    "RQ005_ACE": "RQ-005 ACE/TACE — Lotação Incorreta",
    "GHOST":     "Ghost Payroll",
    "MISSING":   "Missing Registration",
    "RQ006":     "RQ-006 — Estabelecimentos Fantasma",
    "RQ007":     "RQ-007 — Estabelecimentos Ausentes Local",
    "RQ008":     "RQ-008 — Profissionais Fantasma (CNS)",
    "RQ009":     "RQ-009 — Profissionais Ausentes Local",
    "RQ010":     "RQ-010 — Divergência CBO",
    "RQ011":     "RQ-011 — Divergência CH",
}

st.title("🔍 Por Regra")

reader: HistoricoReader = st.session_state["reader"]

regra = st.sidebar.selectbox(
    "Regra",
    options=list(_REGRA_DESC.keys()),
    format_func=lambda r: _REGRA_DESC[r],
)

disponiveis = reader.listar_competencias_para_regra(regra)

if not disponiveis:
    st.warning(
        f"Sem registros arquivados para **{_REGRA_DESC[regra]}**. "
        "Execute o pipeline para gerar o histórico."
    )
    st.stop()

competencia = st.sidebar.selectbox(
    "Competência",
    options=disponiveis[::-1],
    index=0,
)

kpis   = reader.carregar_kpis(competencia)
deltas = reader.carregar_delta(competencia)
valor  = kpis.get(regra, 0)
delta  = deltas.get(regra, 0)

col_metric, _ = st.columns([1, 4])
with col_metric:
    st.metric(
        label=_REGRA_DESC[regra],
        value=valor,
        delta=f"+{delta}" if delta > 0 else str(delta),
        delta_color="inverse",
    )

df = reader.carregar_registros(regra, competencia)

if df.empty:
    st.warning(f"Sem registros para {_REGRA_DESC[regra]} em {competencia}.")
    st.stop()

mostrar_completo = st.checkbox("Mostrar dados completos (CPF/CNS sem máscara)")
df_display = df.copy()
if not mostrar_completo:
    for col in df_display.select_dtypes(include="object").columns:
        upper = col.upper()
        if "CPF" in upper or "CNS" in upper:
            df_display[col] = df_display[col].apply(
                lambda v: f"***{str(v)[-4:]}" if isinstance(v, str) and len(v) >= 4 else v
            )

st.dataframe(df_display, use_container_width=True, hide_index=True)

st.download_button(
    f"⬇ Baixar CSV — {regra} / {competencia}",
    data=df.to_csv(index=False).encode("utf-8-sig"),
    file_name=f"auditoria_{regra.lower()}_{competencia}.csv",
    mime="text/csv",
)
