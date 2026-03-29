"""Página 3 — Por Regra: drill-down com tabs horizontais por regra."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st

from storage.historico_reader import HistoricoReader

_TABS: list[tuple[str, str]] = [
    ("🔴 RQ-008",  "RQ008"),
    ("🔴 Ghost",   "GHOST"),
    ("🟠 RQ-006",  "RQ006"),
    ("🟠 RQ-007",  "RQ007"),
    ("🟠 RQ-009",  "RQ009"),
    ("🟠 Missing", "MISSING"),
    ("🟠 RQ-005a", "RQ005_ACS"),
    ("🟠 RQ-005b", "RQ005_ACE"),
    ("🟡 RQ-003B", "RQ003B"),
    ("🟡 RQ-010",  "RQ010"),
    ("🟢 RQ-011",  "RQ011"),
]

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
competencias = reader.listar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

competencia = st.sidebar.selectbox(
    "Competência",
    options=competencias[::-1],
    index=0,
)

kpis   = reader.carregar_kpis(competencia)
deltas = reader.carregar_delta(competencia)

tabs = st.tabs([label for label, _ in _TABS])

for tab, (_, regra) in zip(tabs, _TABS):
    with tab:
        valor = kpis.get(regra, 0)
        delta = deltas.get(regra, 0)

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
            st.warning(
                f"Sem registros arquivados para **{_REGRA_DESC[regra]}** em {competencia}. "
                "Verifique se o pipeline rodou para essa competência."
            )
            continue

        mostrar_completo = st.checkbox(
            "Mostrar dados completos (CPF/CNS sem máscara)",
            key=f"mask_{regra}",
        )
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
            key=f"dl_{regra}",
        )
