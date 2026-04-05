"""Página 2 — Por Regra: drill-down com lazy load, AgGrid e PII masking."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

from dashboard_components import inject_css, render_aggrid_table, setup_sidebar

import config
from dashboard_status import carregar_status, REGRAS_FONTE
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


@st.cache_data(ttl=300)
def _get_status() -> dict:
    return carregar_status(config.LAST_RUN_PATH, config.DUCKDB_PATH)


def _invalidar_cache_se_competencia_mudou(competencia: str) -> None:
    if st.session_state.get("_por_regra_competencia") != competencia:
        for key in [k for k in st.session_state if k.startswith("df_")]:
            del st.session_state[key]
        st.session_state["_por_regra_competencia"] = competencia


def _carregar_tab(reader: HistoricoReader, regra: str, competencia: str):
    key = f"df_{regra}_{competencia}"
    if key not in st.session_state:
        with st.spinner(f"Carregando {_REGRA_DESC[regra]}..."):
            st.session_state[key] = reader.carregar_glosas_periodo(regra, competencia)
    return st.session_state[key]


def _mascarar_pii(df, mostrar_completo: bool):
    if mostrar_completo:
        return df
    df_display = df.copy()
    for col in df_display.select_dtypes(include="object").columns:
        upper = col.upper()
        if "CPF" in upper or "CNS" in upper:
            df_display[col] = df_display[col].apply(
                lambda v: f"***{str(v)[-4:]}" if isinstance(v, str) and len(v) >= 4 else v
            )
    return df_display


def _render_metrica(regra: str, fonte_ok: bool, kpis: dict, deltas: dict) -> None:
    col_metric, _ = st.columns([1, 4])
    with col_metric:
        if fonte_ok:
            valor = kpis.get(regra, 0)
            delta = deltas.get(regra, 0)
            st.metric(
                label=_REGRA_DESC[regra],
                value=valor,
                delta=f"+{delta}" if delta > 0 else str(delta),
                delta_color="inverse",
            )
        else:
            st.metric(label=_REGRA_DESC[regra], value="—",
                      help=f"Fonte '{REGRAS_FONTE[regra]}' não configurada")


st.title("🔍 Por Regra")
inject_css()

if "reader" not in st.session_state:
    st.session_state["reader"] = HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

competencia = setup_sidebar(reader)
_invalidar_cache_se_competencia_mudou(competencia)

status = _get_status()
kpis   = reader.carregar_kpis(competencia)
deltas = reader.carregar_delta(competencia)

tabs = st.tabs([label for label, _ in _TABS])

for tab, (_, regra) in zip(tabs, _TABS):
    with tab:
        fonte = REGRAS_FONTE[regra]
        fonte_ok: bool = status[fonte].ok is True

        _render_metrica(regra, fonte_ok, kpis, deltas)

        if not fonte_ok:
            st.warning(
                f"⚠ Fonte **{fonte}** não disponível para **{_REGRA_DESC[regra]}**. "
                "Configure a dependência no `.env` e reexecute o pipeline."
            )
            continue

        df = _carregar_tab(reader, regra, competencia)

        if df.empty:
            st.info(
                f"Sem registros para **{_REGRA_DESC[regra]}** em {competencia}. "
                "Verifique se o pipeline rodou para essa competência."
            )
            continue

        mostrar_completo = st.checkbox(
            "Mostrar dados completos (CPF/CNS sem máscara)",
            key=f"mask_{regra}",
        )
        df_display = _mascarar_pii(df, mostrar_completo)

        render_aggrid_table(df_display)

        st.download_button(
            f"⬇ Baixar CSV — {regra} / {competencia}",
            data=df.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"auditoria_{regra.lower()}_{competencia}.csv",
            mime="text/csv",
            key=f"dl_{regra}",
        )
