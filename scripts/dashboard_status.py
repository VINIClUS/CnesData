"""dashboard_status — diagnóstico de dependências para o dashboard CnesData."""

import json
import os
from dataclasses import dataclass
from pathlib import Path

import streamlit as st


@dataclass
class DepStatus:
    """Status de uma dependência do pipeline."""

    ok: bool | None = None
    ts: str | None = None
    erro: str | None = None


def carregar_status(last_run_path: Path, duckdb_path: Path) -> dict[str, DepStatus]:
    """Lê last_run.json e vars de ambiente para diagnosticar cada dependência.

    Args:
        last_run_path: Caminho para data/cache/last_run.json.
        duckdb_path: Caminho para o arquivo DuckDB (verifica existência).

    Returns:
        Dict com chaves 'firebird', 'bigquery', 'hr', 'duckdb'.
    """
    raw = _ler_last_run(last_run_path)
    return {
        "firebird": _status_firebird(raw.get("firebird", {})),
        "bigquery": _status_bigquery(raw.get("bigquery", {})),
        "hr": _status_hr(raw.get("hr", {})),
        "duckdb": _status_duckdb(raw.get("duckdb", {}), duckdb_path),
    }


def renderizar_container_status(
    status: dict[str, DepStatus],
    competencias: list[str],
) -> None:
    """Renderiza st.expander com cards de status das 4 dependências.

    Args:
        status: Resultado de carregar_status().
        competencias: Lista de competências disponíveis no DuckDB (YYYY-MM).
    """
    algum_problema = any(s.ok is False for s in status.values())
    range_str = (
        f"{competencias[0]} → {competencias[-1]}"
        if len(competencias) >= 2
        else (competencias[0] if competencias else "—")
    )
    with st.expander("⚙ Status das dependências", expanded=algum_problema):
        cols = st.columns(4)
        _render_card(cols[0], "CNES Local", "Firebird", status["firebird"], range_str)
        _render_card(
            cols[1],
            "CNES Nacional",
            "BigQuery",
            status["bigquery"],
            range_str if status["bigquery"].ok is True else "—",
        )
        _render_card(
            cols[2],
            "Histórico",
            "DuckDB",
            status["duckdb"],
            f"{len(competencias)} competência(s)",
        )
        _render_card(cols[3], "RH / Folha", "HR/XLSX", status["hr"])


def _render_card(
    col,
    titulo: str,
    fonte: str,
    s: DepStatus,
    range_str: str | None = None,
) -> None:
    with col:
        if s.ok is True:
            icon, label = "🟢", "con."
        elif s.ok is False:
            icon, label = "🔴", f"erro: {s.erro or 'ver logs'}"
        else:
            icon, label = "🟡", "não configurada"
        ts_str = f" · {s.ts[:16].replace('T', ' ')}" if s.ts else ""
        range_html = (
            f'<div style="font-size:10px;margin-top:4px;color:#aaa">'
            f"Range: <strong>{range_str}</strong></div>"
            if range_str
            else ""
        )
        st.markdown(
            f'<div style="border:1px solid #2d4a7a;border-radius:6px;'
            f'padding:8px;background:#0d1b2a">'
            f'<div style="font-size:10px;color:#888">{titulo}</div>'
            f"<div>{icon} <strong>{fonte}</strong></div>"
            f'<div style="color:#888;font-size:11px">{label}{ts_str}</div>'
            f"{range_html}</div>",
            unsafe_allow_html=True,
        )


def _ler_last_run(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _status_firebird(entry: dict) -> DepStatus:
    if not all(os.getenv(v) for v in ("DB_PATH", "DB_PASSWORD", "FIREBIRD_DLL")):
        return DepStatus(ok=None)
    if not entry:
        return DepStatus(ok=None)
    return DepStatus(ok=entry.get("ok"), ts=entry.get("ts"), erro=entry.get("erro"))


def _status_bigquery(entry: dict) -> DepStatus:
    if not os.getenv("GCP_PROJECT_ID"):
        return DepStatus(ok=None)
    if not entry:
        return DepStatus(ok=None)
    return DepStatus(ok=entry.get("ok"), ts=entry.get("ts"), erro=entry.get("erro"))


def _status_hr(entry: dict) -> DepStatus:
    folha = os.getenv("FOLHA_HR_PATH")
    if not folha or not Path(folha).exists():
        return DepStatus(ok=None)
    if not entry:
        return DepStatus(ok=None)
    return DepStatus(ok=entry.get("ok"), ts=entry.get("ts"), erro=entry.get("erro"))


def _status_duckdb(entry: dict, duckdb_path: Path) -> DepStatus:
    if not duckdb_path.exists():
        return DepStatus(ok=False, erro="arquivo não encontrado")
    if not entry:
        return DepStatus(ok=None)
    return DepStatus(ok=entry.get("ok"), ts=entry.get("ts"), erro=entry.get("erro"))
