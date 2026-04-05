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


@dataclass
class CardInfo:
    """Informações de exibição para um card de status."""

    titulo: str
    fonte: str
    range_str: str | None = None
    detalhe: str | None = None


REGRAS_FONTE: dict[str, str] = {
    "RQ008": "bigquery", "GHOST": "hr",       "RQ006": "bigquery",
    "RQ007": "bigquery", "RQ009": "bigquery",  "MISSING": "hr",
    "RQ005_ACS": "firebird", "RQ005_ACE": "firebird",
    "RQ003B": "firebird", "RQ010": "bigquery", "RQ011": "bigquery",
}


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
    cobertura: tuple[int, int],
    duckdb_path: Path,
) -> None:
    """Renderiza st.expander com cards de status das 5 dependências.

    Args:
        status: Resultado de carregar_status().
        competencias: Lista de competências válidas no DuckDB (YYYY-MM).
        cobertura: Tupla (válidas, total) de reader.contar_competencias().
        duckdb_path: Caminho do arquivo DuckDB (para detalhe do card).
    """
    algum_problema = any(s.ok is False for s in status.values())
    local_range = (
        f"{competencias[0]} → {competencias[-1]}"
        if len(competencias) >= 2
        else (competencias[0] if competencias else "—")
    )
    validas, total = cobertura
    duckdb_range = f"{validas} válidas / {total} disponíveis"
    bq_result = _consultar_range_bigquery() if status["bigquery"].ok is not None else None
    bq_range_str = f"{bq_result[0]} → {bq_result[1]}" if bq_result else "—"
    datasus_status = _verificar_datasus()
    token_detalhe = "token: configurado" if os.getenv("DATASUS_AUTH_TOKEN") else "sem token"
    hr_path = os.getenv("FOLHA_HR_PATH", "")
    hr_detalhe = Path(hr_path).name if hr_path else "não configurado"
    with st.expander("⚙ Status das dependências", expanded=algum_problema):
        cols = st.columns(5)
        _render_card(
            cols[0],
            CardInfo("CNES Local", "Firebird", local_range,
                     Path(os.getenv("DB_PATH", "")).name or "—"),
            status["firebird"],
        )
        _render_card(
            cols[1],
            CardInfo("CNES Nacional", "BigQuery", bq_range_str,
                     os.getenv("GCP_PROJECT_ID", "—")),
            status["bigquery"],
        )
        _render_card(
            cols[2],
            CardInfo("Histórico", "DuckDB", duckdb_range, duckdb_path.name),
            status["duckdb"],
        )
        _render_card(
            cols[3], CardInfo("RH / Folha", "HR/XLSX", None, hr_detalhe), status["hr"]
        )
        _render_card(
            cols[4],
            CardInfo("API DATASUS", "DATASUS", "apidadosabertos.saude.gov.br", token_detalhe),
            datasus_status,
        )


@st.cache_data(ttl=86_400)
def _consultar_range_bigquery() -> tuple[str, str] | None:
    project_id = os.getenv("GCP_PROJECT_ID")
    id_municipio = os.getenv("ID_MUNICIPIO_IBGE7")
    if not project_id or not id_municipio:
        return None
    return _executar_range_query(project_id, id_municipio)


def _executar_range_query(project_id: str, id_municipio: str) -> tuple[str, str] | None:
    if not project_id or not id_municipio:
        return None
    if not id_municipio.isdigit():  # bd.read_sql não suporta bind params; validar antes de interpolar
        return None
    try:
        import basedosdados as bd
        query = f"""
            SELECT
                MIN(CONCAT(CAST(ano AS STRING), '-',
                    LPAD(CAST(mes AS STRING), 2, '0'))) AS min_comp,
                MAX(CONCAT(CAST(ano AS STRING), '-',
                    LPAD(CAST(mes AS STRING), 2, '0'))) AS max_comp
            FROM (
                SELECT DISTINCT ano, mes
                FROM `basedosdados.br_ms_cnes.profissional`
                WHERE id_municipio = '{id_municipio}'
            )
        """
        df = bd.read_sql(query, billing_project_id=project_id)
        if df.empty or df["min_comp"].iloc[0] is None:
            return None
        return str(df["min_comp"].iloc[0]), str(df["max_comp"].iloc[0])
    except Exception:
        return None


@st.cache_data(ttl=3_600)
def _verificar_datasus() -> DepStatus:
    token = os.getenv("DATASUS_AUTH_TOKEN")
    if not token:
        return DepStatus(ok=None)
    return _executar_health_check_datasus(
        "https://apidadosabertos.saude.gov.br/v1/cnes/estabelecimentos",
        token,
    )


def _executar_health_check_datasus(url: str, token: str) -> DepStatus:
    try:
        import requests
        headers = {"Authorization": f"Bearer {token}"}
        r = requests.get(url, headers=headers, params={"limit": 1}, timeout=5)
        if r.status_code < 300:
            return DepStatus(ok=True)
        if r.status_code == 401:
            return DepStatus(ok=False, erro="token inválido")
        return DepStatus(ok=False, erro=f"HTTP {r.status_code}")
    except Exception:
        return DepStatus(ok=False, erro="inacessível")


def _render_card(col, info: CardInfo, s: DepStatus) -> None:
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
            f"Range: <strong>{info.range_str}</strong></div>"
            if info.range_str
            else ""
        )
        detalhe_html = (
            f'<div style="font-size:9px;margin-top:2px;color:#777">{info.detalhe}</div>'
            if info.detalhe
            else ""
        )
        st.markdown(
            f'<div style="border:1px solid #2d4a7a;border-radius:6px;'
            f'padding:8px;background:#0d1b2a">'
            f'<div style="font-size:10px;color:#888">{info.titulo}</div>'
            f"<div>{icon} <strong>{info.fonte}</strong></div>"
            f'<div style="color:#888;font-size:11px">{label}{ts_str}</div>'
            f"{range_html}{detalhe_html}</div>",
            unsafe_allow_html=True,
        )


def renderizar_container_diretorios(
    output_path: Path,
    historico_dir: Path,
    duckdb_path: Path,
) -> None:
    """Renderiza expander com os diretórios de saída configurados.

    Args:
        output_path: Caminho do CSV principal de saída.
        historico_dir: Diretório de histórico de CSVs.
        duckdb_path: Caminho do arquivo DuckDB.
    """
    with st.expander("📁 Diretórios de Saída", expanded=False):
        st.markdown(f"**DuckDB:** `{duckdb_path}`")
        st.markdown(f"**Histórico:** `{historico_dir}`")
        st.markdown(f"**CSV saída:** `{output_path}`")


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
