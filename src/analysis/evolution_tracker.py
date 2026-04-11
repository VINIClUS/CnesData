"""@deprecated: Será reestruturado em pipeline separado. Rastreamento histórico de anomalias por competência CNES — WP-006."""

import json
import logging
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

warnings.warn(
    "evolution_tracker será reestruturado em pipeline separado.",
    DeprecationWarning,
    stacklevel=2,
)

logger = logging.getLogger(__name__)


@dataclass
class Snapshot:
    """Métricas de uma competência CNES."""
    data_competencia: str
    total_vinculos: int
    total_ghost: int
    total_missing: int
    total_rq005: int


@dataclass
class Delta:
    """Variação de métricas entre duas competências consecutivas."""
    data_anterior: str
    data_atual: str
    delta_vinculos: int
    delta_ghost: int
    delta_missing: int
    delta_rq005: int
    tendencia: str


def criar_snapshot(
    data_competencia: str,
    df_vinculos: pd.DataFrame,
    df_ghost: pd.DataFrame,
    df_missing: pd.DataFrame,
    df_rq003b: pd.DataFrame,
    df_acs: pd.DataFrame,
    df_ace: pd.DataFrame,
) -> Snapshot:
    """Cria um snapshot com as contagens de anomalias da competência.

    Args:
        data_competencia: Competência no formato 'YYYY-MM'.
        df_vinculos: Todos os vínculos ativos extraídos do CNES.
        df_ghost: Resultado de detectar_folha_fantasma().
        df_missing: Resultado de detectar_registro_ausente().
        df_rq003b: Resultado de detectar_multiplas_unidades().
        df_acs: Resultado de auditar_lotacao_acs_tacs().
        df_ace: Resultado de auditar_lotacao_ace_tace().

    Returns:
        Snapshot com os totais calculados.
    """
    snapshot = Snapshot(
        data_competencia=data_competencia,
        total_vinculos=len(df_vinculos),
        total_ghost=len(df_ghost),
        total_missing=len(df_missing),
        total_rq005=len(df_rq003b) + len(df_acs) + len(df_ace),
    )
    logger.info(
        "snapshot criado competencia=%s vinculos=%d ghost=%d missing=%d rq005=%d",
        data_competencia,
        snapshot.total_vinculos,
        snapshot.total_ghost,
        snapshot.total_missing,
        snapshot.total_rq005,
    )
    return snapshot


def salvar_snapshot(snapshot: Snapshot, diretorio: Path) -> Path:
    """Persiste o snapshot em JSON no diretório indicado.

    Args:
        snapshot: Snapshot a serializar.
        diretorio: Diretório onde o arquivo será salvo (criado se inexistente).

    Returns:
        Caminho do arquivo JSON criado.
    """
    diretorio.mkdir(parents=True, exist_ok=True)
    caminho = diretorio / f"snapshot_{snapshot.data_competencia}.json"
    caminho.write_text(
        json.dumps(asdict(snapshot), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("snapshot salvo caminho=%s", caminho)
    return caminho


def carregar_snapshots(diretorio: Path) -> list[Snapshot]:
    """Carrega todos os snapshots do diretório, ordenados por data.

    Args:
        diretorio: Diretório contendo arquivos snapshot_*.json.

    Returns:
        Lista de Snapshots ordenada por data_competencia (crescente).
    """
    if not diretorio.exists():
        return []
    arquivos = sorted(diretorio.glob("snapshot_*.json"))
    return [
        Snapshot(**json.loads(arquivo.read_text(encoding="utf-8")))
        for arquivo in arquivos
    ]


def calcular_delta(anterior: Snapshot, atual: Snapshot) -> Delta:
    """Calcula a variação de métricas entre dois snapshots consecutivos.

    Args:
        anterior: Snapshot da competência anterior.
        atual: Snapshot da competência atual.

    Returns:
        Delta com as variações e tendência calculada.
    """
    delta_ghost = atual.total_ghost - anterior.total_ghost
    delta_missing = atual.total_missing - anterior.total_missing
    delta_rq005 = atual.total_rq005 - anterior.total_rq005
    delta_vinculos = atual.total_vinculos - anterior.total_vinculos

    soma_anomalias = delta_ghost + delta_missing + delta_rq005
    if soma_anomalias < 0:
        tendencia = "MELHORA"
    elif soma_anomalias > 0:
        tendencia = "PIORA"
    else:
        tendencia = "ESTAVEL"

    delta = Delta(
        data_anterior=anterior.data_competencia,
        data_atual=atual.data_competencia,
        delta_vinculos=delta_vinculos,
        delta_ghost=delta_ghost,
        delta_missing=delta_missing,
        delta_rq005=delta_rq005,
        tendencia=tendencia,
    )
    logger.info(
        "delta calculado anterior=%s atual=%s tendencia=%s",
        anterior.data_competencia,
        atual.data_competencia,
        tendencia,
    )
    return delta


def historico_completo(diretorio: Path) -> list[Delta]:
    """Retorna todos os deltas consecutivos entre snapshots salvos.

    Args:
        diretorio: Diretório com arquivos snapshot_*.json.

    Returns:
        Lista de Deltas em ordem cronológica. Vazia se < 2 snapshots.
    """
    snapshots = carregar_snapshots(diretorio)
    if len(snapshots) < 2:
        return []
    return [
        calcular_delta(snapshots[i], snapshots[i + 1])
        for i in range(len(snapshots) - 1)
    ]
