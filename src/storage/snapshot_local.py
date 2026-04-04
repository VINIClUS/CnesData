"""snapshot_local.py — Persistência parquet de snapshots locais por competência."""
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class SnapshotLocal:
    """Snapshot imutável de profissionais e estabelecimentos de uma competência."""

    df_prof: pd.DataFrame
    df_estab: pd.DataFrame
    cbo_lookup: dict[str, str]


def snapshot_existe(competencia: str, historico_dir: Path) -> bool:
    """Retorna True se o snapshot de profissionais existir para a competência.

    Args:
        competencia: Competência no formato YYYY-MM.
        historico_dir: Diretório raiz do histórico (config.HISTORICO_DIR).

    Returns:
        True quando o arquivo parquet de profissionais existir.
    """
    return (historico_dir / competencia / "snapshot_local_prof.parquet").exists()


def salvar_snapshot(
    competencia: str, historico_dir: Path, snapshot: SnapshotLocal
) -> None:
    """Persiste profissionais, estabelecimentos e cbo_lookup como arquivos locais.

    Args:
        competencia: Competência no formato YYYY-MM.
        historico_dir: Diretório raiz do histórico (config.HISTORICO_DIR).
        snapshot: SnapshotLocal a persistir.
    """
    pasta = historico_dir / competencia
    pasta.mkdir(parents=True, exist_ok=True)
    snapshot.df_prof.to_parquet(pasta / "snapshot_local_prof.parquet", index=False)
    snapshot.df_estab.to_parquet(pasta / "snapshot_local_estab.parquet", index=False)
    (pasta / "snapshot_cbo_lookup.json").write_text(
        json.dumps(snapshot.cbo_lookup, ensure_ascii=False), encoding="utf-8"
    )


def carregar_snapshot(competencia: str, historico_dir: Path) -> SnapshotLocal:
    """Lê snapshot de profissionais, estabelecimentos e cbo_lookup do disco.

    Args:
        competencia: Competência no formato YYYY-MM.
        historico_dir: Diretório raiz do histórico (config.HISTORICO_DIR).

    Returns:
        SnapshotLocal carregado do disco.

    Raises:
        FileNotFoundError: Se algum arquivo do snapshot não existir.
    """
    pasta = historico_dir / competencia
    df_prof = pd.read_parquet(pasta / "snapshot_local_prof.parquet")
    df_estab = pd.read_parquet(pasta / "snapshot_local_estab.parquet")
    cbo_lookup: dict[str, str] = json.loads(
        (pasta / "snapshot_cbo_lookup.json").read_text(encoding="utf-8")
    )
    return SnapshotLocal(df_prof=df_prof, df_estab=df_estab, cbo_lookup=cbo_lookup)
