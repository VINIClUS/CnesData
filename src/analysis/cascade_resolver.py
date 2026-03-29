"""cascade_resolver.py — Resolução de falsos positivos RQ-006 via API DATASUS."""

import logging
import time
from typing import Protocol

import pandas as pd

from ingestion.cnes_oficial_web_adapter import STATUS_LAG

logger = logging.getLogger(__name__)


class VerificadorCnes(Protocol):
    """Contrato para qualquer adapter que verifique estabelecimentos na API oficial."""

    def verificar_estabelecimento(self, cnes: str) -> str: ...


def resolver_lag_rq006(
    df: pd.DataFrame,
    verificador: VerificadorCnes,
    sleep_entre_chamadas: float = 0.5,
) -> pd.DataFrame:
    """Filtra falsos positivos RQ-006 via API DATASUS oficial.

    Itera sobre CNES do DataFrame, consulta a API para cada um e anota
    STATUS_VERIFICACAO. Linhas com STATUS_LAG são removidas do resultado.

    Args:
        df: Estabelecimentos fantasma detectados pela RQ-006.
        verificador: Adapter HTTP compatível com VerificadorCnes Protocol.
        sleep_entre_chamadas: Delay em segundos entre requisições (rate limiting).

    Returns:
        DataFrame anotado com coluna STATUS_VERIFICACAO, sem linhas LAG resolvidas.
    """
    if df.empty:
        return df.copy()

    statuses = []
    for cnes in df["CNES"]:
        statuses.append(verificador.verificar_estabelecimento(cnes))
        time.sleep(sleep_entre_chamadas)

    resultado = df.copy()
    resultado["STATUS_VERIFICACAO"] = statuses

    n_lag = (resultado["STATUS_VERIFICACAO"] == STATUS_LAG).sum()
    n_remanescentes = (resultado["STATUS_VERIFICACAO"] != STATUS_LAG).sum()

    logger.info(
        "cascade_rq006 total=%d lag_removidos=%d remanescentes=%d",
        len(df), n_lag, n_remanescentes,
    )

    return resultado[resultado["STATUS_VERIFICACAO"] != STATUS_LAG].copy()
