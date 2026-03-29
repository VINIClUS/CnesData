"""Adapter para API de Dados Abertos do Ministério da Saúde — CNES oficial."""

import logging

import requests
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://apidadosabertos.saude.gov.br/v1/cnes/estabelecimentos"

STATUS_CONFIRMADO = "CRITICO"
STATUS_LAG = "RESOLVIDO: LAG_BASE_DOS_DADOS"
STATUS_INDISPONIVEL = "API_INDISPONIVEL"


class _ServidorIndisponivel(Exception):
    pass


class CnesOficialWebAdapter:
    """Consulta estabelecimentos na API DATASUS oficial.

    Args:
        session: Sessão HTTP injetável. None = cria uma nova.
        auth_token: Bearer token opcional (necessário se API exigir autenticação).
    """

    def __init__(
        self,
        session: requests.Session | None = None,
        auth_token: str | None = None,
    ) -> None:
        self._session = session or requests.Session()
        if auth_token:
            self._session.headers["Authorization"] = f"Bearer {auth_token}"

    def verificar_estabelecimento(self, cnes: str) -> str:
        """Verifica se CNES existe na API DATASUS oficial.

        Args:
            cnes: Código CNES (7 dígitos).

        Returns:
            STATUS_CONFIRMADO | STATUS_LAG | STATUS_INDISPONIVEL
        """
        try:
            return self._chamar_com_retry(cnes)
        except (RetryError, requests.Timeout, requests.ConnectionError):
            logger.warning("api_oficial=indisponivel cnes=%s", cnes)
            return STATUS_INDISPONIVEL

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(_ServidorIndisponivel),
        reraise=False,
    )
    def _chamar_com_retry(self, cnes: str) -> str:
        resp = self._session.get(f"{_BASE_URL}/{cnes}", timeout=10)
        if resp.status_code in (500, 503):
            raise _ServidorIndisponivel(f"status={resp.status_code} cnes={cnes}")
        return STATUS_LAG if resp.status_code == 200 else STATUS_CONFIRMADO
