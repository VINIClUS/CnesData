"""Adapter para API de Dados Abertos do Ministério da Saúde — CNES oficial."""

import logging

import requests

from pipeline.circuit_breaker import CircuitBreaker, CircuitBreakerAberto

logger = logging.getLogger(__name__)

_BASE_URL = "https://apidadosabertos.saude.gov.br/v1/cnes/estabelecimentos"

STATUS_CONFIRMADO = "CRITICO"
STATUS_LAG = "RESOLVIDO: LAG_BASE_DOS_DADOS"
STATUS_INDISPONIVEL = "API_INDISPONIVEL"


class CnesOficialWebAdapter:
    """Consulta estabelecimentos na API DATASUS oficial.

    Args:
        session: Sessão HTTP injetável. None = cria uma nova.
        auth_token: Bearer token opcional (necessário se API exigir autenticação).
        circuit_breaker: CircuitBreaker injetável. None = cria um novo (threshold=3).
    """

    def __init__(
        self,
        session: requests.Session | None = None,
        auth_token: str | None = None,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        self._session = session or requests.Session()
        if auth_token:
            self._session.headers["Authorization"] = f"Bearer {auth_token}"
        self._breaker = circuit_breaker or CircuitBreaker(failure_threshold=3, service_name="DATASUS")

    def verificar_estabelecimento(self, cnes: str) -> str:
        """Verifica se CNES existe na API DATASUS oficial.

        Args:
            cnes: Código CNES (7 dígitos).

        Returns:
            STATUS_CONFIRMADO | STATUS_LAG | STATUS_INDISPONIVEL
        """
        try:
            return self._breaker.call(self._chamar_api, cnes)
        except CircuitBreakerAberto:
            return STATUS_INDISPONIVEL
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
            logger.warning("api_oficial=indisponivel cnes=%s err=%s", cnes, exc)
            return STATUS_INDISPONIVEL

    def _chamar_api(self, cnes: str) -> str:
        resp = self._session.get(f"{_BASE_URL}/{cnes}", timeout=10)
        if resp.status_code in (500, 503):
            raise requests.HTTPError(f"status={resp.status_code} cnes={cnes}")
        return STATUS_LAG if resp.status_code == 200 else STATUS_CONFIRMADO
