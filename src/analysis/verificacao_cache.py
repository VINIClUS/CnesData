"""CachingVerificadorCnes — decorator de cache TTL para verificações DATASUS."""

import json
import logging
import time
from pathlib import Path

from analysis.cascade_resolver import VerificadorCnes

logger = logging.getLogger(__name__)

_TTL_PADRAO: int = 86_400


class CachingVerificadorCnes:
    """Decorator sobre VerificadorCnes com cache persistente JSON e TTL.

    Args:
        verificador: Implementação real do protocolo VerificadorCnes.
        caminho_cache: Arquivo JSON para persistência entre execuções.
        ttl_segundos: Tempo de vida de uma entrada (padrão: 86400 = 24h).
    """

    def __init__(
        self,
        verificador: VerificadorCnes,
        caminho_cache: Path,
        ttl_segundos: int = _TTL_PADRAO,
    ) -> None:
        self._verificador = verificador
        self._caminho_cache = caminho_cache
        self._ttl = ttl_segundos
        self._cache: dict[str, tuple[str, float]] = self._carregar()

    def verificar_estabelecimento(self, cnes: str) -> str:
        """Retorna status do cache se válido; senão delega ao verificador real.

        Args:
            cnes: Código CNES (7 dígitos).

        Returns:
            STATUS_CONFIRMADO | STATUS_LAG | STATUS_INDISPONIVEL
        """
        agora = time.time()
        if cnes in self._cache:
            status, gravado_em = self._cache[cnes]
            if agora - gravado_em < self._ttl:
                logger.info("cache_hit cnes=%s status=%s", cnes, status)
                return status

        status = self._verificador.verificar_estabelecimento(cnes)
        self._atualizar_cache(cnes, status)
        return status

    def _atualizar_cache(self, cnes: str, status: str) -> None:
        self._cache[cnes] = (status, time.time())
        self._persistir()

    def _carregar(self) -> dict[str, tuple[str, float]]:
        if not self._caminho_cache.exists():
            return {}
        try:
            raw = json.loads(self._caminho_cache.read_text(encoding="utf-8"))
            return {k: (v[0], float(v[1])) for k, v in raw.items()}
        except (json.JSONDecodeError, KeyError, IndexError, TypeError):
            logger.warning("cache_corrompido path=%s reiniciando", self._caminho_cache)
            return {}

    def _persistir(self) -> None:
        self._caminho_cache.parent.mkdir(parents=True, exist_ok=True)
        self._caminho_cache.write_text(
            json.dumps(self._cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
