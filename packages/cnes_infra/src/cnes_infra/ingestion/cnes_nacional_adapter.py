"""Adapter: BigQuery (basedosdados) → schema padronizado da camada de ingestão."""

import logging
import pickle
import time
from collections.abc import Callable
from pathlib import Path

import polars as pl
from cnes_domain.contracts.columns import SCHEMA_ESTABELECIMENTO, SCHEMA_PROFISSIONAL

from cnes_infra.ingestion.web_client import CnesWebClient

logger = logging.getLogger(__name__)

_FONTE_NACIONAL: str = "NACIONAL"
_TTL_CACHE_PADRAO: int = 3_600

_MAP_ESTABELECIMENTO: dict[str, str] = {
    "id_estabelecimento_cnes": "CNES",
    "cnpj_mantenedora": "CNPJ_MANTENEDORA",
    "id_natureza_juridica": "NATUREZA_JURIDICA",
    "tipo_unidade": "TIPO_UNIDADE",
    "indicador_vinculo_sus": "VINCULO_SUS",
    "id_municipio_6": "COD_MUNICIPIO",
}

_MAP_PROFISSIONAL: dict[str, str] = {
    "id_estabelecimento_cnes": "CNES",
    "cartao_nacional_saude": "CNS",
    "nome": "NOME_PROFISSIONAL",
    "cbo_2002": "CBO",
    "tipo_vinculo": "TIPO_VINCULO",
    "indicador_atende_sus": "SUS",
    "carga_horaria_ambulatorial": "CH_AMBULATORIAL",
    "carga_horaria_outros": "CH_OUTRAS",
    "carga_horaria_hospitalar": "CH_HOSPITALAR",
}


class CnesNacionalAdapter:
    """Adapter entre o BigQuery (basedosdados) e o schema padronizado."""

    def __init__(
        self,
        billing_project_id: str,
        id_municipio: str,
        cache_dir: Path | None = None,
        ttl_cache_segundos: int = _TTL_CACHE_PADRAO,
    ) -> None:
        self._client = CnesWebClient(billing_project_id)
        self._id_municipio = id_municipio
        self._cache_dir = cache_dir
        self._ttl = ttl_cache_segundos

    def _ler_ou_cachear(
        self, chave: str, buscar: Callable[[], pl.DataFrame],
    ) -> pl.DataFrame:
        if self._cache_dir is None:
            return buscar()
        caminho = self._cache_dir / f"{chave}.pkl"
        if caminho.exists() and time.time() - caminho.stat().st_mtime < self._ttl:
            cached = self._ler_cache(caminho)
            if cached is not None:
                logger.info("cache_hit chave=%s", chave)
                return cached
        df = buscar()
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        caminho.write_bytes(pickle.dumps(df))
        logger.info("cache_gravado chave=%s rows=%d", chave, len(df))
        return df

    def _ler_cache(self, caminho: Path) -> pl.DataFrame | None:
        try:
            return pickle.loads(caminho.read_bytes())
        except Exception:
            logger.warning(
                "cache_corrompido chave=%s removendo", caminho.stem,
            )
            caminho.unlink(missing_ok=True)
            return None

    def listar_estabelecimentos(
        self, competencia: tuple[int, int] | None = None,
    ) -> pl.DataFrame:
        """Retorna estabelecimentos nacionais (FONTE=NACIONAL).

        Args:
            competencia: (ano, mes) obrigatório para o adapter nacional.

        Returns:
            DataFrame conforme SCHEMA_ESTABELECIMENTO.

        Raises:
            ValueError: Se competencia for None.
        """
        if competencia is None:
            raise ValueError("nacional_adapter competencia=obrigatoria")
        ano, mes = competencia
        chave = f"estab_{self._id_municipio}_{ano}_{mes:02d}"
        return self._ler_ou_cachear(
            chave, lambda: self._buscar_estabelecimentos(ano, mes),
        )

    def _buscar_estabelecimentos(
        self, ano: int, mes: int,
    ) -> pl.DataFrame:
        df = self._client.fetch_estabelecimentos(
            self._id_municipio, ano, mes,
        )
        df = df.rename(_MAP_ESTABELECIMENTO)
        df = df.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("NOME_FANTASIA"),
            pl.when(pl.col("VINCULO_SUS") == 1)
            .then(pl.lit("S"))
            .when(pl.col("VINCULO_SUS") == 0)
            .then(pl.lit("N"))
            .otherwise(pl.lit("N"))
            .alias("VINCULO_SUS"),
            pl.lit(_FONTE_NACIONAL).alias("FONTE"),
        )
        logger.debug(
            "listar_estabelecimentos fonte=NACIONAL rows=%d", len(df),
        )
        return df.select(list(SCHEMA_ESTABELECIMENTO))

    def listar_profissionais(
        self, competencia: tuple[int, int] | None = None,
    ) -> pl.DataFrame:
        """Retorna vínculos nacionais (FONTE=NACIONAL).

        Args:
            competencia: (ano, mes) obrigatório para o adapter nacional.

        Returns:
            DataFrame conforme SCHEMA_PROFISSIONAL.

        Raises:
            ValueError: Se competencia for None.
        """
        if competencia is None:
            raise ValueError("nacional_adapter competencia=obrigatoria")
        ano, mes = competencia
        chave = f"prof_{self._id_municipio}_{ano}_{mes:02d}"
        return self._ler_ou_cachear(
            chave, lambda: self._buscar_profissionais(ano, mes),
        )

    def _buscar_profissionais(
        self, ano: int, mes: int,
    ) -> pl.DataFrame:
        df = self._client.fetch_profissionais(
            self._id_municipio, ano, mes,
        )
        df = df.rename(_MAP_PROFISSIONAL)
        df = df.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("CPF"),
            pl.lit(None).cast(pl.Utf8).alias("NOME_SOCIAL"),
            pl.lit(None).cast(pl.Utf8).alias("SEXO"),
            pl.when(pl.col("SUS") == 1)
            .then(pl.lit("S"))
            .when(pl.col("SUS") == 0)
            .then(pl.lit("N"))
            .otherwise(pl.lit("N"))
            .alias("SUS"),
            (
                pl.col("CH_AMBULATORIAL").fill_null(0)
                + pl.col("CH_OUTRAS").fill_null(0)
                + pl.col("CH_HOSPITALAR").fill_null(0)
            ).cast(pl.Int64).alias("CH_TOTAL"),
            pl.lit(_FONTE_NACIONAL).alias("FONTE"),
        )
        logger.debug(
            "listar_profissionais fonte=NACIONAL rows=%d", len(df),
        )
        return df.select(list(SCHEMA_PROFISSIONAL))
