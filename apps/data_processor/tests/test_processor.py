"""Testes unitarios do processor — download, transform, persist."""
import gzip
import io
import uuid
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from cnes_domain.pipeline.circuit_breaker import CircuitBreaker
from cnes_infra.storage.job_queue import Job
from data_processor.processor import (
    _download_parquet,
    process_job,
)


def _make_job(
    source_system: str = "cnes_profissional",
) -> Job:
    return Job(
        id=uuid.uuid4(),
        status="PROCESSING",
        source_system=source_system,
        tenant_id="355030",
        payload_id=uuid.uuid4(),
        object_key="355030/cnes_profissional/abc.parquet.gz",
        competencia="2024-12",
    )


def _make_parquet_bytes(compressed: bool = True) -> bytes:
    df = pl.DataFrame({
        "cpf": ["12345678901"],
        "nome": ["TESTE"],
        "cnes": ["1234567"],
    })
    buf = io.BytesIO()
    df.write_parquet(buf)
    raw = buf.getvalue()
    if compressed:
        return gzip.compress(raw)
    return raw


@pytest.fixture
def breaker():
    return CircuitBreaker(service_name="test")


class TestDownloadParquet:

    def test_rejeita_null_url(self, breaker):
        with pytest.raises(ValueError, match="null_storage"):
            _download_parquet("null://bucket/key", breaker)

    @patch("data_processor.processor.httpx")
    def test_descomprime_gzip(self, mock_httpx, breaker):
        data = _make_parquet_bytes(compressed=True)
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.iter_bytes.return_value = [data]
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_httpx.stream.return_value = mock_resp

        df = _download_parquet(
            "http://minio:9000/bucket/test.parquet.gz", breaker,
        )
        assert len(df) == 1
        assert "cpf" in df.columns

    @patch("data_processor.processor.httpx")
    def test_parquet_sem_compressao(self, mock_httpx, breaker):
        data = _make_parquet_bytes(compressed=False)
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.iter_bytes.return_value = [data]
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_httpx.stream.return_value = mock_resp

        df = _download_parquet(
            "http://minio:9000/bucket/test.parquet", breaker,
        )
        assert len(df) == 1


class TestProcessJobValidation:

    def test_levanta_se_object_key_none(self):
        job = Job(
            id=uuid.uuid4(),
            status="PROCESSING",
            source_system="cnes_profissional",
            tenant_id="355030",
            payload_id=uuid.uuid4(),
            object_key=None,
            competencia="2024-12",
        )
        with pytest.raises(ValueError, match="object_key_missing"):
            process_job(MagicMock(), MagicMock(), job)

    def test_levanta_se_competencia_none(self):
        job = Job(
            id=uuid.uuid4(),
            status="PROCESSING",
            source_system="cnes_profissional",
            tenant_id="355030",
            payload_id=uuid.uuid4(),
            object_key="key",
            competencia=None,
        )
        with pytest.raises(ValueError, match="competencia_missing"):
            process_job(MagicMock(), MagicMock(), job)


class TestProcessJob:

    @patch("data_processor.processor._download_parquet")
    @patch("data_processor.processor.PostgresUnitOfWork")
    @patch("data_processor.processor.mapear_vinculos")
    @patch("data_processor.processor.mapear_profissionais")
    @patch("data_processor.processor.extrair_fonte")
    @patch("data_processor.processor.transformar")
    @patch("data_processor.processor.CnesLocalAdapter")
    def test_profissional_chama_uow_e_row_mapper(
        self, mock_cnes_cls, mock_transform, mock_fonte,
        mock_map_prof, mock_map_vinc, mock_uow_cls,
        mock_download,
    ):
        df_raw = pl.DataFrame({"CPF_PROF": ["12345678901"]})
        df_adapted = pl.DataFrame({"CPF": ["12345678901"]})
        mock_download.return_value = df_raw
        mock_cnes_inst = MagicMock()
        mock_cnes_inst.listar_profissionais.return_value = df_adapted
        mock_cnes_cls.return_value = mock_cnes_inst
        mock_transform.return_value = df_adapted
        mock_fonte.return_value = "LOCAL"
        mock_map_prof.return_value = [{"cpf": "12345678901"}]
        mock_map_vinc.return_value = [{"cpf": "12345678901"}]

        mock_uow = MagicMock()
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)
        mock_uow_cls.return_value = mock_uow

        engine = MagicMock()
        storage = MagicMock()
        storage.get_presigned_download_url.return_value = (
            "http://test"
        )

        job = _make_job("cnes_profissional")
        process_job(engine, storage, job)

        mock_cnes_cls.assert_called_once_with(df_raw)
        mock_cnes_inst.listar_profissionais.assert_called_once()
        mock_transform.assert_called_once()
        mock_fonte.assert_called_once()
        mock_map_prof.assert_called_once()
        mock_map_vinc.assert_called_once_with(
            "2024-12", mock_transform.return_value,
        )
        mock_uow.profissionais.gravar.assert_called_once()
        mock_uow.vinculos.snapshot_replace.assert_called_once_with(
            "2024-12", "LOCAL", mock_map_vinc.return_value,
        )

    @patch("data_processor.processor._download_parquet")
    @patch("data_processor.processor.PostgresUnitOfWork")
    @patch("data_processor.processor.mapear_estabelecimentos")
    @patch("data_processor.processor.CnesLocalAdapter")
    def test_estabelecimento_aplica_adapter_sem_transformar(
        self, mock_cnes_cls, mock_map_estab, mock_uow_cls,
        mock_download,
    ):
        df_raw = pl.DataFrame({"NOME_FANTA": ["UBS TESTE"]})
        df_adapted = pl.DataFrame(
            {"NOME_FANTASIA": ["UBS TESTE"]},
        )
        mock_download.return_value = df_raw
        mock_cnes_inst = MagicMock()
        mock_cnes_inst.listar_estabelecimentos.return_value = (
            df_adapted
        )
        mock_cnes_cls.return_value = mock_cnes_inst
        mock_map_estab.return_value = [{"cnes": "1234567"}]

        mock_uow = MagicMock()
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)
        mock_uow_cls.return_value = mock_uow

        engine = MagicMock()
        storage = MagicMock()
        storage.get_presigned_download_url.return_value = (
            "http://test"
        )

        job = _make_job("cnes_estabelecimento")
        process_job(engine, storage, job)

        mock_cnes_cls.assert_called_once_with(df_raw)
        mock_cnes_inst.listar_estabelecimentos.assert_called_once()
        mock_uow.estabelecimentos.gravar.assert_called_once()
        mock_uow.profissionais.gravar.assert_not_called()


class TestProcessJobSihd:
    @patch("data_processor.processor._download_parquet")
    @patch("data_processor.processor.PostgresUnitOfWork")
    @patch("data_processor.processor.mapear_vinculos")
    @patch("data_processor.processor.mapear_profissionais")
    @patch("data_processor.processor.extrair_fonte")
    @patch("data_processor.processor.transformar")
    @patch("data_processor.processor.SihdLocalAdapter")
    def test_sihd_producao_chama_sihd_adapter(
        self,
        mock_sihd_cls,
        mock_transform,
        mock_fonte,
        mock_map_prof,
        mock_map_vinc,
        mock_uow_cls,
        mock_download,
    ):
        import uuid

        import polars as pl

        from cnes_infra.storage.job_queue import Job
        from data_processor.processor import process_job

        df_raw = pl.DataFrame({"AH_NUM_AIH": ["12345"]})
        df_adapted = pl.DataFrame({"CPF": ["12345678901"]})
        mock_download.return_value = df_raw

        mock_sihd_inst = MagicMock()
        mock_sihd_inst.listar_aihs.return_value = df_adapted
        mock_sihd_cls.return_value = mock_sihd_inst

        mock_transform.return_value = df_adapted
        mock_fonte.return_value = "LOCAL"
        mock_map_prof.return_value = []
        mock_map_vinc.return_value = []

        mock_uow = MagicMock()
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)
        mock_uow_cls.return_value = mock_uow

        engine = MagicMock()
        storage = MagicMock()
        storage.get_presigned_download_url.return_value = "http://test"

        job = Job(
            id=uuid.uuid4(),
            status="PROCESSING",
            source_system="sihd_producao",
            tenant_id="355030",
            payload_id=uuid.uuid4(),
            object_key="key/sihd.parquet.gz",
            competencia="2024-12",
        )
        process_job(engine, storage, job)
        mock_sihd_cls.assert_called_once_with(df_raw)
        mock_sihd_inst.listar_aihs.assert_called_once()
