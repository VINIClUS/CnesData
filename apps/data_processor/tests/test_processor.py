"""Testes unitários do processor — download, transform, persist."""
import gzip
import io
import uuid
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

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


class TestDownloadParquet:

    def test_rejeita_null_url(self):
        with pytest.raises(ValueError, match="null_storage"):
            _download_parquet("null://bucket/key")

    @patch("data_processor.processor.httpx")
    def test_descomprime_gzip(self, mock_httpx):
        data = _make_parquet_bytes(compressed=True)
        resp = MagicMock()
        resp.content = data
        resp.raise_for_status = MagicMock()
        mock_httpx.get.return_value = resp

        df = _download_parquet("http://minio:9000/bucket/test.parquet.gz")
        assert len(df) == 1
        assert "cpf" in df.columns

    @patch("data_processor.processor.httpx")
    def test_parquet_sem_compressao(self, mock_httpx):
        data = _make_parquet_bytes(compressed=False)
        resp = MagicMock()
        resp.content = data
        resp.raise_for_status = MagicMock()
        mock_httpx.get.return_value = resp

        df = _download_parquet("http://minio:9000/bucket/test.parquet")
        assert len(df) == 1


class TestProcessJob:

    @patch("data_processor.processor._get_competencia")
    @patch("data_processor.processor._get_object_key")
    @patch("data_processor.processor._download_parquet")
    @patch("data_processor.processor.PostgresAdapter")
    @patch("data_processor.processor.transformar")
    @patch("data_processor.processor.CnesLocalAdapter")
    def test_profissional_chama_adapter_transformar_e_gravar(
        self, mock_cnes_adapter_cls, mock_transform,
        mock_pg_cls, mock_download, mock_key, mock_comp,
    ):
        mock_key.return_value = "355030/cnes_profissional/abc.parquet.gz"
        mock_comp.return_value = "2024-12"
        df_raw = pl.DataFrame({"CPF_PROF": ["12345678901"]})
        df_adapted = pl.DataFrame({"CPF": ["12345678901"]})
        mock_download.return_value = df_raw
        mock_cnes_inst = MagicMock()
        mock_cnes_inst.listar_profissionais.return_value = df_adapted
        mock_cnes_adapter_cls.return_value = mock_cnes_inst
        mock_transform.return_value = df_adapted
        mock_pg = MagicMock()
        mock_pg_cls.return_value = mock_pg

        engine = MagicMock()
        storage = MagicMock()
        storage.get_presigned_download_url.return_value = "http://test"

        job = _make_job("cnes_profissional")
        process_job(engine, storage, job)

        mock_cnes_adapter_cls.assert_called_once_with(df_raw)
        mock_cnes_inst.listar_profissionais.assert_called_once()
        mock_transform.assert_called_once()
        mock_pg.gravar_profissionais.assert_called_once_with(
            "2024-12", mock_transform.return_value,
        )

    @patch("data_processor.processor._get_competencia")
    @patch("data_processor.processor._get_object_key")
    @patch("data_processor.processor._download_parquet")
    @patch("data_processor.processor.PostgresAdapter")
    @patch("data_processor.processor.CnesLocalAdapter")
    def test_estabelecimento_aplica_adapter_sem_transformar(
        self, mock_cnes_adapter_cls, mock_pg_cls,
        mock_download, mock_key, mock_comp,
    ):
        mock_key.return_value = "355030/cnes_estabelecimento/abc.parquet.gz"
        mock_comp.return_value = "2024-12"
        df_raw = pl.DataFrame({"NOME_FANTA": ["UBS TESTE"]})
        df_adapted = pl.DataFrame({"NOME_FANTASIA": ["UBS TESTE"]})
        mock_download.return_value = df_raw
        mock_cnes_inst = MagicMock()
        mock_cnes_inst.listar_estabelecimentos.return_value = df_adapted
        mock_cnes_adapter_cls.return_value = mock_cnes_inst
        mock_pg = MagicMock()
        mock_pg_cls.return_value = mock_pg

        engine = MagicMock()
        storage = MagicMock()
        storage.get_presigned_download_url.return_value = "http://test"

        job = _make_job("cnes_estabelecimento")
        process_job(engine, storage, job)

        mock_cnes_adapter_cls.assert_called_once_with(df_raw)
        mock_cnes_inst.listar_estabelecimentos.assert_called_once()
        mock_pg.gravar_estabelecimentos.assert_called_once()
        mock_pg.gravar_profissionais.assert_not_called()

    @patch("data_processor.processor._get_object_key")
    def test_levanta_se_object_key_missing(self, mock_key):
        mock_key.return_value = None
        engine = MagicMock()
        storage = MagicMock()
        job = _make_job()

        with pytest.raises(ValueError, match="object_key_missing"):
            process_job(engine, storage, job)
