"""Testes do CnesExtractor."""

from unittest.mock import MagicMock

import polars as pl
import pytest

from cnes_domain.models.extraction import ExtractionParams
from dump_agent.extractors.cnes_extractor import CnesExtractor
from dump_agent.extractors.protocol import Extractor
from dump_agent.io_guard import SpoolGuard, SpoolLimitExceeded


class TestExtractorProtocol:
    def test_cnes_extractor_implementa_protocol(self):
        assert isinstance(CnesExtractor(), Extractor)


class TestCnesExtractor:
    def _make_params(
        self, intent: str = "profissionais",
    ) -> ExtractionParams:
        return ExtractionParams(
            intent=intent,
            competencia="2026-03",
            cod_municipio="354130",
        )

    def _mock_cursor(self, rows, columns):
        cur = MagicMock()
        cur.description = [(c,) for c in columns]
        cur.fetchmany = MagicMock(side_effect=[rows, []])
        cur.close = MagicMock()
        return cur

    def test_extract_profissionais_gera_parquet(self, tmp_path):
        columns = [
            "CPF_PROF", "COD_CNS", "NOME_PROF",
            "NO_SOCIAL", "SEXO", "DATA_NASC",
            "COD_CBO", "IND_VINC", "TP_SUS_NAO_SUS",
            "CARGA_HORARIA_TOTAL", "CG_HORAAMB",
            "CGHORAOUTR", "CGHORAHOSP", "CNES",
            "NOME_FANTA", "TP_UNID_ID", "CODMUNGEST",
        ]
        rows = [
            (
                "12345678901", "123456", "JOAO", None,
                "M", "1990-01-01", "225125", "1", "S",
                40, 20, 10, 10, "1234567", "UBS CENTRAL",
                "1", "354130",
            ),
        ]
        mock_con = MagicMock()
        mock_con.cursor.return_value = self._mock_cursor(
            rows, columns,
        )

        extractor = CnesExtractor()
        guard = SpoolGuard(max_bytes=50 * 1024 * 1024)
        path = extractor.extract(
            self._make_params("profissionais"),
            mock_con,
            tmp_path,
            guard,
        )

        assert path.exists()
        assert path.suffix == ".parquet"
        df = pl.read_parquet(path)
        assert len(df) == 1
        assert "CPF_PROF" in df.columns

    def test_extract_retorna_colunas_raw_sem_rename(
        self, tmp_path,
    ):
        columns = ["CPF_PROF", "COD_CNS", "NOME_PROF"]
        rows = [("111", "222", "MARIA")]
        mock_con = MagicMock()
        mock_con.cursor.return_value = self._mock_cursor(
            rows, columns,
        )

        extractor = CnesExtractor()
        guard = SpoolGuard(max_bytes=50 * 1024 * 1024)
        path = extractor.extract(
            self._make_params("profissionais"),
            mock_con,
            tmp_path,
            guard,
        )
        df = pl.read_parquet(path)
        assert "CPF_PROF" in df.columns
        assert "CPF" not in df.columns
        assert "NOME_PROFISSIONAL" not in df.columns

    def test_extract_empty_result(self, tmp_path):
        columns = ["CPF_PROF", "COD_CNS"]
        mock_con = MagicMock()
        cur = MagicMock()
        cur.description = [(c,) for c in columns]
        cur.fetchmany = MagicMock(return_value=[])
        cur.close = MagicMock()
        mock_con.cursor.return_value = cur

        extractor = CnesExtractor()
        guard = SpoolGuard(max_bytes=50 * 1024 * 1024)
        path = extractor.extract(
            self._make_params(), mock_con, tmp_path, guard,
        )
        df = pl.read_parquet(path)
        assert len(df) == 0
        assert "CPF_PROF" in df.columns

    def test_extract_tracks_bytes_via_guard(self, tmp_path):
        columns = ["CPF_PROF", "COD_CNS"]
        rows = [("111", "222")]
        mock_con = MagicMock()
        mock_con.cursor.return_value = self._mock_cursor(
            rows, columns,
        )

        guard = SpoolGuard(max_bytes=50 * 1024 * 1024)
        CnesExtractor().extract(
            self._make_params(), mock_con, tmp_path, guard,
        )
        assert guard.total_bytes > 0

    def test_extract_raises_on_spool_limit(self, tmp_path):
        columns = ["CPF_PROF", "COD_CNS"]
        rows = [("111", "222")]
        mock_con = MagicMock()
        mock_con.cursor.return_value = self._mock_cursor(
            rows, columns,
        )

        guard = SpoolGuard(max_bytes=1)
        with pytest.raises(SpoolLimitExceeded):
            CnesExtractor().extract(
                self._make_params(),
                mock_con,
                tmp_path,
                guard,
            )
