"""Testes do SihdExtractor."""

from unittest.mock import MagicMock

import polars as pl

from cnes_domain.models.extraction import ExtractionParams
from dump_agent.extractors.protocol import Extractor
from dump_agent.extractors.sihd_extractor import SihdExtractor
from dump_agent.io_guard import SpoolGuard


class TestSihdExtractor:
    def _make_params(self) -> ExtractionParams:
        return ExtractionParams(
            intent="sihd_producao",
            competencia="2026-03",
            cod_municipio="354130",
        )

    def _mock_cursor(self, rows, columns):
        cur = MagicMock()
        cur.description = [(c,) for c in columns]
        cur.fetchmany = MagicMock(side_effect=[rows, []])
        cur.close = MagicMock()
        return cur

    def test_implementa_protocol(self):
        assert isinstance(SihdExtractor(), Extractor)

    def test_extract_gera_parquet_raw(self, tmp_path):
        columns = [
            "AH_NUM_AIH", "AH_CNES", "AH_CMPT",
            "AH_PACIENTE_NOME", "AH_PACIENTE_NUMERO_CNS",
        ]
        rows = [("12345", "1234567", "202603", "JOAO", "123456")]
        mock_con = MagicMock()
        mock_con.cursor.return_value = self._mock_cursor(rows, columns)

        extractor = SihdExtractor()
        guard = SpoolGuard(max_bytes=50 * 1024 * 1024)
        path = extractor.extract(
            self._make_params(), mock_con, tmp_path, guard,
        )

        assert path.exists()
        df = pl.read_parquet(path)
        assert len(df) == 1
        assert "AH_NUM_AIH" in df.columns
        assert "NUM_AIH" not in df.columns

    def test_competencia_formato_aaaamm(self, tmp_path):
        columns = ["AH_NUM_AIH", "AH_CMPT"]
        rows = [("12345", "202603")]
        mock_con = MagicMock()
        mock_cur = self._mock_cursor(rows, columns)
        mock_con.cursor.return_value = mock_cur

        SihdExtractor().extract(
            self._make_params(), mock_con, tmp_path,
            SpoolGuard(max_bytes=50 * 1024 * 1024),
        )
        mock_cur.execute.assert_called_once()
        call_args = mock_cur.execute.call_args
        assert call_args[0][1] == ("202603",)
