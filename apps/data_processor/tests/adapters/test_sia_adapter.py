from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import uuid4

import polars as pl

from data_processor.adapters.sia_adapter import (
    map_apa_to_fato,
    map_bpi_to_fato,
)


class _Lookup:
    def __init__(self, m: dict) -> None:
        self._m = m

    def procedimento_sk(self, c: str) -> int | None:
        return self._m.get(("P", c))

    def profissional_sk(self, c: str) -> int | None:
        return self._m.get(("PROF", c))

    def estabelecimento_sk(self, c: str) -> int | None:
        return self._m.get(("E", c))

    def cid10_sk(self, c: str) -> int | None:
        return self._m.get(("CID", c))


class TestSIAAPA:
    def test_mapeia_apa(self) -> None:
        df = pl.DataFrame({
            "apa_cmp": ["202601"],
            "apa_cnes": ["2269481"],
            "apa_cnsexe": ["700987654321098"],
            "apa_proc": ["0301010056"],
            "apa_cbo": ["225125"],
            "apa_cid": ["J00"],
            "apa_dtfin": [date(2026, 1, 31)],
            "apa_qtapr": [5],
            "apa_vlapr": [1000],
        })
        lookup = _Lookup({
            ("P", "0301010056"): 100,
            ("E", "2269481"): 200,
            ("PROF", "700987654321098"): 500,
            ("CID", "J00"): 1000,
        })
        fatos = map_apa_to_fato(
            df, lookup, job_id=uuid4(), extracao_ts=datetime.now(UTC),
        )
        assert len(fatos) == 1
        assert fatos[0].fonte_sistema == "SIA_APA"
        assert fatos[0].valor_aprov_cents == 1000


class TestSIABPI:
    def test_mapeia_bpi(self) -> None:
        df = pl.DataFrame({
            "bpi_cmp": ["202601"],
            "bpi_cnes": ["2269481"],
            "bpi_cnsmed": ["700987654321098"],
            "bpi_cbo": ["225125"],
            "bpi_proc": ["0301010064"],
            "bpi_cid": ["K02"],
            "bpi_dtaten": [date(2026, 1, 10)],
            "bpi_qt": [3],
        })
        lookup = _Lookup({
            ("P", "0301010064"): 101,
            ("E", "2269481"): 200,
            ("PROF", "700987654321098"): 500,
            ("CID", "K02"): 1001,
        })
        fatos = map_bpi_to_fato(
            df, lookup,
            job_id=uuid4(), extracao_ts=datetime.now(UTC), historico=False,
        )
        assert fatos[0].fonte_sistema == "SIA_BPI"

    def test_historico_marca_fonte_sia_bpihst(self) -> None:
        df = pl.DataFrame({
            "bpi_cmp": ["202512"],
            "bpi_cnes": ["2269481"],
            "bpi_cnsmed": ["7001"],
            "bpi_cbo": ["225125"],
            "bpi_proc": ["0301010064"],
            "bpi_cid": ["K02"],
            "bpi_dtaten": [date(2025, 12, 10)],
            "bpi_qt": [1],
        })
        lookup = _Lookup({
            ("P", "0301010064"): 101,
            ("E", "2269481"): 200,
            ("PROF", "7001"): 500,
            ("CID", "K02"): 1001,
        })
        fatos = map_bpi_to_fato(
            df, lookup,
            job_id=uuid4(), extracao_ts=datetime.now(UTC), historico=True,
        )
        assert fatos[0].fonte_sistema == "SIA_BPIHST"
