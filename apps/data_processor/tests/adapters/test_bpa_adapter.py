from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import uuid4

import polars as pl

from data_processor.adapters.bpa_adapter import (
    map_bpa_c_to_fato,
    map_bpa_i_to_fato,
)


class _DimLookup:
    def __init__(self, mapping: dict) -> None:
        self._m = mapping

    def procedimento_sk(self, code: str) -> int | None:
        return self._m.get(("PROC", code))

    def profissional_sk(self, cns: str) -> int | None:
        return self._m.get(("PROF", cns))

    def estabelecimento_sk(self, cnes: str) -> int | None:
        return self._m.get(("ESTAB", cnes))

    def cid10_sk(self, code: str) -> int | None:
        return self._m.get(("CID", code))


class TestMapBPAC:
    def test_mapeia_linha_completa(self) -> None:
        df = pl.DataFrame({
            "nu_competencia": ["202601"],
            "co_cnes": ["2269481"],
            "co_procedimento": ["0301010056"],
            "qt_aprovada": [10],
            "co_cbo": ["225125"],
        })
        lookup = _DimLookup({
            ("PROC", "0301010056"): 100,
            ("ESTAB", "2269481"): 200,
        })
        job = uuid4()
        ts = datetime.now(UTC)

        fatos = map_bpa_c_to_fato(df, lookup, job_id=job, extracao_ts=ts)
        assert len(fatos) == 1
        f = fatos[0]
        assert f.sk_procedimento == 100
        assert f.sk_estabelecimento == 200
        assert f.qtd == 10
        assert f.fonte_sistema == "BPA_C"

    def test_dim_miss_retorna_vazio_linha(self) -> None:
        df = pl.DataFrame({
            "nu_competencia": ["202601"],
            "co_cnes": ["X"],
            "co_procedimento": ["UNKNOWN"],
            "qt_aprovada": [1],
            "co_cbo": ["225125"],
        })
        lookup = _DimLookup({})
        fatos = map_bpa_c_to_fato(df, lookup, job_id=uuid4(),
                                   extracao_ts=datetime.now(UTC))
        assert fatos == []


class TestMapBPAI:
    def test_mapeia_linha_individualizada(self) -> None:
        df = pl.DataFrame({
            "nu_competencia": ["202601"],
            "co_cnes": ["2269481"],
            "nu_cns_pac": ["700123456789012"],
            "co_procedimento": ["0301010064"],
            "co_cbo": ["225125"],
            "co_cid10": ["J00"],
            "dt_atendimento": [date(2026, 1, 15)],
            "qt_aprovada": [1],
            "nu_cns_prof": ["700987654321098"],
        })
        lookup = _DimLookup({
            ("PROF", "700987654321098"): 500,
            ("ESTAB", "2269481"): 200,
            ("PROC", "0301010064"): 101,
            ("CID", "J00"): 1000,
        })
        fatos = map_bpa_i_to_fato(df, lookup, job_id=uuid4(),
                                   extracao_ts=datetime.now(UTC))
        assert len(fatos) == 1
        f = fatos[0]
        assert f.sk_profissional == 500
        assert f.sk_cid_principal == 1000
        assert f.fonte_sistema == "BPA_I"
