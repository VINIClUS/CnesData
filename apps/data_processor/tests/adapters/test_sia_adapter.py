from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import uuid4

import polars as pl
import pytest

from data_processor.adapters.sia_adapter import (
    canonicalize_bpi,
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

    def competencia_sk(self, yyyymm: str) -> int | None:
        return self._m.get(("COMP", yyyymm))


class TestSIAAPA:
    def test_mapeia_apa(self) -> None:
        df = pl.DataFrame({
            "prd_cmp": ["202601"],
            "prd_uid": ["2269481"],
            "apa_cnsexe": ["700987654321098"],
            "prd_pa": ["0301010056"],
            "prd_cbo": ["225125"],
            "prd_cidpri": ["J00"],
            "apa_dtfim": ["20260131"],
            "prd_qt_p": [5],
            "prd_vl_a": [1000],
        })
        lookup = _Lookup({
            ("P", "0301010056"): 100,
            ("E", "2269481"): 200,
            ("PROF", "700987654321098"): 500,
            ("CID", "J00"): 1000,
            ("COMP", "202601"): 73,
        })
        fatos = map_apa_to_fato(
            df, lookup, job_id=uuid4(), extracao_ts=datetime.now(UTC),
        )
        assert len(fatos) == 1
        assert fatos[0].fonte_sistema == "SIA_APA"
        assert fatos[0].valor_aprov_cents == 1000
        assert fatos[0].sk_competencia == 73
        assert fatos[0].dt_atendimento == date(2026, 1, 31)

    def test_competencia_dim_miss_retorna_vazio_linha(self) -> None:
        df = pl.DataFrame({
            "prd_cmp": ["999999"],
            "prd_uid": ["2269481"],
            "apa_cnsexe": ["700987654321098"],
            "prd_pa": ["0301010056"],
            "prd_cbo": ["225125"],
            "prd_cidpri": ["J00"],
            "apa_dtfim": ["20260131"],
            "prd_qt_p": [5],
            "prd_vl_a": [1000],
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
        assert fatos == []


class TestSIABPI:
    def test_mapeia_bpi(self) -> None:
        df = pl.DataFrame({
            "bpi_cmp": ["202601"],
            "bpi_uid": ["2269481"],
            "bpi_cnsmed": ["700987654321098"],
            "bpi_cbo": ["225125"],
            "bpi_pa": ["0301010064"],
            "bpi_cid": ["K02"],
            "bpi_dtaten": ["20260110"],
            "bpi_qt_p": [3],
        })
        lookup = _Lookup({
            ("P", "0301010064"): 101,
            ("E", "2269481"): 200,
            ("PROF", "700987654321098"): 500,
            ("CID", "K02"): 1001,
            ("COMP", "202601"): 73,
        })
        fatos = map_bpi_to_fato(
            df, lookup,
            job_id=uuid4(), extracao_ts=datetime.now(UTC), historico=False,
        )
        assert fatos[0].fonte_sistema == "SIA_BPI"
        assert fatos[0].sk_competencia == 73

    def test_historico_marca_fonte_sia_bpihst(self) -> None:
        df = pl.DataFrame({
            "bpi_cmp": ["202512"],
            "bpi_uid": ["2269481"],
            "bpi_cnsmed": ["7001"],
            "bpi_cbo": ["225125"],
            "bpi_pa": ["0301010064"],
            "bpi_cid": ["K02"],
            "bpi_dtaten": ["00000000"],
            "bpi_qt_p": [1],
        })
        lookup = _Lookup({
            ("P", "0301010064"): 101,
            ("E", "2269481"): 200,
            ("PROF", "7001"): 500,
            ("CID", "K02"): 1001,
            ("COMP", "202512"): 72,
        })
        fatos = map_bpi_to_fato(
            df, lookup,
            job_id=uuid4(), extracao_ts=datetime.now(UTC), historico=True,
        )
        assert fatos[0].fonte_sistema == "SIA_BPIHST"
        assert fatos[0].sk_competencia == 72
        assert fatos[0].dt_atendimento is None


class TestContratoRaw:
    def test_rejeita_raw_com_colunas_sinteticas_antigas(self) -> None:
        legado = pl.DataFrame({
            "bpi_cmp": ["202601"], "bpi_cnes": ["2269481"], "bpi_cnsmed": ["7001"],
            "bpi_cbo": ["225125"], "bpi_proc": ["0301010064"], "bpi_cid": ["K02"],
            "bpi_dtaten": [date(2026, 1, 10)], "bpi_qt": [1], "bpi_folha": [1], "bpi_seq": [1],
        })

        with pytest.raises(ValueError, match="sia_schema_invalid subtype=SIA_BPI column=bpi_uid"):
            canonicalize_bpi(legado, "SIA_BPI")

    def test_data_texto_invalida_vira_flag(self) -> None:
        raw = pl.DataFrame({
            "bpi_uid": ["2269481"] * 3, "bpi_cmp": ["202601"] * 3, "bpi_cnsmed": ["7001"] * 3,
            "bpi_cbo": ["225125"] * 3, "bpi_flh": ["001"] * 3, "bpi_seq": ["01", "02", "x"],
            "bpi_pa": ["0301010064"] * 3, "bpi_cid": [""] * 3,
            "bpi_dtaten": ["20260110", "", "20261341"], "bpi_qt_p": [1, 1, 1],
            "bpi_qt_a": [1, None, 1],
        })

        canonical = canonicalize_bpi(raw, "SIA_BPI")

        assert canonical["dt_atendimento"].to_list() == [date(2026, 1, 10), None, None]
        assert canonical["dt_atendimento_invalida"].to_list() == [False, True, True]
        assert canonical["folha"].to_list() == [1, 1, 1]
        assert canonical["seq"].to_list() == [1, 2, None]
