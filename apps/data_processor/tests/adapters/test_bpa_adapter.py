"""Testes das transformações puras do bpa_adapter sobre o raw S_PRD."""

from __future__ import annotations

from datetime import date

import polars as pl

from data_processor.adapters.bpa_adapter import (
    RAW_SCHEMA,
    canonicalize,
    prepare_raw,
    quality_issues,
    with_record_ids,
)


def _raw(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "prd_uid": "2269481", "prd_cmp": "202608", "prd_org": "BPI", "prd_flh": "001",
        "prd_seq": "01", "prd_pa": "0301010072", "prd_cbo": "225125", "prd_cid": "J00",
        "prd_idade": "046", "prd_dtaten": "20260805", "prd_cnsmed": "999000000000101",
        "prd_qt_p": 1.0,
    }
    row.update(overrides)
    return row


def _keyed(rows: list[dict[str, object]], file_subtype: str = "BPA_I") -> pl.DataFrame:
    frame = pl.DataFrame(rows, schema_overrides={"prd_qt_p": pl.Float64})
    return with_record_ids(prepare_raw(frame), file_subtype)


def _codes(rows: list[dict[str, object]], file_subtype: str = "BPA_I") -> list[str]:
    return quality_issues(_keyed(rows, file_subtype), file_subtype, "2026-08")["code"].to_list()


def test_prepare_raw_descarta_colunas_de_pii() -> None:
    frame = pl.DataFrame([_raw(prd_cnspac="999000000000001", prd_cpf_pcnte="90000000001",
                               prd_nmpac="PACIENTE TESTE 001")])

    prepared = prepare_raw(frame)

    assert set(prepared.columns) == set(RAW_SCHEMA)


def test_prepare_raw_converte_branco_em_nulo_e_materializa_colunas() -> None:
    frame = pl.DataFrame([{"prd_uid": " 2269481 ", "prd_cid": "    "}])

    row = prepare_raw(frame).to_dicts()[0]

    assert row["prd_uid"] == "2269481"
    assert row["prd_cid"] is None
    assert row["prd_qt_p"] is None


def test_prepare_raw_de_parquet_sem_colunas_nao_cria_linha() -> None:
    assert prepare_raw(pl.DataFrame()).height == 0


def test_prepare_raw_preserva_coluna_op_de_delta() -> None:
    frame = pl.DataFrame([{**_raw(), "_op": "U"}])

    assert prepare_raw(frame).columns[-1] == "_op"


def test_source_record_id_diferencia_linhas_identicas_pelo_ordinal() -> None:
    keyed = _keyed([_raw(), _raw()])

    assert keyed["_ordinal"].to_list() == [0, 1]
    assert keyed["source_record_id"].n_unique() == 2


def test_source_record_id_depende_do_subtipo() -> None:
    bpa_i = _keyed([_raw()], "BPA_I")["source_record_id"][0]
    bpa_c = _keyed([_raw(prd_org="BPA")], "BPA_C")["source_record_id"][0]

    assert bpa_i != bpa_c


def test_linha_valida_nao_gera_quality_issue() -> None:
    assert _codes([_raw()]) == []


def test_registra_codigo_de_referencia_desconhecido() -> None:
    codes = _codes([_raw(prd_uid="12", prd_pa="ABC", prd_cbo="22A", prd_cid="Z9")])

    assert sorted(codes) == ["cbo_invalido", "cid_invalido", "cnes_invalido", "sigtap_invalido"]


def test_registra_quantidade_fracionaria_nula_ou_zero() -> None:
    rows = [_raw(prd_seq="01", prd_qt_p=2.5), _raw(prd_seq="02", prd_qt_p=None),
            _raw(prd_seq="03", prd_qt_p=0.0)]

    assert _codes(rows) == ["quantidade_invalida"] * 3


def test_trata_data_de_atendimento_nula_e_invalida_explicitamente() -> None:
    rows = [_raw(prd_seq="01", prd_dtaten=""), _raw(prd_seq="02", prd_dtaten="20261345")]

    assert sorted(_codes(rows)) == ["data_atendimento_ausente", "data_atendimento_invalida"]


def test_bpa_c_nao_exige_data_cid_nem_cns() -> None:
    row = _raw(prd_org="BPA", prd_dtaten=None, prd_cid=None, prd_cnsmed=None)

    assert _codes([row], "BPA_C") == []


def test_registra_competencia_e_origem_divergentes() -> None:
    codes = _codes([_raw(prd_cmp="202607", prd_org="BPA")])

    assert sorted(codes) == ["competencia_divergente", "origem_divergente"]


def test_canonicaliza_codigos_e_marca_linha_invalida() -> None:
    keyed = _keyed([_raw(prd_seq="01"), _raw(prd_seq="02", prd_pa="ABC", prd_qt_p=3.0)])
    issues = quality_issues(keyed, "BPA_I", "2026-08")

    rows = canonicalize(keyed, "BPA_I", "2026-08", issues["source_record_id"]).to_dicts()

    assert rows[0]["sigtap"] == "0301010072"
    assert rows[0]["data_atendimento"] == date(2026, 8, 5)
    assert rows[0]["quantidade"] == 1
    assert rows[0]["idade"] == 46
    assert rows[0]["valido"] is True
    assert rows[1]["sigtap"] is None
    assert rows[1]["quantidade"] == 3
    assert rows[1]["valido"] is False


def test_canonicaliza_bpa_c_sem_cid_nem_data() -> None:
    keyed = _keyed([_raw(prd_org="BPA")], "BPA_C")
    issues = quality_issues(keyed, "BPA_C", "2026-08")

    row = canonicalize(keyed, "BPA_C", "2026-08", issues["source_record_id"]).to_dicts()[0]

    assert row["cid"] is None
    assert row["data_atendimento"] is None
    assert row["competencia"] == "2026-08"
