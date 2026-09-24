"""TDD de reconcile_sihd: totais por CNES/procedimento/competencia e divergencias."""

from __future__ import annotations

import polars as pl
import pytest

from apps.data_processor.tests.sources.sihd import (
    FakeObjectStore,
    fixture_request,
    json_rows,
    normalize_all,
    normalize_request,
    put_raw,
    raw_rows,
    raw_spec,
    read_parquet,
    reconcile_request,
)
from cnes_contracts.manifests.raw import SourceType
from data_processor.sources.sihd.normalize import normalize_sihd
from data_processor.sources.sihd.reconcile import reconcile_sihd

_INTERNACOES = "normalized-run-1-unit-internacao-1-internacoes"
_PROCEDIMENTOS = "normalized-run-1-unit-proc-aih-1-procedimentos_aih"
_QUALITY_INTERNACAO = "normalized-run-1-unit-internacao-1-quality_issues_internacao"
_QUALITY_PROC = "normalized-run-1-unit-proc-aih-1-quality_issues_proc_aih"
_IDS = [_INTERNACOES, _PROCEDIMENTOS]


def _row(
    group: tuple[str, str], aih: str, totals: tuple[int, int], dates: tuple[str, str] | None
) -> dict[str, object]:
    cnes, procedimento = group
    qtd, valor = totals
    dt_internacao, dt_saida = dates if dates is not None else (None, None)
    return {
        "CNES": cnes, "PROCEDIMENTO": procedimento, "COMPETENCIA": "2026-01",
        "AIH_IDS": [aih], "aih_count": 1, "procedimento_qtd": qtd, "valor_centavos": valor,
        "dt_internacao_min": dt_internacao, "dt_saida_max": dt_saida,
        "_source_manifest_ids": _IDS,
    }


_AIH1 = ("2026-01-03", "2026-01-08")
_AIH2 = ("2026-01-10", "2026-01-12")
_AIH3 = ("2026-01-15", "2026-01-20")
_EXPECTED_ROWS = [
    _row(("0765432", "0303010037"), "3541300000.9", (1, 1000), None),
    _row(("1234567", "0303010037"), "3541300000.1", (1, 15025), _AIH1),
    _row(("1234567", "0303140151"), "3541300000.3", (3, 10000), _AIH3),
    _row(("1234567", "04110100"), "3541300000.2", (1, 2010), _AIH2),
    _row(("1234567", "0411010034"), "3541300000.2", (1, 48000), _AIH2),
    _row(("1234567", "0802010083"), "3541300000.1", (3, 268), _AIH1),
]
_OE = "3541300000"
_EXPECTED_DIVERGENCES = [
    ("cnes_divergente", f"3526100000033|0303140151|2026-01|{_OE}.3.1", "CNES",
     "1234567|0765432", _PROCEDIMENTOS),
    ("cnes_divergente", f"3526100000033|0303140151|2026-01|{_OE}.3.2", "CNES",
     "1234567|0765432", _PROCEDIMENTOS),
    ("internacao_sem_proc", f"3526100000044|0303010037|2026-01|{_OE}.4", "AIH_ID",
     f"{_OE}.4", _INTERNACOES),
    ("proc_sem_internacao", f"3526100000099|0303010037|2026-01|{_OE}.9.1", "AIH_ID",
     f"{_OE}.9", _PROCEDIMENTOS),
    ("quality:campo_obrigatorio_ausente", f"3526100000033|0303140151|2026-01|{_OE}.3.2",
     "VALOR", None, _QUALITY_PROC),
    ("quality:codigo_desconhecido", f"3526100000022|0411010034|2026-01|{_OE}.2",
     "SITUACAO", "7", _QUALITY_INTERNACAO),
    ("quality:codigo_desconhecido", f"3526100000022|04110100|2026-01|{_OE}.2.2",
     "PROCEDIMENTO", "04110100", _QUALITY_PROC),
    ("quality:data_invalida", f"3526100000044|0303010037|2026-01|{_OE}.4",
     "DT_SAIDA", "20260231", _QUALITY_INTERNACAO),
]


def _reconciled() -> tuple[FakeObjectStore, object]:
    store = FakeObjectStore()
    result = reconcile_sihd(reconcile_request(normalize_all(store)), store)
    return store, result


def test_reconcilia_totais_exatos_por_cnes_procedimento_competencia() -> None:
    store, result = _reconciled()
    frame = read_parquet(store, result.reconciliation_manifest.object_key)
    assert json_rows(frame) == _EXPECTED_ROWS
    assert result.reconciliation_manifest.row_count == len(_EXPECTED_ROWS)


def test_divergencias_e_qualidade_chegam_ao_arquivo_de_divergencias() -> None:
    store, result = _reconciled()
    frame = read_parquet(store, result.divergence_manifest.object_key)
    assert frame.rows() == _EXPECTED_DIVERGENCES
    assert result.divergence_manifest.row_count == len(_EXPECTED_DIVERGENCES)


def test_kpis_sihd_sao_inteiros_e_exatos() -> None:
    _, result = _reconciled()
    assert result.kpis == {
        "internacao_count": 4,
        "procedimento_count": 7,
        "valor_total_centavos": 76303,
        "reconciled_row_count": 6,
        "divergence_count": 4,
        "quality_issue_count": 4,
    }


def test_reconciliacao_e_idempotente_em_bytes() -> None:
    first, first_result = _reconciled()
    second, _ = _reconciled()
    for manifest in (first_result.reconciliation_manifest, first_result.divergence_manifest):
        assert first.objects[manifest.object_key] == second.objects[manifest.object_key]


def test_manifests_de_reconciliacao_apontam_para_os_destinos_do_layout() -> None:
    store, result = _reconciled()
    request = reconcile_request(normalize_all(FakeObjectStore()))
    assert result.reconciliation_manifest.object_key == request.reconciliation_key
    assert result.divergence_manifest.object_key == request.divergence_key
    for manifest in (result.reconciliation_manifest, result.divergence_manifest):
        assert manifest.object_sha256 == store.stat(manifest.object_key).sha256


def test_exige_os_quatro_manifests_normalizados() -> None:
    store = FakeObjectStore()
    manifests = normalize_all(store)
    with pytest.raises(ValueError, match="normalized_manifests_incomplete"):
        reconcile_sihd(reconcile_request(manifests[1:]), store)


def test_rejeita_manifest_normalizado_fora_do_layout() -> None:
    store = FakeObjectStore()
    manifests = list(normalize_all(store))
    stray = manifests[0].object_key.rsplit("/", 1)[0] + "/outro.parquet"
    manifests[0] = manifests[0].model_copy(update={"object_key": stray})
    with pytest.raises(ValueError, match="normalized_manifests_incomplete"):
        reconcile_sihd(reconcile_request(tuple(manifests)), store)


def test_rejeita_manifest_normalizado_de_outra_fonte() -> None:
    store = FakeObjectStore()
    manifests = list(normalize_all(store))
    foreign_key = manifests[0].object_key.replace("/SIHD/", "/CNES_LOCAL/")
    manifests[0] = manifests[0].model_copy(
        update={"source_type": SourceType.CNES_LOCAL, "object_key": foreign_key}
    )
    with pytest.raises(ValueError, match="unexpected_source_type"):
        reconcile_sihd(reconcile_request(tuple(manifests)), store)


def test_junta_procedimento_a_internacao_pela_chave_composta_sem_num_aih() -> None:
    store = FakeObjectStore()
    procs = raw_rows("SIHD_PROC_AIH")
    procs[0]["PA_NUM_AIH"] = None
    manifests = [
        *normalize_sihd(fixture_request(store, "SIHD_INTERNACAO"), store).manifests,
        *normalize_sihd(
            normalize_request(
                (put_raw(store, raw_spec("SIHD_PROC_AIH"), pl.DataFrame(procs)),),
                "SIHD_PROC_AIH",
            ),
            store,
        ).manifests,
    ]
    result = reconcile_sihd(reconcile_request(tuple(manifests)), store)
    totals = json_rows(read_parquet(store, result.reconciliation_manifest.object_key))
    assert totals[1] == _EXPECTED_ROWS[1]
    divergences = read_parquet(store, result.divergence_manifest.object_key)
    orphans = divergences.filter(pl.col("kind") == "proc_sem_internacao")
    assert orphans["value"].to_list() == [f"{_OE}.9"]


def test_procedimento_sem_identidade_de_aih_fica_fora_dos_totais_e_e_reportado() -> None:
    store = FakeObjectStore()
    procs = raw_rows("SIHD_PROC_AIH")
    procs[-1]["PA_SEQ_PRINC"] = None
    manifests = [
        *normalize_sihd(fixture_request(store, "SIHD_INTERNACAO"), store).manifests,
        *normalize_sihd(
            normalize_request(
                (put_raw(store, raw_spec("SIHD_PROC_AIH"), pl.DataFrame(procs)),),
                "SIHD_PROC_AIH",
            ),
            store,
        ).manifests,
    ]
    result = reconcile_sihd(reconcile_request(tuple(manifests)), store)
    totals = json_rows(read_parquet(store, result.reconciliation_manifest.object_key))
    assert totals == _EXPECTED_ROWS[1:]
    assert result.kpis["valor_total_centavos"] == sum(row["valor_centavos"] for row in totals)
    divergences = read_parquet(store, result.divergence_manifest.object_key)
    missing = divergences.filter(pl.col("kind") == "quality:campo_obrigatorio_ausente")
    assert "SEQ_PRINC" in missing["field"].to_list()
    orphans = divergences.filter(pl.col("kind") == "proc_sem_internacao")
    assert orphans["value"].to_list() == [None]
