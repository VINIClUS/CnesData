"""Probe ddb-throughput: teto OnDemand 100/25 lido de volta e carga observada, nunca afirmada."""

import json
from pathlib import Path

import pytest

from tests.integration.aws_capability._b1_testing import NAME
from tests.integration.aws_capability._probe_testing import (
    INDEXES,
    TABLE,
    THROUGHPUT,
    ProbeLab,
    make_probe_lab,
    payload_text,
)
from tests.integration.aws_capability._service_contracts import ApiError, Reply
from tests.integration.aws_capability.contracts import RequestContext, ServiceFailure

OBSERVED = {
    "tx_code": "ok",
    "tx_reason_codes": {},
    "batch_1_code": "ok",
    "batch_1_unprocessed": 0,
    "batch_2_code": "ok",
    "batch_2_unprocessed": 0,
    "batch_3_code": "ok",
    "batch_3_unprocessed": 0,
    "batch_4_code": "ok",
    "batch_4_unprocessed": 0,
    "batch_accepted_items": 100,
    "batch_elapsed_seconds": 4.0,
    "batch_rate_items_per_second": 25.0,
    "readback_code": "ok",
    "readback_count": 200,
}


@pytest.fixture
def lab(tmp_path: Path) -> ProbeLab:
    return make_probe_lab(tmp_path, "ddb-throughput")


def run(lab: ProbeLab) -> dict:
    return dict(lab.services.probe(lab.resources, lab.spec, lab.context))


def operations(lab: ProbeLab) -> list[str]:
    return [operation for _, operation, _ in lab.aws.calls]


def requests(lab: ProbeLab, operation: str) -> list[dict]:
    return [dict(r.payload) for r in lab.aws.requests if r.operation == operation]


def key_of(item: dict) -> tuple[str, str]:
    return item["pk"]["S"], item["sk"]["S"]


def test_observa_codigos_contagens_e_taxa(lab: ProbeLab) -> None:
    assert run(lab) == OBSERVED


def test_ordem_espera_active_transacao_quatro_lotes_e_contagem(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "ddb-throughput", ddb_ready_describes=3)
    lab.aws.tables[NAME].status = "CREATING"
    lab.aws.tables[NAME].ready_in = 3
    run(lab)
    assert operations(lab) == (
        ["describe-table"] * 4
        + ["transact-write-items"]
        + ["batch-write-item"] * 4
        + ["scan"]
    )
    assert len(lab.pauses) == 3
    assert set(lab.pauses) == {2.0}


def test_transacao_tem_100_puts_em_chaves_distintas_na_tabela_do_run(lab: ProbeLab) -> None:
    run(lab)
    (payload,) = requests(lab, "transact-write-items")
    assert set(payload) == {"TransactItems"}
    actions = payload["TransactItems"]
    assert len(actions) == 100
    assert all(set(a) == {"Put"} and a["Put"]["TableName"] == NAME for a in actions)
    assert len({key_of(a["Put"]["Item"]) for a in actions}) == 100


def test_quatro_lotes_de_25_puts_sem_chaves_repetidas_nem_da_transacao(lab: ProbeLab) -> None:
    run(lab)
    batches = requests(lab, "batch-write-item")
    assert len(batches) == 4
    keys = set()
    for payload in batches:
        assert set(payload) == {"RequestItems"}
        ((table, writes),) = payload["RequestItems"].items()
        assert table == NAME
        assert len(writes) == 25
        assert all(set(w) == {"PutRequest"} for w in writes)
        keys |= {key_of(w["PutRequest"]["Item"]) for w in writes}
    (tx,) = requests(lab, "transact-write-items")
    keys |= {key_of(a["Put"]["Item"]) for a in tx["TransactItems"]}
    assert len(keys) == 200


def test_contagem_e_scan_fortemente_consistente_so_com_count(lab: ProbeLab) -> None:
    run(lab)
    assert requests(lab, "scan") == [
        {"TableName": NAME, "Select": "COUNT", "ConsistentRead": True}
    ]


def test_contagem_vem_do_scan_e_nao_do_que_foi_enviado(tmp_path: Path) -> None:
    lab = make_probe_lab(
        tmp_path, "ddb-throughput", batch_unprocessed={2: 5}, ddb_tx_reasons={"Rejected": 1}
    )
    observed = run(lab)
    assert observed["readback_count"] == 95
    assert observed["batch_2_unprocessed"] == 5
    assert observed["batch_accepted_items"] == 95
    assert observed["batch_rate_items_per_second"] == 23.75


def test_cancelamento_da_transacao_vira_mapa_de_codigos_com_contagem(tmp_path: Path) -> None:
    reasons = {"None": 97, "ThrottlingError": 3}
    lab = make_probe_lab(tmp_path, "ddb-throughput", ddb_tx_reasons=reasons)
    observed = run(lab)
    assert observed["tx_code"] == "TransactionCanceledException"
    assert observed["tx_reason_codes"] == reasons
    assert operations(lab).count("batch-write-item") == 4
    assert observed["readback_count"] == 100


def test_throttle_em_chamada_e_observacao_e_nao_interrompe(tmp_path: Path) -> None:
    lab = make_probe_lab(
        tmp_path,
        "ddb-throughput",
        ddb_errors={"transact-write-items": "ThrottlingException"},
        batch_errors={2: "ProvisionedThroughputExceededException"},
    )
    observed = run(lab)
    assert observed["tx_code"] == "ThrottlingException"
    assert observed["tx_reason_codes"] == {}
    assert observed["batch_2_code"] == "ProvisionedThroughputExceededException"
    assert observed["batch_2_unprocessed"] == 25
    assert observed["batch_accepted_items"] == 75
    assert observed["readback_count"] == 75
    assert operations(lab)[-1] == "scan"


@pytest.mark.parametrize("operation", ["transact-write-items", "batch-write-item", "scan"])
def test_acesso_negado_nas_etapas_observadas_falha_fechado_sem_chamadas_seguintes(
    tmp_path: Path, operation: str
) -> None:
    lab = make_probe_lab(
        tmp_path, "ddb-throughput", ddb_errors={operation: "AccessDeniedException"}
    )
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=throughput_call_failed",)
    tail = {
        "transact-write-items": ["transact-write-items"],
        "batch-write-item": ["transact-write-items", "batch-write-item"],
        "scan": ["transact-write-items", *["batch-write-item"] * 4, "scan"],
    }[operation]
    assert operations(lab) == ["describe-table", *tail]


def test_negado_em_um_lote_interrompe_os_lotes_seguintes(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "ddb-throughput", batch_errors={2: "AccessDeniedException"})
    with pytest.raises(ServiceFailure):
        run(lab)
    assert operations(lab).count("batch-write-item") == 2
    assert "scan" not in operations(lab)


def test_falha_de_transporte_nao_vira_observacao(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "ddb-throughput")

    def local_failure(payload: dict, who: str) -> Reply:
        return Reply(False, {}, ApiError("operation_failed", {}, "local"))

    lab.aws._dynamodb_scan = local_failure  # type: ignore[method-assign]
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=throughput_call_failed",)


def test_codigo_fora_do_alfabeto_vira_other(tmp_path: Path) -> None:
    lab = make_probe_lab(
        tmp_path, "ddb-throughput", ddb_errors={"transact-write-items": "Weird Code!"}
    )
    assert run(lab)["tx_code"] == "Other"


def test_scan_paginado_soma_paginas(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "ddb-throughput", scan_page_size=60)
    observed = run(lab)
    assert observed["readback_count"] == 200
    scans = requests(lab, "scan")
    assert len(scans) == 4
    assert "ExclusiveStartKey" not in scans[0]
    assert scans[1]["ExclusiveStartKey"] == {"offset": {"N": "60"}}


def test_scan_sem_fim_e_limitado_pelas_paginas_do_perfil(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "ddb-throughput", scan_page_size=1)
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=readback_incomplete",)
    assert len(requests(lab, "scan")) == lab.budget.limits.pages


def test_espera_active_e_limitada_e_nao_escreve(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "ddb-throughput")
    lab.aws.tables[NAME].status = "CREATING"
    lab.aws.tables[NAME].ready_in = 10**6
    lab.budget.deadline = lab.clock[0] + 10**6
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=table_not_ready",)
    assert set(operations(lab)) == {"describe-table"}
    assert len(lab.aws.calls) == 30
    assert len(lab.pauses) == 29


def test_tabela_ainda_inexistente_na_espera_e_tentada_de_novo(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "ddb-throughput")
    table = lab.aws.tables.pop(NAME)
    original = lab.aws._dynamodb_describe_table
    seen: list[int] = []

    def appearing(payload: dict, who: str):
        seen.append(1)
        if len(seen) == 3:
            lab.aws.tables[NAME] = table
        return original(payload, who)

    lab.aws._dynamodb_describe_table = appearing  # type: ignore[method-assign]
    assert run(lab)["tx_code"] == "ok"
    assert operations(lab)[:3] == ["describe-table"] * 3


def broken(breakage: str) -> dict[str, object] | None:
    if breakage == "ausente":
        return None
    if breakage == "leitura":
        return {**THROUGHPUT, "MaxReadRequestUnits": 99}
    return {**THROUGHPUT, "MaxWriteRequestUnits": 26}


@pytest.mark.parametrize(
    "target", ["table", *INDEXES], ids=lambda target: f"mismatch-{target}"
)
@pytest.mark.parametrize("breakage", ["leitura", "escrita", "ausente"])
def test_divergencia_de_teto_na_leitura_falha_antes_de_qualquer_escrita(
    lab: ProbeLab, target: str, breakage: str
) -> None:
    table = lab.aws.tables[NAME]
    replacement = broken(breakage)
    if target == "table":
        table.throughput = replacement
    else:
        table.index_throughput[target] = replacement
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == (f"probe=throughput_mismatch target={target}",)
    assert set(operations(lab)) == {"describe-table"}


def test_gsi_ausente_na_leitura_e_divergencia(lab: ProbeLab) -> None:
    del lab.aws.tables[NAME].index_throughput["gsi4"]
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=throughput_mismatch target=gsi4",)


def test_so_valores_simples_nas_observacoes(lab: ProbeLab) -> None:
    observed = run(lab)
    text = json.dumps(observed)
    assert NAME not in text
    assert "arn:" not in text
    assert "cap#" not in text
    assert all(isinstance(v, str | int | float | dict) for v in observed.values())
    assert "cap#" in payload_text(lab)


@pytest.mark.parametrize("case", ["lane", "extra", "vazio", "spec"])
def test_recusa_recursos_invalidos_sem_io(lab: ProbeLab, case: str) -> None:
    resources, context, spec = lab.resources, lab.context, lab.spec
    if case == "lane":
        context = RequestContext(lab.budget, "cleanup")
    elif case == "extra":
        resources = [*resources, TABLE]
    elif case == "vazio":
        resources = []
    else:
        spec = lab.spec.__class__(**{**lab.spec.__dict__, "run_id": "deadbeef"})
    with pytest.raises(ServiceFailure) as error:
        lab.services.probe(resources, spec, context)
    assert error.value.args[0].startswith("probe=")
    assert lab.aws.calls == []
