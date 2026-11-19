"""Lookup S3 de multipart uploads prova o par exato por leitura pontual."""

import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_roots_testing import (
    ReadLab,
    load_fixture,
    make_read_lab,
    respond,
)
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Limits,
    Ownership,
    RequestContext,
    Resource,
    ServiceFailure,
)
from tests.integration.aws_capability.test_service_s3_children_lookup import assert_prefix

DATA = load_fixture(Path(__file__).parent / "fixtures/cli/children/s3_upload_lookup.json")
UPLOAD = Resource(**DATA["resource"])


@pytest.fixture
def read_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> Iterator[ReadLab]:
    lab = make_read_lab(monkeypatch, tmp_path, caplog, capsys)
    yield lab
    lab.check_private()


def owner() -> Ownership:
    return Ownership(
        "111111111111",
        {
            "Project": "CnesData",
            "Purpose": "aws-capability",
            "Run": "a1b2c3d4",
            "ExpiresAt": "1060",
        },
        "us-east-2",
    )


def trace(*names: str) -> list[tuple[str, str, str, dict[str, object]]]:
    return [tuple(DATA["calls"][name]) for name in names]


def enqueue(lab: ReadLab, *names: str) -> None:
    for name in names:
        respond(lab, DATA["replies"][name])


def enqueue_error(lab: ReadLab, name: str) -> None:
    code, stdout, stderr = DATA["errors"][name]
    lab.process.outcomes.append((code, stdout, stderr))


def assert_trace(lab: ReadLab, *names: str) -> None:
    expected = list(names)
    for index, name in enumerate(expected):
        if name == "parts" and lab.process.calls[index][0][2] == "list-multipart-uploads":
            expected[index] = "legacy_parts"
    assert_process(lab, trace(*expected))


@pytest.mark.parametrize("reply", ["parts", "parts_truncated", "parts_ids_only"])
def test_lookup_presente_com_parts_vazias_prova_ids_exatos(
    read_lab: ReadLab, reply: str
) -> None:
    enqueue(read_lab, "location", "tags", reply)
    assert read_lab.services.lookup(UPLOAD, read_lab.context) == owner()
    assert_process(read_lab, trace("location", "tags", "parts"))


def test_fronteira_entrega_resposta_list_parts_valida_sem_setup_artificial(
    read_lab: ReadLab,
) -> None:
    respond(read_lab, DATA["replies"]["parts"])
    reply = read_lab.services.ports.commands.call(
        Request("s3api", "list-parts", DATA["calls"]["parts"][3]), read_lab.context
    )
    assert reply.ok
    assert reply.data == DATA["replies"]["parts"]
    assert_process(read_lab, trace("parts"))


def test_lookup_upload_tem_trace_location_tags_list_parts_sem_enumeracao(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags", "parts_bridge")
    read_lab.services.lookup(UPLOAD, read_lab.context)
    assert [call[0][2] for call in read_lab.process.calls] == [
        "get-bucket-location",
        "get-bucket-tagging",
        "list-parts",
    ]
    assert all("list-multipart-uploads" not in call[0] for call in read_lab.process.calls)
    assert all("abort-multipart-upload" not in call[0] for call in read_lab.process.calls)


def test_no_such_upload_remoto_de_list_parts_retorna_none(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags")
    enqueue_error(read_lab, "remote_no_upload")
    assert read_lab.services.lookup(UPLOAD, read_lab.context) is None
    assert_trace(read_lab, "location", "tags", "parts")


@pytest.mark.parametrize("phase", ["location", "tags"])
def test_no_such_upload_remoto_no_root_falha_fechado_sem_avancar(
    read_lab: ReadLab, phase: str
) -> None:
    prefix = ["location"] if phase == "tags" else []
    enqueue(read_lab, *prefix)
    enqueue_error(read_lab, "remote_no_upload")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(UPLOAD, read_lab.context)
    assert error.value.args != ("service=not_found",)
    read_lab.check_private(error.value)
    assert_process(read_lab, trace(*prefix, phase))


def test_cleanup_lookup_preserva_contrato_trace_e_tres_debitos(read_lab: ReadLab) -> None:
    read_lab.context = RequestContext(read_lab.context.budget, "cleanup")
    enqueue(read_lab, "location", "tags", "parts_bridge")
    assert read_lab.services.lookup(UPLOAD, read_lab.context) == owner()
    assert_process(read_lab, trace("location", "tags", "parts"))
    assert read_lab.context.budget.work_used == 0
    assert read_lab.context.budget.limits == Limits()


def test_no_such_upload_local_falha_fechado(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags")
    enqueue_error(read_lab, "local_no_upload")
    with pytest.raises(ServiceFailure):
        read_lab.services.lookup(UPLOAD, read_lab.context)
    assert_trace(read_lab, "location", "tags", "parts")


@pytest.mark.parametrize(
    "reply",
    [
        "parts_wrong_bucket",
        "parts_wrong_key",
        "parts_wrong_upload",
        "parts_missing_bucket",
        "parts_missing_key",
        "parts_missing_upload",
        "parts_bad_truncated",
        "parts_bad_parts",
    ],
)
def test_resposta_list_parts_invalida_falha_fechado(read_lab: ReadLab, reply: str) -> None:
    enqueue(read_lab, "location", "tags", reply)
    with pytest.raises(ServiceFailure):
        read_lab.services.lookup(UPLOAD, read_lab.context)
    assert_trace(read_lab, "location", "tags", "parts")


def test_inspect_converte_ausencia_conclusiva_em_not_found(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags")
    enqueue_error(read_lab, "remote_no_upload")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(UPLOAD, read_lab.context)
    assert error.value.args == ("service=not_found",)
    assert_trace(read_lab, "location", "tags", "parts")


def test_dependents_de_folha_converte_ausencia_conclusiva_em_not_found(
    read_lab: ReadLab,
) -> None:
    enqueue(read_lab, "location", "tags")
    enqueue_error(read_lab, "remote_no_upload")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(UPLOAD, None, read_lab.context)
    assert error.value.args == ("service=not_found",)
    assert_trace(read_lab, "location", "tags", "parts")


def test_segunda_busca_refaz_root_e_para_antes_de_list_parts_se_tags_sumirem(
    read_lab: ReadLab,
) -> None:
    enqueue(read_lab, "location", "tags", "parts_bridge", "location", "tags_empty")
    assert read_lab.services.lookup(UPLOAD, read_lab.context) == owner()
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(UPLOAD, read_lab.context)
    assert error.value.args == ("service=not_owned",)
    assert_trace(read_lab, "location", "tags", "parts", "location", "tags")


@pytest.mark.parametrize("failure", ["remote_no_bucket", "remote_denied"])
def test_falha_no_root_nao_vira_ausencia_de_upload(read_lab: ReadLab, failure: str) -> None:
    enqueue_error(read_lab, failure)
    with pytest.raises(ServiceFailure):
        read_lab.services.lookup(UPLOAD, read_lab.context)
    assert_process(read_lab, trace("location"))


@pytest.mark.parametrize(
    "failure",
    [
        "remote_no_bucket",
        "remote_no_key",
        "remote_404",
        "remote_no_upload_exception",
        "bad_json",
        "empty_stdout",
    ],
)
def test_falha_ambigua_em_list_parts_falha_fechado(read_lab: ReadLab, failure: str) -> None:
    enqueue(read_lab, "location", "tags")
    enqueue_error(read_lab, failure)
    with pytest.raises(ServiceFailure):
        read_lab.services.lookup(UPLOAD, read_lab.context)
    assert_trace(read_lab, "location", "tags", "parts")


@pytest.mark.parametrize("reply", ["location_foreign", "tags_foreign"])
def test_root_estrangeiro_interrompe_antes_de_list_parts(read_lab: ReadLab, reply: str) -> None:
    if reply == "location_foreign":
        enqueue(read_lab, reply, "tags")
    else:
        enqueue(read_lab, "location", reply)
    with pytest.raises(ServiceFailure):
        read_lab.services.lookup(UPLOAD, read_lab.context)
    assert_process(read_lab, trace("location", "tags"))


def test_deadline_interrompe_antes_de_list_parts(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags")
    read_lab.context.budget.now = lambda: 1060 if len(read_lab.process.calls) >= 2 else 1000
    with pytest.raises(BudgetExhausted):
        read_lab.services.lookup(UPLOAD, read_lab.context)
    assert_process(read_lab, trace("location", "tags"))


def test_reserva_de_cleanup_interrompe_antes_de_list_parts(read_lab: ReadLab) -> None:
    for _ in range(38):
        read_lab.context.budget.acquire("work")
    enqueue(read_lab, "location", "tags")
    with pytest.raises(BudgetExhausted) as error:
        read_lab.services.lookup(UPLOAD, read_lab.context)
    assert error.value.args == ("budget=requests",)
    assert_prefix(read_lab, trace("location", "tags"), 38)
    assert read_lab.context.budget.work_used == 40
    assert read_lab.context.budget.limits == Limits()


def test_list_parts_remoto_nao_vaza_canary_nem_persistencia(read_lab: ReadLab) -> None:
    enqueue_error(read_lab, "remote_no_upload")
    reply = read_lab.services.ports.commands.call(
        Request("s3api", "list-parts", DATA["calls"]["parts"][3]), read_lab.context
    )
    assert reply.error is not None
    assert reply.error.code == "NoSuchUpload"
    assert reply.error.details["Message"] == "R1_PRIVATE_CANARY"
    assert_process(read_lab, trace("parts"))


def test_fixture_carrega_sem_chaves_duplicadas() -> None:
    assert DATA["resource"]["service"] == "s3"
    assert json.loads(DATA["resource"]["resource_id"])["child_id"]["upload_id"] == (
        "upload-private-1"
    )
