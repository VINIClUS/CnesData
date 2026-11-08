"""Lookup de client exige root owned e pertença fresca em cada leitura."""

import json
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_cognito_testing import load_fixture, make_read_lab
from tests.integration.aws_capability._service_read_assertions import assert_private, assert_process
from tests.integration.aws_capability._service_read_testing import ReadLab
from tests.integration.aws_capability.contracts import Ownership, Resource, ServiceFailure

DATA = load_fixture(Path(__file__).parent / "fixtures/cli/children/cognito_client.json")
CLIENT = Resource(**DATA["resource"])
TRACE = [tuple(call) for call in DATA["expected"]["trace"]]


@pytest.fixture
def read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    return make_read_lab(monkeypatch, tmp_path)


def enqueue(lab: ReadLab, *names: str) -> None:
    for name in names:
        lab.process.outcomes.append((0, json.dumps(DATA["replies"][name]), ""))


def enqueue_error(lab: ReadLab, name: str) -> None:
    lab.process.outcomes.append(tuple(DATA["errors"][name]))


def assert_owner(owner: Ownership | None) -> None:
    assert owner == Ownership(
        "111111111111",
        {
            "Project": "CnesData",
            "Purpose": "aws-capability",
            "Run": "a1b2c3d4",
            "ExpiresAt": "1060",
        },
        "us-east-2",
    )


def test_lookup_client_exige_pool_opaco_tags_e_pertenca_frescos(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags", "client")
    assert_owner(read_lab.services.lookup(CLIENT, read_lab.context))
    assert_process(read_lab, TRACE)


def test_inspect_client_exige_a_mesma_prova(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags", "client")
    assert_owner(read_lab.services.inspect(CLIENT, read_lab.context))
    assert_process(read_lab, TRACE)


def test_segunda_leitura_do_client_nao_reutiliza_tags(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags", "client", "root")
    read_lab.process.outcomes.append(
        (0, '{"Tags":{"Project":"CnesData","Purpose":"aws-capability","ExpiresAt":"1060"}}', "")
    )
    assert_owner(read_lab.services.lookup(CLIENT, read_lab.context))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(CLIENT, read_lab.context)
    assert_private(error.value, read_lab, "")
    assert error.value.args == ("service=not_owned",)
    assert_process(read_lab, TRACE + TRACE[:2])


@pytest.mark.parametrize("case", DATA["invalid_clients"])
def test_cliente_trocado_ou_pool_divergente_nao_herda_tags(read_lab: ReadLab, case: str) -> None:
    enqueue(read_lab, "root", "tags")
    read_lab.process.outcomes.append((0, json.dumps(DATA["invalid_clients"][case]), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(CLIENT, read_lab.context)
    assert_private(error.value, read_lab, "")
    assert error.value.args == ("service=describe_invalid",)
    assert_process(read_lab, TRACE)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("Arn", "arn:aws:cognito-idp:us-east-2:222222222222:userpool/us-east-2_X9opaqueQ"),
        ("Arn", "arn:aws:cognito-idp:us-west-1:111111111111:userpool/us-east-2_X9opaqueQ"),
        ("Name", "foreign-pool"),
        ("Id", "us-east-2_other"),
    ],
)
def test_root_estrangeiro_bloqueia_describe_do_client(
    read_lab: ReadLab, field: str, value: str
) -> None:
    reply = load_fixture(Path(__file__).parent / "fixtures/cli/children/cognito_client.json")
    reply["replies"]["root"]["UserPool"][field] = value
    read_lab.process.outcomes.append((0, json.dumps(reply["replies"]["root"]), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(CLIENT, read_lab.context)
    assert_private(error.value, read_lab, "")
    assert error.value.args == ("service=describe_invalid",)
    assert_process(read_lab, TRACE[:1])


@pytest.mark.parametrize(
    ("field", "value"),
    [("Project", "other"), ("Purpose", "other"), ("Run", "e5f6a7b8"), ("ExpiresAt", "1061")],
)
def test_tags_de_outro_dono_bloqueiam_describe_do_client(
    read_lab: ReadLab, field: str, value: str
) -> None:
    enqueue(read_lab, "root")
    tags = dict(DATA["replies"]["tags"]["Tags"])
    tags[field] = value
    read_lab.process.outcomes.append((0, json.dumps({"Tags": tags}), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(CLIENT, read_lab.context)
    assert_private(error.value, read_lab, "")
    assert error.value.args == ("service=not_owned",)
    assert_process(read_lab, TRACE[:2])


def test_nome_do_root_fora_do_run_nao_herda_tags_corretas(read_lab: ReadLab) -> None:
    foreign = Resource("cognito", CLIENT.resource_id, "foreign-pool")
    root = dict(DATA["replies"]["root"]["UserPool"], Name="foreign-pool")
    read_lab.process.outcomes.append((0, json.dumps({"UserPool": root}), ""))
    enqueue(read_lab, "tags")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(foreign, read_lab.context)
    assert_private(error.value, read_lab, "")
    assert error.value.args == ("service=not_owned",)
    assert_process(read_lab, TRACE[:2])


@pytest.mark.parametrize("method", ["lookup", "inspect"])
def test_client_ausente_exige_root_comprovado(read_lab: ReadLab, method: str) -> None:
    enqueue(read_lab, "root", "tags")
    enqueue_error(read_lab, "not_found")
    if method == "lookup":
        assert read_lab.services.lookup(CLIENT, read_lab.context) is None
    else:
        with pytest.raises(ServiceFailure) as error:
            read_lab.services.inspect(CLIENT, read_lab.context)
        assert_private(error.value, read_lab, "")
        assert error.value.args == ("service=not_found",)
    assert_process(read_lab, TRACE)


@pytest.mark.parametrize("phase", ["root", "tags"])
@pytest.mark.parametrize("method", ["lookup", "inspect"])
def test_root_ausente_ou_falha_de_tags_nao_prova_ausencia_do_client(
    read_lab: ReadLab, phase: str, method: str
) -> None:
    if phase == "tags":
        enqueue(read_lab, "root")
    enqueue_error(read_lab, "not_found")
    with pytest.raises(ServiceFailure) as error:
        getattr(read_lab.services, method)(CLIENT, read_lab.context)
    assert_private(error.value, read_lab, "")
    assert error.value.args == (
        "service=not_found" if phase == "root" else "service=request_failed",
    )
    assert_process(read_lab, TRACE[: 1 if phase == "root" else 2])


def test_client_presente_ausente_presente_nao_usa_cache(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags", "client", "root", "tags")
    enqueue_error(read_lab, "not_found")
    enqueue(read_lab, "root", "tags", "client")
    assert_owner(read_lab.services.lookup(CLIENT, read_lab.context))
    assert read_lab.services.lookup(CLIENT, read_lab.context) is None
    assert_owner(read_lab.services.lookup(CLIENT, read_lab.context))
    assert_process(read_lab, TRACE * 3)


def test_pool_regional_com_sufixo_opaco_independente_do_run(read_lab: ReadLab) -> None:
    resource = Resource(**DATA["opaque_pool"]["resource"])
    read_lab.process.outcomes.append((0, json.dumps(DATA["opaque_pool"]["root"]), ""))
    enqueue(read_lab, "tags")
    read_lab.process.outcomes.append((0, json.dumps(DATA["opaque_pool"]["client"]), ""))
    assert_owner(read_lab.services.lookup(resource, read_lab.context))
    assert_process(read_lab, [tuple(call) for call in DATA["opaque_pool"]["trace"]])


def test_pool_sem_formato_regional_falha_antes_de_io(read_lab: ReadLab) -> None:
    resource = Resource(
        "cognito",
        '{"child_id":{"client_id":"4m7k8syntheticclient"},"kind":"cognito.client",'
        '"parent_id":"poolOpaqueQ9","root_id":"poolOpaqueQ9",'
        '"run":"a1b2c3d4","service":"cognito","v":1}',
        "cnesdata-cap-a1b2c3d4",
    )
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource, read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    assert_private(error.value, read_lab, "")
    assert_process(read_lab, [])
