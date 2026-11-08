"""Filhos Cognito exigem prova nova do root, pai e identidade API exata."""

import json
import subprocess
import traceback
from collections.abc import Iterator
from copy import deepcopy
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_cognito_testing import load_fixture, make_read_lab
from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_read_testing import ReadLab
from tests.integration.aws_capability.contracts import Resource, ServiceFailure
from tests.integration.aws_capability.test_service_cognito_client_lookup import assert_owner
from tests.integration.aws_capability.test_service_cognito_client_safety import (
    _assert_channels,
    _capture_channels,
    _channels,
)

DATA = load_fixture(Path(__file__).parent / "fixtures/cli/children/cognito_tree.json")
CURSORS = load_fixture(Path(__file__).parent / "fixtures/cli/children/cognito_cursors.json")
KINDS = ("domain", "server", "user", "branding")


@pytest.fixture
def read_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> Iterator[ReadLab]:
    _capture_channels(caplog)
    lab = make_read_lab(monkeypatch, tmp_path)
    yield lab
    assert_private_c2(None, lab, _channels(caplog, capsys))


def resource(kind: str) -> Resource:
    return Resource(**DATA["resources"][kind])


def trace(*names: str) -> list[tuple]:
    return [tuple(DATA["calls"][name]) for name in names]


def enqueue(lab: ReadLab, *names: str) -> None:
    for name in names:
        lab.process.outcomes.append((0, json.dumps(DATA["replies"][name]), ""))


def remote_error(lab: ReadLab, code: str) -> None:
    lab.process.outcomes.append(
        (254, "", json.dumps({"Code": code, "Message": "R1_PRIVATE_CANARY"}))
    )


def assert_private_c2(error: BaseException | None, lab: ReadLab, captured: str) -> None:
    _assert_channels(error, lab, captured)
    public = captured
    if error is not None:
        public += "".join(traceback.format_exception(error)) + repr(error)
    public += repr(lab.services) + repr(lab.services.ports) + repr(lab.services.ports.commands)
    for path in lab.directory.rglob("*"):
        public += str(path.relative_to(lab.directory))
        if path.is_file() and not path.is_symlink():
            public += path.read_text(errors="replace")
    assert all(value not in public for value in DATA["private_values"])


def assert_failure(error: BaseException, lab: ReadLab) -> None:
    assert_private_c2(error, lab, "")


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("method", ["lookup", "inspect"])
def test_filho_exige_readback_exato_e_root_opaco_fresco(
    read_lab: ReadLab, kind: str, method: str
) -> None:
    script = DATA["lookup_scripts"][kind]
    enqueue(read_lab, *script)
    assert_owner(getattr(read_lab.services, method)(resource(kind), read_lab.context))
    assert_process(read_lab, trace(*script))


@pytest.mark.parametrize("kind", KINDS)
def test_segunda_leitura_do_filho_nao_reutiliza_tags(read_lab: ReadLab, kind: str) -> None:
    script = DATA["lookup_scripts"][kind]
    enqueue(read_lab, *script, "root")
    tags = dict(DATA["replies"]["tags"]["Tags"])
    del tags["Run"]
    read_lab.process.outcomes.append((0, json.dumps({"Tags": tags}), ""))
    assert_owner(read_lab.services.lookup(resource(kind), read_lab.context))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource(kind), read_lab.context)
    assert error.value.args == ("service=not_owned",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace(*script, "root", "tags"))


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("phase", ["root", "tags"])
def test_root_ausente_e_falha_de_tags_nao_provam_ausencia_do_filho(
    read_lab: ReadLab, kind: str, phase: str
) -> None:
    if phase == "tags":
        enqueue(read_lab, "root")
    remote_error(read_lab, "ResourceNotFoundException")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource(kind), read_lab.context)
    assert error.value.args == (
        "service=not_found" if phase == "root" else "service=request_failed",
    )
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root", "tags")[: 1 if phase == "root" else 2])


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("Arn", "arn:aws:cognito-idp:us-east-2:222222222222:userpool/us-east-2_X9opaqueQ"),
        ("Arn", "arn:aws:cognito-idp:us-west-1:111111111111:userpool/us-east-2_X9opaqueQ"),
        ("Name", "foreign-pool"),
        ("Id", "us-east-2_other"),
    ],
)
def test_root_divergente_bloqueia_readback_do_filho(
    read_lab: ReadLab, kind: str, field: str, value: str
) -> None:
    root = deepcopy(DATA["replies"]["root"])
    root["UserPool"][field] = value
    read_lab.process.outcomes.append((0, json.dumps(root), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(resource(kind), read_lab.context)
    assert error.value.args == ("service=describe_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root"))


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("field", ["Project", "Purpose", "Run", "ExpiresAt"])
def test_tag_divergente_bloqueia_readback_do_filho(
    read_lab: ReadLab, kind: str, field: str
) -> None:
    enqueue(read_lab, "root")
    tags = dict(DATA["replies"]["tags"]["Tags"])
    tags[field] = "R1_PRIVATE_CANARY"
    read_lab.process.outcomes.append((0, json.dumps({"Tags": tags}), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(resource(kind), read_lab.context)
    assert error.value.args == ("service=not_owned",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root", "tags"))


@pytest.mark.parametrize(
    "kind_wrapper_field",
    [
        ("domain", "DomainDescription", "Domain"),
        ("domain", "DomainDescription", "UserPoolId"),
        ("domain", "DomainDescription", "AWSAccountId"),
        ("server", "ResourceServer", "Identifier"),
        ("server", "ResourceServer", "UserPoolId"),
        ("user", "", "Username"),
        ("branding", "ManagedLoginBranding", "UserPoolId"),
        ("branding", "ManagedLoginBranding", "ManagedLoginBrandingId"),
    ],
)
@pytest.mark.parametrize("value", [None, 1, "other", "missing"])
def test_identidade_api_divergente_nao_herda_ownership(
    read_lab: ReadLab, kind_wrapper_field: tuple, value: object
) -> None:
    kind, wrapper, field = kind_wrapper_field
    script = DATA["lookup_scripts"][kind]
    enqueue(read_lab, *script[:-1])
    reply = deepcopy(DATA["replies"][kind])
    target = reply[wrapper] if wrapper else reply
    if value == "missing":
        del target[field]
    else:
        target[field] = value
    read_lab.process.outcomes.append((0, json.dumps(reply), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource(kind), read_lab.context)
    assert error.value.args == ("service=describe_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace(*script))


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("reply", [{}, [], "invalid", None])
def test_shape_ausente_nao_confirma_ausencia_do_filho(
    read_lab: ReadLab, kind: str, reply: object
) -> None:
    script = DATA["lookup_scripts"][kind]
    enqueue(read_lab, *script[:-1])
    read_lab.process.outcomes.append((0, json.dumps(reply), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource(kind), read_lab.context)
    assert error.value.args == (
        "service=describe_invalid" if isinstance(reply, dict) else "service=describe_failed",
    )
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace(*script))


@pytest.mark.parametrize(
    "kind_code",
    [
        ("server", "ResourceNotFoundException"),
        ("user", "UserNotFoundException"),
        ("branding", "ResourceNotFoundException"),
    ],
)
@pytest.mark.parametrize("method", ["lookup", "inspect"])
def test_ausencia_do_filho_exige_codigo_da_operacao_e_pais_comprovados(
    read_lab: ReadLab, kind_code: tuple[str, str], method: str
) -> None:
    kind, code = kind_code
    script = DATA["lookup_scripts"][kind]
    enqueue(read_lab, *script[:-1])
    remote_error(read_lab, code)
    if method == "lookup":
        assert read_lab.services.lookup(resource(kind), read_lab.context) is None
    else:
        with pytest.raises(ServiceFailure) as error:
            read_lab.services.inspect(resource(kind), read_lab.context)
        assert error.value.args == ("service=not_found",)
        assert_failure(error.value, read_lab)
    assert_process(read_lab, trace(*script))


@pytest.mark.parametrize(
    ("kind", "code"),
    [
        ("domain", "ResourceNotFoundException"),
        ("user", "ResourceNotFoundException"),
        ("server", "UserNotFoundException"),
        ("branding", "UserNotFoundException"),
    ],
)
def test_ausencia_ambigua_e_domain_permanecem_falha(
    read_lab: ReadLab, kind: str, code: str
) -> None:
    script = DATA["lookup_scripts"][kind]
    enqueue(read_lab, *script[:-1])
    remote_error(read_lab, code)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource(kind), read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace(*script))


@pytest.mark.parametrize("field", ["ClientId", "UserPoolId"])
def test_branding_exige_client_comprovado_antes_da_consulta(read_lab: ReadLab, field: str) -> None:
    enqueue(read_lab, "root", "tags")
    reply = deepcopy(DATA["replies"]["client"])
    reply["UserPoolClient"][field] = "other"
    read_lab.process.outcomes.append((0, json.dumps(reply), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource("branding"), read_lab.context)
    assert error.value.args == ("service=describe_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root", "tags", "client"))


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("v", True),
        ("v", 1.0),
        ("v", 2),
        ("run", "e5f6a7b8"),
        ("service", "s3"),
        ("kind", "cognito.unknown"),
        ("root_id", "bad-pool"),
        ("parent_id", "foreign"),
        ("child_id", {}),
        ("extra", "field"),
    ],
)
def test_envelope_do_filho_invalido_recusa_antes_de_io(
    read_lab: ReadLab, kind: str, field: str, value: object
) -> None:
    original = resource(kind)
    envelope = json.loads(original.resource_id)
    envelope[field] = value
    invalid = Resource("cognito", json.dumps(envelope), original.name)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(invalid, read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


@pytest.mark.parametrize("kind", KINDS)
def test_filho_presente_ausente_presente_refaz_todas_as_provas(
    read_lab: ReadLab, kind: str
) -> None:
    script = DATA["lookup_scripts"][kind]
    enqueue(read_lab, *script, *script[:-1])
    remote_error(
        read_lab, "UserNotFoundException" if kind == "user" else "ResourceNotFoundException"
    )
    enqueue(read_lab, *script)
    assert_owner(read_lab.services.lookup(resource(kind), read_lab.context))
    if kind == "domain":
        with pytest.raises(ServiceFailure) as error:
            read_lab.services.lookup(resource(kind), read_lab.context)
        assert_failure(error.value, read_lab)
    else:
        assert read_lab.services.lookup(resource(kind), read_lab.context) is None
    assert_owner(read_lab.services.lookup(resource(kind), read_lab.context))
    assert_process(read_lab, trace(*script) * 3)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("failure", [*DATA["errors"], "timeout", "oserror"])
def test_falha_ambigua_do_filho_nao_vira_ausencia_nem_retry(
    read_lab: ReadLab, kind: str, failure: str
) -> None:
    script = DATA["lookup_scripts"][kind]
    enqueue(read_lab, *script[:-1])
    if failure == "timeout":
        outcome = subprocess.TimeoutExpired("aws", 7, output="R1_PRIVATE_CANARY")
    elif failure == "oserror":
        outcome = OSError("R1_PRIVATE_CANARY")
    else:
        outcome = tuple(DATA["errors"][failure])
    read_lab.process.outcomes.append(outcome)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource(kind), read_lab.context)
    assert error.value.args == ("service=describe_failed",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace(*script))


def test_username_privado_valido_alcanca_api_sem_publicar_id(read_lab: ReadLab) -> None:
    private = Resource(
        "cognito",
        '{"child_id":{"username":"R1_PRIVATE_CANARY"},"kind":"cognito.user",'
        '"parent_id":"us-east-2_X9opaqueQ","root_id":"us-east-2_X9opaqueQ",'
        '"run":"a1b2c3d4","service":"cognito","v":1}',
        "cnesdata-cap-a1b2c3d4",
    )
    enqueue(read_lab, "root", "tags")
    read_lab.process.outcomes.append((0, '{"Username":"R1_PRIVATE_CANARY"}', ""))
    assert_owner(read_lab.services.lookup(private, read_lab.context))
    assert_process(
        read_lab,
        [
            *trace("root", "tags"),
            (
                "cognito-idp",
                "admin-get-user",
                "us-east-2",
                {"UserPoolId": "us-east-2_X9opaqueQ", "Username": "R1_PRIVATE_CANARY"},
            ),
        ],
    )


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("field", ["extra", "duplicate"])
def test_envelope_com_child_extra_ou_duplicado_nao_autoriza_filho(
    read_lab: ReadLab, kind: str, field: str
) -> None:
    original = resource(kind)
    if field == "extra":
        duplicate = original.resource_id.replace('"child_id":{', '"child_id":{"extra":"x",', 1)
    else:
        duplicate = original.resource_id.replace('"v":1', '"v":1,"v":1', 1)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(Resource("cognito", duplicate, original.name), read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("root_id", "us-east-2_other"),
        ("parent_id", "us-east-2_other"),
        ("run", "e5f6a7b8"),
        ("kind", "cognito.user"),
        ("child_id", {"client_id": "otherclient"}),
    ],
)
def test_parent_branding_tem_de_ser_client_canonico_coerente(
    read_lab: ReadLab, field: str, value: object
) -> None:
    original = resource("branding")
    envelope = json.loads(original.resource_id)
    parent = json.loads(envelope["parent_id"])
    parent[field] = value
    envelope["parent_id"] = json.dumps(parent, sort_keys=True, separators=(",", ":"))
    invalid = Resource("cognito", json.dumps(envelope), original.name)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(invalid, read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


@pytest.mark.parametrize("kind", KINDS)
def test_nome_do_filho_fora_do_run_nao_e_autorizado_pelas_tags(
    read_lab: ReadLab, kind: str
) -> None:
    original = resource(kind)
    foreign = Resource("cognito", original.resource_id, "foreign-pool")
    root = deepcopy(DATA["replies"]["root"])
    root["UserPool"]["Name"] = "foreign-pool"
    read_lab.process.outcomes.append((0, json.dumps(root), ""))
    enqueue(read_lab, "tags")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(foreign, read_lab.context)
    assert error.value.args == ("service=not_owned",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root", "tags"))
