"""Policies IAM exigem envelope fechado e prova fresca do role global."""

import json
import subprocess
import traceback
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import ExpectedCall, assert_process
from tests.integration.aws_capability._service_roots_testing import (
    ReadLab,
    load_fixture,
    make_read_lab,
    respond,
)
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Limits,
    Ownership,
    Resource,
    ServiceFailure,
)
from tests.integration.aws_capability.test_service_cognito_client_safety import (
    _assert_channels,
    _channels,
)

DATA = load_fixture(Path(__file__).parent / "fixtures/cli/children/iam.json")
CURSORS = load_fixture(Path(__file__).parent / "fixtures/cli/children/iam_cursors.json")


@pytest.fixture
def read_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> Iterator[ReadLab]:
    lab = make_read_lab(monkeypatch, tmp_path, caplog, capsys)
    yield lab
    assert_failure(None, lab)


def assert_failure(error: BaseException | None, lab: ReadLab) -> None:
    public = _channels(lab.channels.caplog, lab.channels.capsys)
    _assert_channels(error, lab, public)
    if error is not None:
        public += "".join(traceback.format_exception(error)) + repr(error)
    public += repr(lab.services) + repr(lab.services.ports) + repr(lab.services.ports.commands)
    for path in lab.directory.rglob("*"):
        public += str(path.relative_to(lab.directory))
        if path.is_file() and not path.is_symlink():
            public += path.read_text(errors="replace")
    assert all(value not in public for value in DATA["private_values"])


def resource(name: str) -> Resource:
    return Resource(**DATA["resources"][name])


def trace(*names: str) -> list[ExpectedCall]:
    return [tuple(DATA["calls"].get(name, CURSORS["calls"].get(name))) for name in names]


def enqueue(lab: ReadLab, *names: str) -> None:
    for name in names:
        respond(lab, DATA["replies"].get(name, CURSORS["replies"].get(name)))


def remote_error(lab: ReadLab, code: str) -> None:
    lab.process.outcomes.append(
        (254, "", json.dumps({"Code": code, "Message": "C4_PRIVATE_DOCUMENT"}))
    )


def owner() -> Ownership:
    return Ownership(
        "111111111111",
        {
            "Project": "CnesData",
            "Purpose": "aws-capability",
            "Run": "a1b2c3d4",
            "ExpiresAt": "1060",
        },
        "aws-global",
    )


@pytest.mark.parametrize("method", ["lookup", "inspect"])
def test_policy_sem_run_no_nome_exige_role_tags_e_get_exatos(
    read_lab: ReadLab, method: str
) -> None:
    enqueue(read_lab, "root", "tags", "policy")
    assert getattr(read_lab.services, method)(resource("policy"), read_lab.context) == owner()
    assert_process(read_lab, trace("root", "tags", "policy"))


@pytest.mark.parametrize("name", ["minimum", "maximum"])
def test_nome_de_policy_preserva_limites_um_e_128_do_modelo(read_lab: ReadLab, name: str) -> None:
    child = resource(name)
    policy = json.loads(child.resource_id)["child_id"]["policy_name"]
    enqueue(read_lab, "root", "tags")
    respond(read_lab, dict(DATA["replies"]["policy"], PolicyName=policy))
    assert read_lab.services.lookup(child, read_lab.context) == owner()
    assert_process(
        read_lab,
        [
            *trace("root", "tags"),
            (
                "iam",
                "get-role-policy",
                "us-east-1",
                {
                    "RoleName": "cnesdata-cap-a1b2c3d4",
                    "PolicyName": policy,
                },
            ),
        ],
    )


def test_tags_paginadas_completas_precedem_get_role_policy(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags_first", "tags_last", "policy")
    assert read_lab.services.lookup(resource("policy"), read_lab.context) == owner()
    assert_process(read_lab, trace("root", "tags", "tags_next", "policy"))


def test_segunda_leitura_nao_reutiliza_tags_removidas(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags", "policy", "root")
    respond(read_lab, {"Tags": []})
    assert read_lab.services.lookup(resource("policy"), read_lab.context) == owner()
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(resource("policy"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=not_owned",)
    assert_process(read_lab, trace("root", "tags", "policy", "root", "tags"))


@pytest.mark.parametrize("method", ["lookup", "inspect"])
def test_no_such_entity_so_confirma_policy_apos_role_owned(read_lab: ReadLab, method: str) -> None:
    enqueue(read_lab, "root", "tags")
    remote_error(read_lab, "NoSuchEntity")
    if method == "lookup":
        assert read_lab.services.lookup(resource("policy"), read_lab.context) is None
    else:
        with pytest.raises(ServiceFailure) as error:
            read_lab.services.inspect(resource("policy"), read_lab.context)
        assert_failure(error.value, read_lab)
        assert error.value.args == ("service=not_found",)
    assert_process(read_lab, trace("root", "tags", "policy"))


@pytest.mark.parametrize("phase", ["root", "tags"])
def test_root_ou_tags_ausentes_nao_provam_ausencia_da_policy(read_lab: ReadLab, phase: str) -> None:
    names = [] if phase == "root" else ["root"]
    enqueue(read_lab, *names)
    remote_error(read_lab, "NoSuchEntity")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource("policy"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace(*names, phase))


@pytest.mark.parametrize(
    "failure", ["NoSuchEntityException", "AccessDenied", "404", "local", "json", "timeout"]
)
def test_falha_ambigua_no_get_policy_nao_e_none_nem_retry(read_lab: ReadLab, failure: str) -> None:
    enqueue(read_lab, "root", "tags")
    if failure == "local":
        read_lab.process.outcomes.append((252, "", '{"Code":"NoSuchEntity"}'))
    elif failure == "json":
        read_lab.process.outcomes.append((0, "C4_PRIVATE_DOCUMENT", ""))
    elif failure == "timeout":
        read_lab.process.outcomes.append(
            subprocess.TimeoutExpired("aws", 7, output="C4_PRIVATE_DOCUMENT")
        )
    else:
        remote_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource("policy"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=describe_failed",)
    assert_process(read_lab, trace("root", "tags", "policy"))


@pytest.mark.parametrize(
    "changes",
    [
        {"RoleName": "OtherRole"},
        {"PolicyName": "OtherPolicy"},
        {"PolicyDocument": None},
        {"PolicyDocument": {}},
        {"PolicyDocument": ""},
    ],
)
def test_resposta_policy_exige_pai_nome_e_documento_modelados(
    read_lab: ReadLab, changes: dict
) -> None:
    enqueue(read_lab, "root", "tags")
    respond(read_lab, dict(DATA["replies"]["policy"], **changes))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource("policy"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=describe_invalid",)
    assert_process(read_lab, trace("root", "tags", "policy"))


@pytest.mark.parametrize("field", ["RoleName", "PolicyName", "PolicyDocument"])
def test_campo_obrigatorio_ausente_nao_confirma_policy(read_lab: ReadLab, field: str) -> None:
    enqueue(read_lab, "root", "tags")
    reply = dict(DATA["replies"]["policy"])
    del reply[field]
    respond(read_lab, reply)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(resource("policy"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=describe_invalid",)
    assert_process(read_lab, trace("root", "tags", "policy"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("v", True),
        ("v", 1.0),
        ("run", "foreign"),
        ("service", "s3"),
        ("kind", "iam.attachment"),
        ("parent_id", "OtherRole"),
        ("root_id", "OtherRole"),
        ("extra", "x"),
        ("child_id", None),
        ("child_id", {"policy_name": ""}),
        ("child_id", {"policy_name": "with space"}),
        ("child_id", {"policy_name": 1}),
        ("child_id", {"policy_name": "P", "extra": "x"}),
    ],
)
def test_envelope_fechado_invalido_recusa_antes_de_io(
    read_lab: ReadLab, field: str, value: object
) -> None:
    valid = resource("policy")
    payload = json.loads(valid.resource_id)
    payload[field] = value
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(Resource("iam", json.dumps(payload), valid.name), read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=resource_invalid",)
    assert_process(read_lab, [])


@pytest.mark.parametrize(
    "case", ["duplicate", "duplicate_child", "malformed", "missing", "oversize", "wrong_name"]
)
def test_recurso_fabricado_duplicado_ou_incompleto_nao_e_posse(
    read_lab: ReadLab, case: str
) -> None:
    child = resource("policy")
    payload = json.loads(child.resource_id)
    name = child.name
    if case == "missing":
        del payload["child_id"]["policy_name"]
    elif case == "oversize":
        payload["child_id"]["policy_name"] = "P" * 129
    elif case == "wrong_name":
        name = "OtherRole"
    encoded = json.dumps(payload)
    if case == "duplicate":
        encoded = encoded.replace('"v": 1', '"v": 1,"v": 1')
    elif case == "duplicate_child":
        encoded = encoded.replace('"policy_name":', '"policy_name":"OtherPolicy","policy_name":')
    elif case == "malformed":
        encoded = "{"
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(Resource("iam", encoded, name), read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=resource_invalid",)
    assert_process(read_lab, [])


@pytest.mark.parametrize("field", ["Arn", "Path", "RoleName"])
def test_role_de_outra_conta_regiao_ou_nome_bloqueia_policy(read_lab: ReadLab, field: str) -> None:
    role = dict(DATA["replies"]["root"]["Role"])
    role[field] = {
        "Arn": "arn:aws:iam:us-east-2:222222222222:role/OtherRole",
        "Path": "/foreign/",
        "RoleName": "OtherRole",
    }[field]
    respond(read_lab, {"Role": role})
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource("policy"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=describe_invalid",)
    assert_process(read_lab, trace("root"))


def test_presente_ausente_presente_reconsulta_role_e_policy(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags", "policy", "root", "tags")
    remote_error(read_lab, "NoSuchEntity")
    enqueue(read_lab, "root", "tags", "policy")
    assert read_lab.services.lookup(resource("policy"), read_lab.context) == owner()
    assert read_lab.services.lookup(resource("policy"), read_lab.context) is None
    assert read_lab.services.lookup(resource("policy"), read_lab.context) == owner()
    assert_process(read_lab, trace("root", "tags", "policy") * 3)


@pytest.mark.parametrize("cut", [0, 2])
def test_deadline_interrompe_antes_do_proximo_readback(read_lab: ReadLab, cut: int) -> None:
    enqueue(read_lab, *("root", "tags")[:cut])
    read_lab.context.budget.now = lambda: 1060 if len(read_lab.process.calls) >= cut else 1000
    with pytest.raises(BudgetExhausted) as error:
        read_lab.services.lookup(resource("policy"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("budget=deadline",)
    assert_process(read_lab, trace(*("root", "tags")[:cut]))
    assert read_lab.context.budget.limits == Limits()


def test_documento_privado_na_resposta_cli_nao_aparece_em_repr(read_lab: ReadLab) -> None:
    from tests.integration.aws_capability.cli import Request

    enqueue(read_lab, "policy")
    reply = read_lab.services.ports.commands.call(
        Request(
            "iam",
            "get-role-policy",
            {
                "RoleName": "cnesdata-cap-a1b2c3d4",
                "PolicyName": "Read_Private+=,.@-",
            },
        ),
        read_lab.context,
    )
    assert reply.ok
    assert reply.data["PolicyDocument"] == {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "C4_PRIVATE_DOCUMENT",
                "Effect": "Allow",
                "Action": "dynamodb:ListTables",
                "Resource": "*",
            }
        ],
    }
    assert "C4_PRIVATE_DOCUMENT" not in repr(reply)
    assert_failure(None, read_lab)
    assert_process(read_lab, trace("policy"))


def test_erro_privado_da_policy_preserva_codigo_sem_publicar_details(read_lab: ReadLab) -> None:
    from tests.integration.aws_capability.cli import Request

    remote_error(read_lab, "AccessDenied")
    reply = read_lab.services.ports.commands.call(
        Request(
            "iam",
            "get-role-policy",
            {
                "RoleName": "cnesdata-cap-a1b2c3d4",
                "PolicyName": "Read_Private+=,.@-",
            },
        ),
        read_lab.context,
    )
    assert reply.error is not None
    assert (reply.error.origin, reply.error.code) == ("service", "AccessDenied")
    assert reply.error.details["Message"] == "C4_PRIVATE_DOCUMENT"
    assert "C4_PRIVATE_DOCUMENT" not in repr(reply) + repr(reply.error)
    assert_failure(None, read_lab)
    assert_process(read_lab, trace("policy"))


def test_reserva_real_bloqueia_get_policy_apos_dois_readbacks(read_lab: ReadLab) -> None:
    from tests.integration.aws_capability.test_service_s3_children_lookup import assert_prefix

    for _ in range(38):
        read_lab.context.budget.acquire("work")
    enqueue(read_lab, "root", "tags")
    with pytest.raises(BudgetExhausted) as error:
        read_lab.services.lookup(resource("policy"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("budget=requests",)
    assert_prefix(read_lab, trace("root", "tags"), 38)
    assert read_lab.process.outcomes == []
    assert read_lab.context.budget.work_used == 40
    assert read_lab.context.budget.limits == Limits()


@pytest.mark.parametrize(
    "arn",
    [
        "arn:aws:iam::222222222222:role/cnesdata-cap/cnesdata-cap-a1b2c3d4",
        "arn:aws:iam:us-east-2:111111111111:role/cnesdata-cap/cnesdata-cap-a1b2c3d4",
    ],
)
def test_conta_e_regiao_iam_sao_validadas_independentemente(read_lab: ReadLab, arn: str) -> None:
    respond(read_lab, {"Role": dict(DATA["replies"]["root"]["Role"], Arn=arn)})
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(resource("policy"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=describe_invalid",)
    assert_process(read_lab, trace("root"))
