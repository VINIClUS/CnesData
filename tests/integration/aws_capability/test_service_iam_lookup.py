"""Lookup IAM distingue ausência remota conclusiva e falhas inconclusivas."""

import json
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_roots_testing import (
    ReadLab,
    load_fixture,
    make_read_lab,
)
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import Resource, ServiceFailure

FIXTURES = Path(__file__).parent / "fixtures" / "cli" / "r4"
ROLE = Resource("iam", "CapRole", "CapRole")
GET = ("iam", "get-role", "us-east-1", {"RoleName": "CapRole"})
TAGS = ("iam", "list-role-tags", "us-east-1", {"RoleName": "CapRole", "MaxItems": 100})


def data(name: str) -> dict:
    return load_fixture(FIXTURES / f"{name}.json")


def reply(lab: ReadLab, value: object) -> None:
    lab.process.outcomes.append((0, json.dumps(value), ""))


def error_reply(lab: ReadLab, case: str) -> None:
    lab.process.outcomes.append(tuple(data("errors")[case]))


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


def test_fronteira_iam_preserva_wire_code_payload_ambiente_e_ordem(read_lab: ReadLab) -> None:
    error_reply(read_lab, "remote_no_entity")
    observed = read_lab.services.ports.commands.call(
        Request("iam", "get-role", {"RoleName": "CapRole"}), read_lab.context
    )
    assert observed.error is not None
    assert (observed.error.origin, observed.error.code) == ("service", "NoSuchEntity")
    assert_process(read_lab, [GET])


def test_lookup_none_exige_no_such_entity_remoto_no_get_role(read_lab: ReadLab) -> None:
    error_reply(read_lab, "remote_no_entity")
    assert read_lab.services.lookup(ROLE, read_lab.context) is None
    read_lab.check_private()
    assert_process(read_lab, [GET])


def test_inspect_converte_somente_ausencia_conclusiva_em_not_found(read_lab: ReadLab) -> None:
    error_reply(read_lab, "remote_no_entity")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROLE, read_lab.context)
    assert error.value.args == ("service=not_found",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [GET])


@pytest.mark.parametrize(
    "case",
    [
        "remote_no_entity_exception",
        "local_no_entity",
        "remote_denied",
        "remote_403",
        "remote_404",
        "bad_json",
        "empty_stdout",
        "bad_stderr",
    ],
)
def test_get_role_inconclusivo_nunca_vira_none(read_lab: ReadLab, case: str) -> None:
    error_reply(read_lab, case)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROLE, read_lab.context)
    assert error.value.args == ("service=describe_failed",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [GET])


def test_timeout_no_get_role_falha_fechado(read_lab: ReadLab) -> None:
    read_lab.process.outcomes.append(subprocess.TimeoutExpired(["aws", "iam"], 7))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROLE, read_lab.context)
    assert error.value.args == ("service=describe_failed",)
    assert_process(read_lab, [GET])


@pytest.mark.parametrize(
    "case",
    [
        "remote_no_entity",
        "remote_no_entity_exception",
        "remote_denied",
        "remote_403",
        "remote_404",
        "local_no_entity",
        "bad_json",
        "empty_stdout",
        "bad_stderr",
    ],
)
def test_erro_de_tags_apos_role_valido_nunca_apaga_role(read_lab: ReadLab, case: str) -> None:
    reply(read_lab, data("iam")["role"])
    error_reply(read_lab, case)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROLE, read_lab.context)
    assert error.value.args == ("service=request_failed",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [GET, TAGS])


def test_timeout_em_tags_apos_role_valido_falha_fechado(read_lab: ReadLab) -> None:
    reply(read_lab, data("iam")["role"])
    read_lab.process.outcomes.append(subprocess.TimeoutExpired(["aws", "iam"], 7))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROLE, read_lab.context)
    assert error.value.args == ("service=request_failed",)
    assert_process(read_lab, [GET, TAGS])
