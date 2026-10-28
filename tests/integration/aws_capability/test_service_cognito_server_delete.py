"""Remoção segura do resource server Cognito por readback literal."""

from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_cognito_root_testing import (
    ROOT,
    RootLab,
    assert_private_channels,
    root_lab,
)
from tests.integration.aws_capability._service_cognito_testing import load_fixture
from tests.integration.aws_capability._service_read_assertions import (
    FIXED_FLAGS,
    assert_environment,
)
from tests.integration.aws_capability.contracts import RequestContext, Resource, ServiceFailure

__all__ = ["root_lab"]

DATA = load_fixture(
    Path(__file__).parent / "fixtures" / "cli" / "b3" / "cognito_server_delete.json"
)
SERVER = Resource(
    "cognito",
    '{"child_id":{"identifier":"https://api.cnesdata.com.br"},'
    '"kind":"cognito.server","parent_id":"us-east-2_PoolRoot123",'
    '"root_id":"us-east-2_PoolRoot123","run":"a1b2c3d4",'
    '"service":"cognito","v":1}',
    ROOT.name,
)
CLIENT = Resource(
    "cognito",
    '{"child_id":{"client_id":"synthetic-client"},"kind":"cognito.client",'
    '"parent_id":"us-east-2_PoolRoot123","root_id":"us-east-2_PoolRoot123",'
    '"run":"a1b2c3d4","service":"cognito","v":1}',
    ROOT.name,
)
MALFORMED = Resource(
    "cognito",
    '{"v":1,"v":1,"run":"a1b2c3d4","service":"cognito",'
    '"root_id":"us-east-2_PoolRoot123","parent_id":"us-east-2_PoolRoot123",'
    '"kind":"cognito.server","child_id":{"identifier":"https://api.cnesdata.com.br"}}',
    ROOT.name,
)


def _outcome(name: str) -> tuple[int, str, str] | BaseException:
    reply = DATA["replies"].get(name)
    if name == "X_timeout":
        return subprocess.TimeoutExpired("aws", 7, output="", stderr="")
    if reply is None:
        raise AssertionError(f"fixture=reply_missing:{name}")
    if "exit" in reply:
        return reply["exit"], reply["stdout"], reply["stderr"]
    return 0, json.dumps(reply), ""


def _queue(lab: RootLab, trace: list[list[str]]) -> None:
    for _, alias in trace:
        lab.process.outcomes.append(_outcome(alias))


def _assert_trace(lab: RootLab, trace: list[list[str]]) -> None:
    assert len(lab.process.calls) == len(trace), "stage=physical_trace"
    for (argv, options), (request_id, _) in zip(lab.process.calls, trace, strict=True):
        expected = DATA["requests"][request_id]
        assert argv == [
            "aws",
            expected["service"],
            expected["operation"],
            "--region",
            expected["region"],
            *FIXED_FLAGS,
            "7",
            "--cli-read-timeout",
            "7",
        ]
        assert json.loads(options["input"]) == expected["stdin"]
        assert options["timeout"] == 7
        assert options["shell"] is False
        assert options["capture_output"] is True
        assert options["text"] is True
        assert options["check"] is False
        assert_environment(options["env"], expected["region"])
    assert lab.process.observed_debits == list(range(1, len(trace) + 1))
    assert lab.context.budget.used == len(trace)
    assert lab.process.outcomes == []


def _assert_failure(error: ServiceFailure, lab: RootLab, expected: str) -> None:
    assert error.args == (expected,)
    assert_private_channels(lab, error)


def _delete_context(lab: RootLab, lane: str = "cleanup") -> RequestContext:
    return RequestContext(lab.context.budget, lane)


def test_remove_server_proprietario_com_trace_literal(root_lab: RootLab) -> None:
    trace = DATA["traces"]["owned"]
    _queue(root_lab, trace)
    result = root_lab.services.delete(SERVER, True, _delete_context(root_lab))
    assert result is None
    _assert_trace(root_lab, trace)


@pytest.mark.parametrize(
    ("alias", "expected"),
    [("D_identity", "service=describe_invalid"), ("T_other", "service=not_owned")],
    ids=["identity", "tags"],
)
def test_divergencia_do_dono_do_root_bloqueia_server(
    root_lab: RootLab, alias: str, expected: str
) -> None:
    trace_name = "identity" if alias == "D_identity" else "tags"
    trace = DATA["traces"][trace_name]
    _queue(root_lab, trace)
    with pytest.raises(ServiceFailure) as captured:
        root_lab.services.delete(SERVER, True, _delete_context(root_lab))
    _assert_failure(captured.value, root_lab, expected)
    _assert_trace(root_lab, trace)


@pytest.mark.parametrize(
    "case",
    [
        "identifier",
        "scope",
        "name",
        "scope_description",
        "user_pool_id",
        "missing_field",
        "extra_field",
    ],
    ids=[
        "identifier",
        "scope",
        "name",
        "scope_description",
        "user_pool_id",
        "missing_field",
        "extra_field",
    ],
)
def test_divergencia_no_readback_do_server_bloqueia_delete(root_lab: RootLab, case: str) -> None:
    trace = DATA["traces"][case]
    _queue(root_lab, trace)
    with pytest.raises(ServiceFailure) as captured:
        root_lab.services.delete(SERVER, True, _delete_context(root_lab))
    _assert_failure(captured.value, root_lab, "service=describe_invalid")
    _assert_trace(root_lab, trace)


@pytest.mark.parametrize(
    "case",
    ["lane", "kind", "envelope", "name", "bypass"],
    ids=["lane", "kind", "envelope", "name", "bypass"],
)
def test_rejeita_entrada_local_sem_io_sem_chamada(root_lab: RootLab, case: str) -> None:
    if case == "bypass":
        for bypass in (0, 1, None):
            with pytest.raises(ServiceFailure) as captured:
                root_lab.services.delete(SERVER, cast("bool", bypass), _delete_context(root_lab))
            _assert_failure(captured.value, root_lab, "service=cognito_delete_invalid")
        _assert_trace(root_lab, [])
        return
    resource = SERVER
    context = _delete_context(root_lab)
    if case == "lane":
        context = _delete_context(root_lab, "work")
    elif case == "kind":
        resource = CLIENT
    elif case == "envelope":
        resource = MALFORMED
    else:
        resource = replace(SERVER, name="cnesdata-cap-a1b2c3d4-suffix")
    with pytest.raises(ServiceFailure) as captured:
        root_lab.services.delete(resource, True, context)
    _assert_failure(captured.value, root_lab, "service=cognito_delete_invalid")
    _assert_trace(root_lab, [])


@pytest.mark.parametrize(
    ("alias", "expected"),
    [
        ("X_error", "service=cognito_delete_failed"),
        ("X_timeout", "service=request_failed"),
        ("X_invalid", "service=cognito_delete_failed"),
        ("X_nonempty", "service=cognito_delete_invalid"),
    ],
    ids=["remote_error", "timeout", "invalid_stdout", "nonempty_stdout"],
)
def test_delete_ambiguo_nao_reintenta(root_lab: RootLab, alias: str, expected: str) -> None:
    trace_name = {
        "X_error": "delete_error",
        "X_timeout": "delete_timeout",
        "X_invalid": "delete_invalid",
        "X_nonempty": "delete_nonempty",
    }[alias]
    trace = DATA["traces"][trace_name]
    _queue(root_lab, trace)
    with pytest.raises(ServiceFailure) as captured:
        root_lab.services.delete(SERVER, True, _delete_context(root_lab))
    _assert_failure(captured.value, root_lab, expected)
    _assert_trace(root_lab, trace)


def test_cleanup_exige_lookup_apos_remocao(root_lab: RootLab) -> None:
    trace = DATA["traces"]["cleanup_present"]
    _queue(root_lab, trace)
    journal = Journal(root_lab.services.spec)
    journal.save()
    before = journal.path.read_bytes()
    cleanup = Cleanup(journal, root_lab.ports)
    with pytest.raises(SafetyFailure) as captured:
        cleanup.delete(SERVER)
    assert captured.value.args == ("lifecycle=cleanup_failed",)
    assert journal.path.read_bytes() == before
    _assert_trace(root_lab, trace)


def test_cleanup_confirma_ausencia_por_leitura(root_lab: RootLab) -> None:
    trace = DATA["traces"]["cleanup_absent"]
    _queue(root_lab, trace)
    journal = Journal(root_lab.services.spec)
    journal.save()
    before = journal.path.read_bytes()
    Cleanup(journal, root_lab.ports).delete(SERVER)
    assert journal.path.read_bytes() == before
    _assert_trace(root_lab, trace)
