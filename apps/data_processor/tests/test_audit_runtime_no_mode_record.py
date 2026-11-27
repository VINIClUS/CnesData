"""O runtime do audit nunca monta ModeCanaryContext nem lê o record ou o control do owner."""
from __future__ import annotations

import ast
import json
from dataclasses import fields
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import boto3
import pytest
from moto import mock_aws

from cnes_infra.aws.processor_settings import AwsAuditDispatchSettings
from data_processor import aws_composition
from data_processor.audit_dispatch import run_audit_dispatch_once
from data_processor.aws_composition import AwsAuditRuntime, build_aws_audit_runtime
from packages.cnes_infra.tests.aws.test_mode_canary_ledger import _NOW, _OWNER, _TENANT
from packages.cnes_infra.tests.contracts.clock import _event
from packages.cnes_infra.tests.control_plane.outbox_cursor_support import OutboxHarness
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import _TABLE_NAME

from .test_mode_canary import _LOCATOR, _SYNTHETIC_EVENT

if TYPE_CHECKING:
    from collections.abc import Iterator

    from boto3.session import Session

_SOURCE = Path(aws_composition.__file__)
_FORBIDDEN = frozenset({"ModeCanaryContext", "_mode_canary_parts", "_build_assembly",
                        "load_unit_binding", "initialize_control"})


@pytest.fixture
def outbox(tmp_path: Path) -> Iterator[OutboxHarness]:
    with mock_aws():
        yield OutboxHarness("dynamodb", tmp_path)


def _function(name: str) -> ast.FunctionDef:
    tree = ast.parse(_SOURCE.read_text(encoding="utf-8"))
    [node] = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name]
    return node


def _forbidden(*args: object, **kwargs: object) -> None:
    raise AssertionError("audit_runtime=mode_canary_context")


def test_build_do_audit_runtime_nao_referencia_o_contexto_de_modo() -> None:
    names = {node.id for node in ast.walk(_function("build_aws_audit_runtime"))
             if isinstance(node, ast.Name)}
    assert names & _FORBIDDEN == set()


def test_audit_runtime_tem_so_control_plane_sink_clock_e_retencao() -> None:
    assert [field.name for field in fields(AwsAuditRuntime)] == [
        "control_plane", "sink", "clock", "retention_days"]


def test_build_do_audit_runtime_nao_constroi_contexto_de_modo(
        outbox: OutboxHarness, monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("ModeCanaryContext", "_mode_canary_parts", "load_unit_binding"):
        monkeypatch.setattr(aws_composition, name, _forbidden)
    boto3.client("s3", region_name="us-east-1").create_bucket(
        Bucket="audit-sem-record", ObjectLockEnabledForBucket=True)
    settings = AwsAuditDispatchSettings("us-east-1", _TABLE_NAME, "audit-sem-record", 3650,
                                        None, None, production_policy=False)
    runtime = build_aws_audit_runtime(settings, cast("Session", boto3.Session()), lambda: _NOW)
    assert type(runtime) is AwsAuditRuntime


def test_despacho_do_audit_nao_le_record_nem_control_do_canary(outbox: OutboxHarness) -> None:
    outbox.put(_event(_SYNTHETIC_EVENT, tenant_id=_TENANT))
    runtime = AwsAuditRuntime(outbox.adapter, outbox.sink("s3"), lambda: _NOW, 3650)
    assert run_audit_dispatch_once(runtime, _LOCATOR) == 0
    requests: list[Any] = [request for _, request in outbox.sdk.calls]
    assert requests
    sent = json.dumps(requests, default=str)
    assert f"PROD_CANARY#{_OWNER}" not in sent
    assert "PROD_CANARY_CONTROL#" not in sent
