"""Recuperação AWS pública com adapters reais e fronteiras SDK offline."""

import importlib
import io
import json
import logging
import os
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from apps.data_processor.tests.orchestration.test_publisher import (
    _FakeObjectStore,
    _materialize_unit,
    _reconcile_unit,
    _run,
    _seed_manifest,
)
from cnes_contracts.manifests.outputs import OutputManifest, RunManifest
from cnes_contracts.manifests.raw import SourceType
from cnes_domain.control_plane.commands import (
    ClaimRunUnit,
    CommitRunUnit,
    FinishRunDispatch,
    PutRunUnits,
    ReserveRunDispatch,
)
from cnes_domain.control_plane.entities import ManifestRef, OutboxEvent, RunDependency, RunUnit
from cnes_domain.control_plane.enums import DispatchOutcome, RunStage, RunState, RunUnitState
from cnes_infra.audit.s3_object_lock_sink import S3ObjectLockAuditSink
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.object_store.s3 import S3ObjectStore
from data_processor.orchestration.unit_worker import UnitWorker
from data_processor.pipeline.source_registry import SourceRegistry
from data_processor.pipeline.stage_processor import StageProcessor
from packages.cnes_infra.tests.aws.production_values import PRODUCTION_VALUES
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    ClientSpy,
    _create_table,
)

_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)
_ASL = (Path(__file__).parents[3]
        / "packages/cnes_infra/tests/fixtures/step_functions/production_public_ip.json")
_OFFLINE = {
    "DB_URL": "postgresql+psycopg://offline:offline@127.0.0.1:1/offline",
    "PYTHON_DOTENV_DISABLED": "1", "AWS_EC2_METADATA_DISABLED": "true",
    "AWS_ACCESS_KEY_ID": "offline", "AWS_SECRET_ACCESS_KEY": "offline",
    "BILLING_MODE": "disabled", "BILLING_ENFORCEMENT_MODE": "off",
}
_WORKER_KEYS = (
    "PROFILE", "AWS_PRODUCTION_POLICY", "AWS_ENVIRONMENT", "AWS_REGION",
    "AWS_CONTROL_PLANE_TABLE", "AWS_DATA_BUCKET", "AWS_STATE_MACHINE_ARN",
    "AWS_PROCESSOR_CONTAINER_NAME", "AWS_PROCESSOR_MAX_CONCURRENCY",
    "AWS_PROCESSOR_LEASE_SECONDS", "AWS_PROCESSOR_RECOVERY_BATCH_SIZE",
    "AWS_PUBLIC_SUBNET_IDS", "AWS_SECURITY_GROUP_ID",
)


def _values(variant: str = "full") -> dict[str, str]:
    values = dict(PRODUCTION_VALUES) if variant == "full" else {
        key: PRODUCTION_VALUES[key] for key in _WORKER_KEYS
    }
    if variant == "malformed":
        values.update({
            "AUTH_MODE": "invalid", "OIDC_ISSUER": "invalid", "OIDC_AUDIENCE": "",
            "OIDC_DOMAIN": "invalid", "AWS_AUDIT_BUCKET": "",
            "AWS_AUDIT_RETENTION_DAYS": "invalid", "AWS_SERVING_URL_TTL_SECONDS": "invalid",
        })
    return values | _OFFLINE | {"AWS_CONTROL_PLANE_TABLE": _TABLE_NAME}


@dataclass
class _OfflineAws:
    app: ModuleType
    dynamodb: ClientSpy
    control_plane: DynamoDBControlPlane
    store: S3ObjectStore
    step_functions: Mock
    session: Mock
    session_constructor: Mock

    def reset_observations(self) -> None:
        self.dynamodb.query_requests.clear()
        self.session.reset_mock()
        self.session_constructor.reset_mock()
        self.step_functions.reset_mock()


def _resources() -> tuple[ClientSpy, Any]:
    credentials: dict[str, Any] = {
        "region_name": "us-east-2", "aws_access_key_id": "offline",
        "aws_secret_access_key": "offline",
    }
    dynamodb = ClientSpy(boto3.client("dynamodb", **credentials))
    _create_table(dynamodb)
    s3 = boto3.client("s3", **credentials)
    for bucket, locked in (
        (PRODUCTION_VALUES["AWS_DATA_BUCKET"], False),
        (PRODUCTION_VALUES["AWS_AUDIT_BUCKET"], True),
    ):
        s3.create_bucket(
            Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
            ObjectLockEnabledForBucket=locked,
        )
    return dynamodb, s3


@pytest.fixture(name="offline_aws")
def _offline_aws(monkeypatch: pytest.MonkeyPatch) -> Iterator[_OfflineAws]:
    with patch.dict(os.environ, _values(), clear=True), mock_aws():
        app = importlib.import_module("data_processor.main")
        dynamodb, s3 = _resources()
        sf = Mock(spec_set=["describe_state_machine", "start_execution", "describe_execution"])
        sf.describe_state_machine.return_value = {
            "type": "STANDARD", "definition": _ASL.read_text(encoding="utf-8"),
        }
        sf.start_execution.side_effect = AssertionError("unexpected=start_execution")
        sf.describe_execution.side_effect = AssertionError("unexpected=describe_execution")
        clients = {"dynamodb": dynamodb, "s3": s3, "stepfunctions": sf}
        session = Mock(spec_set=["client"])
        session.client.side_effect = lambda service, **_: clients[service]
        constructor = Mock(return_value=session)
        monkeypatch.setattr(app, "Session", constructor)
        monkeypatch.setattr(app, "init_telemetry", Mock())
        for module in (app, importlib.import_module("data_processor.aws_entrypoint")):
            monkeypatch.setattr(module, "_utc_now", lambda: _NOW)
        with patch("data_processor.deadline.threading.Timer", autospec=True):
            yield _OfflineAws(
                app, dynamodb, DynamoDBControlPlane(dynamodb, _TABLE_NAME, lambda: _NOW),
                S3ObjectStore(s3, PRODUCTION_VALUES["AWS_DATA_BUCKET"], ""),
                sf, session, constructor,
            )


async def _invoke_recovery(
    aws: _OfflineAws, values: dict[str, str],
) -> tuple[int, list[dict[str, Any]]]:
    output = io.StringIO()
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    root.handlers[:] = []
    try:
        with (
            patch.dict(os.environ, values, clear=True),
            patch.object(sys, "argv", ["data_processor", "recover-once"]),
            patch.object(sys, "stdout", output),
        ):
            rc = await aws.app.main()
        return rc, [json.loads(line) for line in output.getvalue().splitlines()]
    finally:
        for handler in root.handlers[:]:
            handler.close()
        root.handlers[:] = handlers
        root.setLevel(level)


def _assert_recovery(aws: _OfflineAws, events: list[dict[str, Any]]) -> None:
    aws.session_constructor.assert_called_once_with()
    assert sorted(call.args[0] for call in aws.session.client.call_args_list) == [
        "dynamodb", "s3", "stepfunctions",
    ]
    queries = [q for q in aws.dynamodb.query_requests if q.get("IndexName") == "gsi4"]
    assert queries
    assert queries[0]["Limit"] == 100
    assert all(q["ExpressionAttributeValues"] == {
        ":partition": {"S": "RUN_RECOVERABLE"},
    } for q in queries)
    completed = [event for event in events if event["event"] == "processor_recovery_completed"]
    assert len(completed) == 1
    assert completed[0]["failed"] == 0
    aws.step_functions.describe_state_machine.assert_called_once_with(
        stateMachineArn=PRODUCTION_VALUES["AWS_STATE_MACHINE_ARN"],
    )
    aws.step_functions.start_execution.assert_not_called()


def _event(unit_id: str) -> OutboxEvent:
    return OutboxEvent(
        tenant_id="354130", event_id=f"seed-{unit_id}", event_type="fixture.unit_committed",
        aggregate_id="run-a", payload={}, created_at=_NOW, delivered_at=None,
    )


def _complete_units(control_plane: DynamoDBControlPlane, units: tuple[RunUnit, ...]) -> None:
    pending = tuple(unit.model_copy(update={
        "state": RunUnitState.PENDING, "attempt": 0, "fencing_token": 0,
        "output_manifests": (),
    }) for unit in units)
    control_plane.put_run_units(PutRunUnits(
        tenant_id="354130", run_id="run-a", expected_run_state=RunState.PROCESSING,
        units=pending,
    ))
    dispatch = control_plane.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id="354130", run_id="run-a", wave_id="1111111111111111",
        unit_ids=("unit-m", "unit-n", "unit-r"), now=_NOW, lease_seconds=3600,
    ))
    for unit in units:
        claimed = control_plane.claim_run_unit(ClaimRunUnit(
            tenant_id="354130", run_id="run-a", unit_id=unit.unit_id,
            dispatch_id=dispatch.dispatch_id, owner="fixture", now=_NOW, lease_seconds=3600,
        ))
        assert isinstance(claimed, RunUnit)
        control_plane.commit_run_unit(CommitRunUnit(
            tenant_id="354130", run_id="run-a", unit_id=unit.unit_id,
            dispatch_id=dispatch.dispatch_id, owner="fixture", fencing_token=claimed.fencing_token,
            output_manifests=unit.output_manifests,
        ), _event(unit.unit_id))
    control_plane.finish_run_dispatch(FinishRunDispatch(
        tenant_id="354130", run_id="run-a", dispatch_id=dispatch.dispatch_id,
        outcome=DispatchOutcome.SUCCEEDED, finished_at=_NOW,
    ))


def _normalize_seed(objects: _FakeObjectStore) -> RunUnit:
    key = "normalized/354130/CNES_LOCAL/2026-01/run-a/part-n.parquet"
    prefix = "tmp/354130/run-a/unit-n/1"
    sidecar = f"{prefix}/manifests/manifest-n/manifest.json"
    manifest = OutputManifest(
        manifest_version=1, manifest_id="manifest-n", tenant_id="354130",
        layer="normalized", source_type=SourceType.CNES_LOCAL, competencia="2026-01",
        run_id="run-a", unit_id="unit-n", attempt=1, schema_version="gold-v1",
        object_key=key, object_sha256=sha256(b"payload-n").hexdigest(),
        row_count=10, created_at=_NOW,
    )
    objects.objects[f"{prefix}/{key}"] = b"payload-n"
    objects.objects[sidecar] = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    return RunUnit(
        tenant_id="354130", run_id="run-a", unit_id="unit-n", stage=RunStage.NORMALIZE,
        source_type="CNES_LOCAL", file_subtype="ST", partition="all", depends_on_unit_ids=(),
        input_manifests=(ManifestRef(
            manifest_id="input", manifest_key="raw/354130/CNES_LOCAL/2026-01/input/manifest.json",
        ),), state=RunUnitState.SUCCEEDED, attempt=1, fencing_token=1, lease_owner=None,
        lease_until=None, dispatch_id=None, error_code=None,
        output_manifests=(ManifestRef(manifest_id="manifest-n", manifest_key=sidecar),),
    )


def _seed_publication(aws: _OfflineAws) -> None:
    objects = _FakeObjectStore()
    normalize = _normalize_seed(objects)
    _, reconcile = _seed_manifest(objects, "unit-r", 1, suffix="a")
    _, materialize = _seed_manifest(objects, "unit-m", 1, suffix="b")
    for key, body in objects.objects.items():
        aws.store.put(key, io.BytesIO(body), sha256(body).hexdigest())
    run = _run().model_copy(update={"dependencies": (
        RunDependency(source_type="CNES_LOCAL", file_subtype="ST", required=True),
    )})
    aws.control_plane.put_run(run.model_copy(update={"state": RunState.PROCESSING}))
    _complete_units(aws.control_plane, (
        normalize,
        _reconcile_unit((reconcile,)).model_copy(update={"depends_on_unit_ids": ("unit-n",)}),
        _materialize_unit((materialize,)).model_copy(update={"depends_on_unit_ids": ("unit-r",)}),
    ))
    aws.control_plane.put_run(run)
    persisted = aws.control_plane.list_run_units("354130", "run-a")
    assert len(persisted) == 3
    assert all(unit.state is RunUnitState.SUCCEEDED for unit in persisted)
    for ref in (*normalize.output_manifests, reconcile, materialize):
        with aws.store.open(ref.manifest_key) as sidecar:
            assert json.load(sidecar)["manifest_id"] == ref.manifest_id
    run = aws.control_plane.get_run("354130", "run-a")
    assert run is not None
    assert run.state is RunState.PUBLISHING
    aws.reset_observations()


def _publication_snapshot(aws: _OfflineAws) -> tuple[Any, str]:
    pointer = aws.control_plane.get_dataset_pointer("354130", "gold")
    assert pointer is not None
    assert (pointer.dataset_name, pointer.version_id) == ("gold", "run-a")
    run = aws.control_plane.get_run("354130", "run-a")
    assert run is not None
    assert run.state is RunState.PUBLISHED
    version = aws.control_plane.get_dataset_version("354130", "gold", "run-a")
    assert version is not None
    assert version.run_manifest_key == "reconciliation/354130/2026-01/run-a/run-manifest.json"
    with aws.store.open(version.run_manifest_key) as body:
        manifest = RunManifest.model_validate_json(body.read())
    assert (manifest.tenant_id, manifest.dataset_name, manifest.run_id) == (
        "354130", "gold", "run-a",
    )
    assert {item.manifest_id for item in manifest.outputs} == {
        "manifest-a", "manifest-b", "manifest-n",
    }
    for suffix in ("a", "b"):
        key = f"reconciliation/354130/2026-01/run-a/part-{suffix}.parquet"
        with aws.store.open(key) as body:
            assert body.read() == f"payload-{suffix}".encode()
    with aws.store.open("normalized/354130/CNES_LOCAL/2026-01/run-a/part-n.parquet") as body:
        assert body.read() == b"payload-n"
    published = [event for event in aws.control_plane.read_outbox_page(None, 100).events
                 if event.event_type == "reconciliation.published"]
    assert len(published) == 1
    return pointer, published[0].event_id


@pytest.mark.asyncio
@pytest.mark.parametrize("variant", ["full", "minimal", "malformed"])
async def test_recovery_compoe_e_conclui_com_ambiente_do_worker(
    offline_aws: _OfflineAws, variant: str,
) -> None:
    rc, events = await _invoke_recovery(offline_aws, _values(variant))

    assert rc == 0, events
    _assert_recovery(offline_aws, events)
    completed = next(e for e in events if e["event"] == "processor_recovery_completed")
    assert (completed["scanned"], completed["recovered"]) == (0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("forbidden_type", [
    SourceRegistry, StageProcessor, UnitWorker, S3ObjectLockAuditSink,
], ids=lambda cls: cls.__name__)
async def test_recovery_nao_constroi_dependencias_de_unidade_ou_auditoria(
    offline_aws: _OfflineAws, forbidden_type: type,
) -> None:
    forbidden = Mock(side_effect=AssertionError(f"forbidden={forbidden_type.__name__}"))
    with patch.object(forbidden_type, "__init__", forbidden):
        rc, events = await _invoke_recovery(offline_aws, _values())

    assert rc == 0, (forbidden_type.__name__, forbidden.call_count, events)
    forbidden.assert_not_called()
    _assert_recovery(offline_aws, events)


@pytest.mark.asyncio
@pytest.mark.parametrize("variant", ["full", "minimal"])
async def test_recovery_publica_e_reexecuta_sem_duplicar_resultado(
    offline_aws: _OfflineAws, variant: str,
) -> None:
    _seed_publication(offline_aws)
    rc, events = await _invoke_recovery(offline_aws, _values(variant))

    assert rc == 0, events
    _assert_recovery(offline_aws, events)
    snapshot = _publication_snapshot(offline_aws)
    offline_aws.reset_observations()
    rc, events = await _invoke_recovery(offline_aws, _values(variant))

    assert rc == 0, events
    _assert_recovery(offline_aws, events)
    assert _publication_snapshot(offline_aws) == snapshot


@pytest.mark.asyncio
async def test_billing_invalido_falha_antes_de_session(offline_aws: _OfflineAws) -> None:
    rc, events = await _invoke_recovery(
        offline_aws, _values() | {"BILLING_ENFORCEMENT_MODE": "invalid"},
    )

    assert rc == 2, events
    offline_aws.session_constructor.assert_not_called()
    offline_aws.session.client.assert_not_called()
    assert offline_aws.dynamodb.query_requests == []


@pytest.mark.asyncio
async def test_recovery_classifica_falha_real_de_cliente_como_operacional(
    offline_aws: _OfflineAws,
) -> None:
    offline_aws.step_functions.describe_state_machine.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "offline"}}, "DescribeStateMachine",
    )
    rc, events = await _invoke_recovery(offline_aws, _values())

    assert rc == 1
    failures = [event for event in events if event["event"] == "processor_entrypoint_failed"]
    assert len(failures) == 1
    assert (failures[0]["exit_code"], failures[0]["exception_type"]) == (
        1, "ProcessorExecutionUnavailable",
    )
    offline_aws.step_functions.describe_state_machine.assert_called_once_with(
        stateMachineArn=PRODUCTION_VALUES["AWS_STATE_MACHINE_ARN"],
    )
    offline_aws.step_functions.start_execution.assert_not_called()
    assert offline_aws.dynamodb.query_requests == []
