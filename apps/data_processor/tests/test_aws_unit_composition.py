"""Unidades AWS reais, publicação e preservação do permit da própria execução."""

import io
import json
import logging
import os
import sys
from dataclasses import replace
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pytest

from apps.data_processor.tests.pipeline.test_materialize_cnes import _divergence_frame
from apps.data_processor.tests.pipeline.test_normalize_cnes_local import (
    _raw_manifest,
    _row,
    _serialize,
)
from apps.data_processor.tests.test_aws_mode_composition import (
    _NOW,
    _TABLE_NAME,
    _offline_aws,
    _OfflineAws,
    _values,
)
from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.raw import SourceType
from cnes_domain.billing.commands import AuthorizedRunCommand, SnapshotWrite
from cnes_domain.billing.execution import RunExecutionBindingCommand
from cnes_domain.billing.models import RunAuthorization
from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    ClaimRunUnit,
    CommitRunUnit,
    FinishRunDispatch,
    PutRunUnits,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import ManifestRef, OutboxEvent, Run, RunDispatch, RunUnit
from cnes_domain.control_plane.enums import DispatchOutcome, RunStage, RunState, RunUnitState
from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    BindUnitPermit,
    PermitHolder,
)
from cnes_domain.ports.processing import RunUnitMessage
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from packages.cnes_infra.tests.billing.billing_factories import make_snapshot
from packages.cnes_infra.tests.billing.quota_support import (
    make_reserve_command,
    make_run_request,
    seed_capacity,
)

__all__ = ["_offline_aws"]
_TENANT = "354130"
_RUN = "fixture-cnes-run-v1"
_FIXTURES = Path(__file__).parents[3] / "docs/fixtures/data-plane"
_EXECUTION_PREFIX = "arn:aws:states:us-east-2:000000000000:execution:cnesdata-prod:"


def _put(aws: _OfflineAws, key: str, body: bytes) -> None:
    aws.store.put(key, io.BytesIO(body), sha256(body).hexdigest())


def _event(name: str) -> OutboxEvent:
    return OutboxEvent(
        tenant_id=_TENANT, event_id=f"seed:{name}", event_type=f"fixture.{name}",
        aggregate_id=_RUN, payload={}, created_at=_NOW, delivered_at=None,
    )


def _raw_input(aws: _OfflineAws) -> ManifestRef:
    key = "raw/354130/CNES_LOCAL/2026-01/full-1/data.parquet"
    body = _serialize([_row("11111111111", "111111111111111")])
    raw = _raw_manifest("full-1", 1, key, sha256(body).hexdigest(), 1).model_copy(update={
        "file_subtype": "CNES_VINCULO", "size_bytes": len(body),
    })
    sidecar = "raw/354130/CNES_LOCAL/2026-01/full-1/manifest.json"
    _put(aws, key, body)
    _put(aws, sidecar, raw.model_dump_json(exclude_none=False, by_alias=False).encode())
    return ManifestRef(manifest_id="cnes-local-full-1", manifest_key=sidecar)


def _graph(raw: ManifestRef) -> tuple[RunUnit, ...]:
    common: dict[str, Any] = {
        "tenant_id": _TENANT, "run_id": _RUN, "partition": "all",
        "state": RunUnitState.PENDING, "attempt": 0, "fencing_token": 0,
        "lease_owner": None, "lease_until": None, "dispatch_id": None,
        "output_manifests": (), "error_code": None,
    }
    return (
        RunUnit(**common, unit_id="unit-n", stage=RunStage.NORMALIZE,
                source_type="CNES_LOCAL", file_subtype="CNES_VINCULO",
                depends_on_unit_ids=(), input_manifests=(raw,)),
        RunUnit(**common, unit_id="unit-r", stage=RunStage.RECONCILE,
                source_type=None, file_subtype=None, depends_on_unit_ids=("unit-n",),
                input_manifests=()),
        RunUnit(**common, unit_id="unit-m", stage=RunStage.MATERIALIZE,
                source_type=None, file_subtype=None, depends_on_unit_ids=("unit-r",),
                input_manifests=()),
    )


def _seed_graph(aws: _OfflineAws, companion: str | None = None) -> None:
    from cnes_domain.control_plane.entities import RunDependency

    dependencies = (RunDependency(
        source_type="CNES_LOCAL", file_subtype="CNES_VINCULO", required=True,
    ),)
    request = make_run_request(
        run_id=_RUN, competencia="2026-01", dataset_name="cnes", dependencies=dependencies,
        requested_concurrency=1, estimated_scan_bytes=0,
    )
    if companion is None:
        aws.control_plane.put_run(Run(
            tenant_id=_TENANT, run_id=_RUN, competencia="2026-01", dataset_name="cnes",
            state=RunState.PROCESSING, dependencies=dependencies, missing_sources=(),
            created_at=_NOW,
        ))
    else:
        _authorized_run(aws, request, companion)
        aws.control_plane.transition_run(TransitionRun(
            tenant_id=_TENANT, run_id=_RUN, expected_state=RunState.WAITING_INPUTS,
            new_state=RunState.PROCESSING,
        ), _event("processing"))
    aws.control_plane.put_run_units(PutRunUnits(
        tenant_id=_TENANT, run_id=_RUN, expected_run_state=RunState.PROCESSING,
        units=_graph(_raw_input(aws)),
    ))
    assert len(aws.control_plane.list_run_units(_TENANT, _RUN)) == 3
    assert (aws.control_plane.get_run_billing_state(_TENANT, _RUN) is not None) == (
        companion is not None
    )


def _authorized_run(aws: _OfflineAws, request: Any, companion: str) -> None:
    if companion == "disabled":
        authorization = RunAuthorization("ba_01", "plan_v1", 1, 1, None, _NOW)
        aws.control_plane.create_unmetered_run(AuthorizedRunCommand(request, authorization))
        return
    snapshot = make_snapshot(
        period_start=_NOW, period_end=_NOW + timedelta(days=30),
        valid_until=_NOW + timedelta(days=30), updated_at=_NOW,
    )
    projection = DynamoEntitlementProjection(aws.dynamodb, _TABLE_NAME, lambda: _NOW)
    assert projection.compare_and_set_snapshot(SnapshotWrite(0, snapshot, ()))
    seed_capacity(aws.dynamodb)
    command = make_reserve_command(snapshot, 1)
    command = replace(command, request=request, expires_at=_NOW + timedelta(minutes=15))
    DynamoQuotaReservations(aws.dynamodb, _TABLE_NAME, lambda: _NOW).reserve_and_create_run(command)


def _dispatch(aws: _OfflineAws, unit_id: str, wave: int) -> RunDispatch:
    dispatch = aws.control_plane.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=_TENANT, run_id=_RUN, wave_id=f"{wave:016x}", unit_ids=(unit_id,),
        now=_NOW, lease_seconds=3600,
    ))
    execution_ref = _EXECUTION_PREFIX + dispatch.dispatch_id
    bound = aws.control_plane.bind_run_dispatch(BindRunDispatch(
        tenant_id=_TENANT, run_id=_RUN, dispatch_id=dispatch.dispatch_id,
        execution_ref=execution_ref, now=_NOW, lease_seconds=3600,
    ))
    state = aws.control_plane.get_run_billing_state(_TENANT, _RUN)
    if state is not None:
        aws.control_plane.bind_run_execution(RunExecutionBindingCommand(
            tenant_id=_TENANT, run_id=_RUN, wave_id=bound.wave_id,
            dispatch_id=bound.dispatch_id, generation=bound.generation,
            execution_ref=execution_ref, unit_ids=(unit_id,),
            expected_previous_dispatch_id=state.execution_dispatch_id,
            expected_previous_execution_ref=state.execution_ref,
            expected_entitlement_version=1, expected_fencing_token=0, bound_at=_NOW,
        ))
    return bound


def _hold(aws: _OfflineAws, dispatch: RunDispatch) -> Any:
    holder = PermitHolder(_TENANT, _RUN, dispatch.dispatch_id)
    permit = aws.control_plane.acquire_unit_permit(AcquireUnitPermit("prod", holder, _NOW, 3600))
    assert permit.permit is not None
    aws.control_plane.bind_unit_permit(BindUnitPermit(
        "prod", permit.permit.token, str(dispatch.execution_ref), _NOW,
    ))
    return aws.control_plane.observe_environment_gate("prod").permit


def _seed_output(aws: _OfflineAws, unit_id: str, key: str, body: bytes) -> ManifestRef:
    normalized = key.startswith("normalized/")
    serving = key.startswith("serving/")
    manifest = OutputManifest(
        manifest_version=1, manifest_id=f"fixture-{unit_id}-{Path(key).stem}",
        tenant_id=_TENANT,
        layer="normalized" if normalized else "serving" if serving else "reconciliation",
        source_type=SourceType.CNES_LOCAL if normalized else None, competencia="2026-01",
        run_id=_RUN, unit_id=unit_id, attempt=1,
        schema_version=("cnes-serving-v1" if serving else "cnes-normalized-v1" if normalized
                        else "cnes-divergence-v1" if "divergences" in key
                        else "cnes-reconciliation-v1"),
        object_key=key, object_sha256=sha256(body).hexdigest(),
        row_count=1 if serving else pl.read_parquet(io.BytesIO(body)).height, created_at=_NOW,
    )
    prefix = f"tmp/354130/{_RUN}/{unit_id}/1"
    sidecar = f"{prefix}/manifests/{manifest.manifest_id}/manifest.json"
    _put(aws, f"{prefix}/{key}", body)
    _put(aws, sidecar, manifest.model_dump_json(exclude_none=False, by_alias=False).encode())
    return ManifestRef(manifest_id=manifest.manifest_id, manifest_key=sidecar)


def _complete(
    aws: _OfflineAws, dispatch: RunDispatch, unit_id: str, refs: tuple[ManifestRef, ...],
) -> None:
    claimed = aws.control_plane.claim_run_unit(ClaimRunUnit(
        tenant_id=_TENANT, run_id=_RUN, unit_id=unit_id, dispatch_id=dispatch.dispatch_id,
        owner="fixture", now=_NOW, lease_seconds=3600,
    ))
    assert isinstance(claimed, RunUnit)
    aws.control_plane.commit_run_unit(CommitRunUnit(
        tenant_id=_TENANT, run_id=_RUN, unit_id=unit_id, dispatch_id=dispatch.dispatch_id,
        owner="fixture", fencing_token=claimed.fencing_token, output_manifests=refs,
    ), _event(unit_id))
    aws.control_plane.finish_run_dispatch(FinishRunDispatch(
        tenant_id=_TENANT, run_id=_RUN, dispatch_id=dispatch.dispatch_id,
        outcome=DispatchOutcome.SUCCEEDED, finished_at=_NOW,
    ))


def _prepare_materialize(aws: _OfflineAws, companion: str | None = None) -> RunDispatch:
    _seed_graph(aws, companion)
    prefix = f"normalized/354130/CNES_LOCAL/2026-01/{_RUN}"
    normal = _seed_output(
        aws, "unit-n", f"{prefix}/cnes_local.parquet",
        (_FIXTURES / "cnes-local-v1.parquet").read_bytes(),
    )
    _complete(aws, _dispatch(aws, "unit-n", 1), "unit-n", (normal,))
    prefix = f"reconciliation/354130/2026-01/{_RUN}"
    gold = _seed_output(
        aws, "unit-r", f"{prefix}/cnes.parquet", (_FIXTURES / "cnes-gold-v2.parquet").read_bytes(),
    )
    output = io.BytesIO()
    _divergence_frame().write_parquet(output)
    divergences = _seed_output(
        aws, "unit-r", f"{prefix}/cnes_divergences.parquet", output.getvalue(),
    )
    _complete(aws, _dispatch(aws, "unit-r", 2), "unit-r", (gold, divergences))
    return _dispatch(aws, "unit-m", 3)


def _message(dispatch: RunDispatch, unit_id: str = "unit-m") -> RunUnitMessage:
    return RunUnitMessage(
        tenant_id=_TENANT, run_id=_RUN, unit_id=unit_id, wave_id=dispatch.wave_id,
        dispatch_id=dispatch.dispatch_id, owner=str(dispatch.execution_ref), now=_NOW,
        lease_seconds=3600,
    )


async def _invoke_unit(
    aws: _OfflineAws, values: dict[str, str], dispatch: RunDispatch,
) -> tuple[int, list[dict[str, Any]]]:
    values = values | {
        "TENANT_ID": _TENANT, "RUN_ID": _RUN, "UNIT_ID": "unit-m",
        "WAVE_ID": dispatch.wave_id, "DISPATCH_ID": dispatch.dispatch_id,
        "EXECUTION_OWNER": str(dispatch.execution_ref), "LEASE_SECONDS": "3600",
    }
    root, stream = logging.getLogger(), io.StringIO()
    handlers, level = root.handlers[:], root.level
    root.handlers[:] = []
    try:
        with (patch.dict(os.environ, values, clear=True),
              patch.object(sys, "argv", ["data_processor"]), patch.object(sys, "stdout", stream)):
            rc = await aws.app.main()
        return rc, [json.loads(line) for line in stream.getvalue().splitlines()]
    finally:
        for handler in root.handlers[:]:
            handler.close()
        root.handlers[:] = handlers
        root.setLevel(level)


def _assert_materialized(aws: _OfflineAws) -> None:
    units = {unit.unit_id: unit for unit in aws.control_plane.list_run_units(_TENANT, _RUN)}
    unit = units["unit-m"]
    assert (unit.state, unit.attempt, unit.fencing_token) == (RunUnitState.SUCCEEDED, 1, 1)
    assert len(unit.output_manifests) == 1
    with aws.store.open(unit.output_manifests[0].manifest_key) as body:
        sidecar = json.load(body)
    assert (sidecar["unit_id"], sidecar["layer"], sidecar["attempt"]) == ("unit-m", "serving", 1)
    with aws.store.open(f"serving/354130/{_RUN}/overview.json") as body:
        actual = json.load(body)
    expected = json.loads((_FIXTURES / "cnes-serving-v1.json").read_text())
    expected["generated_at"] = "2026-01-15T12:00:00Z"
    assert actual == expected
    run = aws.control_plane.get_run(_TENANT, _RUN)
    assert run is not None
    assert run.state is RunState.PUBLISHED
    pointer = aws.control_plane.get_dataset_pointer(_TENANT, "cnes")
    assert pointer is not None
    assert pointer.version_id == "fixture-cnes-run-v1"
    events = aws.control_plane.read_outbox_page(None, 100).events
    assert sum(e.event_type == "run_unit.succeeded" for e in events) == 1
    assert sum(e.event_type == "reconciliation.published" for e in events) == 1


def _assert_normalized(aws: _OfflineAws, clock: str = "2026-01-15T12:00:00+00:00") -> None:
    unit = next(u for u in aws.control_plane.list_run_units(_TENANT, _RUN) if u.unit_id == "unit-n")
    assert (unit.state, unit.attempt, unit.fencing_token) == (RunUnitState.SUCCEEDED, 1, 1)
    with aws.store.open(unit.output_manifests[0].manifest_key) as body:
        sidecar = json.load(body)
    assert sidecar["object_key"] == (
        "normalized/354130/CNES_LOCAL/2026-01/fixture-cnes-run-v1/cnes_local.parquet"
    )
    key = f"tmp/354130/{_RUN}/unit-n/1/{sidecar['object_key']}"
    with aws.store.open(key) as body:
        frame = pl.read_parquet(body)
    assert tuple(frame.columns) == (
        "CPF", "CNS", "NOME_PROFISSIONAL", "NOME_SOCIAL", "SEXO", "CBO", "CNES",
        "TIPO_VINCULO", "SUS", "CH_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS", "CH_HOSPITALAR",
        "FONTE", "_source_manifest_id", "_source_snapshot_id", "_source_type", "_normalized_at",
    )
    assert frame.to_dicts() == [{
        "CPF": "11111111111", "CNS": "111111111111111", "NOME_PROFISSIONAL": "Ana",
        "NOME_SOCIAL": None, "SEXO": "F", "CBO": "000001", "CNES": "0000001",
        "TIPO_VINCULO": "1", "SUS": "S", "CH_TOTAL": 40, "CH_AMBULATORIAL": 40,
        "CH_OUTRAS": 0, "CH_HOSPITALAR": 0, "FONTE": "LOCAL",
        "_source_manifest_id": "cnes-local-full-1", "_source_snapshot_id": "full-1",
        "_source_type": "CNES_LOCAL", "_normalized_at": clock,
    }]


@pytest.mark.asyncio
@pytest.mark.parametrize("variant", ["full", "minimal", "malformed"])
async def test_unidade_materializa_publica_e_preserva_permit_da_execucao(
    offline_aws: _OfflineAws, variant: str,
) -> None:
    dispatch = _prepare_materialize(offline_aws)
    permit = _hold(offline_aws, dispatch)
    offline_aws.reset_observations()

    rc, events = await _invoke_unit(offline_aws, _values(variant), dispatch)

    assert rc == 0, events
    _assert_materialized(offline_aws)
    assert offline_aws.control_plane.observe_environment_gate("prod").permit == permit
    assert sorted(call.args[0] for call in offline_aws.session.client.call_args_list) == [
        "dynamodb", "s3", "stepfunctions",
    ]
    offline_aws.step_functions.describe_execution.assert_not_called()
    offline_aws.step_functions.start_execution.assert_not_called()
