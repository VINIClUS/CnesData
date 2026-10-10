"""Fencing de unidade/dispatch e idempotência com expiração lógica no runtime AWS composto."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, cast

import pytest

from cnes_domain.control_plane.commands import BeginIdempotency, ClaimRunUnit
from cnes_domain.control_plane.enums import RunUnitState
from cnes_domain.control_plane.errors import Conflict, FenceRejected
from data_processor.orchestration.attempt_store import unit_attempt_prefix
from tests.integration.aws._harness import (
    LEASE_SECONDS,
    TENANT,
    active_dispatch,
    claim_unit,
    commit_unit,
    idempotency_item,
    launch_frozen_cnes_run,
    units_of,
)

if TYPE_CHECKING:
    from tests.integration.aws._harness import AwsTestRuntime

pytestmark = [pytest.mark.dynamodb_local, pytest.mark.s3_integration]

_HASH_OLD = "a" * 64
_HASH_NEW = "b" * 64


def _begin(now: datetime, request_hash: str, resource_id: str) -> BeginIdempotency:
    return BeginIdempotency(
        tenant_id=TENANT, scope="run", key="request-1", request_hash=request_hash,
        resource_id=resource_id, now=now, expires_at=now + timedelta(hours=1),
    )


def test_worker_com_fence_antigo_nao_commita_unit(aws_runtime: AwsTestRuntime) -> None:
    run = launch_frozen_cnes_run(aws_runtime)
    dispatch = active_dispatch(aws_runtime, run)
    old = claim_unit(aws_runtime, dispatch, "worker-old", 30)
    aws_runtime.clock.advance(timedelta(seconds=31))
    new = claim_unit(aws_runtime, dispatch, "worker-new", 30)

    assert dispatch.lease_until > aws_runtime.clock.now()
    assert new.fencing_token == old.fencing_token + 1
    assert (new.unit_id, new.attempt) == (old.unit_id, old.attempt + 1)
    assert unit_attempt_prefix(new) != unit_attempt_prefix(old)
    with pytest.raises(FenceRejected, match="unit_fence_rejected"):
        commit_unit(aws_runtime, old, "manifest-old")
    committed = commit_unit(aws_runtime, new, "manifest-new")
    assert tuple(ref.manifest_id for ref in committed.output_manifests) == ("manifest-new",)
    assert committed.state is RunUnitState.SUCCEEDED


def test_idempotencia_expirada_pode_ser_reclamada_com_item_ttl_presente(
    aws_runtime: AwsTestRuntime,
) -> None:
    control_plane = aws_runtime.api.control_plane
    started = aws_runtime.clock.now()
    first = control_plane.begin_idempotency(_begin(started, _HASH_OLD, "run-old"))
    replay = control_plane.begin_idempotency(_begin(started, _HASH_OLD, "run-other"))
    with pytest.raises(Conflict, match="idempotency_hash_conflict"):
        control_plane.begin_idempotency(_begin(started, _HASH_NEW, "run-new"))
    aws_runtime.clock.advance(timedelta(hours=2))
    item = idempotency_item(aws_runtime, "run", "request-1")

    outcome = control_plane.begin_idempotency(
        _begin(aws_runtime.clock.now(), _HASH_NEW, "run-new"),
    )

    assert (first.created, replay.created, replay.record.resource_id) == (True, False, "run-old")
    assert int(item["expires_at"]["N"]) == int((started + timedelta(hours=1)).timestamp())
    assert (outcome.created, outcome.record.resource_id) == (True, "run-new")


def test_task_ecs_de_dispatch_antigo_nao_reclama_unit(aws_runtime: AwsTestRuntime) -> None:
    run = launch_frozen_cnes_run(aws_runtime)
    first = active_dispatch(aws_runtime, run)
    aws_runtime.clock.advance(first.lease_until - aws_runtime.clock.now() + timedelta(seconds=1))
    aws_runtime.processor.coordinator.resume(run.tenant_id, run.run_id)
    current = active_dispatch(aws_runtime, run)

    assert (current.wave_id, current.generation) == (first.wave_id, first.generation + 1)
    assert current.dispatch_id != first.dispatch_id
    stale_claim = ClaimRunUnit(
        tenant_id=first.tenant_id, run_id=first.run_id, unit_id=first.unit_ids[0],
        dispatch_id=first.dispatch_id, owner=cast("str", first.execution_ref),
        now=aws_runtime.clock.now(), lease_seconds=LEASE_SECONDS,
    )
    assert aws_runtime.processor.control_plane.claim_run_unit(stale_claim) is None
    assert {(unit.state, unit.attempt) for unit in units_of(aws_runtime, run)} == {
        (RunUnitState.PENDING, 0),
    }
