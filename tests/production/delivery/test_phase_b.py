"""Produtor nativo de membership e oráculos fechados de Phase B."""

from __future__ import annotations

import copy
import hashlib
import json
from datetime import timedelta
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from cnes_domain.control_plane.commands import ClaimRunDispatchStart, PutRunUnits
from cnes_domain.control_plane.dispatch_start import DispatchStartClaim, request_sha256
from cnes_domain.control_plane.enums import RunStage, RunState
from cnes_domain.control_plane.environment_gate import AcquireUnitPermit
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission, dispatch_holder
from cnes_domain.ports.processing import StartRunExecution
from cnes_infra.executor.step_functions import StepFunctionsExecutionProbe
from packages.cnes_infra.tests.contracts.clock import MutableClock, _reserve, _run, _unit
from packages.cnes_infra.tests.control_plane.test_dynamodb_promotion_snapshot import NOW
from packages.cnes_infra.tests.control_plane.test_dynamodb_promotion_snapshot import (
    ddb as ddb_fixture,
)
from scripts.promotion.environment_gate import DrainError, close_and_drain
from scripts.promotion.phase_b import (
    API_POLICY,
    MACHINE,
    _context_from_admitted,
    build_phase_b_evidence,
    review_phase_b_plan,
)
from scripts.promotion.verify_routes import build_closed_ready
from tests.production.delivery.test_canary import activation as activation_fixture
from tests.production.delivery.test_canary import case as case_fixture
from tests.production.delivery.test_canary import github as github_fixture

ddb = ddb_fixture
activation = activation_fixture
case = case_fixture
github = github_fixture
MACHINE_ARN = "arn:aws:states:us-east-2:123456789012:stateMachine:cnesdata-prod"
COMMON = {
    "repository": "VINIClUS/CnesData",
    "release_id": "r1",
    "source_sha": "a" * 40,
    "candidate_sha256": "b" * 64,
    "canary_id": "c" * 32,
    "activation_sha256": "d" * 64,
    "promotion_id": "r1",
    "promotion_generation": 100,
    "caller": {
        "repository": "VINIClUS/CnesData",
        "source_sha": "a" * 40,
        "ref": "refs/heads/develop",
        "workflow_path": ".github/workflows/promote-production.yml",
        "workflow_sha": "a" * 40,
        "run_id": 123,
        "run_attempt": 1,
    },
}
EXPIRATIONS = [
    {
        "kind": "activation",
        "object_sha256": "d" * 64,
        "expires_at": (NOW + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
    }
]


class Probe:
    def __init__(self, request=None, terminal=False):
        self.request, self.terminal = request, terminal

    def describe_execution(self, **kwargs):
        if not self.terminal:
            raise ClientError({"Error": {"Code": "ExecutionDoesNotExist"}}, "DescribeExecution")
        assert self.request is not None
        return {
            "executionArn": kwargs["executionArn"],
            "status": "SUCCEEDED",
            "input": json.dumps(self.request.model_dump(mode="json"), sort_keys=True),
        }


class Clock:
    def __init__(self, recover=lambda: None):
        self.value, self.recover = 0.0, recover

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds
        self.recover()


class Gate:
    def close_promotion_fence(self, command):
        raise AssertionError("expired inputs must not close the fence")


class Source:
    def __init__(self, member):
        self.member = member


def _member():
    return {}


def _seed(adapter):
    run = _run("run-a")
    adapter.put_run(run)
    units = (_unit("unit-a"), _unit("unit-b"))
    downstream = {"source_type": None, "file_subtype": None, "input_manifests": ()}
    reconcile = units[0].model_copy(
        update=downstream
        | {
            "unit_id": "reconcile",
            "stage": RunStage.RECONCILE,
            "depends_on_unit_ids": tuple(unit.unit_id for unit in units),
        }
    )
    materialize = units[0].model_copy(
        update=downstream
        | {
            "unit_id": "materialize",
            "stage": RunStage.MATERIALIZE,
            "depends_on_unit_ids": ("reconcile",),
        }
    )
    adapter.put_run_units(
        PutRunUnits(
            tenant_id=run.tenant_id,
            run_id=run.run_id,
            expected_run_state=RunState.PROCESSING,
            units=(*units, reconcile, materialize),
        )
    )
    dispatch = _reserve(adapter, MutableClock(NOW))
    request = StartRunExecution(
        tenant_id=dispatch.tenant_id,
        run_id=dispatch.run_id,
        wave_id=dispatch.wave_id,
        dispatch_id=dispatch.dispatch_id,
        unit_ids=dispatch.unit_ids,
        max_concurrency=1,
    )
    holder = dispatch_holder(dispatch, request)
    permit = adapter.acquire_unit_permit(AcquireUnitPermit("prod", holder, NOW, 300)).permit
    return dispatch, request, permit


def _claim(adapter, dispatch, request, permit):
    claim = DispatchStartClaim(
        dispatch_id=dispatch.dispatch_id,
        generation=dispatch.generation,
        permit_token=permit.token,
        fence_version=0,
        request_sha256=request_sha256(request),
    )
    command = ClaimRunDispatchStart(
        tenant_id=dispatch.tenant_id,
        run_id=dispatch.run_id,
        dispatch_id=dispatch.dispatch_id,
        claim=claim,
        now=NOW,
        lease_seconds=300,
    )
    assert adapter.claim_run_dispatch_start(command) is not None


@pytest.fixture(params=["empty", "reserved", "terminal"])
def native_close(ddb, request):
    adapter, _ = ddb
    mode = request.param
    client = Probe()
    if mode != "empty":
        dispatch, execution, permit = _seed(adapter)
        client.request = execution
        if mode == "terminal":
            _claim(adapter, dispatch, execution, permit)
            client.terminal = True
    probe = StepFunctionsExecutionProbe(cast("Any", client), MACHINE_ARN)
    admission = EnvironmentAdmission(adapter, probe, "prod", 300, adapter)
    from cnes_domain.ports.processing import ExecutionCallbacks, ExecutionPolicyConfig
    from cnes_infra.executor.step_functions import StepFunctionsExecutor
    from data_processor.orchestration.coordinator import (
        CoordinatorDependencies,
        PipelineCoordinator,
        allow_execution,
        noop_execution_started,
    )

    executor = StepFunctionsExecutor(cast("Any", client), MACHINE_ARN)
    dependencies = CoordinatorDependencies(
        adapter, executor, cast("Any", None), lambda: NOW + timedelta(seconds=1)
    )
    execution = ExecutionPolicyConfig(
        1, 300, ExecutionCallbacks(allow_execution, noop_execution_started), admission
    )
    coordinator = PipelineCoordinator(dependencies, execution)
    clock = Clock(lambda: coordinator.recover())
    result = close_and_drain(
        adapter, probe, None, "prod", "r1", NOW, expirations=EXPIRATIONS, clock=clock, common=COMMON
    )
    return adapter, probe, result, mode


def test_producer_nativo_conserva_holder_e_prova_original(native_close):
    adapter, _, close, mode = native_close
    state = adapter.observe_environment_gate("prod")
    assert state.permit is None
    assert len(state.closed_membership) == (0 if mode == "empty" else 1)
    assert len(close["members"]) == len(state.closed_membership)
    if mode != "empty":
        assert close["members"][0]["resolution"] == (
            "TERMINAL" if mode == "terminal" else "CANCEL_BEFORE_SEND"
        )
        assert close["members"][0]["request_sha256"] == state.closed_membership[0]["request_sha256"]
    assert all(expiry in close["input_expirations"] for expiry in EXPIRATIONS)
    assert len(close["input_expirations"]) == len(EXPIRATIONS) + 1
    receipt = state.fence.close_receipt
    digest = hashlib.sha256(
        (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    assert {
        "kind": "envelope",
        "object_sha256": digest,
        "expires_at": receipt["expires_at"],
    } in close["input_expirations"]
    assert "closed_membership_sha256" not in close


def test_starting_ausente_nunca_vira_cancelamento(ddb):
    adapter, _ = ddb
    dispatch, execution, permit = _seed(adapter)
    _claim(adapter, dispatch, execution, permit)
    probe = StepFunctionsExecutionProbe(cast("Any", Probe()), MACHINE_ARN)
    admission = EnvironmentAdmission(adapter, probe, "prod", 300, adapter)
    clock = Clock(lambda: admission.reconcile(NOW + timedelta(seconds=1)))
    expiration = [{**EXPIRATIONS[0], "expires_at": (NOW + timedelta(seconds=2)).isoformat()}]
    with pytest.raises(DrainError, match="deadline_exceeded"):
        close_and_drain(
            adapter,
            probe,
            None,
            "prod",
            "r1",
            NOW,
            expirations=expiration,
            clock=clock,
            common=COMMON,
        )
    assert adapter.observe_environment_gate("prod").permit is not None


def test_input_expirado_nao_fecha_nem_em_ambiente_ocioso():
    expiration = [{**EXPIRATIONS[0], "expires_at": (NOW - timedelta(seconds=1)).isoformat()}]
    with pytest.raises(DrainError, match="deadline_exceeded"):
        close_and_drain(
            cast("Any", Gate()),
            cast("Any", Probe()),
            None,
            "prod",
            "r1",
            NOW,
            expirations=expiration,
            common=COMMON,
        )


def _phase_plan(context, initial):
    machine = copy.deepcopy(context.machine)
    old = None if initial else copy.deepcopy(machine)
    if old is not None:
        definition = json.loads(old["definition"])
        definition["States"]["RunUnits"]["ItemProcessor"]["States"]["RunProcessor"]["Parameters"][
            "TaskDefinition"
        ] = context.phase_a["active_before"] or "unit:1"
        old["definition"] = json.dumps(definition)
    changes = [
        {
            "mode": "managed",
            "address": MACHINE,
            "change": {
                "actions": ["create" if initial else "update"],
                "before": old,
                "after": machine,
            },
        }
    ]
    if initial:
        old_policy = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "table"}],
        }
        policy = copy.deepcopy(old_policy)
        policy["Statement"] += [
            {
                "Effect": "Allow",
                "Action": ["states:DescribeStateMachine", "states:StartExecution"],
                "Resource": machine["arn"],
            },
            {
                "Effect": "Allow",
                "Action": ["states:DescribeExecution", "states:StopExecution"],
                "Resource": machine["arn"].replace(":stateMachine:", ":execution:") + ":*",
            },
        ]
        changes.append(
            {
                "mode": "managed",
                "address": API_POLICY,
                "change": {
                    "actions": ["update"],
                    "before": {"policy": old_policy},
                    "after": {"policy": policy},
                },
            }
        )
    return {"resource_changes": changes}


@pytest.mark.parametrize("initial", [True, False])
def test_oraculos_validam_delta_exato_e_rejeitam_role_definicao_e_grants(case, initial):
    contract, _, _, _ = case
    context = _context_from_admitted(contract)
    oracle = context.oracle | {"active_before": None if initial else "unit:1"}
    plan = _phase_plan(context, initial)
    assert review_phase_b_plan(plan, oracle["active_before"], oracle) == (
        "initial" if initial else "established"
    )
    bad = copy.deepcopy(plan)
    bad["resource_changes"][0]["change"]["after"]["role_arn"] = "other-role"
    with pytest.raises(ValueError):
        review_phase_b_plan(bad, oracle["active_before"], oracle)
    bad = copy.deepcopy(plan)
    bad["resource_changes"][0]["change"]["after"]["definition"] = (
        '{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","End":true}}}'
    )
    with pytest.raises(ValueError):
        review_phase_b_plan(bad, oracle["active_before"], oracle)
    if initial:
        bad = copy.deepcopy(plan)
        bad["resource_changes"][1]["change"]["after"]["policy"]["Statement"][0]["Action"] = "s3:*"
        with pytest.raises(ValueError, match="other_grants"):
            review_phase_b_plan(bad, None, oracle)


@pytest.fixture
def original_bundle(native_close):
    adapter, probe, close, _ = native_close
    prefix = "arn:aws:ecs:us-east-2:123456789012:task-definition/cnesdata-prod-processor-"
    routes = {
        "mode": "initial",
        "state_machine_arn": MACHINE_ARN,
        "unit_revision": prefix + "unit:2",
        "recovery_revision": prefix + "recovery:1",
        "audit_revision": prefix + "audit:1",
        "unit_environment_state_machine_arn": MACHINE_ARN,
        "recovery_environment_state_machine_arn": MACHINE_ARN,
    }
    before = {"serial": 1, "state_sha256": "1" * 64, "object_version": "v1"}
    before["lineage"] = "synthetic-phase-b-state"
    after = before | {"serial": 2, "state_sha256": "2" * 64, "object_version": "v2"}
    phase = build_phase_b_evidence(
        close,
        before,
        after,
        b"original binary",
        b"independent audit",
        b"applied binary log",
        routes,
        NOW + timedelta(seconds=2),
    )
    observed = NOW + timedelta(seconds=3)
    ready = build_closed_ready(
        close,
        phase,
        adapter,
        probe,
        observed,
        COMMON["caller"],
        COMMON["promotion_generation"],
        observed,
        observed + timedelta(seconds=180),
    )

    def canonical(obj):
        return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode()

    return {
        "close-drain.json": canonical(close),
        "phase-b.json": canonical(phase),
        "closed-ready.json": canonical(ready),
    }, routes


def test_closed_ready_reobserva_provas_originais_apos_b(original_bundle):
    files, _ = original_bundle
    close, ready = json.loads(files["close-drain.json"]), json.loads(files["closed-ready.json"])
    assert close["common"] == ready["common"]
    assert len(close["members"]) == len(ready["members"])
    for member in ready["members"]:
        assert member["observed_at"] == ready["observed_at"]
    assert ready["close_drain_sha256"] == hashlib.sha256(files["close-drain.json"]).hexdigest()
