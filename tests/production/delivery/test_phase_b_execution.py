"""Operação Phase B com subprocesso e AWS substituídos apenas nas fronteiras."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from cnes_infra.executor.step_functions import StepFunctionsExecutionProbe
from scripts.promotion.environment_gate import close_and_drain
from scripts.promotion.phase_b import _context_from_admitted, execute_reviewed_phase_b
from scripts.promotion.run_canary import _contract
from scripts.promotion.verify_routes import NativePhaseB
from tests.production.delivery.test_activation_manifest import _reseal_phase, admit, prepare
from tests.production.delivery.test_phase_b import COMMON, MACHINE_ARN, Clock, Probe, _phase_plan
from tests.production.delivery.test_phase_b import activation as activation_fixture
from tests.production.delivery.test_phase_b import case as case_fixture
from tests.production.delivery.test_phase_b import ddb as ddb_fixture
from tests.production.delivery.test_phase_b import github as github_fixture

activation = activation_fixture
case = case_fixture
ddb = ddb_fixture
github = github_fixture


class Boundary:
    def __init__(self, context):
        self.context, self.serial, self.calls = context, 1, []
        self.failure = None
        self.drift = False
        self.revisions = {}
        self.scheduler_reads, self.task_reads = [], []

    def get_object(self, **_):
        resources = []
        for mode, arn in self.revisions.items():
            attributes = {
                "name": mode,
                "group_name": "prod",
                "target": [
                    {
                        "arn": self.context.canary["cluster"],
                        "ecs_parameters": [{"task_definition_arn": arn}],
                    }
                ],
            }
            resources.append(
                {"type": "aws_scheduler_schedule", "instances": [{"attributes": attributes}]}
            )
        state = {"serial": self.serial, "resources": resources}
        state["lineage"] = "synthetic-phase-b-state"
        return {"VersionId": f"v{self.serial}", "Body": io.BytesIO(json.dumps(state).encode())}

    def describe_state_machine(self, **_):
        return {
            "stateMachineArn": self.context.machine["arn"],
            "definition": self.context.machine["definition"],
        }

    def get_schedule(self, **request):
        self.scheduler_reads.append(request)
        return {
            "Target": {
                "Arn": self.context.canary["cluster"],
                "EcsParameters": {
                    "TaskDefinitionArn": self.revisions[request["Name"]]
                    + ("9" if self.drift else "")
                },
            }
        }

    def describe_task_definition(self, **request):
        self.task_reads.append(request["taskDefinition"])
        return {
            "taskDefinition": {
                "taskDefinitionArn": request["taskDefinition"],
                "containerDefinitions": [
                    {
                        "environment": [
                            {"name": "AWS_STATE_MACHINE_ARN", "value": self.context.machine["arn"]}
                        ]
                    }
                ],
            }
        }

    def run(self, args, **_):
        self.calls.append(args)
        if args[1] == "show":
            initial = self.context.phase_a["active_before"] is None
            raw = json.dumps(_phase_plan(self.context, initial)).encode()
        elif args[1] == "apply":
            self.serial += 1
            if self.failure:
                raise self.failure
            raw = b"original apply stdout"
        else:
            raw = b"No changes"
        return subprocess.CompletedProcess(args, 0, raw, b"")


@pytest.fixture
def operation(case, activation, ddb, tmp_path, monkeypatch):
    contract, *_ = case
    context = _context_from_admitted(contract, prepare(activation))
    adapter, client = ddb
    assert context.activation is not None
    now = datetime.now(UTC)
    common = {
        **COMMON,
        **{
            key: contract[key]
            for key in ("repository", "release_id", "source_sha", "candidate_sha256", "canary_id")
        },
        "activation_sha256": hashlib.sha256(context.activation).hexdigest(),
        "promotion_id": contract["release_id"],
    }
    close = close_and_drain(
        adapter,
        StepFunctionsExecutionProbe(cast("Any", Probe()), MACHINE_ARN),
        None,
        "prod",
        contract["release_id"],
        now,
        clock=Clock(),
        common=common,
        expirations=[
            {
                "kind": "activation",
                "object_sha256": common["activation_sha256"],
                "expires_at": (now + timedelta(hours=1)).isoformat(),
            }
        ],
    )
    boundary = Boundary(context)
    clients: dict[str, Any] = dict.fromkeys(("s3", "stepfunctions", "ecs", "scheduler"), boundary)
    clients["dynamodb"] = client
    native = NativePhaseB(
        tmp_path, clients,
        {"bucket": "state", "key": "prod", "control_plane_table": adapter._table_name},
    )
    monkeypatch.setattr("scripts.promotion.verify_routes.subprocess.run", boundary.run)
    binary, audit, approval = _approved_binary(tmp_path, native, common)
    return SimpleNamespace(
        context=context,
        close=close,
        native=native,
        approval=approval,
        boundary=boundary,
        adapter=adapter,
        binary=binary,
        audit=audit,
    )


def execute(op):
    return execute_reviewed_phase_b(op.context, op.close, op.native, op.approval)


def test_aplica_bytes_originais_com_checkpoint_e_rotas_reais(operation):
    result = execute(operation)
    assert result["plan_sha256"] == hashlib.sha256(operation.binary.read_bytes()).hexdigest()
    assert result["audit_sha256"] == operation.approval["audit_sha256"]
    assert result["before"]["object_version"] == "v1"
    assert result["after"]["object_version"] == "v2"
    assert result["routes"]["mode"] == "initial"
    assert [call[1] for call in operation.boundary.calls] == ["show", "apply", "plan"]


@pytest.mark.parametrize("changed", ["binary", "audit", "checkpoint"])
def test_recusa_mutacao_antes_de_aplicar(operation, changed):
    if changed == "checkpoint":
        operation.boundary.serial += 1
    else:
        getattr(operation, changed).write_bytes(b"changed original")
    with pytest.raises(ValueError):
        execute(operation)
    assert not any(call[1] == "apply" for call in operation.boundary.calls)
    assert operation.adapter.observe_environment_gate("prod").fence.state.value == "CLOSED"


@pytest.mark.parametrize("failure", ["partial_apply", "scheduler_drift"])
def test_falha_apos_mutacao_mantem_closed_sem_emitir_sucesso(
    operation, failure, tmp_path, activation
):
    if failure == "partial_apply":
        operation.boundary.failure = subprocess.CalledProcessError(1, "tofu apply")
    else:
        _establish(operation, tmp_path, activation)
        operation.boundary.drift = True
    with pytest.raises((subprocess.CalledProcessError, ValueError)):
        execute(operation)
    assert operation.boundary.serial == 2
    assert operation.adapter.observe_environment_gate("prod").fence.state.value == "CLOSED"


def test_usa_copia_privada_dos_bytes_auditados_ate_o_apply(operation, monkeypatch):
    original_run = operation.boundary.run
    applied = []

    def mutate_original(args, **kwargs):
        from pathlib import Path

        response = original_run(args, **kwargs)
        if args[1] == "show":
            operation.binary.write_bytes(b"replaced caller path")
        if args[1] == "apply":
            applied.append(Path(args[-1]).read_bytes())
            assert Path(args[-1]) != operation.binary
        return response

    monkeypatch.setattr("scripts.promotion.verify_routes.subprocess.run", mutate_original)
    result = execute(operation)
    assert applied == [b"original reviewed binary"]
    assert result["plan_sha256"] == hashlib.sha256(applied[0]).hexdigest()


def _approved_binary(tmp_path, native, common):
    binary, audit = tmp_path / "phase-b.tfplan", tmp_path / "independent-audit.json"
    binary.write_bytes(b"original reviewed binary")
    report = {
        "decision": "ACCEPT",
        "plan_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "checkpoint": native.checkpoint()[0],
        "source_sha": common["source_sha"],
    }
    audit.write_text(json.dumps(report))
    approval = {
        "plan_path": str(binary),
        "audit_path": str(audit),
        "audit_sha256": hashlib.sha256(audit.read_bytes()).hexdigest(),
    }
    return binary, audit, approval


def test_phase_b_inicial_aceita_state_sem_schedulers(operation):
    assert operation.boundary.revisions == {}
    result = execute(operation)
    assert result["routes"]["mode"] == "initial"
    for mode in ("unit", "recovery", "audit"):
        assert result["routes"][f"{mode}_revision"] == operation.context.canary["revisions"][mode]
    assert operation.adapter.observe_environment_gate("prod").fence.state.value == "CLOSED"

    assert operation.boundary.scheduler_reads == []
    assert set(operation.boundary.task_reads) == set(operation.context.canary["revisions"].values())


def _establish(operation, tmp_path, activation):
    payload = json.loads(prepare(activation))
    unit = operation.context.canary["revisions"]["unit"]
    old = unit.rsplit(":", 1)[0] + ":1"
    activation["proof"].update(
        active_before=old, retained_before=[old], retained_after=sorted([old, unit])
    )
    activation["request"]["inputs"]["retained_unit_task_definition_arns"] = sorted([old, unit])
    raw = _reseal_phase(activation, payload)
    contract = _contract(admit(activation, raw), operation.context.canary["_governed"])
    operation.context = _context_from_admitted(contract, raw)
    operation.close["common"]["activation_sha256"] = hashlib.sha256(raw).hexdigest()
    operation.boundary.context = operation.context
    prefix = operation.context.canary["revisions"]["unit"].rsplit("unit:", 1)[0]
    operation.boundary.revisions = {mode: prefix + mode + ":1" for mode in ("recovery", "audit")}
    _, _, operation.approval = _approved_binary(
        tmp_path, operation.native, operation.close["common"]
    )


def test_phase_b_estabelecida_preserva_ambos_schedulers_originais(operation, tmp_path, activation):
    _establish(operation, tmp_path, activation)
    result = execute(operation)
    assert result["routes"]["mode"] == "established"
    for mode, revision in operation.boundary.revisions.items():
        assert result["routes"][f"{mode}_revision"] == revision
        assert revision != operation.context.canary["revisions"][mode]
    assert len(operation.boundary.scheduler_reads) == 2


@pytest.mark.parametrize("missing", [("recovery",), ("audit",), ("recovery", "audit")])
def test_phase_b_estabelecida_rejeita_scheduler_ausente(operation, tmp_path, missing, activation):
    _establish(operation, tmp_path, activation)
    for mode in missing:
        del operation.boundary.revisions[mode]
    _, state = operation.native.checkpoint()
    with pytest.raises(ValueError, match="original_schedulers_required"):
        operation.native.routes(operation.context, state)


@pytest.mark.parametrize("mode", ["recovery", "audit"])
def test_phase_b_inicial_rejeita_scheduler_ja_existente(operation, mode):
    operation.boundary.revisions[mode] = operation.context.canary["revisions"][mode]
    _, state = operation.native.checkpoint()
    with pytest.raises(ValueError, match="initial_schedulers_must_be_absent"):
        operation.native.routes(operation.context, state)
