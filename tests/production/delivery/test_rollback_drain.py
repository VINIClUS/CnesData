"""Rollback, drain e poda das revisões exatas do processor."""

from __future__ import annotations

import copy
import hashlib
import json
import subprocess
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from cnes_domain.control_plane.environment_gate import ReleaseUnitPermit
from scripts.promotion import drain, rollback
from scripts.promotion import prune_revisions as prune
from scripts.promotion.publish_dashboard import StaticChannel
from tests.production.delivery import test_api_static_rollback as st
from tests.production.delivery import test_phase_c as pc
from tests.production.delivery._api_static_fixtures import FakeStaticPort

ACCOUNT, NOW = "123456789012", datetime(2026, 11, 14, 12, tzinfo=UTC)
URI = f"{ACCOUNT}.dkr.ecr.us-east-2.amazonaws.com/cnesdata-prod-processor"
MODES = ("unit", "recovery", "audit")
DIGESTS = {n: "sha256:" + str(n) * 64 for n in range(1, 7)}
AGES = {1: 40, 2: 35, 3: 10, 4: 1, 5: 3, 6: 2}
phase_c, operation = pc.phase_c, pc.operation
activation, case, ddb, github = pc.activation, pc.case, pc.ddb, pc.github
MACHINE_AT = "module.processing.aws_sfn_state_machine.processor[0]"
SFN_POLICY = "module.processing.aws_iam_role_policy.step_functions_ecs[0]"


def _arn(mode, number):
    family = f"cnesdata-prod-processor-{mode}"
    return f"arn:aws:ecs:us-east-2:{ACCOUNT}:task-definition/{family}:{number}"


def _tag(number):
    return f"candidate-{'a' * 40}-{number}-1"


class Registry:
    def __init__(self, releases=(1, 2, 3, 4), page=2):
        self.images = {DIGESTS[n]: [NOW - timedelta(days=AGES[n]), [_tag(n)]] for n in releases}
        self.revisions = {_arn(m, n): DIGESTS[n] for n in releases for m in MODES}
        self.page, self.calls, self.reads, self.failures, self.after_read = page, [], 0, [], {}
        self.endless, self.status = False, {}

    def _paged(self, items, key, request, endless=False):
        start = int(request.get("nextToken") or 0)
        body = {key: items[start:start + self.page]}
        if endless:
            return body | {"nextToken": str((start + self.page) % len(items))}
        more = start + self.page < len(items)
        return body | ({"nextToken": str(start + self.page)} if more else {})

    def describe_images(self, **request):
        assert request["repositoryName"] == "cnesdata-prod-processor"
        self.reads += "nextToken" not in request
        self.after_read.pop(self.reads, lambda: None)()
        details = [{"imageDigest": d, "imagePushedAt": at, **({"imageTags": tags} if tags else {})}
                   for d, (at, tags) in sorted(self.images.items())]
        return self._paged(details, "imageDetails", request)

    def list_task_definitions(self, **request):
        assert request["status"] == "ACTIVE"
        family = request["familyPrefix"]
        arns = sorted(a for a in self.revisions if a.split("/")[1].startswith(family))
        return self._paged(arns, "taskDefinitionArns", request, self.endless)

    def describe_task_definition(self, **request):
        arn = request["taskDefinition"]
        image, status = f"{URI}@{self.revisions[arn]}", self.status.get(arn, "ACTIVE")
        return {"taskDefinition": {"taskDefinitionArn": arn, "status": status,
                                   "containerDefinitions": [{"image": image}]}}

    def deregister_task_definition(self, **request):
        self.calls.append(("deregister", request["taskDefinition"]))
        del self.revisions[request["taskDefinition"]]

    def batch_delete_image(self, **request):
        self.calls.append(("delete", request["repositoryName"], request["imageIds"]))
        for item in request["imageIds"]:
            self.images.pop(item["imageDigest"])
        return {"imageIds": request["imageIds"], "failures": self.failures}


def _refs(current=4, prior=3, in_flight=(), retained=None, drained=True):
    units = retained if retained is not None else (prior, current)
    target = replace(TARGET, current=frozenset(_arn(m, current) for m in MODES),
                     retained=frozenset(_arn("unit", n) for n in units))
    result = drain.DrainResult(drained, frozenset(in_flight), (), (), 2, target)
    return prune.PruneReferences.from_drain(result, {_arn(m, prior) for m in MODES})


def _prune(registry, refs=None):
    return prune.execute_prune({"ecr": registry, "ecs": registry}, refs or _refs(), NOW)


def _deleted(registry):
    return [item[2][0]["imageDigest"] for item in registry.calls if item[0] == "delete"]


def _deregistered(registry):
    return sorted(item[1] for item in registry.calls if item[0] == "deregister")


def test_protege_atual_anterior_e_referenciada_por_execucao():
    registry = Registry()
    evidence = _prune(registry, _refs(in_flight={_arn("unit", 2)}))
    assert evidence["outcome"] == "PLANNED"
    assert _deleted(registry) == [DIGESTS[1]] == evidence["prune"]
    assert _deregistered(registry) == sorted(_arn(m, 1) for m in MODES)
    assert all(_arn(m, n) in registry.revisions for m in MODES for n in (2, 3, 4))
    assert set(registry.images) == {DIGESTS[n] for n in (2, 3, 4)}


def test_so_revisao_antiga_sem_referencia_e_elegivel():
    registry = Registry()
    registry.revisions[_arn("recovery", 5)] = DIGESTS[2]
    evidence = _prune(registry, _refs(retained=(1, 3, 4)))
    assert _deleted(registry) == []
    assert evidence["prune"] == []
    assert evidence["outcome"] == "PLANNED"
    assert _deregistered(registry) == []
    assert evidence["retained_after"] == sorted(_arn("unit", n) for n in (3, 4))
    registry = Registry()
    evidence = _prune(registry)
    assert _deleted(registry) == [DIGESTS[1], DIGESTS[2]]
    assert _deregistered(registry) == sorted(_arn(m, n) for m in MODES for n in (1, 2))
    assert evidence["deregistered"] == _deregistered(registry)
    registry = Registry()
    registry.images[DIGESTS[3]][0] = NOW - timedelta(days=31)
    _prune(registry, _refs(prior=2, retained=(2, 4)))
    assert _deleted(registry) == [DIGESTS[1]]


def test_mapeia_revisao_para_digest_e_consome_processor_retention(monkeypatch):
    seen = []
    real = prune.plan_processor_retention
    monkeypatch.setattr(prune, "plan_processor_retention",
                        lambda *args: seen.append(args) or real(*args))
    registry = Registry(releases=(1, 2, 3, 4, 5, 6))
    evidence = _prune(registry, _refs(current=6, prior=5, in_flight={_arn("audit", 4)},
                                      retained=(3, 5, 6)))
    snapshot, candidate, now = seen[0]
    assert (candidate.digest, now) == (DIGESTS[6], NOW)
    assert candidate.first_admitted_at == NOW - timedelta(days=2)
    references = snapshot.references
    assert (references.current, references.prior) == ({DIGESTS[6]}, {DIGESTS[5]})
    assert references.in_flight == {DIGESTS[4]}
    assert references.retained_revisions == {DIGESTS[n] for n in (3, 5, 6)}
    assert {image.digest for image in snapshot.history} == set(DIGESTS.values())
    assert snapshot.complete is True
    assert evidence["prune"] == [DIGESTS[1], DIGESTS[2]] == _deleted(registry)


def _untag(registry, number):
    registry.images[DIGESTS[number]][1] = []


INCOMPLETE = {
    "digest_referenciado_sem_tag": lambda r: _untag(r, 3),
    "revisao_fora_do_repositorio": lambda r: r.revisions.update({_arn("unit", 1): "x@" + "y"}),
    "revisao_referenciada_ausente": lambda r: r.revisions.pop(_arn("audit", 3)),
    "paginas_sem_fim": lambda r: setattr(r, "endless", True),
    "revisao_inativa": lambda r: r.status.update({_arn("unit", 1): "INACTIVE"}),
    "revisao_de_outra_familia": lambda r: r.revisions.update(
        {_arn("unit", 1).replace("-unit:", "-unitx:"): DIGESTS[1]}),
    "release_com_dois_digests": lambda r: r.revisions.update({_arn("recovery", 4): DIGESTS[3]}),
}


@pytest.mark.parametrize("case_name", sorted(INCOMPLETE))
def test_inventario_incompleto_nao_poda(case_name):
    registry = Registry()
    INCOMPLETE[case_name](registry)
    evidence = _prune(registry)
    assert evidence["outcome"] == "REJECTED_INCOMPLETE"
    assert registry.calls == []
    assert evidence["retained_after"] == sorted(_refs().retained)


def test_digest_com_retencao_vencida_e_podado():
    registry = Registry(releases=(1, 3, 4))
    evidence = _prune(registry)
    assert registry.calls[-1] == ("delete", "cnesdata-prod-processor",
                                  [{"imageDigest": DIGESTS[1]}])
    assert [call[0] for call in registry.calls] == ["deregister"] * 3 + ["delete"]
    assert DIGESTS[1] not in registry.images
    assert _tag(1) not in str(registry.images)
    assert evidence["readbacks"][0] == evidence["readbacks"][1] != evidence["readbacks"][2]
    registry = Registry(releases=(1, 2, 3, 4, 5, 6))
    for number in (1, 2):
        registry.images[DIGESTS[number]][0] = NOW - timedelta(days=5 + number)
    _prune(registry, _refs(current=6, prior=5, retained=(5, 6)))
    assert _deleted(registry) == [DIGESTS[3]]


def _changed(registry):
    registry.after_read[2] = lambda: registry.images.update({DIGESTS[5]: [NOW, [_tag(5)]]})


UNCERTAIN = {
    "inventario_muda_entre_leituras": (_changed, "prune=readback_changed"),
    "falha_no_delete": (lambda r: setattr(r, "failures", [{"failureCode": "x"}]),
                        "prune=delete_failed"),
    "imagem_volta_no_readback": (lambda r: r.after_read.update(
        {3: lambda: r.images.setdefault(DIGESTS[1], [NOW, [_tag(1)]])}), "prune=post_readback"),
    "leitura_final_truncada": (lambda r: r.after_read.update(
        {3: lambda: setattr(r, "endless", True)}), "prune=post_readback_incomplete"),
}


@pytest.mark.parametrize("case_name", sorted(UNCERTAIN))
def test_incerteza_no_readback_mantem_recursos(case_name):
    registry = Registry(releases=(1, 3, 4))
    edit, reason = UNCERTAIN[case_name]
    edit(registry)
    with pytest.raises(ValueError, match=reason):
        _prune(registry)
    if case_name == "inventario_muda_entre_leituras":
        assert registry.calls == []


def test_sem_drain_mantem_revisoes_digests_e_grants():
    registry = Registry()
    refs = _refs(retained=(1, 2, 3, 4), drained=False)
    evidence = _prune(registry, refs)
    assert evidence["outcome"] == "NOT_DRAINED"
    assert registry.calls == []
    assert registry.reads == 0
    assert evidence["retained_after"] == sorted(refs.retained)


PRIORS = {
    "revisao_atual": {_arn(m, 4) for m in MODES},
    "familia_faltando": {_arn(m, 3) for m in MODES[:2]},
    "duas_por_familia": {_arn(m, 3) for m in MODES} | {_arn("audit", 2)},
    "unit_nao_retida": {_arn(m, 2) for m in MODES},
    "vazia_com_retida_antiga": set(),
}


@pytest.mark.parametrize("case_name", sorted(PRIORS))
def test_poda_so_aceita_anterior_coerente_com_o_drain(case_name):
    result = drain.DrainResult(True, frozenset(), (), (), 2, TARGET)
    with pytest.raises(ValueError, match="prune=prior"):
        prune.PruneReferences.from_drain(result, PRIORS[case_name])
    first = replace(result, target=replace(TARGET, retained=frozenset({_arn("unit", 4)})))
    assert prune.PruneReferences.from_drain(first, set()).prior == frozenset()


MACHINE = f"arn:aws:states:us-east-2:{ACCOUNT}:stateMachine:cnesdata-prod-processor"
EXECUTION = MACHINE.replace(":stateMachine:", ":execution:") + ":{}"
OLD, NEW = EXECUTION.format("antiga"), EXECUTION.format("nova")
TARGET = drain.DrainTarget(
    state_machine_arn=MACHINE, cluster_arn=f"arn:aws:ecs:us-east-2:{ACCOUNT}:cluster/x",
    switched_at=NOW, current=frozenset(_arn(m, 4) for m in MODES),
    retained=frozenset(_arn("unit", n) for n in (3, 4)), seconds=60)


class Work:
    def __init__(self, executions=(), tasks=(), done_at=None, stops_end=False):
        self.executions = {arn: NOW + timedelta(seconds=delta) for arn, delta in executions}
        self.tasks = {f"task/{i}": arn for i, arn in enumerate(tasks)}
        self.done_at, self.stops_end, self.reads, self.stopped = done_at, stops_end, 0, []

    def list_executions(self, **request):
        assert (request["stateMachineArn"], request["statusFilter"]) == (MACHINE, "RUNNING")
        self.reads += 1
        if self.reads == self.done_at:
            self.executions, self.tasks = {}, {}
        return {"executions": [{"executionArn": arn, "startDate": start}
                               for arn, start in self.executions.items()]}

    def list_tasks(self, **request):
        assert (request["cluster"], request["desiredStatus"]) == (TARGET.cluster_arn, "RUNNING")
        return {"taskArns": sorted(self.tasks)}

    def describe_tasks(self, **request):
        return {"tasks": [{"taskArn": arn, "taskDefinitionArn": self.tasks[arn]}
                          for arn in request["tasks"]], "failures": []}

    def stop_execution(self, **request):
        self.stopped.append((request["executionArn"], request["cause"]))
        if self.stops_end:
            self.executions.pop(request["executionArn"])
        return {}


class Ticker:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def _drain(work, approval=None):
    ticker = Ticker()
    result = drain.drain_old_work({"stepfunctions": work, "ecs": work}, TARGET, ticker, approval)
    assert ticker.now <= TARGET.seconds + drain.POLL_SECONDS
    return result


def test_execucao_standard_ativa_mantem_definicao_sem_aprovacao():
    work = Work(executions=[(OLD, -60), (NEW, 60)])
    result = _drain(work)
    assert result.drained is False
    assert (result.executions, work.stopped, result.cancelled) == ((OLD,), [], ())
    assert result.in_flight == {_arn("unit", 3)}
    approval = drain.CancelApproval("dono", "rollback", ((OLD, "tenant-1", "run-1"),))
    work = Work(executions=[(OLD, -60), (NEW, 60)], stops_end=True)
    result = _drain(work, approval)
    assert result.drained is True
    assert work.stopped == [(OLD, "tenant_id=tenant-1 run_id=run-1")]
    assert result.cancelled == ({"execution_arn": OLD, "tenant_id": "tenant-1", "run_id": "run-1",
                                 "approval_sha256": approval.sha256()},)
    work = Work(executions=[(OLD, -60)], stops_end=True)
    other = drain.CancelApproval("dono", "rollback", ((NEW, "tenant-1", "run-1"),))
    assert (_drain(work, other).drained, work.stopped) == (False, [])
    for bad in ((EXECUTION.format("*"), "t", "r"), ("arn:aws:states:x", "t", "r"), (OLD, "", "r")):
        with pytest.raises(ValueError, match="drain=approval"):
            _drain(Work(), drain.CancelApproval("dono", "rollback", (bad,)))
    with pytest.raises(ValueError, match="drain=approval"):
        _drain(Work(), replace(approval, approver=""))


def test_drain_vencido_mantem_revisoes_e_grants_antigos():
    work = Work(tasks=[_arn("recovery", 3), _arn("unit", 4), "arn:aws:ecs:outra:1"])
    result = _drain(work)
    assert result.drained is False
    assert (result.in_flight, result.target) == ({_arn("recovery", 3)}, TARGET)
    registry = Registry()
    refs = prune.PruneReferences.from_drain(result, {_arn(m, 3) for m in MODES})
    assert (refs.drained, refs.in_flight, refs.current) == (False, result.in_flight, TARGET.current)
    evidence = _prune(registry, refs)
    assert (evidence["outcome"], registry.calls) == ("NOT_DRAINED", [])
    assert evidence["retained_after"] == sorted(refs.retained)
    work = Work(tasks=[_arn("recovery", 3)], done_at=2)
    result = _drain(work)
    assert (result.drained, result.in_flight, result.target) == (True, frozenset(), TARGET)
    assert work.reads == 3


def _prior_context(ctx):
    unit, old = ctx.canary["revisions"]["unit"], ctx.phase_a["active_before"]
    machine = {**ctx.machine, "definition": ctx.machine["definition"].replace(unit, old)}
    revisions = {mode: old.replace("unit:", f"{mode}:") for mode in MODES}
    return replace(ctx, canary={**ctx.canary, "revisions": revisions}, machine=machine)


def _rollback_plan(op, rb):
    now, then = rb.current.canary["revisions"], rb.prior.canary["revisions"]
    changes = [pc._change(MACHINE_AT, ["update"], rb.current.machine, rb.prior.machine),
               pc._change(SFN_POLICY, ["no-op"], {"a": 1}, {"a": 1})]
    for mode in pc.MODES:
        changes += [pc._change(pc.SCHEDULE.format(mode), ["update"],
                               pc._schedule(op.ctx, mode, now[mode]),
                               pc._schedule(op.ctx, mode, then[mode])),
                    pc._change(pc.RUN_POLICY.format(mode), ["update"],
                               pc._policy(op.ctx, mode, now[mode]),
                               pc._policy(op.ctx, mode, then[mode]))]
    variables = {name: {"value": value} for name, value in rb.tfvars.items()}
    return {"variables": variables, "resource_changes": changes}


def _rollback_tofu(op, rb, plan):
    def run(args, **_):
        if args[1] == "apply":
            op.boundary.serial += 1
            op.boundary.context = rb.prior
            op.boundary.revisions = {m: rb.prior.canary["revisions"][m] for m in pc.MODES}
            op.stray(op.boundary)
        out = {"show": json.dumps(plan).encode(), "apply": b"rollback apply"}.get(args[1], b"plan")
        return subprocess.CompletedProcess(args, 0, out, b"")
    return run


def _rollback_approval(op, **override):
    binary, audit = (op.native.directory / f"rollback.{ext}" for ext in ("tfplan", "json"))
    binary.write_bytes(b"reviewed rollback")
    audit.write_text(json.dumps({
        "decision": "ACCEPT", "phase": "rollback", "checkpoint": op.native.checkpoint()[0],
        "plan_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "source_sha": op.close["common"]["source_sha"], **override}))
    return {"plan_path": str(binary), "audit_path": str(audit),
            "audit_sha256": hashlib.sha256(audit.read_bytes()).hexdigest()}


def _swap(plan, address, side, value):
    pc._at(plan, address)[side] = value


def test_rollback_restaura_as_tres_rotas_e_grants_juntos(phase_c):
    current, evidence_b = phase_c.context, phase_c.phase_b
    if current.phase_a["active_before"] is None:
        with pytest.raises(ValueError, match="rollback=no_prior"):
            rollback.rollback_context(current, current, evidence_b)
        return
    pc._through(phase_c)
    prior = _prior_context(current)
    old, unit = prior.canary["revisions"], current.canary["revisions"]["unit"]
    rb = rollback.rollback_context(prior, current, evidence_b)
    assert rb.tfvars == {
        "processor_active_unit_task_definition_arn": old["unit"],
        "processor_retained_unit_task_definition_arns": sorted([old["unit"], unit]),
        "scheduler_recovery_task_definition_arn": old["recovery"],
        "scheduler_audit_task_definition_arn": old["audit"]}
    other = {**old, "recovery": old["recovery"].replace(":1", ":9")}
    for reason, args in (
        ("rollback=prior_unit", (replace(prior, canary={**prior.canary, "revisions": {
            **old, "unit": unit}}), current, evidence_b)),
        ("rollback=prior_schedulers", (replace(prior, canary={**prior.canary, "revisions": other}),
                                       current, evidence_b)),
        ("rollback=phase_b_binding", (prior, current, {**evidence_b, "common": {
            **evidence_b["common"], "activation_sha256": "0" * 64}}))):
        with pytest.raises(ValueError, match=reason):
            rollback.rollback_context(*args)
    plan = _rollback_plan(phase_c, rb)
    variables = rollback.VARIABLES
    pc._rejects(rollback.review_rollback_plan, plan, rb, [
        ("rollback=delta", lambda bad: bad["resource_changes"].pop()),
        ("rollback=delta", lambda bad: _swap(bad, SFN_POLICY, "actions", ["update"])),
        ("delta=task_revision", lambda bad: pc._foreign(bad, pc.TASK)),
        ("rollback=machine", lambda bad: _swap(bad, MACHINE_AT, "after", current.machine)),
        ("rollback=machine", lambda bad: _swap(bad, MACHINE_AT, "before", prior.machine)),
        ("rollback=schedule", lambda bad: pc._ecs(bad, "audit").update(task_definition_arn=unit)),
        ("rollback=schedule", lambda bad: pc._after(bad).update(state="DISABLED")),
        ("rollback=run_policy", lambda bad: _swap(bad, pc.RUN_POLICY.format("audit"), "after",
                                                  pc._policy(phase_c.ctx, "audit", unit))),
        ("rollback=variables", lambda bad: bad["variables"][variables["unit"]].update(value=unit)),
        ("rollback=variables", lambda bad: bad["variables"][rollback.RETAINED].update(
            value=[old["unit"]]))])
    run = _rollback_tofu(phase_c, rb, plan)
    phase_c.mp.setattr("scripts.promotion.verify_routes.subprocess.run", run)
    serial, native, close = phase_c.boundary.serial, phase_c.native, phase_c.close
    stale = {**close, "common": {**close["common"], "activation_sha256": "0" * 64}}
    with pytest.raises(ValueError, match="rollback=activation_binding"):
        rollback.execute_reviewed_rollback(rb, native, _rollback_approval(phase_c), stale)
    foreign = _rollback_approval(phase_c, phase="x")
    with pytest.raises(ValueError, match="rollback=exact_independent_audit_required"):
        rollback.execute_reviewed_rollback(rb, native, foreign, close)
    pc._permit(phase_c)
    with pytest.raises(ValueError, match="fence=permit_in_flight"):
        rollback.execute_reviewed_rollback(rb, native, _rollback_approval(phase_c), close)
    assert phase_c.boundary.serial == serial
    held = phase_c.adapter.observe_environment_gate("prod").permit
    released = ReleaseUnitPermit("prod", held.token, datetime.now(UTC))
    assert phase_c.adapter.release_unit_permit(released)
    audit = current.canary["revisions"]["audit"]
    def stray(boundary):
        boundary.revisions.update(audit=audit)
        phase_c.stray = lambda _: None

    phase_c.stray = stray
    with pytest.raises(ValueError, match="rollback=routes"):
        rollback.execute_reviewed_rollback(rb, native, _rollback_approval(phase_c), close)
    evidence = rollback.execute_reviewed_rollback(rb, native, _rollback_approval(phase_c), close)
    assert evidence["routes"] == old
    assert evidence["kind"] == "rollback-processor"
    assert evidence["prior_activation_sha256"] == hashlib.sha256(prior.activation).hexdigest()
    assert phase_c.boundary.serial == serial + 2
    assert phase_c.adapter.observe_environment_gate("prod").fence.state.value == "CLOSED"


def test_restore_de_api_e_estatico_nao_espera_convergencia_do_processor():
    port = FakeStaticPort()
    _, inventory, publication = st._published(port)
    target = rollback.ApiStaticRollback(st.CALLER, st.READY, StaticChannel(port, st.TARGET),
                                        inventory, publication)
    journal, seen = [], []

    def converge():
        seen.append(len(port.calls))
        raise ValueError("processor=pending")

    handoff = st.handoff("recovery_required", st.decision())
    with pytest.raises(ValueError, match="processor=pending"):
        rollback.rollback_release(
            lambda: rollback.restore_api_static(handoff, target, port.now), converge,
            journal.append)
    restored = journal[0]["api_static"]
    assert (journal[0]["processor"], restored["host_action"]) == ("converging", "rollback-previous")
    assert restored["fence"] == "CLOSED"
    assert [key for key, _ in restored["restored"]] == ["theme-init.js", "index.html"]
    assert seen == [len(port.calls)]
    for key in st.ROOT:
        assert port.body(key) == st.release_files("v0")[key]
    assert len(journal) == 1
    before = copy.deepcopy(port.calls)
    for raw in (st.handoff("rolled_back"), None):
        with pytest.raises(ValueError, match="rollback=host_step"):
            rollback.restore_api_static(raw, target, port.now)
    assert port.calls == before
