"""Criação e readback estritos do resource server por processo offline."""

from __future__ import annotations

import json
import os
import stat
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._durability import assert_durable
from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._service_cognito_root_testing import root_lab
from tests.integration.aws_capability._service_read_assertions import (
    assert_environment,
    assert_rejected,
)
from tests.integration.aws_capability.contracts import (
    Limits,
    RequestContext,
    Resource,
    ServiceFailure,
)

if TYPE_CHECKING:
    from tests.integration.aws_capability._service_cognito_root_testing import RootLab

__all__ = ["root_lab"]

FIXTURE = Path(__file__).parent / "fixtures/cli/b3/cognito_server_create.json"
DATA = json.loads(FIXTURE.read_text())
ROOT, CHILD = Resource(**DATA["root"]), Resource(**DATA["child"])
BOUNDARIES: dict[Path, dict[str, Any]] = {}


@pytest.fixture
def server_lab(root_lab: RootLab) -> RootLab:
    root_lab.services = replace(
        root_lab.services, spec=replace(root_lab.services.spec, probe="cognito-resource-server")
    )
    root_lab.context.budget.limits = Limits(512, 8, 2, 240, 256)
    policy = replace(root_lab.ports.policy, limits=Limits(512, 8, 2, 240, 256))
    root_lab.ports = replace(root_lab.ports, services=root_lab.services, policy=policy)
    _watch(root_lab)
    return root_lab


def _method(lab: RootLab, name: str) -> Any:
    method = getattr(lab.services, name, None)
    assert callable(method), f"interface=AwsServices.{name}_absent"
    return method


def _ready(lab: RootLab) -> None:
    assert "cognito-resource-server" in PROFILES, "interface=resource_server_profile_absent"
    _method(lab, "create_child")
    _method(lab, "lookup_server_exact")


def _start(lab: RootLab, remote: dict[str, Any] | None = None) -> None:
    state = BOUNDARIES[lab.directory]
    state.update(
        active=True, failed=False, remote=remote, proof=[], attempts=[], checks=[], saved=[]
    )


def _watch(lab: RootLab) -> None:
    recorded, replaced = subprocess.run, os.replace
    state: dict[str, Any] = {"active": False, "failed": False, "proof": [], "attempts": []}
    BOUNDARIES[lab.directory] = state
    operations = {v["operation"]: k for k, v in DATA["requests"].items()}

    def run(argv: list[str], **options: Any) -> Any:
        name = operations[argv[2]]
        if name in {"R", "C", "U", "X", "P"}:
            state["attempts"] += [name] if state["failed"] else []
            assert not state["failed"], "oracle=mutation_after_failed_save"
            state["proof"].clear()
        result = recorded(argv, **options)
        reply = [result.returncode, result.stdout, result.stderr]
        state["proof"].append(name if reply in DATA["proof_replies"].get(name, []) else "!")
        return result

    def replace_file(source: Any, target: Any) -> None:
        raw = Path(source).read_bytes()
        _saving(lab, raw)
        replaced(source, target)
        state.setdefault("saved", []).append(json.loads(raw))
        remote = state.get("remote")
        if state["active"] and remote is not None and DATA["child"] in json.loads(raw)["created"]:
            remote["requires_retirement"] = True

    lab.monkeypatch.setattr(subprocess, "run", run)
    lab.monkeypatch.setattr(os, "replace", replace_file)


def _saving(lab: RootLab, raw: bytes) -> None:
    state, data = BOUNDARIES[lab.directory], json.loads(raw)
    if not state["active"] or state.get("remote") is None:
        return
    if data["created"] == [DATA["root"]] and state["remote"]["requires_retirement"]:
        _retirement_ready(state, data)


def _retirement_ready(state: dict[str, Any], data: dict[str, Any]) -> None:
    assert (state["remote"]["parent"], data) == (True, DATA["states"]["retired"])
    tokens = state["proof"]
    absence = [i + 3 for i in range(len(tokens) - 2) if tokens[i : i + 3] == ["D", "T", "S"]]
    assert absence, "oracle=retirement_without_owned_absence"
    inventory = ["L", "D", "T", "LDDB", "LS3", "LIAM"]
    assert any(
        tokens[j : j + 6] == inventory for i in absence for j in range(i, len(tokens) - 5)
    ), "oracle=retirement_without_fresh_inventory"
    state["checks"].append(list(tokens))


def _seed(lab: RootLab, name: str) -> bytes:
    path = lab.directory / "a1b2c3d4.json"
    raw = json.dumps(DATA["states"][name]).encode()
    temporary = lab.directory / ".r3-seed"
    with temporary.open("wb") as handle:
        temporary.chmod(0o600)
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    descriptor = os.open(lab.directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return raw


def _disk(lab: RootLab, name: str) -> Journal:
    fresh = Journal(lab.services.spec)
    fresh.load()
    assert json.loads(fresh.path.read_bytes()) == DATA["states"][name]
    assert json.loads(fresh.content()) == DATA["states"][name]
    return fresh


def _queue(lab: RootLab, aliases: list[str]) -> None:
    for alias in aliases:
        reply = DATA["replies"][alias]
        if "exception" in reply:
            outcome = subprocess.TimeoutExpired("aws", 7, output="", stderr="")
        elif "exit" in reply:
            outcome = (reply["exit"], reply["stdout"], reply["stderr"])
        else:
            outcome = (0, json.dumps(reply), "")
        lab.process.outcomes.append(outcome)


def _request(argv: list[str], options: dict[str, Any], name: str) -> None:
    expected = DATA["requests"][name]
    assert argv == expected["argv"]
    assert json.loads(options["input"]) == expected["stdin"]
    assert_environment(options["env"], expected["region"])
    assert options["timeout"] == 7
    assert options["shell"] is options["check"] is False
    assert options["capture_output"] is options["text"] is True


def _trace(lab: RootLab, aliases: list[str], lanes: list[str]) -> None:
    assert len(lab.process.calls) == len(aliases), "oracle=physical_trace"
    for (argv, options), alias in zip(lab.process.calls, aliases, strict=True):
        _request(argv, options, alias.split("_")[0])
    assert lab.lanes == lanes
    assert lab.process.observed_debits == list(range(1, len(aliases) + 1))
    assert lab.context.budget.used == len(aliases)
    assert lab.context.budget.work_used == lanes.count("work")
    assert lab.process.outcomes == []


def _control(lab: RootLab) -> None:
    create = _method(lab, "create_child")
    _queue(lab, ["D", "T", "C", "S"])
    assert create(ROOT, "cognito.server", lab.context) == CHILD
    _trace(lab, ["D", "T", "C", "S"], ["work"] * 4)


def _rejected(lab: RootLab, aliases: list[str], changes: dict[str, Any] | None = None) -> None:
    _control(lab)
    before = _seed(lab, "child_unknown")
    values = {"parent": ROOT, "kind": "cognito.server", "context": lab.context, **(changes or {})}
    if "spec" in values:
        lab.services = replace(lab.services, spec=values["spec"])
    _queue(lab, aliases)
    with pytest.raises(ServiceFailure) as captured:
        _method(lab, "create_child")(values["parent"], values["kind"], values["context"])
    assert_rejected(captured.value)
    assert (lab.directory / "a1b2c3d4.json").read_bytes() == before
    _trace(lab, ["D", "T", "C", "S", *aliases], ["work"] * (4 + len(aliases)))


def _probe_rejected(lab: RootLab, changes: dict[str, Any]) -> None:
    _control(lab)
    _queue(lab, ["D", "T", "S"])
    observed = lab.services.probe([ROOT, CHILD], lab.services.spec, lab.context)
    assert observed == {"resource_server_lookup": "exact_present"}
    values = {"resources": [ROOT, CHILD], "spec": lab.services.spec, "context": lab.context}
    values.update(changes)
    with pytest.raises(ServiceFailure) as captured:
        lab.services.probe(values["resources"], values["spec"], values["context"])
    assert_rejected(captured.value)
    _trace(lab, ["D", "T", "C", "S", "D", "T", "S"], ["work"] * 7)


def _fault(lab: RootLab, state: str, boundary: str, interrupt: bool = False) -> list[bool]:
    synced, replaced = os.fsync, os.replace
    fired = [False]

    def fail(raw: bytes) -> None:
        _saving(lab, raw)
        if not fired[0] and json.loads(raw) == DATA["states"][state]:
            fired[0] = True
            BOUNDARIES[lab.directory]["failed"] = True
            if interrupt:
                raise KeyboardInterrupt
            raise OSError("native=r3_journal_fault")

    def sync(fd: int) -> None:
        directory = stat.S_ISDIR(os.fstat(fd).st_mode)
        if boundary == "file" and not directory:
            fail(Path(os.readlink(f"/proc/self/fd/{fd}")).read_bytes())
        synced(fd)
        if boundary == "directory" and directory:
            fail((lab.directory / "a1b2c3d4.json").read_bytes())

    def replace_file(source: Any, target: Any) -> None:
        if boundary == "replace":
            fail(Path(source).read_bytes())
        replaced(source, target)

    lab.monkeypatch.setattr(os, "fsync", sync)
    lab.monkeypatch.setattr(os, "replace", replace_file)
    return fired


def _delete_boundary(lab: RootLab, remote: dict[str, Any], request: str) -> None:
    saved = json.loads((lab.directory / "a1b2c3d4.json").read_bytes())
    assert saved["operation_state"] != "unknown", "oracle=delete_before_resolution"
    assert "pending_service" not in saved
    assert "pending_resource" not in saved
    assert_durable(lab.durability, lab.directory / "a1b2c3d4.json")
    if request == "P":
        assert saved["created"] == [DATA["root"]], "oracle=parent_before_retirement"
        assert remote["child"] is False
        if remote["requires_retirement"]:
            assert saved["phase"] == "cleanup_requested"
        elif remote["requires_rejection"]:
            assert DATA["states"]["rejected"] in BOUNDARIES[lab.directory]["saved"]
        remote["parent"] = False
    else:
        assert remote["requires_retirement"], "oracle=delete_unadopted_child"
        remote["child"] = False


def _cleanup_reply(lab: RootLab, remote: dict[str, Any], request: str) -> str:
    if request in {"X", "P"}:
        _delete_boundary(lab, remote, request)
        return request
    if request == "D":
        return "D_missing" if not remote["parent"] else remote.get("D", "D_inactive")
    if request == "S":
        assert remote["parent"], "oracle=child_lookup_after_parent_delete"
        return remote.get("S", "S" if remote["child"] else "S_missing")
    if request == "L":
        return "L" if remote["parent"] else "L_empty"
    if request == "LS":
        return "LS" if remote["child"] else "LS_empty"
    return remote.get(request, request)


def _run_cleanup(lab: RootLab, remote: dict[str, Any]) -> tuple[Any, Cleanup, list[str]]:
    fresh = Journal(lab.services.spec)
    fresh.load()
    remote["requires_retirement"] = CHILD in fresh.created
    remote["requires_rejection"] = "pending_resource" in json.loads(fresh.path.read_bytes())
    _start(lab, remote)
    cleanup = Cleanup(fresh, lab.ports)
    names: list[str] = []
    start = len(lab.process.calls)
    operations = {v["operation"]: k for k, v in DATA["requests"].items()}

    def reply(argv: list[str], options: dict[str, Any]) -> None:
        name = operations[argv[2]]
        _request(argv, options, name)
        names.append(name)
        _queue(lab, [_cleanup_reply(lab, remote, name)])

    lab.at_request = reply
    try:
        outcome = cleanup.execute()
    finally:
        lab.at_request = None
        BOUNDARIES[lab.directory]["active"] = False
        assert BOUNDARIES[lab.directory]["attempts"] == [], "oracle=mutation_after_failed_save"
    assert lab.lanes[start:] == ["cleanup"] * len(names)
    assert not set(names) & {"R", "C"}, "oracle=recovery_recreated"
    assert lab.process.outcomes == []
    assert lab.process.observed_debits == list(range(1, len(lab.process.calls) + 1))
    return outcome, cleanup, names


def _complete(lab: RootLab, present: bool) -> tuple[Any, Any, list[str]]:
    initial = json.loads((lab.directory / "a1b2c3d4.json").read_bytes())
    remote = {"parent": True, "child": present}
    outcome, cleanup, names = _run_cleanup(lab, remote)
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    _disk(lab, "complete")
    saves = BOUNDARIES[lab.directory]["saved"]
    assert DATA["states"]["cleanup_empty"] in saves
    if remote["requires_retirement"]:
        assert DATA["states"]["retired"] in saves
    elif "pending_resource" in initial:
        assert DATA["states"]["rejected"] in saves
        assert "X" not in names
    return outcome, cleanup, names


def _strict_control(lab: RootLab) -> None:
    _ready(lab)
    _queue(lab, ["D", "T", "S"])
    assert (
        _method(lab, "lookup_server_exact")(CHILD, RequestContext(lab.context.budget, "cleanup"))
        == "exact_present"
    )
    _trace(lab, ["D", "T", "S"], ["cleanup"] * 3)


def _unsafe(lab: RootLab, aliases: list[str], result: str) -> None:
    _strict_control(lab)
    _queue(lab, aliases)
    assert (
        _method(lab, "lookup_server_exact")(CHILD, RequestContext(lab.context.budget, "cleanup"))
        == result
    )
    _trace(lab, ["D", "T", "S", *aliases], ["cleanup"] * (3 + len(aliases)))
    before = _seed(lab, "child_unknown")
    override = {a.split("_")[0]: a for a in aliases if "_" in a}
    outcome, _, names = _run_cleanup(lab, {"parent": True, "child": True, **override})
    assert outcome.status != "succeeded"
    assert names[: len(aliases)] == [a.split("_")[0] for a in aliases]
    assert (lab.directory / "a1b2c3d4.json").read_bytes() == before
    assert not set(names) & {"X", "P"}
    _disk(lab, "child_unknown")


def test_a01_rejeita_cleanup(server_lab: RootLab) -> None:
    _rejected(server_lab, [], {"context": RequestContext(server_lab.context.budget, "cleanup")})


def test_a02_rejeita_kind(server_lab: RootLab) -> None:
    _rejected(server_lab, [], {"kind": "cognito.client"})


def test_a03_rejeita_perfil(server_lab: RootLab) -> None:
    _rejected(server_lab, [], {"spec": replace(server_lab.services.spec, probe="cognito-login")})


def test_a04_rejeita_bypass(server_lab: RootLab) -> None:
    _rejected(
        server_lab, [], {"spec": replace(server_lab.services.spec, allow_governance_bypass=False)}
    )


def test_a05_rejeita_servico(server_lab: RootLab) -> None:
    _rejected(server_lab, [], {"parent": replace(ROOT, service="s3")})


def test_a06_rejeita_nome(server_lab: RootLab) -> None:
    _rejected(server_lab, [], {"parent": replace(ROOT, name="cnesdata-cap-e5f6a7b8")})


def test_a07_rejeita_regiao(server_lab: RootLab) -> None:
    _rejected(server_lab, [], {"parent": replace(ROOT, resource_id="us-west-2_Abc123")})


def test_a08_rejeita_filho_como_root(server_lab: RootLab) -> None:
    _rejected(server_lab, [], {"parent": CHILD})


def test_b01_bloqueia_pool_divergente(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D_pool"])


def test_b02_bloqueia_conta_divergente(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D_account"])


def test_b03_bloqueia_regiao_divergente(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D_region"])


def test_b04_bloqueia_tag_run(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T_run"])


def test_b05_bloqueia_tag_purpose(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T_purpose"])


def test_d01_rejeita_wrapper_extra(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C_wrapper"])


def test_d02_rejeita_pool(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C_pool"])


def test_d03_rejeita_identifier(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C_identifier"])


def test_d04_rejeita_nome(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C_name"])


def test_d05_rejeita_scope_nome(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C_scope_name"])


def test_d06_rejeita_scope_descricao(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C_scope_description"])


def test_d07_rejeita_scope_duplo(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C_scope_double"])


def test_d08_rejeita_nome_omitido(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C_name_missing"])


def test_d09_rejeita_campo_extra(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C_extra"])


def test_e01_rejeita_wrapper_extra(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C", "S_wrapper"])


def test_e02_rejeita_pool(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C", "S_pool"])


def test_e03_rejeita_identifier(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C", "S_identifier"])


def test_e04_rejeita_nome(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C", "S_name"])


def test_e05_rejeita_scope_nome(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C", "S_scope_name"])


def test_e06_rejeita_scope_descricao(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C", "S_scope_description"])


def test_e07_rejeita_scope_duplo(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C", "S_scope_double"])


def test_e08_rejeita_nome_omitido(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C", "S_name_missing"])


def test_e09_rejeita_campo_extra(server_lab: RootLab) -> None:
    _rejected(server_lab, ["D", "T", "C", "S_extra"])


def test_k17_rejeita_probe_spec(server_lab: RootLab) -> None:
    _probe_rejected(server_lab, {"spec": replace(server_lab.services.spec, run_id="e5f6a7b8")})


def test_k18_rejeita_probe_sem_filho(server_lab: RootLab) -> None:
    _probe_rejected(server_lab, {"resources": [ROOT]})


def test_k19_rejeita_probe_parent(server_lab: RootLab) -> None:
    foreign = CHILD.resource_id.replace(
        '"parent_id":"us-east-2_Abc123"', '"parent_id":"us-east-2_Other999"'
    )
    _probe_rejected(server_lab, {"resources": [ROOT, replace(CHILD, resource_id=foreign)]})


def test_k20_rejeita_probe_cleanup(server_lab: RootLab) -> None:
    _probe_rejected(server_lab, {"context": RequestContext(server_lab.context.budget, "cleanup")})
