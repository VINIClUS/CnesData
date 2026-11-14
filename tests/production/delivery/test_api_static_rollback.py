"""Rollback API/estático: handoff do host, decisão de recovery e restauração dos entrypoints."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

from scripts.promotion.publish_dashboard import (
    PublicationError,
    StaticChannel,
    StaticTarget,
    UploadDeadlineError,
    UploadWindow,
    publish_release,
    read_candidate,
    take_inventory,
)
from scripts.promotion.rollback_api_static import (
    HostStep,
    after_host,
    host_outcome,
    recovery_window,
    rollback_static,
)
from tests.production.delivery._api_static_fixtures import (
    BUCKET,
    DISTRIBUTION,
    NOW,
    FakeStaticPort,
    baseline,
    dist_tar,
    release_files,
)

TARGET = StaticTarget(BUCKET, DISTRIBUTION)


def _scan(port: FakeStaticPort) -> UploadWindow:
    return UploadWindow(port.now, NOW + timedelta(seconds=180))
ROOT = ("index.html", "theme-init.js")
CALLER = {
    "repository": "VINIClUS/CnesData",
    "source_sha": "a" * 40,
    "ref": "refs/heads/develop",
    "workflow_path": ".github/workflows/promote-production.yml",
    "workflow_sha": "a" * 40,
    "run_id": 123,
    "run_attempt": 1,
}
READY = "c" * 64
# EXT-3 host gate: known terminal failure before the Nginx switch restores the exact prior
# checkpoint (ROLLED_BACK); an uncertain switch/reload/tunnel effect is RECOVERY_REQUIRED.
STEPS = (
    ("pull", "rolled_back"), ("start", "rolled_back"), ("readiness", "rolled_back"),
    ("auth", "rolled_back"), ("aws", "rolled_back"), ("local-smoke", "rolled_back"),
    ("dispatcher-validate", "rolled_back"), ("dispatcher-switch", "recovery_required"),
    ("tunnel-smoke", "recovery_required"),
)


def _utc(seconds: int) -> str:
    return (NOW + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def decision(choice: int = 0, fixed: int = 1500) -> dict:
    deadline = min(fixed, choice + 420)
    return {
        "version": 1, "operation_id": "1" * 64, "binding_sha256": "2" * 64, "generation": 7,
        "choice_at": _utc(choice), "choice_monotonic_ms": 1000, "clock_boot_id": "3" * 64,
        "fixed_deadline": _utc(fixed), "recovery_deadline": _utc(deadline),
        "allocated_seconds": max(0, deadline - choice), "convergence_budget_seconds": 300,
        "finalization_budget_seconds": 60, "uncertainty_budget_seconds": 60,
        "checkpoint_sha256": "4" * 64,
    }


def handoff(status: str, recovery: dict | None = None, **changes) -> bytes:
    value = {
        "version": 1, "kind": "host-handoff", "caller": CALLER, "producer_sha": "5" * 40,
        "closed_ready_sha256": READY, "original_request_sha256": "6" * 64, "status": status,
        "response_sha256": "7" * 64, "proof_sha256": "8" * 64,
        "receipt_sha256": None if status == "recovery_required" else "9" * 64,
        "witness_sha256": None if status == "recovery_required" else "a" * 64,
        "operation_id": "1" * 64, "binding_sha256": "2" * 64, "generation": 7,
        "recovery_decision": recovery, "recovery_proof_sha256": None,
    } | changes
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _published(port: FakeStaticPort):
    old = [(key, port.seed(key, body, NOW - timedelta(days=30)))
           for key, body in sorted(release_files("v0").items())]
    files = {key: body + b"-v1" if key in ROOT else body
             for key, body in release_files("v0").items()}
    candidate = read_candidate(dist_tar(files))
    channel = StaticChannel(port, TARGET)
    inventory = take_inventory(channel, baseline(old, None), candidate, _scan(port))
    port.scope(inventory.decision.upload_order, tuple(r.key for r in inventory.rollback_versions))
    window = UploadWindow(port.now, NOW + timedelta(seconds=180))
    return old, inventory, publish_release(channel, inventory, candidate, window)


def test_rollback_restaura_index_e_theme_init() -> None:
    port = FakeStaticPort()
    old, inventory, publication = _published(port)
    before = len(port.calls)
    window = UploadWindow(port.now, port.now() + timedelta(seconds=300))
    receipt = rollback_static(StaticChannel(port, TARGET), inventory, publication, window)
    calls = port.calls[before:]
    copies = [params for _, _, op, params in calls if op == "CopyObject"]
    assert [p["Key"] for p in copies] == ["theme-init.js", "index.html"]
    assert [p["CopySource"]["VersionId"] for p in copies] == [dict(old)["theme-init.js"],
                                                             dict(old)["index.html"]]
    assert {role for role, *_ in calls} == {"recovery"}
    assert {op for _, _, op, _ in calls} == {"CopyObject", "CreateInvalidation"}
    invalidation = next(p for _, _, op, p in calls if op == "CreateInvalidation")
    assert invalidation["InvalidationBatch"]["Paths"]["Items"] == ["/index.html", "/theme-init.js"]
    for key in ROOT:
        assert port.body(key) == release_files("v0")[key]
    assert dict(receipt.restored)["index.html"] == port.version("index.html")
    assert receipt.residual is False


def test_rollback_sem_entrypoint_escrito_nao_toca_raiz() -> None:
    port = FakeStaticPort()
    _, inventory, _ = _published(port)
    before = len(port.calls)
    window = UploadWindow(port.now, port.now() + timedelta(seconds=300))
    receipt = rollback_static(StaticChannel(port, TARGET), inventory, None, window)
    assert port.calls[before:] == []
    assert (receipt.restored, receipt.residual, receipt.invalidation_id) == ((), False, None)


def test_primeira_publicacao_sem_par_anterior_fica_residual_sem_restaurar() -> None:
    port = FakeStaticPort()
    candidate = read_candidate(dist_tar(release_files("v1")))
    channel = StaticChannel(port, TARGET)
    inventory = take_inventory(channel, baseline(None, None), candidate, _scan(port))
    port.scope(inventory.decision.upload_order, ())
    window = UploadWindow(port.now, NOW + timedelta(seconds=180))
    publication = publish_release(channel, inventory, candidate, window)
    before = len(port.calls)
    receipt = rollback_static(channel, inventory, publication, window)
    assert port.calls[before:] == []
    assert receipt.residual is True


def test_rollback_com_checksum_divergente_falha_sem_invalidar() -> None:
    port = FakeStaticPort()
    _, inventory, publication = _published(port)
    port.objects["theme-init.js"][0].checksum = "AAAA"
    before = len(port.calls)
    window = UploadWindow(port.now, port.now() + timedelta(seconds=300))
    with pytest.raises(PublicationError, match="checksum"):
        rollback_static(StaticChannel(port, TARGET), inventory, publication, window)
    assert [op for _, _, op, _ in port.calls[before:]] == ["CopyObject"]


def test_prazo_vem_da_decisao_original_de_recovery() -> None:
    clock = lambda: NOW + timedelta(seconds=100)  # noqa: E731
    window = recovery_window(decision(choice=60), clock)
    assert window.deadline == NOW + timedelta(seconds=360)
    assert recovery_window(decision(choice=60, fixed=200), clock).deadline == (
        NOW + timedelta(seconds=200))
    forged = decision(choice=60) | {"recovery_deadline": _utc(600)}
    with pytest.raises(PublicationError, match="decision"):
        recovery_window(forged, clock)
    port = FakeStaticPort()
    _, inventory, publication = _published(port)
    late = recovery_window(decision(choice=-400), port.now)
    before = len(port.calls)
    with pytest.raises(UploadDeadlineError):
        rollback_static(StaticChannel(port, TARGET), inventory, publication, late)
    assert port.calls[before:] == []


@pytest.mark.parametrize(("step", "status"), STEPS)
def test_falha_do_candidato_restaura_api_e_libera_lease(step: str, status: str) -> None:
    original = decision() if status == "recovery_required" else None
    outcome = host_outcome(handoff(status, original), caller=CALLER, closed_ready_sha256=READY)
    plan = after_host(outcome)
    assert plan.step is not HostStep.PUBLISH_STATIC, step
    assert plan.fence == "CLOSED"
    if status == "recovery_required":
        assert (plan.step, plan.host_action) == (HostStep.RECOVERY, "rollback-previous")
        assert plan.decision == original
    else:
        assert (plan.step, plan.host_action, plan.decision) == (
            HostStep.FENCED_FAILURE, None, None)


def test_handoff_aceito_libera_so_a_publicacao_estatica() -> None:
    plan = after_host(host_outcome(handoff("accepted"), caller=CALLER, closed_ready_sha256=READY))
    assert (plan.step, plan.fence, plan.decision) == (HostStep.PUBLISH_STATIC, "CLOSED", None)
    assert plan.host_action is None


@pytest.mark.parametrize(
    "raw",
    [
        None,
        handoff("accepted", caller=CALLER | {"run_attempt": 2}),
        handoff("accepted", closed_ready_sha256="d" * 64),
        handoff("recovery_required", None),
        handoff("recovery_required", decision() | {"convergence_budget_seconds": 420}),
        handoff("accepted", extra=True),
        handoff("rejected"),
    ],
)
def test_handoff_ausente_ou_divergente_fica_unknown_e_fechado(raw: bytes | None) -> None:
    plan = after_host(host_outcome(raw, caller=CALLER, closed_ready_sha256=READY))
    assert (plan.step, plan.fence, plan.decision) == (HostStep.UNKNOWN, "CLOSED", None)
