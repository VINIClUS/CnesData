"""Adapter ABI2 da promoção estática e workflow manual com host gate fixado."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts.promotion import promote_api
from scripts.promotion._api_static_handoff import HOST_GATE_SHA
from scripts.promotion.build_host_archive import (
    CALLER_REF,
    HostRelease,
    build_archive,
    host_manifest,
    render_compose,
)
from scripts.promotion.publish_dashboard import PublicationError
from tests.production.delivery._api_static_fixtures import (
    NOW,
    FakeStaticPort,
    baseline,
    dist_tar,
    release_files,
    sha,
)
from tests.production.delivery.test_api_static_rollback import CALLER, READY, decision, handoff

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/promote-production.yml"
SHA = "a" * 40
CANDIDATE_ID = f"candidate-{SHA}-77-1"
ARN = "arn:aws:cloudfront::123456789012:distribution/E2EXAMPLE123"
IMAGE = "ghcr.io/viniclus/cnesdata/central_api@sha256:" + "e" * 64
CONFIG = json.dumps({"AWS_REGION": "us-east-2"}).encode()
ENTRYPOINTS = ("index.html", "theme-init.js")
PLAN = {"evidence_id": "plan-1", "evidence_sha256": "1" * 64, "inventory_sha256": "2" * 64,
        "dependencies_sha256": "3" * 64, "host_preflight_sha256": "4" * 64,
        "host_key_fingerprint": "SHA256:abcdefghijklmnop"}
OWNER = {"repository": "VINIClUS/CnesData", "source_sha": SHA, "run_id": 123, "run_attempt": 1,
         "release_sha256": "5" * 64, "generation": 7, "session_sha256": "6" * 64,
         "owner_sha256": "7" * 64}


def _epoch(seconds: int) -> int:
    return int((NOW + timedelta(seconds=seconds)).timestamp())


PREPARE = {"close": None, "fixed_deadline": None, "input_deadline": _epoch(3600),
           "stage": "application-prepare", "admitted_at": _epoch(0), "deadline": _epoch(180),
           "decision": None, "preparation_deadline": _epoch(180), "sequence": 0,
           "checkpoint_sha256": "8" * 64}
STATIC = PREPARE | {"close": _epoch(0), "fixed_deadline": _epoch(1800), "stage": "static",
                    "deadline": _epoch(300), "sequence": 3}
RECOVERY = STATIC | {"stage": "recovery", "deadline": _epoch(300), "sequence": 4}


@dataclass
class FakeContext:
    port: FakeStaticPort
    inputs: dict[str, bytes]
    observed: list[tuple[str, bytes]] = field(default_factory=list)

    def read(self, name: str) -> bytes:
        return self.inputs[name]

    def guard(self) -> None:
        self.port.guard()

    def observe(self, kind: str, raw: bytes) -> str:
        self.observed.append((kind, raw))
        return sha(raw)

    def call(self, role: str, service: str, operation: str, parameters: dict[str, Any]) -> Any:
        return self.port.call(role, service, operation, parameters)

    def evidence_snapshot(self) -> dict[str, Any]:
        return {"call_receipts_sha256": "e" * 64, "pending": [], "unknown": []}


@pytest.fixture(autouse=True)
def _relogio(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(promote_api, "CLOCK", lambda: NOW)


def _request(operation: str, inputs: dict[str, bytes], stage: dict[str, Any]) -> dict[str, Any]:
    return {"abi_version": 2, "operation": operation, "repository": OWNER["repository"],
            "source_sha": SHA, "owner": OWNER, "checkpoint_sha256": stage["checkpoint_sha256"],
            "activation_sha256": "9" * 64, "stage": stage,
            "inputs": {name: {"sha256": sha(raw), "size": len(raw)}
                       for name, raw in sorted(inputs.items())}}


def _archive(static_release: str = CANDIDATE_ID) -> bytes:
    compose = render_compose(IMAGE, 18081)
    release = HostRelease(SHA, IMAGE, PLAN, "deploy", NOW + timedelta(hours=6), static_release)
    manifest = host_manifest(release, compose, CONFIG, NOW)
    return build_archive(manifest, compose, CONFIG)


def _inventory_inputs(files: dict[str, bytes], **changes: bytes) -> dict[str, bytes]:
    dashboard = dist_tar(files)
    candidate = json.dumps({"schema_version": 1, "repository": OWNER["repository"],
                            "source_sha": SHA, "release_id": CANDIDATE_ID,
                            "dashboard_sha256": sha(dashboard)}).encode()
    values = {"candidate": candidate, "dashboard-artifact": dashboard, "archive": _archive(),
              "static-baseline": baseline(None, None)}
    return values | changes


def _run(port: FakeStaticPort, operation: str, inputs: dict[str, bytes], stage=PREPARE):
    context = FakeContext(port, inputs)
    result = promote_api.execute_shared_operation(operation, _request(operation, inputs, stage),
                                                  context)
    return result, context


def _planned(port: FakeStaticPort, inputs: dict[str, bytes]) -> tuple[bytes, bytes]:
    _, context = _run(port, "inventory", inputs)
    inventory = context.observed[0][1]
    plan = promote_api.static_plan(inventory, ARN)
    record = json.loads(plan)
    port.scope(tuple(record["candidate_keys"]),
               tuple(item["key"] for item in record["rollback_versions"]))
    return inventory, plan


def _publish_inputs(inputs: dict[str, bytes], inventory: bytes, plan: bytes,
                    receipt: bytes) -> dict[str, bytes]:
    return {"candidate": inputs["candidate"], "dashboard-artifact": inputs["dashboard-artifact"],
            "static-plan": plan, "inventory": inventory, "host-receipt": receipt}


def _accepted(**changes: Any) -> bytes:
    return handoff("accepted", None, producer_sha=HOST_GATE_SHA, **changes)


def _seeded(port: FakeStaticPort) -> tuple[dict[str, bytes], list[tuple[str, str]]]:
    old = [(key, port.seed(key, body, NOW - timedelta(days=30)))
           for key, body in sorted(release_files("v0").items())]
    files = {key: body + b"-v1" if key in ENTRYPOINTS else body
             for key, body in release_files("v0").items()}
    return files, old


def test_workflow_so_tem_workflow_dispatch_e_host_outcome_pelo_handoff() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert set(workflow.get("on", workflow.get(True))) == {"workflow_dispatch"}
    assert workflow["permissions"] == {}
    jobs = workflow["jobs"]
    assert list(jobs) == ["preamble", "closed-ready", "host-gate", "host-outcome"]
    assert CALLER_REF in jobs["preamble"]["if"]
    for name, job in jobs.items():
        if name != "host-gate":
            assert job["runs-on"] == "ubuntu-latest"
        for step in job.get("steps", []):
            if "uses" in step:
                assert re.fullmatch(r"[\w/-]+@[0-9a-f]{40}", step["uses"]), step["uses"]
    gate = jobs["host-gate"]
    assert gate["uses"] == ("VINIClUS/infra-ansible/.github/workflows/"
                            f"reusable-cnesdata-host-deploy-gate.yml@{HOST_GATE_SHA}")
    assert set(gate["with"]) == {"plan_evidence_id", "plan_evidence_digest", "plan_run_id",
                                 "artifact_id", "artifact_digest"}
    assert gate["secrets"] == {"host_governance": "${{ secrets.CNESDATA_HOST_GOVERNANCE }}"}
    ready = jobs["closed-ready"]
    assert all("uses" not in step for step in ready["steps"])
    assert "exit 1" in ready["steps"][-1]["run"]
    assert "closed_ready_sha256" in ready["outputs"]
    outcome = jobs["host-outcome"]
    assert "needs.host-gate.result" in outcome["if"]
    [step] = [step for step in outcome["steps"] if "run" in step]
    assert "python -m scripts.promotion._api_static_handoff" in step["run"]
    assert '--closed-ready-sha256 "$CLOSED_READY_SHA256"' in step["run"]
    assert step["env"]["CLOSED_READY_SHA256"] == (
        "${{ needs.closed-ready.outputs.closed_ready_sha256 }}")


@pytest.mark.parametrize("operation", ["close-drain", "closed-ready", "reopen",
                                       "terminal-readback", "retire-static"])
def test_operacao_fora_da_tarefa_e_recusada(operation: str) -> None:
    with pytest.raises(ValueError, match="operation=unsupported"):
        _run(FakeStaticPort(), operation, _inventory_inputs(release_files()))


def test_inventory_registra_observacao_canonica_e_resultado_abi2() -> None:
    result, context = _run(FakeStaticPort(), "inventory", _inventory_inputs(release_files()))
    [(kind, raw)] = context.observed
    record = json.loads(raw)
    assert kind == "inventory"
    assert raw == promote_api.canonical(record)
    assert record["outcome"] == "PLANNED"
    assert record["upload_order"][-1] == "index.html"
    assert result == {"abi_version": 2, "operation": "inventory",
                      "owner_sha256": OWNER["owner_sha256"], "generation": 7,
                      "checkpoint_sha256": PREPARE["checkpoint_sha256"],
                      "observations": [{"kind": "inventory", "sha256": sha(raw),
                                        "size": len(raw)}],
                      "call_receipts_sha256": "e" * 64, "pending": [], "unknown": []}


def test_inventory_recusa_archive_ou_dashboard_de_outra_release() -> None:
    other = _inventory_inputs(release_files(), archive=_archive(f"candidate-{SHA}-78-1"))
    with pytest.raises(PublicationError, match="archive"):
        _run(FakeStaticPort(), "inventory", other)
    swapped = _inventory_inputs(release_files(),
                                **{"dashboard-artifact": dist_tar(release_files("v9"))})
    with pytest.raises(PublicationError, match="dashboard"):
        _run(FakeStaticPort(), "inventory", swapped)


@pytest.mark.parametrize(
    ("campo", "valor", "erro"),
    [("abi_version", 1, "request=fields"), ("operation", "publish-static", "request=operation"),
     ("repository", "outro/repo", "request=source"), ("source_sha", "b" * 40, "request=source")],
)
def test_request_fora_do_abi2_e_recusado(campo: str, valor: Any, erro: str) -> None:
    inputs = _inventory_inputs(release_files())
    request = _request("inventory", inputs, PREPARE) | {campo: valor}
    with pytest.raises(ValueError, match=erro):
        promote_api.execute_shared_operation("inventory", request, FakeContext(FakeStaticPort(),
                                                                               inputs))


@pytest.mark.parametrize(
    "troca", [lambda raw: raw + b"x", lambda raw: raw[:-1] + bytes([raw[-1] ^ 1])],
)
def test_input_com_bytes_divergentes_e_recusado(troca) -> None:
    inputs = _inventory_inputs(release_files())
    request = _request("inventory", inputs, PREPARE)
    context = FakeContext(FakeStaticPort(), inputs | {"archive": troca(inputs["archive"])})
    with pytest.raises(ValueError, match="input=mismatch name=archive"):
        promote_api.execute_shared_operation("inventory", request, context)


def test_static_plan_espelha_campos_do_iam_498() -> None:
    inventory, raw = _planned(FakeStaticPort(), _inventory_inputs(release_files()))
    plan = json.loads(raw)
    assert set(plan) == {"schema", "bucket", "distribution_arn", "inventory_sha256",
                         "candidate_keys", "rollback_versions", "retire_versions",
                         "multipart_keys"}
    assert plan["inventory_sha256"] == sha(inventory)
    assert plan["candidate_keys"]
    assert plan["candidate_keys"] == sorted(plan["candidate_keys"])
    assert (plan["rollback_versions"], plan["retire_versions"]) == ([], [])
    with pytest.raises(PublicationError, match="static_plan=distribution"):
        promote_api.static_plan(inventory, "arn:aws:cloudfront::1:distribution/x")


def test_static_plan_recusa_release_ja_publicada() -> None:
    port = FakeStaticPort()
    files = release_files()
    seeded = [(key, port.seed(key, body, NOW - timedelta(days=30)))
              for key, body in sorted(files.items())]
    inputs = _inventory_inputs(files, **{"static-baseline": baseline(seeded, None)})
    _, context = _run(port, "inventory", inputs)
    with pytest.raises(PublicationError, match="static_plan=candidate_keys"):
        promote_api.static_plan(context.observed[0][1], ARN)


def test_static_plan_recusa_par_de_rollback_incompleto() -> None:
    port = FakeStaticPort()
    files, old = _seeded(port)
    partial = [ref for ref in old if ref[0] != "theme-init.js"]
    inputs = _inventory_inputs(files, **{"static-baseline": baseline(partial, None)})
    _, context = _run(port, "inventory", inputs)
    with pytest.raises(PublicationError, match="static_plan=rollback_versions"):
        promote_api.static_plan(context.observed[0][1], ARN)


def test_publish_static_publica_com_host_accepted_do_proprio_run() -> None:
    port = FakeStaticPort()
    inputs = _inventory_inputs(release_files())
    inventory, plan = _planned(port, inputs)
    publish = _publish_inputs(inputs, inventory, plan, _accepted())
    result, context = _run(port, "publish-static", publish, STATIC)
    [(kind, raw)] = context.observed
    record = json.loads(raw)
    assert kind == "publication"
    assert raw == promote_api.canonical(record)
    assert record["plan_sha256"] == sha(plan)
    assert record["closed_ready_sha256"] == READY
    assert [key for key, _ in record["uploaded"]][-1] == "index.html"
    assert sorted(port.writes()) == json.loads(plan)["candidate_keys"]
    assert result["observations"] == [{"kind": "publication", "sha256": sha(raw),
                                       "size": len(raw)}]


@pytest.mark.parametrize(
    ("receipt", "erro"),
    [(_accepted(caller=CALLER | {"run_id": 999}), "host_receipt"),
     (handoff("accepted", None), "host_receipt"),
     (handoff("rolled_back", None, producer_sha=HOST_GATE_SHA), "host_receipt"),
     (_accepted(closed_ready_sha256="x"), "host_receipt")],
)
def test_publish_static_sem_host_accepted_do_run_nao_escreve(receipt: bytes, erro: str) -> None:
    port = FakeStaticPort()
    inputs = _inventory_inputs(release_files())
    inventory, plan = _planned(port, inputs)
    with pytest.raises(PublicationError, match=erro):
        _run(port, "publish-static", _publish_inputs(inputs, inventory, plan, receipt), STATIC)
    assert port.writes() == []


def test_publish_static_recusa_plano_de_outra_inventory() -> None:
    port = FakeStaticPort()
    inputs = _inventory_inputs(release_files())
    inventory, plan = _planned(port, inputs)
    other = promote_api.canonical(json.loads(inventory) | {"projected_bytes": 1})
    publish = _publish_inputs(inputs, other, plan, _accepted())
    with pytest.raises(PublicationError, match="static_plan=binding"):
        _run(port, "publish-static", publish, STATIC)
    assert port.writes() == []


def test_publish_static_recusa_bucket_que_mudou_desde_a_inventory() -> None:
    port = FakeStaticPort()
    inputs = _inventory_inputs(release_files())
    inventory, plan = _planned(port, inputs)
    port.uploads.append(("assets/novo.js", "u-1", [10]))
    publish = _publish_inputs(inputs, inventory, plan, _accepted())
    with pytest.raises(PublicationError, match="publish=inventory_drift"):
        _run(port, "publish-static", publish, STATIC)
    assert port.writes() == []


def _published(port: FakeStaticPort) -> dict[str, bytes]:
    files, old = _seeded(port)
    inputs = _inventory_inputs(files, **{"static-baseline": baseline(old, None)})
    inventory, plan = _planned(port, inputs)
    _, context = _run(port, "publish-static",
                      _publish_inputs(inputs, inventory, plan, _accepted()), STATIC)
    publication = context.observed[0][1]
    return {"static-plan": plan, "inventory": inventory, "publication": publication}


def test_rollback_restaura_entrypoints_com_decisao_original_do_host() -> None:
    port = FakeStaticPort()
    inputs = _published(port)
    choice = decision()
    receipt = handoff("recovery_required", choice, producer_sha=HOST_GATE_SHA)
    rollback = inputs | {"host-recovery-receipt": receipt,
                         "recovery-decision": promote_api.canonical(choice)}
    _, context = _run(port, "rollback-api-static", rollback, RECOVERY)
    [(kind, raw)] = context.observed
    record = json.loads(raw)
    assert kind == "rollback"
    assert [key for key, _ in record["restored"]] == ["theme-init.js", "index.html"]
    assert record["residual"] is False
    original = release_files("v0")
    assert {key: port.body(key) for key in ENTRYPOINTS} == {key: original[key]
                                                            for key in ENTRYPOINTS}


def test_rollback_recusa_decisao_diferente_da_do_handoff() -> None:
    port = FakeStaticPort()
    inputs = _published(port)
    receipt = handoff("recovery_required", decision(), producer_sha=HOST_GATE_SHA)
    rollback = inputs | {"host-recovery-receipt": receipt,
                         "recovery-decision": promote_api.canonical(decision(choice=5))}
    with pytest.raises(PublicationError, match="host_recovery_receipt"):
        _run(port, "rollback-api-static", rollback, RECOVERY)
    assert port.writes("CopyObject") == []
