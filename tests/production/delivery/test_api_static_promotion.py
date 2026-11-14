"""Adapter ABI2 da promoção estática e workflow manual com host gate fixado."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pytest

from scripts.promotion import promote_api
from scripts.promotion._api_static_handoff import HOST_GATE_SHA
from scripts.promotion.build_host_archive import (
    HostRelease,
    build_archive,
    host_manifest,
    render_compose,
)
from scripts.promotion.publish_dashboard import PublicationError, UploadDeadlineError
from tests.production.delivery._api_static_fixtures import (
    NOW,
    FakeStaticPort,
    baseline,
    dist_tar,
    release_files,
    sha,
)
from tests.production.delivery.test_api_static_rollback import CALLER, READY, decision, handoff

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


def test_request_com_inputs_fora_do_conjunto_da_operacao_e_recusado() -> None:
    inputs = _inventory_inputs(release_files())
    for changed in (inputs | {"extra": b"x"}, {k: v for k, v in inputs.items() if k != "archive"}):
        request = _request("inventory", changed, PREPARE)
        with pytest.raises(ValueError, match="request=operation"):
            promote_api.execute_shared_operation("inventory", request,
                                                 FakeContext(FakeStaticPort(), changed))


@pytest.mark.parametrize(
    ("operation", "stage"),
    [("inventory", STATIC), ("publish-static", PREPARE),
     ("publish-static", STATIC | {"close": None}), ("rollback-api-static", STATIC)],
)
def test_estagio_fora_da_operacao_e_recusado(operation: str, stage: dict[str, Any]) -> None:
    inputs = dict.fromkeys(sorted(promote_api.INPUTS[operation]), b"x")
    with pytest.raises(ValueError, match="request=stage"):
        promote_api.execute_shared_operation(operation, _request(operation, inputs, stage),
                                             FakeContext(FakeStaticPort(), inputs))


def test_candidato_de_outro_repo_ou_de_outro_sha_e_recusado() -> None:
    files = release_files()
    for changes, erro in (({"repository": "outro/repo"}, "candidate=source"),
                          ({"release_id": f"candidate-{'b' * 40}-77-1"}, "candidate=release")):
        inputs = _inventory_inputs(files)
        candidate = json.loads(inputs["candidate"]) | changes
        with pytest.raises(PublicationError, match=erro):
            _run(FakeStaticPort(), "inventory",
                 inputs | {"candidate": json.dumps(candidate).encode()})


def test_prazo_pre_fence_vem_do_preparation_deadline() -> None:
    port = FakeStaticPort()
    stage = PREPARE | {"preparation_deadline": _epoch(10), "deadline": _epoch(3000)}
    with pytest.raises(UploadDeadlineError, match="deadline="):
        _run(port, "inventory", _inventory_inputs(release_files()), stage)
    assert port.calls == []


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
     (_accepted(caller=CALLER | {"source_sha": "b" * 40}), "host_receipt"),
     (_accepted(caller=CALLER | {"ref": "refs/heads/main"}), "host_receipt"),
     (_accepted(caller=CALLER | {"workflow_path": ".github/workflows/x.yml"}), "host_receipt"),
     (_accepted(caller=CALLER | {"workflow_sha": "b" * 40}), "host_receipt"),
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


def test_publish_static_recusa_dashboard_diferente_do_candidato() -> None:
    port = FakeStaticPort()
    inputs = _inventory_inputs(release_files())
    inventory, plan = _planned(port, inputs)
    publish = _publish_inputs(inputs, inventory, plan, _accepted())
    publish["dashboard-artifact"] = dist_tar(release_files("v9"))
    with pytest.raises(PublicationError, match="candidate=dashboard"):
        _run(port, "publish-static", publish, STATIC)
    assert port.writes() == []


def test_prazo_do_estagio_static_vem_do_deadline_e_nao_do_preparation() -> None:
    port = FakeStaticPort()
    inputs = _inventory_inputs(release_files())
    inventory, plan = _planned(port, inputs)
    stage = STATIC | {"deadline": _epoch(10), "preparation_deadline": _epoch(3000)}
    with pytest.raises(UploadDeadlineError, match="deadline="):
        _run(port, "publish-static", _publish_inputs(inputs, inventory, plan, _accepted()), stage)
    assert port.writes() == []


def _recovery(inputs: dict[str, bytes], choice: dict[str, Any], **changes: Any) -> dict[str, bytes]:
    receipt = handoff("recovery_required", choice, producer_sha=HOST_GATE_SHA, **changes)
    return inputs | {"host-recovery-receipt": receipt,
                     "recovery-decision": promote_api.canonical(choice)}


@pytest.mark.parametrize(
    ("choice", "stage"),
    [(decision(), RECOVERY | {"deadline": _epoch(10)}), (decision(choice=-290), RECOVERY)],
)
def test_rollback_respeita_o_menor_prazo_entre_decisao_e_estagio(
    choice: dict[str, Any], stage: dict[str, Any],
) -> None:
    port = FakeStaticPort()
    inputs = _published(port)
    with pytest.raises(UploadDeadlineError, match="deadline="):
        _run(port, "rollback-api-static", _recovery(inputs, choice), stage)
    assert port.writes("CopyObject") == []


def test_rollback_com_host_rolled_back_nao_restaura() -> None:
    port = FakeStaticPort()
    inputs = _published(port)
    receipt = handoff("rolled_back", None, producer_sha=HOST_GATE_SHA)
    rollback = inputs | {"host-recovery-receipt": receipt,
                         "recovery-decision": promote_api.canonical(decision())}
    with pytest.raises(PublicationError, match="host_recovery_receipt=decision"):
        _run(port, "rollback-api-static", rollback, RECOVERY)
    assert port.writes("CopyObject") == []


def test_rollback_recusa_publicacao_de_outro_plano_ou_incompleta() -> None:
    port = FakeStaticPort()
    inputs = _published(port)
    record = json.loads(inputs["publication"])
    cases = ((record | {"plan_sha256": "f" * 64}, "publication=binding"),
             ({k: v for k, v in record.items() if k != "uploaded"}, "publication=fields"))
    for changed, erro in cases:
        rollback = _recovery(inputs | {"publication": promote_api.canonical(changed)}, decision())
        with pytest.raises(PublicationError, match=erro):
            _run(port, "rollback-api-static", rollback, RECOVERY)
    assert port.writes("CopyObject") == []


def test_rollback_exige_o_mesmo_closed_ready_da_publicacao() -> None:
    port = FakeStaticPort()
    inputs = _published(port)
    rollback = _recovery(inputs, decision(), closed_ready_sha256="d" * 64)
    with pytest.raises(PublicationError, match="host_recovery_receipt=closed_ready"):
        _run(port, "rollback-api-static", rollback, RECOVERY)
    assert port.writes("CopyObject") == []
