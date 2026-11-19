"""Handoff público do host: proveniência do artefato, job, steps, outputs e bytes canônicos."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import logging
import zipfile
from typing import TYPE_CHECKING

import pytest

from scripts.promotion._api_static_handoff import (
    FAIL_STEP,
    GATE_STEP,
    HANDOFF_STEP,
    HOST_GATE_SHA,
    HandoffEvidence,
    authenticated_outcome,
    main,
)
from tests.production.delivery.test_api_static_rollback import CALLER, READY, decision, handoff

if TYPE_CHECKING:
    from pathlib import Path

RUN, ATTEMPT = CALLER["run_id"], CALLER["run_attempt"]
NAME = f"cnesdata-host-handoff-{RUN}-{ATTEMPT}"


def _zip(raw: bytes, name: str = "host-handoff.json") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, raw)
    return buffer.getvalue()


def _raw(status: str, recovery: dict | None = None, **changes) -> bytes:
    return handoff(status, recovery, producer_sha=HOST_GATE_SHA, **changes)


def evidence(status: str = "accepted", raw: bytes | None = None) -> HandoffEvidence:
    recovery = decision() if status == "recovery_required" else None
    raw = raw if raw is not None else _raw(status, recovery)
    payload = _zip(raw)
    failed = status == "recovery_required"
    steps = [
        {"name": GATE_STEP, "number": 4, "status": "completed", "conclusion": "success"},
        {"name": HANDOFF_STEP, "number": 5, "status": "completed", "conclusion": "success"},
        {"name": FAIL_STEP, "number": 6, "status": "completed",
         "conclusion": "failure" if failed else "skipped"},
    ]
    job = {"name": "host-gate / execute", "run_id": RUN, "run_attempt": ATTEMPT,
           "head_sha": CALLER["source_sha"], "status": "completed",
           "conclusion": "failure" if failed else "success",
           "started_at": "2026-11-13T12:00:00Z", "completed_at": "2026-11-13T12:10:00Z",
           "steps": steps}
    artifact = {"id": 55, "name": NAME, "expired": False, "digest": "sha256:" + hashlib.sha256(
        payload).hexdigest(), "created_at": "2026-11-13T12:09:00Z",
        "workflow_run": {"id": RUN, "head_sha": CALLER["source_sha"]}}
    outputs = {"status": "recovery_required" if failed else "accepted",
               "host_evidence_version": "3", "host_evidence_sha256": "7" * 64,
               "platform_evidence_version": "v1", "platform_evidence_sha256": "1" * 64}
    return HandoffEvidence(
        jobs={"total_count": 1, "jobs": [job]},
        artifacts={"total_count": 1, "artifacts": [artifact]},
        archive=payload,
        outputs=outputs,
    )


def _outcome(value: HandoffEvidence):
    return authenticated_outcome(value, caller=CALLER, closed_ready_sha256=READY)


@pytest.mark.parametrize("status", ["accepted", "rolled_back", "recovery_required"])
def test_handoff_autenticado_preserva_status_e_decisao_original(status: str) -> None:
    outcome = _outcome(evidence(status))
    assert outcome.status == status
    assert outcome.decision == (decision() if status == "recovery_required" else None)


def _mutated(path: str, value) -> HandoffEvidence:
    base = evidence("recovery_required")
    jobs, artifacts, outputs = (copy.deepcopy(base.jobs), copy.deepcopy(base.artifacts),
                                dict(base.outputs))
    target = {"job": jobs["jobs"][0], "artifact": artifacts["artifacts"][0], "outputs": outputs,
              "step0": jobs["jobs"][0]["steps"][0], "step1": jobs["jobs"][0]["steps"][1],
              "step2": jobs["jobs"][0]["steps"][2]}
    owner, key = path.split(".")
    target[owner][key] = value
    return HandoffEvidence(jobs=jobs, artifacts=artifacts, archive=base.archive, outputs=outputs)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("job.run_attempt", 2),
        ("job.head_sha", "b" * 40),
        ("job.name", "closed-ready"),
        ("job.status", "in_progress"),
        ("step0.conclusion", "failure"),
        ("step1.conclusion", "failure"),
        ("step2.conclusion", "skipped"),
        ("step1.number", 7),
        ("artifact.name", "cnesdata-host-handoff-123-2"),
        ("artifact.expired", True),
        ("artifact.digest", "sha256:" + "0" * 64),
        ("artifact.created_at", "2026-11-13T12:11:00Z"),
        ("artifact.workflow_run", {"id": 999, "head_sha": "a" * 40}),
        ("outputs.status", "accepted"),
        ("outputs.host_evidence_sha256", "0" * 64),
    ],
)
def test_proveniencia_divergente_fica_unknown(path: str, value) -> None:
    assert _outcome(_mutated(path, value)).status == "unknown"


def test_bytes_nao_canonicos_duplicados_ou_grandes_ficam_unknown() -> None:
    canonical = _raw("accepted")
    spaced = json.dumps(json.loads(canonical), sort_keys=True).encode() + b"\n"
    duplicate = canonical.replace(b'{"binding_sha256"', b'{"status":"accepted","binding_sha256"')
    for raw in (spaced, duplicate, canonical + b" " * 33000):
        assert _outcome(evidence("accepted", raw=raw)).status == "unknown"
    wrong = evidence("accepted")
    renamed = HandoffEvidence(jobs=wrong.jobs, artifacts=wrong.artifacts,
                              archive=_zip(canonical, "other.json"), outputs=wrong.outputs)
    assert _outcome(renamed).status == "unknown"


def test_produtor_fora_do_pin_ou_artefato_ausente_fica_unknown() -> None:
    other = handoff("accepted", producer_sha="f" * 40)
    assert _outcome(evidence("accepted", raw=other)).status == "unknown"
    base = evidence("accepted")
    empty = HandoffEvidence(jobs=base.jobs, artifacts={"total_count": 0, "artifacts": []},
                            archive=b"", outputs=base.outputs)
    assert _outcome(empty).status == "unknown"


def _arguments(tmp_path: Path, value: HandoffEvidence) -> list[str]:
    arguments = ["--closed-ready-sha256", READY]
    for name, content in (("jobs", value.jobs), ("artifacts", value.artifacts),
                          ("outputs", value.outputs), ("caller", CALLER)):
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps(content), encoding="utf-8")
        arguments += [f"--{name}", str(path)]
    archive = tmp_path / "handoff.zip"
    archive.write_bytes(value.archive)
    return [*arguments, "--archive", str(archive)]


@pytest.mark.parametrize(
    ("status", "code", "line"),
    [
        ("accepted", 0, "host_outcome=accepted next=publish-static host_action=none fence=CLOSED"),
        ("rolled_back", 1,
         "host_outcome=rolled_back next=fenced-failure host_action=none fence=CLOSED"),
        ("recovery_required", 1, "host_outcome=recovery_required next=rollback-api-static "
                                 "host_action=rollback-previous fence=CLOSED"),
    ],
)
def test_cli_so_libera_o_estatico_com_host_accepted(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, status: str, code: int, line: str
) -> None:
    with caplog.at_level(logging.INFO):
        assert main(_arguments(tmp_path, evidence(status))) == code
    assert line in caplog.messages


def test_cli_sem_artefato_ou_com_outputs_divergentes_fica_unknown(tmp_path: Path) -> None:
    arguments = _arguments(tmp_path, evidence("accepted"))
    (tmp_path / "handoff.zip").unlink()
    assert main(arguments) == 1
    arguments = _arguments(tmp_path, _mutated("outputs.status", "accepted"))
    assert main(arguments) == 1
