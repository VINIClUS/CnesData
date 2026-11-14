"""Consumidor autenticado do handoff público do host gate EXT-3 (R2a)."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import stat
import sys
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scripts.promotion.rollback_api_static import (
    HostOutcome,
    HostStep,
    after_host,
    host_outcome,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

HOST_GATE_SHA = "1a2a78898211317885b216c16f7a9ec585f98173"
HOST_JOB = "host-gate / execute"
GATE_STEP = "Repeat source gate before OIDC and exchange exactly three SSH frames"
HANDOFF_STEP = "Publish only the closed public handoff projection"
FAIL_STEP = "Fail only after the original recovery handoff is available"
MEMBER = "host-handoff.json"
MEMBER_LIMIT = 32768
ZIP_LIMIT = 65536
_UNKNOWN = HostOutcome("unknown", None)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HandoffEvidence:
    jobs: Mapping[str, Any]
    artifacts: Mapping[str, Any]
    archive: bytes
    outputs: Mapping[str, str]


def authenticated_outcome(
    evidence: HandoffEvidence, *, caller: Mapping[str, Any], closed_ready_sha256: str
) -> HostOutcome:
    """Returns:
    Status e decisão originais; qualquer prova ausente ou divergente vira unknown.
    """
    try:
        job = _host_job(evidence.jobs, caller)
        raw = _member(evidence, caller, job)
    except (KeyError, TypeError, ValueError, zipfile.BadZipFile):
        return _UNKNOWN
    value = _canonical_value(raw)
    if value is None or value.get("producer_sha") != HOST_GATE_SHA:
        return _UNKNOWN
    outcome = host_outcome(raw, caller=caller, closed_ready_sha256=closed_ready_sha256)
    if outcome.status == "unknown" or not _outputs_agree(evidence.outputs, value, job):
        return _UNKNOWN
    return outcome


def main(argv: list[str] | None = None) -> int:
    """Returns:
    0 só com host accepted (próximo passo publish-static); 1 para qualquer outro desfecho.
    """
    parser = argparse.ArgumentParser(description="Consumidor do handoff público do host gate.")
    for name in ("jobs", "artifacts", "outputs", "caller", "archive"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--closed-ready-sha256", required=True)
    arguments = parser.parse_args(argv)
    try:
        documents = [json.loads(getattr(arguments, name).read_text(encoding="utf-8"))
                     for name in ("jobs", "artifacts", "outputs", "caller")]
        archive = arguments.archive.read_bytes()
    except (OSError, ValueError):
        documents, archive = None, b""
    outcome = _UNKNOWN
    if documents is not None:
        jobs, artifacts, outputs, caller = documents
        evidence = HandoffEvidence(jobs, artifacts, archive, outputs)
        outcome = authenticated_outcome(evidence, caller=caller,
                                        closed_ready_sha256=arguments.closed_ready_sha256)
    plan = after_host(outcome)
    logger.info("host_outcome=%s next=%s host_action=%s fence=%s", outcome.status,
                plan.step.value, plan.host_action or "none", plan.fence)
    return 0 if plan.step is HostStep.PUBLISH_STATIC else 1


def _host_job(jobs: Mapping[str, Any], caller: Mapping[str, Any]) -> dict[str, Any]:
    listed = jobs["jobs"]
    found = [job for job in listed if job["name"] == HOST_JOB]
    if jobs["total_count"] != len(listed) or len(listed) > 100 or len(found) != 1:
        raise ValueError("handoff=job")
    job = found[0]
    if (
        job["run_id"] != caller["run_id"]
        or job["run_attempt"] != caller["run_attempt"]
        or job["head_sha"] != caller["source_sha"]
        or job["status"] != "completed"
        or job["conclusion"] not in {"success", "failure"}
    ):
        raise ValueError("handoff=job_identity")
    _require_steps(job)
    return job


def _require_steps(job: Mapping[str, Any]) -> None:
    steps = {step["name"]: step for step in job["steps"]}
    gate, handoff, final = (steps[name] for name in (GATE_STEP, HANDOFF_STEP, FAIL_STEP))
    if not gate["number"] < handoff["number"] < final["number"]:
        raise ValueError("handoff=step_order")
    if (gate["conclusion"], handoff["conclusion"]) != ("success", "success"):
        raise ValueError("handoff=step_outcome")
    expected = "failure" if job["conclusion"] == "failure" else "skipped"
    if final["conclusion"] != expected:
        raise ValueError("handoff=final_step")


def _member(evidence: HandoffEvidence, caller: Mapping[str, Any], job: Mapping[str, Any]) -> bytes:
    name = f"cnesdata-host-handoff-{caller['run_id']}-{caller['run_attempt']}"
    listing = evidence.artifacts["artifacts"]
    if evidence.artifacts["total_count"] != len(listing) or len(listing) != 1:
        raise ValueError("handoff=artifact")
    meta = listing[0]
    if (
        meta["name"] != name
        or meta["expired"] is not False
        or meta["workflow_run"]["id"] != caller["run_id"]
        or meta["workflow_run"]["head_sha"] != caller["source_sha"]
        or not _instant(job["started_at"]) < _instant(meta["created_at"])
        <= _instant(job["completed_at"])
    ):
        raise ValueError("handoff=artifact_identity")
    archive = evidence.archive
    if len(archive) > ZIP_LIMIT or meta["digest"] != "sha256:" + hashlib.sha256(archive).hexdigest():
        raise ValueError("handoff=digest")
    with zipfile.ZipFile(io.BytesIO(archive)) as packed:
        members = packed.infolist()
        if len(members) != 1 or members[0].filename != MEMBER or not _regular(members[0]):
            raise ValueError("handoff=member")
        with packed.open(members[0]) as stream:
            raw = stream.read(MEMBER_LIMIT + 1)
    if not 0 < len(raw) <= MEMBER_LIMIT:
        raise ValueError("handoff=size")
    return raw


def _regular(member: zipfile.ZipInfo) -> bool:
    mode = stat.S_IFMT(member.external_attr >> 16)
    return not member.is_dir() and mode in {0, stat.S_IFREG} and not member.flag_bits & 1


def _canonical_value(raw: bytes) -> dict[str, Any] | None:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate")
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
    except (UnicodeDecodeError, ValueError):
        return None
    text = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    if not isinstance(value, dict) or (text + "\n").encode("utf-8") != raw:
        return None
    return value


def _outputs_agree(outputs: Mapping[str, str], value: Mapping[str, Any], job: Mapping[str, Any]) -> bool:
    recovery = value["status"] == "recovery_required"
    return (
        outputs.get("status") == ("recovery_required" if recovery else "accepted")
        and outputs.get("host_evidence_sha256") == value["response_sha256"]
        and (job["conclusion"] == "failure") == recovery
    )


def _instant(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.exit(main())
