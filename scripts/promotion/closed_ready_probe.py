"""Sonda do closed-ready: espera o artefato do keeper no mesmo run e expõe o hash, sem criar bytes."""

from __future__ import annotations

import io
import json
import logging
import math
import os
import subprocess
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Protocol

from scripts.promotion._closed_ready import object_hash
from scripts.promotion._stage_windows import FINAL_RESERVE, STAGE_SUM_CEILING

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)
POLL_SECONDS: Final = 5
API_ATTEMPTS: Final = 3
API_RETRY_SECONDS: Final = 2
MARGIN_SECONDS: Final = 300
TIMEOUT_SECONDS: Final = STAGE_SUM_CEILING + FINAL_RESERVE + MARGIN_SECONDS
APPROVAL_CAP_SECONDS: Final = 7200
JOB_TIMEOUT_MARGIN_MINUTES: Final = 15
JOB_TIMEOUT_MINUTES: Final = (
    math.ceil((APPROVAL_CAP_SECONDS + TIMEOUT_SECONDS) / 60) + JOB_TIMEOUT_MARGIN_MINUTES)
_KINDS: Final = {"close-drain.json": "close-drain", "phase-b.json": "phase-b",
                 "closed-ready.json": "closed-ready"}


class Api(Protocol):
    def artifacts(self, name: str) -> list[dict[str, Any]]: ...

    def jobs(self) -> list[dict[str, Any]]: ...

    def archive(self, artifact_id: int) -> bytes: ...


def _members(raw: bytes) -> dict[str, Any]:
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            names = archive.namelist()
            if sorted(names) != sorted(_KINDS):
                raise ValueError("closed_ready_probe=members")
            documents = {name: json.loads(archive.read(name)) for name in names}
    except (zipfile.BadZipFile, json.JSONDecodeError) as error:
        raise ValueError("closed_ready_probe=members") from error
    for name, kind in _KINDS.items():
        document = documents[name]
        if not isinstance(document, dict) or document.get("kind") != kind:
            raise ValueError("closed_ready_probe=members")
    return documents


def _concluded(api: Api, application_job: str) -> bool:
    return any(job.get("name") == application_job and job.get("status") == "completed"
               for job in api.jobs())


@dataclass(frozen=True, slots=True)
class Target:
    """Run, tentativa e job do application observados pela sonda."""

    run_id: int
    attempt: int
    application_job: str


@dataclass(frozen=True, slots=True)
class Clock:
    """Relógio e espera injetáveis da sonda."""

    sleep: Callable[[float], None] = time.sleep
    monotonic: Callable[[], float] = time.monotonic
    wall: Callable[[], float] = time.time


def _published(api: Api, name: str) -> str | None:
    found = api.artifacts(name)
    if len(found) > 1:
        raise ValueError("closed_ready_probe=artifact_count")
    if not found:
        return None
    return object_hash(_members(api.archive(found[0]["id"]))["closed-ready.json"])


def wait_for_closed_ready(
    api: Api, target: Target, clock: Clock = Clock(), deadline_seconds: int = TIMEOUT_SECONDS,
) -> str:
    """Returns: sha256 canônico do closed-ready.json publicado pelo keeper neste run.

    Raises: ValueError: prazo, application concluído sem o artefato ou membros fora do contrato.
    """
    name = f"cnesdata-closed-ready-{target.run_id}-{target.attempt}"
    started = clock.monotonic()
    while clock.monotonic() - started < deadline_seconds:
        digest = _published(api, name)
        if digest is not None:
            return digest
        if _concluded(api, target.application_job):
            digest = _published(api, name)
            if digest is None:
                raise ValueError("closed_ready_probe=application_concluded")
            return digest
        clock.sleep(POLL_SECONDS)
    raise ValueError("closed_ready_probe=deadline")


def _application_job(api: Api, name: str) -> dict[str, Any] | None:
    found = [job for job in api.jobs() if job.get("name") == name]
    if len(found) > 1:
        raise ValueError("closed_ready_probe=job_count")
    return found[0] if found else None


def _seconds_running(job: dict[str, Any], clock: Clock) -> int:
    started_at = job.get("started_at")
    if type(started_at) is not str:
        raise ValueError("closed_ready_probe=started_at")
    try:
        started = datetime.fromisoformat(started_at).astimezone(UTC).timestamp()
    except ValueError as error:
        raise ValueError("closed_ready_probe=started_at") from error
    elapsed = clock.wall() - started
    if elapsed < 0:
        logger.info("closed_ready_probe=started_at_ahead skew_seconds=%d", math.ceil(-elapsed))
        return 0
    return int(elapsed)


def wait_for_application_start(
    api: Api, target: Target, clock: Clock = Clock(), cap_seconds: int = APPROVAL_CAP_SECONDS,
) -> int:
    """Returns: segundos desde o started_at do job do application, visto em andamento.

    Raises: ValueError: teto de aprovação, jobs duplicados ou started_at ausente.
    """
    started = clock.monotonic()
    while clock.monotonic() - started < cap_seconds:
        job = _application_job(api, target.application_job)
        if job is not None and job.get("status") in {"in_progress", "completed"}:
            return _seconds_running(job, clock)
        clock.sleep(POLL_SECONDS)
    raise ValueError(f"closed_ready_probe=approval_cap seconds={cap_seconds}")


def wait_for_published(api: Api, target: Target, clock: Clock = Clock()) -> str:
    """Returns: hash do closed-ready, com o relógio de TIMEOUT_SECONDS contado do started_at."""
    running = wait_for_application_start(api, target, clock)
    return wait_for_closed_ready(api, target, clock, max(0, TIMEOUT_SECONDS - running))


class GhApi:
    """Leituras `gh api` do run corrente; o token vem do ambiente do job."""

    def __init__(self, repository: str, run_id: int, attempt: int,
                 run: Callable[..., Any] = subprocess.run,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self._base = f"repos/{repository}/actions"
        self._run, self._attempt = run_id, attempt
        self._exec, self._sleep = run, sleep

    def _get(self, path: str) -> bytes:
        for attempt in range(API_ATTEMPTS):
            try:
                return self._exec(
                    ["gh", "api", f"{self._base}/{path}"],
                    check=True, capture_output=True).stdout
            except (subprocess.CalledProcessError, OSError):
                if attempt + 1 < API_ATTEMPTS:
                    self._sleep(API_RETRY_SECONDS)
        raise ValueError("closed_ready_probe=api")

    def artifacts(self, name: str) -> list[dict[str, Any]]:
        return json.loads(self._get(f"runs/{self._run}/artifacts?name={name}"))["artifacts"]

    def jobs(self) -> list[dict[str, Any]]:
        path = f"runs/{self._run}/attempts/{self._attempt}/jobs?per_page=100"
        return json.loads(self._get(path))["jobs"]

    def archive(self, artifact_id: int) -> bytes:
        return self._get(f"artifacts/{artifact_id}/zip")


def main() -> int:
    run_id, attempt = int(os.environ["RUN_ID"]), int(os.environ["RUN_ATTEMPT"])
    try:
        digest = wait_for_published(
            GhApi(os.environ["REPOSITORY"], run_id, attempt),
            Target(run_id, attempt, os.environ["APPLICATION_JOB"]))
    except ValueError as error:
        logger.error("%s", error)
        return 1
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as stream:
        stream.write(f"closed_ready_sha256={digest}\n")
    return 0


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, format="%(message)s")
    raise SystemExit(main())
