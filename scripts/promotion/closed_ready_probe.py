"""Sonda do closed-ready: espera o artefato do keeper no mesmo run e expõe o hash, sem criar bytes."""

from __future__ import annotations

import io
import json
import logging
import os
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Protocol

from scripts.promotion._closed_ready import object_hash
from scripts.promotion._stage_windows import FINAL_RESERVE, STAGE_SUM_CEILING

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)
POLL_SECONDS: Final = 5
MARGIN_SECONDS: Final = 300
TIMEOUT_SECONDS: Final = STAGE_SUM_CEILING + FINAL_RESERVE + MARGIN_SECONDS
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


def wait_for_closed_ready(
    api: Api, *, run_id: int, attempt: int, application_job: str,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    deadline_seconds: int = TIMEOUT_SECONDS,
) -> str:
    """Returns: sha256 canônico do closed-ready.json publicado pelo keeper neste run.

    Raises: ValueError: prazo, application concluído sem o artefato ou membros fora do contrato.
    """
    name = f"cnesdata-closed-ready-{run_id}-{attempt}"
    started = monotonic()
    while monotonic() - started < deadline_seconds:
        found = api.artifacts(name)
        if len(found) > 1:
            raise ValueError("closed_ready_probe=artifact_count")
        if found:
            return object_hash(_members(api.archive(found[0]["id"]))["closed-ready.json"])
        if _concluded(api, application_job):
            raise ValueError("closed_ready_probe=application_concluded")
        sleep(POLL_SECONDS)
    raise ValueError("closed_ready_probe=deadline")


class GhApi:
    """Leituras `gh api` do run corrente; o token vem do ambiente do job."""

    def __init__(self, repository: str, run_id: int, attempt: int) -> None:
        self._base = f"repos/{repository}/actions"
        self._run, self._attempt = run_id, attempt

    def _get(self, path: str) -> bytes:
        return subprocess.run(
            ["gh", "api", f"{self._base}/{path}"],  # noqa: S607 -- gh do runner, argv fixo.
            check=True, capture_output=True).stdout

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
        digest = wait_for_closed_ready(
            GhApi(os.environ["REPOSITORY"], run_id, attempt), run_id=run_id, attempt=attempt,
            application_job=os.environ["APPLICATION_JOB"])
    except ValueError as error:
        logger.error("%s", error)
        return 1
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as stream:
        stream.write(f"closed_ready_sha256={digest}\n")
    return 0


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, format="%(message)s")
    raise SystemExit(main())
