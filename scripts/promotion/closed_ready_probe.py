"""Sonda do closed-ready: espera o artefato do keeper no mesmo run e expõe o hash, sem criar bytes."""

from __future__ import annotations

import io
import json
import logging
import math
import os
import sys
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Protocol
from urllib.parse import urlparse

from scripts.promotion._closed_ready import object_hash
from scripts.promotion._stage_windows import FINAL_RESERVE, STAGE_SUM_CEILING

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)
POLL_SECONDS: Final = 5
API_ATTEMPTS: Final = 3
API_RETRY_SECONDS: Final = 2
API_TIMEOUT_SECONDS: Final = 20
ASSUMED_RATE_LIMIT: Final = 5000
PROBE_ARTIFACT_READS_PER_POLL: Final = 1
PROBE_JOBS_READS_PER_MINUTE: Final = 1
PROBE_END_STAGES: Final = {"closed-ready": "api"}
CONCLUDED_CHECK_SECONDS: Final = 60 // PROBE_JOBS_READS_PER_MINUTE
_REDIRECTS: Final = (301, 302, 303, 307)
_BLOB_SUFFIXES: Final = (".blob.core.windows.net", ".actions.githubusercontent.com")
MARGIN_SECONDS: Final = 300
TIMEOUT_SECONDS: Final = STAGE_SUM_CEILING + FINAL_RESERVE + MARGIN_SECONDS
APPROVAL_CAP_SECONDS: Final = 7200
JOB_TIMEOUT_MARGIN_MINUTES: Final = 15
JOB_TIMEOUT_MINUTES: Final = (
    math.ceil((APPROVAL_CAP_SECONDS + TIMEOUT_SECONDS) / 60) + JOB_TIMEOUT_MARGIN_MINUTES)
_KINDS: Final = {"close-drain.json": "close-drain", "phase-b.json": "phase-b",
                 "closed-ready.json": "closed-ready"}


class Api(Protocol):
    rate: dict[str, int] | None

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


def require_rate_limit(api: Api) -> None:
    """Raises: ValueError: a primeira resposta sem o limite assumido de GITHUB_TOKEN ou sem números."""
    rate = getattr(api, "rate", None)
    if rate is None or rate["limit"] < ASSUMED_RATE_LIMIT:
        raise ValueError(f"github=rate_limit value={'na' if rate is None else rate['limit']}")


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
    checked: float | None = None
    while clock.monotonic() - started < deadline_seconds:
        digest = _published(api, name)
        if digest is not None:
            return digest
        now = clock.monotonic()
        if checked is None or now - checked >= CONCLUDED_CHECK_SECONDS:
            checked = now
            if _concluded(api, target.application_job):
                return _final_read(api, name, "closed_ready_probe=application_concluded")
        clock.sleep(POLL_SECONDS)
    return _final_read(api, name, "closed_ready_probe=deadline")


def _final_read(api: Api, name: str, failure: str) -> str:
    digest = _published(api, name)
    if digest is None:
        raise ValueError(failure)
    return digest


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
        parsed = datetime.fromisoformat(started_at)
    except ValueError as error:
        raise ValueError("closed_ready_probe=started_at") from error
    if parsed.tzinfo is None:
        raise ValueError("closed_ready_probe=started_at")
    started = parsed.astimezone(UTC).timestamp()
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
    limit_checked = False
    while clock.monotonic() - started < cap_seconds:
        job = _application_job(api, target.application_job)
        if not limit_checked:
            require_rate_limit(api)
            limit_checked = True
        if job is not None and job.get("status") in {"in_progress", "completed"}:
            return _seconds_running(job, clock)
        clock.sleep(POLL_SECONDS)
    raise ValueError(f"closed_ready_probe=approval_cap seconds={cap_seconds}")


def wait_for_published(api: Api, target: Target, clock: Clock = Clock()) -> str:
    """Returns: hash do closed-ready, com o relógio de TIMEOUT_SECONDS contado do started_at."""
    running = wait_for_application_start(api, target, clock)
    return wait_for_closed_ready(api, target, clock, max(0, TIMEOUT_SECONDS - running))


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def _download(url: str) -> Any:
    return urllib.request.urlopen(url, timeout=API_TIMEOUT_SECONDS)


@dataclass(frozen=True, slots=True)
class Transport:
    """Abertura HTTP, download sem token e espera injetáveis do cliente."""

    opener: Any = field(default_factory=lambda: urllib.request.build_opener(_NoRedirect))
    follow: Callable[[str], Any] = _download
    sleep: Callable[[float], None] = time.sleep


class _Transient(Exception):
    pass


def _refuse_rate_limited(error: urllib.error.HTTPError) -> None:
    if error.code in (403, 429) and (error.headers.get("x-ratelimit-remaining") == "0"
                                     or error.headers.get("retry-after") is not None):
        raise ValueError("github=rate_limited")


class HttpApi:
    """Leituras HTTPS do run corrente com GET condicional nas listagens; token do job."""

    def __init__(self, repository: str, target: Target, token: str,
                 transport: Transport | None = None) -> None:
        self._base = f"repos/{repository}/actions"
        self._target, self._token = target, token
        self._transport = transport or Transport()
        self._cache: dict[str, tuple[str, bytes]] = {}
        self.rate: dict[str, int] | None = None

    def _record(self, headers: Any) -> None:
        names = ("limit", "used", "remaining")
        values = [headers.get("x-ratelimit-" + name) for name in names]
        if all(value is not None and value.isdigit() for value in values):
            self.rate = dict(zip(names, map(int, values), strict=True))

    def _fetch(self, path: str, cached: tuple[str, bytes] | None) -> tuple[bytes, str | None]:
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/vnd.github+json",
                   "X-GitHub-Api-Version": "2022-11-28"}
        if cached is not None:
            headers["If-None-Match"] = cached[0]
        request = urllib.request.Request(f"https://api.github.com/{path}", headers=headers)
        try:
            response = self._transport.opener.open(request, timeout=API_TIMEOUT_SECONDS)
        except urllib.error.HTTPError as error:
            self._record(error.headers)
            return self._on_error(error, cached), None
        except OSError as error:
            raise _Transient from error
        with response:
            self._record(response.headers)
            return response.read(), response.headers.get("ETag")

    def _on_error(self, error: urllib.error.HTTPError, cached: tuple[str, bytes] | None) -> bytes:
        if error.code == 304 and cached is not None:
            return cached[1]
        _refuse_rate_limited(error)
        if error.code in _REDIRECTS:
            return self._download(error.headers.get("Location", ""))
        if error.code >= 500:
            raise _Transient
        raise ValueError("closed_ready_probe=api")

    def _download(self, url: str) -> bytes:
        parsed = urlparse(url)
        if parsed.scheme != "https" or not (parsed.hostname or "").endswith(_BLOB_SUFFIXES):
            raise ValueError("closed_ready_probe=api")
        try:
            with self._transport.follow(url) as response:
                return bytes(response.read())
        except OSError as error:
            raise ValueError("closed_ready_probe=api") from error

    def _get(self, path: str, *, conditional: bool) -> bytes:
        cached = self._cache.get(path) if conditional else None
        for attempt in range(API_ATTEMPTS):
            try:
                body, etag = self._fetch(path, cached)
            except _Transient:
                if attempt + 1 < API_ATTEMPTS:
                    self._transport.sleep(API_RETRY_SECONDS)
                continue
            if conditional and etag:
                self._cache[path] = (etag, body)
            return body
        raise ValueError("closed_ready_probe=api")

    def artifacts(self, name: str) -> list[dict[str, Any]]:
        path = f"{self._base}/runs/{self._target.run_id}/artifacts?name={name}"
        return json.loads(self._get(path, conditional=True))["artifacts"]

    def jobs(self) -> list[dict[str, Any]]:
        path = f"{self._base}/runs/{self._target.run_id}/attempts/{self._target.attempt}/jobs?per_page=100"
        return json.loads(self._get(path, conditional=True))["jobs"]

    def archive(self, artifact_id: int) -> bytes:
        return self._get(f"{self._base}/artifacts/{artifact_id}/zip", conditional=False)


def _exit_line(api: Any) -> str:
    rate = getattr(api, "rate", None) or {}
    fields = " ".join(f"x_ratelimit_{name}={rate.get(name, 'na')}" for name in ("limit", "used", "remaining"))
    return f"closed_ready_probe=exit {fields}"


def main() -> int:
    run_id, attempt = int(os.environ["RUN_ID"]), int(os.environ["RUN_ATTEMPT"])
    target = Target(run_id, attempt, os.environ["APPLICATION_JOB"])
    api = HttpApi(os.environ["REPOSITORY"], target, os.environ["GH_TOKEN"])
    try:
        digest = wait_for_published(api, target)
    except ValueError as error:
        logger.error("%s", error)
        return 1
    finally:
        logger.info("%s", _exit_line(api))
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as stream:
        stream.write(f"closed_ready_sha256={digest}\n")
    return 0


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, format="%(message)s", level=logging.INFO)
    raise SystemExit(main())
