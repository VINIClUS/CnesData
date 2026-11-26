"""Sonda static-inventory: espera marker e inventory do keeper, deriva plano estático e mode-canary."""

from __future__ import annotations

import io
import logging
import os
import sys
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Protocol

from scripts.promotion._api_static_records import static_plan
from scripts.promotion.closed_ready_probe import Clock, HttpApi, Target
from scripts.promotion.mode_context_source import (
    mode_canary_file,
    mode_context,
    stage_marker,
    stage_marker_name,
)
from scripts.promotion.publish_dashboard import PublicationError

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

POLL_SECONDS: Final = 10
DEADLINE_SECONDS: Final = 10200
INVENTORY_MEMBER: Final = "inventory.json"
PLAN_MEMBER: Final = "static-plan.json"
MARKER_MEMBER: Final = "index.json"
MODE_CANARY_MEMBER: Final = "mode-canary.json"


class ArtifactApi(Protocol):
    def artifacts(self, name: str) -> list[dict[str, Any]]: ...

    def archive(self, artifact_id: int) -> bytes: ...


class Waiter(Protocol):
    @property
    def monotonic(self) -> Callable[[], float]: ...

    @property
    def sleep(self) -> Callable[[float], None]: ...


def inventory_name(target: Target) -> str:
    """Returns: Nome do artefato DD-2 que o keeper publica com o inventory do run."""
    return f"cnesdata-inventory-{target.run_id}-{target.attempt}"


def _member(raw: bytes, member: str) -> bytes:
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            if archive.namelist() != [member]:
                raise ValueError("static_inventory_probe=members")
            return archive.read(member)
    except (zipfile.BadZipFile, OSError) as error:
        raise ValueError("static_inventory_probe=members") from error


def _wait(api: ArtifactApi, name: str, member: str, clock: Waiter, deadline: int) -> bytes:
    started = clock.monotonic()
    while clock.monotonic() - started < deadline:
        found = api.artifacts(name)
        if len(found) > 1:
            raise ValueError("static_inventory_probe=artifact_count")
        if found:
            return _member(api.archive(found[0]["id"]), member)
        clock.sleep(POLL_SECONDS)
    raise ValueError("static_inventory_probe=deadline")


def wait_for_inventory(
    api: ArtifactApi, target: Target, clock: Waiter = Clock(), deadline: int = DEADLINE_SECONDS,
) -> bytes:
    """Returns: inventory.json do artefato único do keeper neste run e attempt.

    Raises: ValueError: prazo, artefato duplicado ou membros fora do contrato.
    """
    return _wait(api, inventory_name(target), INVENTORY_MEMBER, clock, deadline)


def wait_for_marker(
    api: ArtifactApi, target: Target, clock: Waiter = Clock(), deadline: int = DEADLINE_SECONDS,
) -> bytes:
    """Returns: index.json do marker de abertura do application-prepare neste run e attempt.

    Raises: ValueError: prazo, artefato duplicado ou membros fora do contrato.
    """
    return _wait(api, stage_marker_name(target), MARKER_MEMBER, clock, deadline)


def static_plan_file(inventory: bytes, distribution_arn: str) -> bytes:
    """Returns: static-plan.json, o carrier do binding #498 derivado do inventory do keeper."""
    return static_plan(inventory, distribution_arn)


def _write(directory: str, member: str, body: bytes) -> None:
    output = Path(directory)
    output.mkdir(parents=True, exist_ok=False)
    (output / member).write_bytes(body)


def main() -> int:
    target = Target(int(os.environ["RUN_ID"]), int(os.environ["RUN_ATTEMPT"]),
                    os.environ["APPLICATION_JOB"])
    api = HttpApi(os.environ["REPOSITORY"], target, os.environ["GH_TOKEN"])
    try:
        context = mode_context(os.environ["MODE_CONTEXT"])
        marker = stage_marker(wait_for_marker(api, target), target)
        plan = static_plan_file(wait_for_inventory(api, target), context["distribution_arn"])
        canary = mode_canary_file(context, marker, target, os.environ["SOURCE_SHA"])
    except (ValueError, PublicationError) as error:
        logger.error("%s", error)
        return 1
    _write(os.environ["OUTPUT"], PLAN_MEMBER, plan)
    _write(os.environ["LIFECYCLE_OUTPUT"], MODE_CANARY_MEMBER, canary)
    logger.info("static_inventory_probe=published members=%s,%s", PLAN_MEMBER, MODE_CANARY_MEMBER)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    raise SystemExit(main())
