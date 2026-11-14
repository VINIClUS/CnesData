"""Poda de revisões e digests do processor pelo processor_retention, com readback duplo."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from cnes_domain.orchestration.processor_retention import (
    ProcessorImageReferences,
    ProcessorRetentionSnapshot,
    RetainedProcessorDigest,
    RetentionOutcome,
    plan_processor_retention,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping
    from datetime import datetime

Node = dict[str, Any]
REPOSITORY = "cnesdata-prod-processor"
FAMILIES = tuple(f"{REPOSITORY}-{mode}" for mode in ("unit", "recovery", "audit"))
PAGE_LIMIT = 100
_RELEASE_TAG = re.compile(r"candidate-[0-9a-f]{40}-[1-9][0-9]*-[1-9][0-9]*")
_IMAGE = re.compile(
    rf"[0-9]{{12}}\.dkr\.ecr\.us-east-2\.amazonaws\.com/{REPOSITORY}@(sha256:[0-9a-f]{{64}})"
)
_REVISION = re.compile(
    rf"arn:aws:ecs:us-east-2:[0-9]{{12}}:task-definition/({REPOSITORY}-[a-z]+):([1-9][0-9]*)"
)


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


@dataclass(frozen=True)
class PruneReferences:
    """Revisões protegidas por ARN e o drain que libera a poda."""

    current: frozenset[str]
    prior: frozenset[str]
    in_flight: frozenset[str]
    retained: frozenset[str]
    drained: bool

    @property
    def protected(self) -> frozenset[str]:
        return self.current | self.prior | self.in_flight | self.retained


@dataclass(frozen=True)
class Inventory:
    """Imagens do ECR e revisões ACTIVE do ECS lidas por inteiro, ou incompletas."""

    images: tuple[tuple[str, datetime, tuple[str, ...]], ...]
    revisions: tuple[tuple[str, str], ...]
    complete: bool

    def sha256(self) -> str:
        data = json.dumps(asdict(self), sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(data.encode()).hexdigest()


def _pages(call: Callable[..., Node], key: str, request: Node) -> tuple[list[Any], bool]:
    items: list[Any] = []
    token = None
    for _ in range(PAGE_LIMIT):
        page = call(**request, **({"nextToken": token} if token else {}))
        items.extend(page.get(key, []))
        token = page.get("nextToken")
        if not token:
            return items, True
    return items, False


def _revision_digest(ecs: Any, arn: str) -> str | None:
    definition = ecs.describe_task_definition(taskDefinition=arn)["taskDefinition"]
    containers = definition.get("containerDefinitions") or []
    image = _IMAGE.fullmatch(containers[0].get("image", "")) if len(containers) == 1 else None
    valid = definition.get("taskDefinitionArn") == arn and definition.get("status") == "ACTIVE"
    return image.group(1) if image and valid else None


def read_inventory(clients: Mapping[str, Any]) -> Inventory:
    """Returns: Inventário paginado; página sem fim ou revisão fora do contrato o torna incompleto."""
    details, complete = _pages(clients["ecr"].describe_images, "imageDetails",
                               {"repositoryName": REPOSITORY})
    images = tuple(sorted((item["imageDigest"], item["imagePushedAt"],
                           tuple(sorted(item.get("imageTags") or ()))) for item in details))
    revisions: list[tuple[str, str]] = []
    for family in FAMILIES:
        arns, listed = _pages(clients["ecs"].list_task_definitions, "taskDefinitionArns",
                              {"familyPrefix": family, "status": "ACTIVE"})
        complete = complete and listed
        for arn in arns:
            digest = _revision_digest(clients["ecs"], arn)
            complete = complete and digest is not None and _family(arn) == family
            revisions.append((arn, digest or ""))
    return Inventory(images, tuple(sorted(revisions)), complete)


def _family(arn: str) -> str | None:
    match = _REVISION.fullmatch(arn)
    return match.group(1) if match else None


def _number(arn: str) -> int:
    match = _REVISION.fullmatch(arn)
    return int(match.group(2)) if match else 0


def _snapshot(inventory: Inventory, refs: PruneReferences
              ) -> tuple[ProcessorRetentionSnapshot, RetainedProcessorDigest | None]:
    digests = dict(inventory.revisions)

    def mapped(arns: Iterable[str]) -> frozenset[str]:
        return frozenset(digests[arn] for arn in arns if arn in digests)

    references = ProcessorImageReferences(mapped(refs.current), mapped(refs.prior),
                                          mapped(refs.in_flight), mapped(refs.retained))
    history = tuple(RetainedProcessorDigest(digest, pushed) for digest, pushed, tags
                    in inventory.images if any(_RELEASE_TAG.fullmatch(tag) for tag in tags))
    complete = inventory.complete and refs.protected <= digests.keys()
    current = [image for image in history if image.digest in references.current]
    candidate = current[0] if len(current) == len(references.current) == 1 else None
    return ProcessorRetentionSnapshot(history, references, complete), candidate


def _floors(refs: PruneReferences) -> dict[str, int]:
    floors: dict[str, int] = {}
    for arn in refs.current | refs.prior:
        family = _family(arn)
        if family is not None:
            floors[family] = min(floors.get(family, _number(arn)), _number(arn))
    return floors


def _eligible(inventory: Inventory, refs: PruneReferences,
              prune: tuple[str, ...]) -> tuple[list[str], list[str]]:
    floors = _floors(refs)
    users: dict[str, list[str]] = {}
    for arn, digest in inventory.revisions:
        users.setdefault(digest, []).append(arn)
    digests: list[str] = []
    revisions: list[str] = []
    for digest in prune:
        arns = users.get(digest, [])
        if all(arn not in refs.protected
               and _number(arn) < floors.get(str(_family(arn)), 0) for arn in arns):
            digests.append(digest)
            revisions.extend(arns)
    return digests, sorted(revisions)


def plan_prune(inventory: Inventory, refs: PruneReferences,
               now: datetime) -> tuple[str, list[str], list[str]]:
    """Returns: Desfecho do contrato, digests e revisões elegíveis para a poda."""
    snapshot, candidate = _snapshot(inventory, refs)
    if candidate is None:
        return RetentionOutcome.REJECTED_INCOMPLETE.value, [], []
    decision = plan_processor_retention(snapshot, candidate, now)
    if decision.outcome is not RetentionOutcome.PLANNED:
        return decision.outcome.value, [], []
    digests, revisions = _eligible(inventory, refs, decision.prune)
    return decision.outcome.value, digests, revisions


def _delete(clients: Mapping[str, Any], digests: list[str], revisions: list[str]) -> None:
    for arn in revisions:
        clients["ecs"].deregister_task_definition(taskDefinition=arn)
    for digest in digests:
        result = clients["ecr"].batch_delete_image(
            repositoryName=REPOSITORY, imageIds=[{"imageDigest": digest}])
        _require(not result.get("failures"), "prune=delete_failed")


def _evidence(outcome: str, refs: PruneReferences, removed: tuple[list[str], list[str]],
              readbacks: list[str]) -> Node:
    keep = refs.current | refs.prior | refs.in_flight
    planned = outcome == RetentionOutcome.PLANNED.value
    retained = sorted(a for a in refs.retained if a in keep) if planned else sorted(refs.retained)
    return {"version": 1, "kind": "prune", "outcome": outcome, "prune": removed[0],
            "deregistered": removed[1], "retained_after": retained, "readbacks": readbacks}


def execute_prune(clients: Mapping[str, Any], refs: PruneReferences, now: datetime) -> Node:
    """Returns: Evidência da poda. Raises: ValueError: Readback mudou ou delete incerto."""
    if not refs.drained:
        return _evidence("NOT_DRAINED", refs, ([], []), [])
    first = read_inventory(clients)
    outcome, digests, revisions = plan_prune(first, refs, now)
    if outcome != RetentionOutcome.PLANNED.value:
        return _evidence(outcome, refs, ([], []), [first.sha256()])
    second = read_inventory(clients)
    _require(second == first, "prune=readback_changed")
    _delete(clients, digests, revisions)
    third = read_inventory(clients)
    remaining = {image[0] for image in third.images} | {arn for arn, _ in third.revisions}
    _require(not remaining & {*digests, *revisions}, "prune=post_readback")
    return _evidence(outcome, refs, (digests, revisions),
                     [first.sha256(), second.sha256(), third.sha256()])
