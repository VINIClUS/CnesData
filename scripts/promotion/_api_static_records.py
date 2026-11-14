"""Registros canônicos do adapter estático: inventory, plano do binding #498 e publicação."""

from __future__ import annotations

import hashlib
import json
import re
from typing import TYPE_CHECKING, Any

from cnes_domain.orchestration.static_release import VersionRef
from scripts.promotion.publish_dashboard import (
    ENTRYPOINTS,
    PublicationError,
    PublicationReceipt,
    StaticTarget,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from scripts.promotion.publish_dashboard import Inventory

STATIC_BUCKET = "cnesdata-prod-static-web"
INVENTORY_SCHEMA = "cnesdata.static-inventory/v1"
PLAN_SCHEMA = "cnesdata.static-plan/v1"
PUBLICATION_SCHEMA = "cnesdata.static-publication/v1"
ROLLBACK_SCHEMA = "cnesdata.static-rollback/v1"
PUBLICATION_FIELDS = frozenset({
    "schema", "plan_sha256", "closed_ready_sha256", "uploaded", "already_published",
    "invalidation_id",
})
# retire/retire_latest depend on the clock and are reported, never executed, in this slice.
PLAN_DRIFT_FIELDS = (
    "candidate_sha256", "dashboard_sha256", "baseline", "outcome", "upload_order",
    "already_published", "rollback_versions", "multipart_keys", "projected_bytes",
)
_ARN = re.compile(r"arn:aws:cloudfront::\d{12}:distribution/([A-Z0-9]+)")


def canonical(value: Any) -> bytes:
    text = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return (text + "\n").encode("utf-8")


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def read_object(raw: bytes, name: str, *, strict: bool = True) -> dict[str, Any]:
    """Returns:
    Objeto JSON; com strict, só os bytes canônicos dele.
    Raises:
    PublicationError: JSON inválido, não objeto ou fora da forma canônica.
    """
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PublicationError(f"{name}=json") from error
    if not isinstance(value, dict) or (strict and canonical(value) != raw):
        raise PublicationError(f"{name}=canonical")
    return value


def inventory_record(
    inventory: Inventory, candidate: bytes, dashboard: bytes, baseline: bytes
) -> bytes:
    """Returns:
    Observação canônica da inventory, sem conteúdo de objeto nem credencial.
    """
    decision = inventory.decision
    known = {version.ref: version.sha256 for version in inventory.snapshot.versions}
    return canonical({
        "schema": INVENTORY_SCHEMA,
        "candidate_sha256": sha(candidate),
        "dashboard_sha256": sha(dashboard),
        "baseline": json.loads(baseline),
        "outcome": decision.outcome.value,
        "upload_order": list(decision.upload_order),
        "already_published": list(decision.already_published),
        "retire": _refs(decision.retire),
        "retire_latest": _refs(decision.retire_latest),
        "rollback_versions": [{"key": ref.key, "version_id": ref.version_id,
                               "sha256": known.get(ref)} for ref in inventory.rollback_versions],
        "multipart_keys": list(inventory.multipart_keys),
        "projected_bytes": decision.projected_bytes,
    })


def read_record(raw: bytes) -> dict[str, Any]:
    """Returns:
    Inventory canônica registrada pelo adapter.
    Raises:
    PublicationError: bytes fora da forma canônica ou de outro schema.
    """
    record = read_object(raw, "inventory")
    if record.get("schema") != INVENTORY_SCHEMA:
        raise PublicationError("inventory=schema")
    return record


def static_plan(inventory: bytes, distribution_arn: Any) -> bytes:
    """Returns:
    Carrier canônico do plano; seu sha256 é o static_plan_sha256 do binding #498.
    Raises:
    PublicationError: ARN fora do contrato, nada a publicar ou par de rollback incompleto.
    """
    record = read_record(inventory)
    if not isinstance(distribution_arn, str) or _ARN.fullmatch(distribution_arn) is None:
        raise PublicationError("static_plan=distribution")
    if record["outcome"] != "PLANNED" or not record["upload_order"]:
        raise PublicationError(f"static_plan=candidate_keys outcome={record['outcome']}")
    rollback = record["rollback_versions"]
    if rollback and (sorted(item["key"] for item in rollback) != sorted(ENTRYPOINTS)
                     or any(item["sha256"] is None for item in rollback)):
        raise PublicationError("static_plan=rollback_versions")
    return canonical({
        "schema": PLAN_SCHEMA,
        "bucket": STATIC_BUCKET,
        "distribution_arn": distribution_arn,
        "inventory_sha256": sha(inventory),
        "candidate_keys": sorted(record["upload_order"]),
        "rollback_versions": [{"key": item["key"], "version_id": item["version_id"]}
                              for item in rollback],
        "retire_versions": [],
        "multipart_keys": record["multipart_keys"],
    })


def read_plan(raw: bytes, inventory: bytes) -> dict[str, Any]:
    """Returns:
    Plano cujos bytes são exatamente os derivados da inventory recebida.
    Raises:
    PublicationError: plano de outra inventory ou fora do contrato.
    """
    plan = read_object(raw, "static_plan")
    if static_plan(inventory, plan.get("distribution_arn")) != raw:
        raise PublicationError("static_plan=binding")
    return plan


def static_target(plan: dict[str, Any]) -> StaticTarget:
    match = _ARN.fullmatch(plan["distribution_arn"])
    if match is None:
        raise PublicationError("static_plan=distribution")
    return StaticTarget(plan["bucket"], match.group(1))


def rollback_targets(record: dict[str, Any]) -> tuple[tuple[VersionRef, str | None], ...]:
    return tuple((VersionRef(item["key"], item["version_id"]), item["sha256"])
                 for item in record["rollback_versions"])


def publication_record(plan: bytes, receipt: PublicationReceipt, closed_ready: str) -> bytes:
    return canonical({
        "schema": PUBLICATION_SCHEMA,
        "plan_sha256": sha(plan),
        "closed_ready_sha256": closed_ready,
        "uploaded": [list(item) for item in receipt.uploaded],
        "already_published": list(receipt.already_published),
        "invalidation_id": receipt.invalidation_id,
    })


def read_publication(raw: bytes, plan: bytes) -> tuple[PublicationReceipt, str]:
    """Returns:
    Recibo da publicação registrada para este mesmo plano e o closed_ready dela.
    Raises:
    PublicationError: publicação fora do schema fechado ou de outro plano.
    """
    record = read_object(raw, "publication")
    if set(record) != PUBLICATION_FIELDS or record["schema"] != PUBLICATION_SCHEMA:
        raise PublicationError("publication=fields")
    if record["plan_sha256"] != sha(plan):
        raise PublicationError("publication=binding")
    uploaded = tuple((str(key), str(version)) for key, version in record["uploaded"])
    receipt = PublicationReceipt(uploaded, tuple(record["already_published"]),
                                 record["invalidation_id"])
    return receipt, str(record["closed_ready_sha256"])


def _refs(refs: Iterable[VersionRef]) -> list[dict[str, str]]:
    return [{"key": ref.key, "version_id": ref.version_id} for ref in sorted(refs)]
