"""Provider de admissão sintética: record e control fortes do control plane de produção."""

from __future__ import annotations

import json
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Final, cast

from cnes_domain.orchestration.mode_canary import (
    ModeCanaryBinding,
    ModeCanaryControl,
    ModeCanaryGrant,
    ModeCanaryRecord,
    SyntheticContextRequired,
)
from cnes_infra.control_plane.dynamodb_keys import tenant_partition

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.control_plane.environment_gate import PermitHolder

_TENANT_PREFIX: Final = "prod-canary-"


def canonical_text(mapping: dict[str, Any]) -> str:
    """Returns: JSON ordenado e compacto em UTF-8 com LF terminal único."""
    return json.dumps(mapping, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"


def _refuse(reason: str) -> SyntheticContextRequired:
    return SyntheticContextRequired(f"reason={reason} field=tenant_id")


class ModeCanaryContext:
    """`ModeCanaryAdmissionProvider` sobre o record imutável e o control item do owner."""

    def __init__(self, client: Any, table_name: str, binding: ModeCanaryBinding) -> None:
        self._client = client
        self._table_name = table_name
        self._binding = binding

    def binding_for(self, holder: PermitHolder, now: datetime) -> ModeCanaryGrant:
        """Returns: Grant do holder se record e control do owner estão válidos e ativos.

        Raises:
            SyntheticContextRequired: Holder fora do binding, record ou control ausente,
                adulterado, divergente, selado ou vencido.
        """
        if not self._binding.permits(holder.tenant_id, holder.run_id):
            raise _refuse("run_not_bound")
        record = self._record()
        if holder.run_id not in record.run_ids:
            raise _refuse("run_not_bound")
        control = self._control(record)
        deadline = min(record.expires_at, record.canary_deadline, control.deadline)
        if now >= deadline:
            raise _refuse("context_expired")
        return ModeCanaryGrant(record.promotion_id)

    def _payload(self, sort_key: str, missing: str) -> str:
        key = {"pk": {"S": tenant_partition(self._binding.tenant_id)}, "sk": {"S": sort_key}}
        item = self._client.get_item(
            TableName=self._table_name, Key=key, ConsistentRead=True
        ).get("Item")
        if item is None:
            raise _refuse(missing)
        return cast("str", item["payload"]["S"])

    @staticmethod
    def _mapping(text: str, kind: str) -> dict[str, Any]:
        try:
            mapping = json.loads(text)
        except json.JSONDecodeError as error:
            raise _refuse(f"{kind}_invalid") from error
        if type(mapping) is not dict:
            raise _refuse(f"{kind}_invalid")
        if canonical_text(cast("dict[str, Any]", mapping)) != text:
            raise _refuse(f"{kind}_not_canonical")
        return cast("dict[str, Any]", mapping)

    def _record(self) -> ModeCanaryRecord:
        binding = self._binding
        sort_key = f"PROD_CANARY#{binding.owner_sha256}#{binding.phase}"
        text = self._payload(sort_key, "record_missing")
        if sha256(text.encode()).hexdigest() != binding.record_sha256:
            raise _refuse("record_hash_mismatch")
        mapping = self._mapping(text, "record")
        try:
            record = ModeCanaryRecord.from_mapping(mapping)
        except ValueError as error:
            raise _refuse("record_invalid") from error
        if (
            record.owner_sha256 != binding.owner_sha256
            or record.phase != binding.phase
            or record.context_kind != binding.context_kind
            or binding.tenant_id != f"{_TENANT_PREFIX}{record.canary_id}"
        ):
            raise _refuse("record_mismatch")
        return record

    def _control(self, record: ModeCanaryRecord) -> ModeCanaryControl:
        sort_key = f"PROD_CANARY_CONTROL#{self._binding.owner_sha256}"
        text = self._payload(sort_key, "control_missing")
        mapping = self._mapping(text, "control")
        try:
            control = ModeCanaryControl.from_mapping(mapping)
        except ValueError as error:
            raise _refuse("control_invalid") from error
        generation = record.promotion_generation
        if (
            control.owner_sha256 != record.owner_sha256
            or control.context_kind != record.context_kind
            or control.context_manifest_sha256 != record.context_manifest_sha256
            or (generation is not None and control.generation != generation)
        ):
            raise _refuse("control_mismatch")
        if control.admission_state != "ACTIVE":
            raise _refuse("control_sealed")
        return control


__all__ = ["ModeCanaryContext", "canonical_text"]
