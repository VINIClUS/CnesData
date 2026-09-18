"""Strict processing request and result contracts."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime  # noqa: TC003
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cnes_contracts.manifests.outputs import OutputManifest, ServingDocument  # noqa: TC001
from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType
from cnes_contracts.manifests.validation import (
    _COMPETENCIA_PATTERN,
    _validate_utc,
    manifest_sha256,
)

_STRICT_FROZEN_CONFIG = ConfigDict(frozen=True, strict=True, extra="forbid")
_SAFE_LEAF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _ordered_unique(values: tuple[str, ...]) -> tuple[str, ...]:
    if not values:
        raise ValueError("target_keys_required")
    if len(set(values)) != len(values):
        raise ValueError("target_keys_unique")
    return tuple(sorted(values))


def _validate_target_keys(keys: tuple[str, ...], expected: tuple[str, ...]) -> None:
    for key in keys:
        parts = tuple(key.split("/"))
        if not parts or parts[0] != expected[0]:
            raise ValueError("target_key_layer")
        if len(parts) != len(expected) + 1 or parts[:-1] != expected:
            raise ValueError("target_key_identity")
        if not _SAFE_LEAF.fullmatch(parts[-1]):
            raise ValueError("target_key_invalid")
        if expected[0] == "serving" and not parts[-1].endswith(".json"):
            raise ValueError("target_key_invalid")


def _validate_unique_manifests(manifests: tuple[OutputManifest, ...]) -> None:
    manifest_ids = {item.manifest_id for item in manifests}
    object_keys = {item.object_key for item in manifests}
    if len(manifest_ids) != len(manifests) or len(object_keys) != len(manifests):
        raise ValueError("manifests_unique")


def _output_identity(manifest: OutputManifest) -> tuple[object, ...]:
    return (
        manifest.tenant_id,
        manifest.run_id,
        manifest.competencia,
        manifest.unit_id,
        manifest.attempt,
        manifest.source_type,
    )


def _validate_output_group(
    manifests: tuple[OutputManifest, ...], layer: str, *, nonempty: bool = True
) -> None:
    if nonempty and not manifests:
        raise ValueError("manifests_required")
    if any(item.layer != layer for item in manifests):
        raise ValueError("manifest_layer")
    _validate_unique_manifests(manifests)
    if manifests and any(
        _output_identity(item) != _output_identity(manifests[0]) for item in manifests
    ):
        raise ValueError("manifest_identity")


def _validate_raw_chain(chain: tuple[RawManifest, ...]) -> None:
    first = chain[0]
    if first.snapshot_mode is not SnapshotMode.FULL:
        raise ValueError("chain_full_required")
    for expected_sequence, manifest in enumerate(chain, start=1):
        if manifest.sequence != expected_sequence:
            raise ValueError("chain_sequence")
        if expected_sequence == 1:
            continue
        previous = chain[expected_sequence - 2]
        if manifest.base_snapshot_id != first.snapshot_id:
            raise ValueError("chain_base")
        if manifest.previous_manifest_sha256 != manifest_sha256(previous):
            raise ValueError("chain_hash")


def _validate_raw_manifests(manifests: tuple[RawManifest, ...]) -> None:
    if not manifests:
        raise ValueError("raw_manifests_required")
    grouped: dict[str, list[RawManifest]] = defaultdict(list)
    for manifest in manifests:
        grouped[manifest.file_subtype].append(manifest)
    for chain in grouped.values():
        _validate_raw_chain(tuple(chain))


class NormalizeRequest(BaseModel):
    model_config = _STRICT_FROZEN_CONFIG

    tenant_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    unit_id: str = Field(min_length=1)
    attempt: int = Field(gt=0)
    source_type: SourceType
    raw_manifests: tuple[RawManifest, ...]
    target_keys: tuple[str, ...]
    normalized_at: datetime

    @field_validator("normalized_at")
    @classmethod
    def validate_normalized_at(cls, value: datetime) -> datetime:
        return _validate_utc(value)

    @field_validator("raw_manifests")
    @classmethod
    def order_raw_manifests(cls, values: tuple[RawManifest, ...]) -> tuple[RawManifest, ...]:
        return tuple(sorted(values, key=lambda item: (item.file_subtype, item.sequence)))

    @field_validator("target_keys")
    @classmethod
    def order_target_keys(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return _ordered_unique(values)

    @model_validator(mode="after")
    def validate_request(self) -> Self:
        _validate_raw_manifests(self.raw_manifests)
        first = self.raw_manifests[0]
        expected_identity = (self.tenant_id, self.source_type, first.competencia)
        if any(
            (item.tenant_id, item.source_type, item.competencia) != expected_identity
            for item in self.raw_manifests
        ):
            raise ValueError("raw_identity")
        prefix = (
            "normalized",
            self.tenant_id,
            self.source_type.value,
            first.competencia,
            self.run_id,
        )
        _validate_target_keys(self.target_keys, prefix)
        return self


class NormalizeResult(BaseModel):
    model_config = _STRICT_FROZEN_CONFIG

    manifests: tuple[OutputManifest, ...]

    @model_validator(mode="after")
    def validate_manifests(self) -> Self:
        _validate_output_group(self.manifests, "normalized")
        object_keys = tuple(item.object_key for item in self.manifests)
        if object_keys != tuple(sorted(object_keys)):
            raise ValueError("manifest_order")
        return self


class ReconcileRequest(BaseModel):
    model_config = _STRICT_FROZEN_CONFIG

    tenant_id: str = Field(min_length=1)
    competencia: str = Field(pattern=_COMPETENCIA_PATTERN)
    run_id: str = Field(min_length=1)
    unit_id: str = Field(min_length=1)
    attempt: int = Field(gt=0)
    normalized_manifests: tuple[OutputManifest, ...]
    reconciliation_key: str = Field(min_length=1)
    divergence_key: str = Field(min_length=1)
    reconciled_at: datetime

    @field_validator("reconciled_at")
    @classmethod
    def validate_reconciled_at(cls, value: datetime) -> datetime:
        return _validate_utc(value)

    @model_validator(mode="after")
    def validate_request(self) -> Self:
        if not self.normalized_manifests:
            raise ValueError("normalized_manifests_required")
        if any(item.layer != "normalized" for item in self.normalized_manifests):
            raise ValueError("manifest_layer")
        _validate_unique_manifests(self.normalized_manifests)
        expected = (self.tenant_id, self.competencia, self.run_id)
        if any(
            (item.tenant_id, item.competencia, item.run_id) != expected
            for item in self.normalized_manifests
        ):
            raise ValueError("manifest_identity")
        keys = (self.reconciliation_key, self.divergence_key)
        if len(set(keys)) != len(keys):
            raise ValueError("target_keys_unique")
        _validate_target_keys(keys, ("reconciliation", *expected))
        return self


class ReconcileResult(BaseModel):
    model_config = _STRICT_FROZEN_CONFIG

    reconciliation_manifest: OutputManifest
    divergence_manifest: OutputManifest
    kpis: dict[str, int]

    @model_validator(mode="after")
    def validate_manifests(self) -> Self:
        manifests = (self.reconciliation_manifest, self.divergence_manifest)
        _validate_output_group(manifests, "reconciliation")
        return self


class MaterializeRequest(BaseModel):
    model_config = _STRICT_FROZEN_CONFIG

    tenant_id: str = Field(min_length=1)
    competencia: str = Field(pattern=_COMPETENCIA_PATTERN)
    run_id: str = Field(min_length=1)
    unit_id: str = Field(min_length=1)
    attempt: int = Field(gt=0)
    reconciliation_manifest: OutputManifest
    divergence_manifest: OutputManifest
    missing_sources: tuple[str, ...]
    target_keys: tuple[str, ...]
    generated_at: datetime

    @field_validator("generated_at")
    @classmethod
    def validate_generated_at(cls, value: datetime) -> datetime:
        return _validate_utc(value)

    @field_validator("target_keys")
    @classmethod
    def order_target_keys(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return _ordered_unique(values)

    @model_validator(mode="after")
    def validate_request(self) -> Self:
        manifests = (self.reconciliation_manifest, self.divergence_manifest)
        _validate_output_group(manifests, "reconciliation")
        expected = (self.tenant_id, self.competencia, self.run_id)
        if any((item.tenant_id, item.competencia, item.run_id) != expected for item in manifests):
            raise ValueError("manifest_identity")
        if len(set(self.missing_sources)) != len(self.missing_sources):
            raise ValueError("missing_sources_unique")
        _validate_target_keys(self.target_keys, ("serving", self.tenant_id, self.run_id))
        return self


class MaterializeResult(BaseModel):
    model_config = _STRICT_FROZEN_CONFIG

    manifests: tuple[OutputManifest, ...]
    documents: tuple[ServingDocument, ...]

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        _validate_output_group(self.manifests, "serving")
        if len(self.documents) != len(self.manifests):
            raise ValueError("result_cardinality")
        names = tuple(item.document_name for item in self.documents)
        if len(set(names)) != len(names):
            raise ValueError("documents_unique")
        if names != tuple(sorted(names)):
            raise ValueError("result_association")
        expected_leaves = tuple(f"{name}.json" for name in names)
        if tuple(item.object_key.rsplit("/", 1)[-1] for item in self.manifests) != expected_leaves:
            raise ValueError("result_association")
        tenant_run = (self.manifests[0].tenant_id, self.manifests[0].run_id)
        if any((item.tenant_id, item.run_id) != tenant_run for item in self.documents):
            raise ValueError("result_identity")
        return self
