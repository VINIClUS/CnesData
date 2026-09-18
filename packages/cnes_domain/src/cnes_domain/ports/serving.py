"""Porta de acesso autorizado aos dados servidos."""

from __future__ import annotations

import re
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

_DOCUMENT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*\.json$")
_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _require_non_blank(value: str) -> str:
    if not value.strip():
        raise ValueError("blank_value")
    return value


class _ServingModel(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")


class ServingRequest(_ServingModel):
    user_id: str
    tenant_id: str
    dataset_name: str

    _identities = field_validator("user_id", "tenant_id", "dataset_name")(
        _require_non_blank
    )


class ServingGrant(_ServingModel):
    tenant_id: str
    run_id: str
    version_id: str
    object_keys: tuple[str, ...]

    _identities = field_validator("tenant_id", "run_id", "version_id")(_require_non_blank)

    @model_validator(mode="after")
    def _validate_keys(self) -> ServingGrant:
        if not all(_SAFE_SEGMENT.fullmatch(value) for value in (self.tenant_id, self.run_id)):
            raise ValueError("serving_key_forbidden")
        if not self.object_keys:
            raise ValueError("serving_keys_required")
        if len(set(self.object_keys)) != len(self.object_keys):
            raise ValueError("duplicate_serving_key")
        prefix = f"serving/{self.tenant_id}/{self.run_id}/"
        for key in self.object_keys:
            document = key.removeprefix(prefix)
            if not key.startswith(prefix) or not _DOCUMENT_NAME.fullmatch(document):
                raise ValueError("serving_key_forbidden")
        return self


@runtime_checkable
class ServingAccessPort(Protocol):
    def authorize(self, request: ServingRequest) -> ServingGrant: ...
