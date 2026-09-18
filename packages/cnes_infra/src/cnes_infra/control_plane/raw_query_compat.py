"""Compatibilidade temporária para consultas RAW legadas."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast
from warnings import warn

from cnes_domain.control_plane.queries import (
    LatestSucceededJobQuery,
    RawIdentity,
    RawManifestChainQuery,
    WaitingRunsForDependencyQuery,
)

if TYPE_CHECKING:
    from cnes_domain.control_plane.entities import Job, ManifestRef, Run
    from cnes_domain.ports.control_plane import TypedRawQueryPort

_LATEST_JOB_FIELDS = ("tenant_id", "agent_id", "source_type", "file_subtype", "competencia")
_DEPENDENCY_FIELDS = ("tenant_id", "source_type", "file_subtype", "competencia", "limit")


def _normalize_legacy_call(
    args: tuple[Any, ...], kwargs: dict[str, Any], fields: tuple[str, ...],
    default_limit: int | None = None,
) -> dict[str, Any]:
    if len(args) > len(fields):
        raise TypeError(f"too_many_arguments={len(args)}")
    values = dict(zip(fields, args, strict=False))
    for name, value in kwargs.items():
        if name not in fields:
            raise TypeError(f"unexpected_argument={name}")
        if name in values:
            raise TypeError(f"duplicate_argument={name}")
        values[name] = value
    if default_limit is not None and "limit" not in values:
        values["limit"] = default_limit
    missing = tuple(name for name in fields if name not in values)
    if missing:
        raise TypeError(f"missing_arguments={','.join(missing)}")
    return values


class DeprecatedRawQueryMixin:
    def latest_succeeded_job(self, *args: str, **kwargs: str) -> Job | None:
        warn(
            "method=latest_succeeded_job replacement=query_latest_succeeded_job",
            DeprecationWarning, stacklevel=2,
        )
        values = _normalize_legacy_call(args, kwargs, _LATEST_JOB_FIELDS)
        agent_id = values.pop("agent_id")
        query = LatestSucceededJobQuery(RawIdentity(**values), agent_id)
        return cast("TypedRawQueryPort", self).query_latest_succeeded_job(query)

    def list_raw_manifest_chain(self, *args: Any, **kwargs: Any) -> tuple[ManifestRef, ...]:
        warn(
            "method=list_raw_manifest_chain replacement=query_raw_manifest_chain",
            DeprecationWarning, stacklevel=2,
        )
        values = _normalize_legacy_call(args, kwargs, _DEPENDENCY_FIELDS, 31)
        limit = values.pop("limit")
        query = RawManifestChainQuery(RawIdentity(**values), limit)
        return cast("TypedRawQueryPort", self).query_raw_manifest_chain(query)

    def list_waiting_runs_for_dependency(self, *args: Any, **kwargs: Any) -> tuple[Run, ...]:
        warn(
            "method=list_waiting_runs_for_dependency replacement=query_waiting_runs_for_dependency",
            DeprecationWarning, stacklevel=2,
        )
        values = _normalize_legacy_call(args, kwargs, _DEPENDENCY_FIELDS, 100)
        limit = values.pop("limit")
        query = WaitingRunsForDependencyQuery(RawIdentity(**values), limit)
        return self._query_legacy_waiting_runs_for_dependency(query)

    def _query_legacy_waiting_runs_for_dependency(
        self, query: WaitingRunsForDependencyQuery
    ) -> tuple[Run, ...]:
        return cast("TypedRawQueryPort", self).query_waiting_runs_for_dependency(query)
