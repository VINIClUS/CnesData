"""Porta de acréscimo para eventos de auditoria."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from cnes_domain.control_plane.entities import OutboxEvent


@runtime_checkable
class AuditSinkPort(Protocol):
    def append(self, event: OutboxEvent) -> None: ...
