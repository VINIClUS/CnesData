"""Entrega síncrona de eventos da outbox."""

from dataclasses import dataclass
from datetime import datetime

from cnes_domain.ports.audit import AuditSinkPort
from cnes_domain.ports.control_plane import ControlPlanePort


@dataclass(frozen=True, slots=True)
class DispatchResult:
    """Contadores de um ciclo de entrega."""

    delivered: int
    failed: int


def dispatch_once(
    control_plane: ControlPlanePort,
    sink: AuditSinkPort,
    now: datetime,
    limit: int = 100,
) -> DispatchResult:
    """Entrega uma janela pendente da outbox.

    Args: Portas, instante de entrega e limite do lote.
    Returns: Contadores de eventos entregues e falhos.
    Raises: ValueError para limite inválido; erros da consulta inicial.
    """
    if limit < 1:
        raise ValueError("limit=invalid")

    delivered = 0
    failed = 0
    for event in control_plane.pending_outbox(limit):
        try:
            sink.append(event)
            control_plane.mark_outbox_delivered(event.event_id, now)
        except Exception:
            failed += 1
            continue
        delivered += 1
    return DispatchResult(delivered=delivered, failed=failed)
