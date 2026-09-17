"""Ciclo de vida de Edge Job desacoplado de storage."""

from __future__ import annotations

import re
from hashlib import sha256
from typing import TYPE_CHECKING

from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.enums import AgentState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.control_plane.commands import (
        CancelJob,
        ClaimJob,
        CompleteJob,
        FailJob,
        RenewJobLease,
    )
    from cnes_domain.control_plane.entities import Job
    from cnes_domain.ports.control_plane import ControlPlanePort

DEFAULT_MAX_ATTEMPTS = 5
_ERROR_CODE = re.compile(r"^[A-Z0-9_]{1,64}$")
_RESYNC_PREFIX = "RAW_RESYNC_"

type EventEmitter = Callable[[OutboxEvent], None]
type _JobCommand = ClaimJob | RenewJobLease | CompleteJob | FailJob | CancelJob


def _noop(event: OutboxEvent) -> None:
    return None


class JobLifecycle:
    """Orquestra claim, lease, conclusão, falha e cancelamento de Edge Job."""

    def __init__(
        self,
        control_plane: ControlPlanePort,
        clock: Callable[[], datetime],
        *,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        emit: EventEmitter = _noop,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("positive_value_required")
        self._control_plane = control_plane
        self._clock = clock
        self._max_attempts = max_attempts
        self._emit = emit

    def claim(self, command: ClaimJob) -> Job | None:
        """Args: command: Pedido de reserva do job por um agente.
        Returns: Job reservado, ou None quando não há candidato disponível.
        Raises: Conflict: Agente revogado.
        """
        self._require_active_agent(command.tenant_id, command.owner)
        claimed = self._control_plane.claim_job(command)
        if claimed is not None:
            self._emit(self._event(command, "job.claimed", _claim_payload(claimed)))
        return claimed

    def renew(self, command: RenewJobLease) -> Job:
        """Args: command: Renovação de lease com fence esperado.
        Returns: Job com lease renovada.
        Raises: LeaseLost | FenceRejected: Fence ou lease inválidos.
        """
        renewed = self._control_plane.renew_job_lease(command)
        self._emit(self._event(command, "job.lease_renewed", _lease_payload(renewed)))
        return renewed

    def complete(self, command: CompleteJob) -> Job:
        """Args: command: Conclusão com manifesto resultante.
        Returns: Job concluído.
        Raises: Conflict: Agente revogado. FenceRejected: Fence antigo.
        """
        self._require_active_agent(command.tenant_id, command.owner)
        event = self._event(command, "job.succeeded", _complete_payload(command))
        return self._control_plane.complete_job(command, event)

    def fail(self, command: FailJob) -> Job:
        """Args: command: Falha reportada pelo agente.
        Returns: Job com estado retryable ou final.
        Raises: ValueError: error_code fora do padrão sanitizado.
        """
        _validate_error_code(command.error_code)
        capped = self._cap_attempts(command)
        event = self._event(capped, "job.failed", _fail_payload(capped))
        return self._control_plane.fail_job(capped, event)

    def request_cancel(self, command: CancelJob) -> Job:
        """Args: command: Pedido de cancelamento do job.
        Returns: Job em cancelamento solicitado.
        Raises: Conflict: Job já em estado terminal.
        """
        event = self._event(command, "job.cancel_requested", _cancel_payload(command))
        return self._control_plane.cancel_job(command, event)

    def _require_active_agent(self, tenant_id: str, agent_id: str) -> None:
        agent = self._control_plane.get_agent(tenant_id, agent_id)
        if agent is not None and agent.state is AgentState.REVOKED:
            raise Conflict(ErrorCode.AGENT_REVOKED)

    def _cap_attempts(self, command: FailJob) -> FailJob:
        if not command.retryable or command.error_code.startswith(_RESYNC_PREFIX):
            return command
        job = self._control_plane.get_job(command.tenant_id, command.job_id)
        if job is None or job.attempt < self._max_attempts:
            return command
        return command.model_validate(command.model_dump() | {"retryable": False})

    def _event(self, command: _JobCommand, event_type: str, payload: dict) -> OutboxEvent:
        job_id = command.job_id
        event_id = _event_id(event_type, command.tenant_id, job_id, payload)
        return OutboxEvent(
            tenant_id=command.tenant_id,
            event_id=event_id,
            event_type=event_type,
            aggregate_id=job_id,
            payload=payload,
            created_at=self._clock(),
            delivered_at=None,
        )


def _validate_error_code(error_code: str) -> None:
    if not _ERROR_CODE.fullmatch(error_code):
        raise ValueError("invalid_error_code")


def _claim_payload(job: Job) -> dict:
    return {
        "agent_id": job.lease_owner,
        "attempt": job.attempt,
        "fencing_token": job.fencing_token,
    }


def _lease_payload(job: Job) -> dict:
    return {"agent_id": job.lease_owner, "fencing_token": job.fencing_token}


def _complete_payload(command: CompleteJob) -> dict:
    return {
        "agent_id": command.owner,
        "fencing_token": command.fencing_token,
        "manifest_id": command.manifest.manifest_id,
    }


def _fail_payload(command: FailJob) -> dict:
    return {
        "agent_id": command.owner,
        "fencing_token": command.fencing_token,
        "error_code": command.error_code,
        "retryable": command.retryable,
    }


def _cancel_payload(command: CancelJob) -> dict:
    return {"requested_by": command.requested_by}


def _event_id(event_type: str, tenant_id: str, job_id: str, payload: dict) -> str:
    fields = sorted(f"{key}={value}" for key, value in payload.items())
    identity = "\x1f".join((event_type, tenant_id, job_id, *fields))
    return sha256(identity.encode()).hexdigest()
