"""Admissão de agentes Edge com gate de billing para agentes novos."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING, Protocol
from uuid import uuid4

from cnes_domain.billing.commands import (
    CapacityReservationCommand,
    ConsumeCapacityCommand,
    GateRequest,
    ReleaseCapacityCommand,
)
from cnes_domain.billing.errors import RetryableBillingError
from cnes_domain.billing.models import CapacityKind, EntitlementAction
from cnes_domain.billing.shadow import ShadowObservation
from cnes_domain.control_plane.enums import AgentState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.control_plane.edge_registration import EntitlementFence, NewEdgeAgent

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from central_api.schemas.raw_api import EdgeIdentity
    from central_api.services.billing_gates import ApiBillingGates
    from cnes_domain.billing.models import EntitlementDecision
    from cnes_domain.control_plane.entities import Agent
    from cnes_infra.control_plane.edge_registration import EdgeAgentCreation


class EdgeAgentRegistry(Protocol):
    def get_agent(self, tenant_id: str, agent_id: str) -> Agent | None: ...

    def register_edge_agent(
        self, tenant_id: str, agent_id: str, fingerprint: str, now: datetime,
    ) -> Agent: ...

    def create_edge_agent(self, command: NewEdgeAgent) -> EdgeAgentCreation: ...


def _uuid_key() -> str:
    return uuid4().hex


@dataclass(frozen=True, slots=True)
class _Pending:
    identity: EdgeIdentity
    now: datetime
    fence: EntitlementFence
    reservation_id: str

    @property
    def account(self) -> str:
        return self.fence.billing_account_id

    def command(self) -> NewEdgeAgent:
        return NewEdgeAgent(
            self.identity.tenant_id, self.identity.agent_id,
            self.identity.certificate_fingerprint, self.now, self.reservation_id, self.fence,
        )


class AgentAdmission:
    """Admite agentes Edge; só o agente novo passa por gate e reserva de capacidade.

    Se a escrita canônica falha duas vezes, a reserva não é liberada: ela expira e a
    recuperação de reservas expiradas a devolve. Falha de consumo após a criação
    propaga pelo mesmo motivo.
    """

    def __init__(
        self,
        registry: EdgeAgentRegistry,
        gates: ApiBillingGates | None = None,
        reservation_keys: Callable[[], str] = _uuid_key,
    ) -> None:
        self._registry = registry
        self._gates = gates
        self._reservation_keys = reservation_keys

    def admit(self, identity: EdgeIdentity, now: datetime) -> Agent:
        """Args: identity: Identidade mTLS; now: Instante UTC.
        Returns: Agente persistido.
        Raises: Conflict: Agente revogado.
            BillingError: Gate ou capacidade negados para agente novo.
        """
        gates = self._gates
        if gates is None:
            return self._upsert(identity, now)
        if self._registry.get_agent(identity.tenant_id, identity.agent_id) is not None:
            return self._upsert(identity, now)
        return self._admit_new(gates, identity, now)

    def _upsert(self, identity: EdgeIdentity, now: datetime) -> Agent:
        return self._registry.register_edge_agent(
            identity.tenant_id, identity.agent_id, identity.certificate_fingerprint, now,
        )

    def _admit_new(self, gates: ApiBillingGates, identity: EdgeIdentity, now: datetime) -> Agent:
        account = gates.accounts.resolve(identity.tenant_id)
        decision = gates.gate.authorize_register_agent(GateRequest(account, identity.tenant_id))
        if not gates.enforced:
            gates.observer.observe(
                ShadowObservation(EntitlementAction.REGISTER_AGENT, identity.tenant_id),
            )
            return self._upsert(identity, now)
        fence = EntitlementFence(account, decision.entitlement_version)
        pending = self._reserve(gates, decision, _Pending(identity, now, fence, ""))
        creation = self._create_or_recover(gates, pending)
        if not creation.created:
            self._release(gates, pending, "agent_already_registered")
            return self._upsert(identity, now)
        gates.capacity.consume_capacity(
            ConsumeCapacityCommand(account, pending.reservation_id, now),
        )
        if creation.agent.state is AgentState.REVOKED:
            raise Conflict(ErrorCode.AGENT_REVOKED)
        return creation.agent

    def _reserve(
        self, gates: ApiBillingGates, decision: EntitlementDecision, pending: _Pending,
    ) -> _Pending:
        identity = pending.identity
        digest = sha256(
            f"{pending.account}\x1f{identity.tenant_id}\x1f{identity.agent_id}".encode(),
        ).hexdigest()
        reservation = gates.capacity.reserve_capacity(
            CapacityReservationCommand(
                pending.account, identity.tenant_id, identity.agent_id, CapacityKind.AGENT,
                f"edge_agent#{self._reservation_keys()}", digest,
                decision.entitlement_version, decision.quota_limit,
            ),
        )
        return _Pending(identity, pending.now, pending.fence, reservation.reservation_id)

    def _create_or_recover(self, gates: ApiBillingGates, pending: _Pending) -> EdgeAgentCreation:
        identity = pending.identity
        try:
            return self._registry.create_edge_agent(pending.command())
        except Exception as error:
            if self._registry.get_agent(identity.tenant_id, identity.agent_id) is None:
                self._release(gates, pending, "agent_registration_failed")
                _raise_contention(error)
                raise
        return self._registry.create_edge_agent(pending.command())

    def _release(self, gates: ApiBillingGates, pending: _Pending, reason: str) -> None:
        gates.capacity.release_capacity(
            ReleaseCapacityCommand(pending.account, pending.reservation_id, pending.now, reason),
        )


def _raise_contention(error: Exception) -> None:
    if isinstance(error, Conflict):
        raise RetryableBillingError("agent_registration_contended") from error
