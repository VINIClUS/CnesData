from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from threading import Barrier, Lock, Thread
from typing import TYPE_CHECKING, cast

import pytest

from central_api.schemas.raw_api import EdgeIdentity
from central_api.services.agent_admission import AgentAdmission
from central_api.services.billing_gates import (
    ApiBillingGates,
    BillingAccountMissing,
    TenantAccountResolver,
)
from cnes_domain.billing.errors import EntitlementDenied, QuotaExceeded, RetryableBillingError
from cnes_domain.billing.models import (
    AccessLevel,
    CapacityKind,
    CapacityReservation,
    EntitlementAction,
    EntitlementDecision,
    ReservationStatus,
)
from cnes_domain.control_plane.entities import Agent
from cnes_domain.control_plane.enums import AgentState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_domain.profiles import BillingMode
from cnes_infra.control_plane.edge_registration import EdgeAgentCreation, EntitlementFence

if TYPE_CHECKING:
    from cnes_domain.billing.gate import EntitlementGate
    from cnes_domain.billing.ports import QuotaReservationPort

NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
FINGERPRINT = sha256(b"certificate").hexdigest()
OTHER_FINGERPRINT = sha256(b"other").hexdigest()
ACCOUNT = "acct-1"


def identity(agent_id: str = "agent-1", fingerprint: str = FINGERPRINT) -> EdgeIdentity:
    return EdgeIdentity(
        tenant_id="354130", agent_id=agent_id, certificate_fingerprint=fingerprint,
    )


def make_agent(agent_id: str = "agent-1", fingerprint: str = FINGERPRINT, **updates) -> Agent:
    values = {
        "tenant_id": "354130",
        "agent_id": agent_id,
        "state": AgentState.ACTIVE,
        "version": "1.0",
        "certificate_fingerprint": fingerprint,
        "last_seen_at": NOW,
        "created_at": NOW,
    }
    return Agent(**(values | updates))


class Registry:
    def __init__(self, calls: list[str], agents: tuple[Agent, ...] = ()) -> None:
        self.calls = calls
        self.agents = {item.agent_id: item for item in agents}
        self.owners: dict[str, str] = {}
        self.create_errors: list[Exception] = []
        self.commit_before_error = False
        self.hidden_reads = 0
        self.commands: list = []
        self.lock = Lock()

    def get_agent(self, tenant_id: str, agent_id: str) -> Agent | None:
        self.calls.append("get_agent")
        if self.hidden_reads > 0:
            self.hidden_reads -= 1
            return None
        return self.agents.get(agent_id)

    def register_edge_agent(self, tenant_id, agent_id, fingerprint, now) -> Agent:
        self.calls.append("register")
        current = self.agents.get(agent_id)
        if current is not None and current.state is AgentState.REVOKED:
            raise Conflict("agent_revoked")
        agent = (current or make_agent(agent_id)).model_copy(
            update={"certificate_fingerprint": fingerprint},
        )
        self.agents[agent_id] = agent
        return agent

    def create_edge_agent(self, command) -> EdgeAgentCreation:
        self.calls.append("create")
        with self.lock:
            if self.create_errors:
                if self.commit_before_error:
                    self._store(command)
                raise self.create_errors.pop(0)
            return self._store(command)

    def _store(self, command) -> EdgeAgentCreation:
        self.commands.append(command)
        current = self.agents.get(command.agent_id)
        if current is None:
            created = make_agent(command.agent_id, command.fingerprint)
            self.agents[command.agent_id] = created
            self.owners[command.agent_id] = command.reservation_id
            return EdgeAgentCreation(created, True)
        owned = self.owners.get(command.agent_id) == command.reservation_id
        return EdgeAgentCreation(current, owned)


class Gate:
    def __init__(self, calls: list[str], error: Exception | None = None) -> None:
        self.calls = calls
        self.error = error

    def authorize_register_agent(self, request) -> EntitlementDecision:
        self.calls.append("gate")
        if self.error is not None:
            raise self.error
        return EntitlementDecision(
            EntitlementAction.REGISTER_AGENT, True, AccessLevel.FULL, "allowed", 7, 3,
        )


class Capacity:
    def __init__(self, calls: list[str], limit: int | None = None) -> None:
        self.calls = calls
        self.limit = limit
        self.reserved: list = []
        self.consumed: list = []
        self.released: list = []
        self.barrier: Barrier | None = None
        self.lock = Lock()
        self.reserve_error: Exception | None = None
        self.consume_error: Exception | None = None

    def reserve_capacity(self, command) -> CapacityReservation:
        self.calls.append("reserve")
        if self.reserve_error is not None:
            raise self.reserve_error
        if self.barrier is not None:
            self.barrier.wait()
        with self.lock:
            if self.limit is not None and len(self.reserved) >= self.limit:
                raise QuotaExceeded("quota=agents")
            self.reserved.append(command)
            number = len(self.reserved)
        return CapacityReservation(
            f"res-{number}", command.billing_account_id, command.resource_id,
            CapacityKind.AGENT, ReservationStatus.RESERVED, NOW, NOW + timedelta(minutes=5),
        )

    def consume_capacity(self, command):
        self.calls.append("consume")
        if self.consume_error is not None:
            raise self.consume_error
        self.consumed.append(command)

    def release_capacity(self, command):
        self.calls.append("release")
        self.released.append(command)


@dataclass
class Rig:
    calls: list[str] = field(default_factory=list)
    registry: Registry = field(init=False)
    capacity: Capacity = field(init=False)
    gate: Gate = field(init=False)

    def __post_init__(self) -> None:
        self.registry = Registry(self.calls)
        self.capacity = Capacity(self.calls)
        self.gate = Gate(self.calls)

    def admission(self, mode: BillingMode = BillingMode.STRIPE) -> AgentAdmission:
        accounts = Resolver()
        gates = ApiBillingGates(
            mode,
            cast("EntitlementGate", self.gate),
            cast("QuotaReservationPort", self.capacity),
            cast("TenantAccountResolver", accounts),
        )
        keys = iter(f"key{i}" for i in range(100))
        return AgentAdmission(self.registry, gates, lambda: next(keys))


class Resolver:
    def resolve(self, tenant_id: str) -> str:
        return ACCOUNT


def test_agente_novo_reserva_capacidade_cria_e_consome() -> None:
    rig = Rig()

    admitted = rig.admission().admit(identity(), NOW)

    assert rig.calls == ["get_agent", "gate", "reserve", "create", "consume"]
    assert admitted.agent_id == "agent-1"
    command = rig.capacity.reserved[0]
    assert command.idempotency_key == "edge_agent#key0"
    assert command.request_hash == sha256(f"{ACCOUNT}\x1f354130\x1fagent-1".encode()).hexdigest()
    assert (command.kind, command.entitlement_version, command.limit) == (
        CapacityKind.AGENT, 7, 3,
    )
    assert rig.capacity.consumed[0].reservation_id == "res-1"
    assert rig.registry.owners == {"agent-1": "res-1"}


def test_criacao_e_cercada_pela_versao_de_entitlement_do_gate() -> None:
    rig = Rig()

    rig.admission().admit(identity(), NOW)

    (command,) = rig.registry.commands
    assert command.fence == EntitlementFence(ACCOUNT, 7)


def test_snapshot_alterado_na_criacao_libera_e_nega() -> None:
    rig = Rig()
    rig.registry.create_errors = [EntitlementDenied("reason=snapshot_changed")]

    with pytest.raises(EntitlementDenied, match="snapshot_changed"):
        rig.admission().admit(identity(), NOW)

    assert rig.calls == ["get_agent", "gate", "reserve", "create", "get_agent", "release"]
    assert rig.capacity.consumed == []


def test_rotacao_de_fingerprint_nao_reserva() -> None:
    rig = Rig()
    rig.registry.agents["agent-1"] = make_agent(fingerprint=OTHER_FINGERPRINT)

    admitted = rig.admission().admit(identity(), NOW)

    assert admitted.certificate_fingerprint == FINGERPRINT
    assert rig.calls == ["get_agent", "register"]


def test_request_comum_nao_chama_gate() -> None:
    rig = Rig()
    rig.registry.agents["agent-1"] = make_agent()

    rig.admission().admit(identity(), NOW)

    assert "gate" not in rig.calls
    assert rig.capacity.reserved == []


def test_agente_revogado_continua_conflict() -> None:
    rig = Rig()
    rig.registry.agents["agent-1"] = make_agent(state=AgentState.REVOKED)

    with pytest.raises(Conflict):
        rig.admission().admit(identity(), NOW)

    assert rig.calls == ["get_agent", "register"]


def test_disabled_chama_gate_sem_capacidade() -> None:
    rig = Rig()

    admitted = rig.admission(BillingMode.DISABLED).admit(identity(), NOW)

    assert rig.calls == ["get_agent", "gate", "register"]
    assert admitted.agent_id == "agent-1"


def test_sem_gates_mantem_upsert_legado() -> None:
    rig = Rig()

    admitted = AgentAdmission(rig.registry).admit(identity(), NOW)

    assert rig.calls == ["register"]
    assert admitted.agent_id == "agent-1"


def test_chave_de_reserva_padrao_e_unica() -> None:
    rig = Rig()
    gates = ApiBillingGates(
        BillingMode.STRIPE,
        cast("EntitlementGate", rig.gate),
        cast("QuotaReservationPort", rig.capacity),
        cast("TenantAccountResolver", Resolver()),
    )
    admission = AgentAdmission(rig.registry, gates)

    admission.admit(identity("a"), NOW)
    admission.admit(identity("b"), NOW)

    keys = [item.idempotency_key for item in rig.capacity.reserved]
    assert len(set(keys)) == 2
    assert all(key.startswith("edge_agent#") for key in keys)


def test_falha_de_escrita_sem_commit_libera_reserva() -> None:
    rig = Rig()
    rig.registry.create_errors = [RuntimeError("storage=down")]

    with pytest.raises(RuntimeError, match="storage=down"):
        rig.admission().admit(identity(), NOW)

    assert rig.calls == ["get_agent", "gate", "reserve", "create", "get_agent", "release"]
    assert rig.capacity.released[0].reason_code == "agent_registration_failed"
    assert rig.capacity.consumed == []


def test_contencao_na_criacao_vira_erro_retryable_e_nao_agent_revoked() -> None:
    rig = Rig()
    rig.registry.create_errors = [Conflict(ErrorCode.TRANSACTION_CONFLICT)]

    with pytest.raises(RetryableBillingError, match="agent_registration_contended"):
        rig.admission().admit(identity(), NOW)

    assert rig.calls == ["get_agent", "gate", "reserve", "create", "get_agent", "release"]


def test_falha_de_escrita_com_commit_proprio_consome() -> None:
    rig = Rig()
    rig.registry.create_errors = [RuntimeError("timeout=1")]
    rig.registry.commit_before_error = True

    admitted = rig.admission().admit(identity(), NOW)

    assert admitted.agent_id == "agent-1"
    assert rig.capacity.consumed[0].reservation_id == "res-1"
    assert rig.capacity.released == []
    assert rig.calls[-3:] == ["get_agent", "create", "consume"]


def test_agente_revogado_devolvido_pela_recuperacao_e_negado() -> None:
    rig = Rig()
    rig.registry.create_errors = [RuntimeError("timeout=1")]
    rig.registry.commit_before_error = True
    original = rig.registry._store

    def revoke_then_store(command):
        creation = original(command)
        revoked = creation.agent.model_copy(update={"state": AgentState.REVOKED})
        rig.registry.agents[command.agent_id] = revoked
        return EdgeAgentCreation(revoked, creation.created)

    rig.registry._store = revoke_then_store

    with pytest.raises(Conflict) as raised:
        rig.admission().admit(identity(), NOW)

    assert raised.value.code is ErrorCode.AGENT_REVOKED


def test_falha_de_escrita_com_agente_de_outro_libera() -> None:
    rig = Rig()
    rig.registry.create_errors = [RuntimeError("timeout=1")]
    rig.registry.agents["agent-1"] = make_agent()
    rig.registry.owners["agent-1"] = "res-other"
    rig.registry.hidden_reads = 1

    admitted = rig.admission().admit(identity(), NOW)

    assert admitted.agent_id == "agent-1"
    assert rig.capacity.consumed == []
    assert rig.capacity.released[0].reason_code == "agent_already_registered"
    assert rig.calls[-4:] == ["get_agent", "create", "release", "register"]


def test_segunda_falha_nao_libera() -> None:
    rig = Rig()
    rig.registry.create_errors = [RuntimeError("one=1"), RuntimeError("two=2")]
    rig.registry.commit_before_error = True

    with pytest.raises(RuntimeError, match="two=2"):
        rig.admission().admit(identity(), NOW)

    assert rig.capacity.released == []
    assert rig.capacity.consumed == []


def test_agente_criado_por_outro_pedido_libera_reserva_e_faz_upsert() -> None:
    rig = Rig()
    real_create = rig.registry.create_edge_agent

    def create_after_rival(command):
        rig.registry.agents["agent-1"] = make_agent(fingerprint=OTHER_FINGERPRINT)
        rig.registry.owners["agent-1"] = "res-rival"
        return real_create(command)

    rig.registry.create_edge_agent = create_after_rival

    admitted = rig.admission().admit(identity(), NOW)

    assert admitted.certificate_fingerprint == FINGERPRINT
    assert rig.capacity.consumed == []
    assert rig.capacity.released[0].reason_code == "agent_already_registered"
    assert rig.calls[-2:] == ["release", "register"]


def test_conta_ausente_em_stripe_falha_fechado_antes_do_gate() -> None:
    rig = Rig()
    gates = ApiBillingGates(
        BillingMode.STRIPE,
        cast("EntitlementGate", rig.gate),
        cast("QuotaReservationPort", rig.capacity),
        TenantAccountResolver(BillingMode.STRIPE),
    )

    with pytest.raises(BillingAccountMissing):
        AgentAdmission(rig.registry, gates).admit(identity(), NOW)

    assert rig.calls == ["get_agent"]


def test_negacao_do_gate_nao_reserva() -> None:
    rig = Rig()
    rig.gate.error = EntitlementDenied("reason=blocked")

    with pytest.raises(EntitlementDenied):
        rig.admission().admit(identity(), NOW)

    assert rig.calls == ["get_agent", "gate"]
    assert rig.registry.agents == {}


def test_quota_excedida_nao_cria_agente() -> None:
    rig = Rig()
    rig.capacity.reserve_error = QuotaExceeded("quota=agents")

    with pytest.raises(QuotaExceeded):
        rig.admission().admit(identity(), NOW)

    assert rig.calls == ["get_agent", "gate", "reserve"]
    assert rig.registry.agents == {}


def test_falha_de_consumo_propaga_sem_liberar() -> None:
    rig = Rig()
    rig.capacity.consume_error = RuntimeError("consume=down")

    with pytest.raises(RuntimeError, match="consume=down"):
        rig.admission().admit(identity(), NOW)

    assert "agent-1" in rig.registry.agents
    assert rig.capacity.released == []


def test_disputa_pela_ultima_vaga_cria_um_unico_agente() -> None:
    rig = Rig()
    rig.capacity.limit = 1
    rig.capacity.barrier = Barrier(2)
    admission = rig.admission()
    outcomes: list[object] = []

    def run(agent_id: str) -> None:
        try:
            outcomes.append(admission.admit(identity(agent_id), NOW))
        except QuotaExceeded as error:
            outcomes.append(error)

    threads = [Thread(target=run, args=(name,)) for name in ("agent-a", "agent-b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sum(isinstance(item, Agent) for item in outcomes) == 1
    assert sum(isinstance(item, QuotaExceeded) for item in outcomes) == 1
    assert len(rig.registry.agents) == 1
