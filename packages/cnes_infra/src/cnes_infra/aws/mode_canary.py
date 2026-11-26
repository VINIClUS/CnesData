"""Contexto sintético de produção: record/control fortes, ledger de tickets e facades."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Protocol, cast

from botocore.exceptions import ClientError

from cnes_domain.orchestration.mode_canary import (
    ModeCanaryBinding,
    ModeCanaryControl,
    ModeCanaryGrant,
    ModeCanaryRecord,
    is_canonical_mode_canary_tenant,
)
from cnes_infra.aws.mode_canary_frames import (
    ATTEMPTS,
    CONTROL_ENTITY,
    GENESIS_LEDGER_SHA256,
    INSERT_ONLY,
    SWAP,
    TENANT_PREFIX,
    Effect,
    LedgerState,
    admission,
    canonical_text,
    control_key,
    digest,
    dispatch_effect,
    effect_id,
    ledger_effect,
    parse_control,
    parse_mapping,
    parse_ticket,
    put,
    record_key,
    refuse,
    require_bound,
    require_control,
    stamp,
    start_request,
    ticket_key,
    transition,
)
from cnes_infra.control_plane.dynamodb_keys import tenant_partition

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from cnes_domain.control_plane.environment_gate import PermitHolder
    from cnes_domain.orchestration.mode_canary import SyntheticEffectTicket
    from cnes_domain.ports.processing import StartRunExecution

_SEAL_ATTEMPTS: Final = 64


def start_effect_id(
    record_sha256: str, holder: PermitHolder | StartRunExecution, unit_ids: Sequence[str],
) -> str:
    """Returns: effect_id do start nativo ligado ao record e ao request do dispatch."""
    return effect_id(record_sha256, "start-execution", start_request(holder, unit_ids))


class DispatchReader(Protocol):
    """Leitura forte do dispatch nativo reservado para o run."""

    def get_run_dispatch(self, tenant_id: str, run_id: str) -> Any:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class RuntimeBounds:
    """Teto absoluto e falha autenticados do contexto ativo."""

    deadline: datetime
    fault: str


def _cancelled(error: ClientError) -> bool:
    code = error.response.get("Error", {}).get("Code")
    return code in {"TransactionCanceledException", "ConditionalCheckFailedException"}


class _Store:
    def __init__(self, client: Any, table_name: str, tenant_id: str) -> None:
        self.client = client
        self.table = table_name
        self.pk = tenant_partition(tenant_id)

    def payload(self, sort_key: str) -> str | None:
        key = {"pk": {"S": self.pk}, "sk": {"S": sort_key}}
        item = self.client.get_item(TableName=self.table, Key=key, ConsistentRead=True).get("Item")
        return None if item is None else cast("str", item["payload"]["S"])

    def write(self, request: dict[str, Any]) -> bool:
        try:
            self.client.put_item(**request)
        except ClientError as error:
            if not _cancelled(error):
                raise
            return False
        return True

    def transact(self, items: list[dict[str, Any]]) -> bool:
        try:
            self.client.transact_write_items(TransactItems=items)
        except ClientError as error:
            if not _cancelled(error):
                raise
            return False
        return True

    def control_put(self, sort_key: str, text: str, old: str | None) -> dict[str, Any]:
        item = {"pk": self.pk, "sk": sort_key, "entity": CONTROL_ENTITY, "payload": text}
        if old is None:
            return cast("dict[str, Any]", put(self.table, item, INSERT_ONLY, None)["Put"])
        return cast("dict[str, Any]", put(self.table, item, SWAP, {":old": {"S": old}})["Put"])


def initialize_control(client: Any, table_name: str, tenant_id: str, control: dict[str, Any]):
    """Grava o control ACTIVE inicial, insert-only; replay só com bytes idênticos.

    Raises: SyntheticContextRequired: Tenant, control inválido ou divergente do gravado.
    """
    if not is_canonical_mode_canary_tenant(tenant_id):
        raise refuse("tenant_invalid")
    try:
        parsed = ModeCanaryControl.from_mapping(control)
    except ValueError as error:
        raise refuse("control_invalid") from error
    initial = (parsed.admission_state == "ACTIVE" and not any(parsed.admitted_counts.values())
               and parsed.ledger_sha256 == GENESIS_LEDGER_SHA256)
    if not initial:
        raise refuse("control_invalid")
    store = _Store(client, table_name, tenant_id)
    sort_key = control_key(parsed.owner_sha256)
    text = canonical_text(control)
    if store.write(store.control_put(sort_key, text, None)):
        return
    if store.payload(sort_key) != text:
        raise refuse("control_conflict")


def require_seeded_control(
    client: Any, table_name: str, record: ModeCanaryRecord,
) -> ModeCanaryControl:
    """Returns: Control ACTIVE do owner lido forte, antes de qualquer escrita do recovery-seed.

    Raises: SyntheticContextRequired: control_missing, control_mismatch ou control_sealed.
    """
    store = _Store(client, table_name, f"{TENANT_PREFIX}{record.canary_id}")
    return require_control(store.payload(control_key(record.owner_sha256)), record)


class ModeCanaryContext:
    """`ModeCanaryAdmissionProvider` e ledger de efeitos sobre record e control do owner."""

    def __init__(
        self, client: Any, table_name: str, binding: ModeCanaryBinding, dispatches: DispatchReader,
    ) -> None:
        self._store = _Store(client, table_name, binding.tenant_id)
        self._binding = binding
        self._dispatches = dispatches

    @property
    def binding(self) -> ModeCanaryBinding:
        """Returns: Locator validado do contexto."""
        return self._binding

    def binding_for(self, holder: PermitHolder, now: datetime) -> ModeCanaryGrant:
        """Returns: Grant do holder depois de admitir o ticket do start do seu dispatch.

        Raises:
            SyntheticContextRequired: Holder fora do binding, record ou control ausente,
                adulterado, divergente, selado, vencido, dispatch divergente ou orçamento esgotado.
        """
        if not self._binding.permits(holder.tenant_id, holder.run_id):
            raise refuse("run_not_bound")
        record = self._record()
        require_bound(self._binding, record, holder)
        dispatch = self._dispatches.get_run_dispatch(holder.tenant_id, holder.run_id)
        if dispatch is None or dispatch.dispatch_id != holder.dispatch_id:
            raise refuse("dispatch_mismatch")
        units = tuple(cast("Sequence[str]", dispatch.unit_ids))
        self._admit(record, dispatch_effect(self._binding, record, holder, units), now)
        return ModeCanaryGrant(record.promotion_id)

    def admit_effect(
        self, kind: str, request_sha256: str, target_sha256: str, now: datetime,
    ) -> str:
        """Returns: effect_id do ticket admitido (ou reutilizado) antes do efeito sintético.

        Raises: SyntheticContextRequired: Tipo fora do enum, contexto inválido, selado, vencido
            ou orçamento esgotado.
        """
        effect = ledger_effect(kind, request_sha256, target_sha256)
        return self._admit(self._record(), effect, now)

    def runtime_bounds(self) -> RuntimeBounds:
        """Returns: Menor prazo entre record e control e o fault do record.

        Raises: SyntheticContextRequired: Record ou control ausente, divergente ou selado.
        """
        record = self._record()
        control = self._control(record)
        deadline = min(record.expires_at, record.canary_deadline, control.deadline)
        return RuntimeBounds(deadline, record.fault)

    def require_live(self, effect_id: str, now: datetime) -> None:
        """Raises: SyntheticContextRequired: Control selado/vencido ou ticket não pendente."""
        record = self._record()
        control = self._control(record)
        ticket = self._ticket(effect_id)
        if ticket is None:
            raise refuse("ticket_missing")
        if ticket.state != "PENDING":
            raise refuse(f"ticket_{ticket.state.lower()}")
        if now >= min(ticket.deadline, control.deadline, record.canary_deadline):
            raise refuse("context_expired")

    def settle(self, effect_id: str, receipt_sha256: str) -> None:
        """Liquida o ticket com o recibo original; permitido depois do seal.

        Raises: SyntheticContextRequired: Ticket ausente ou já liquidado com outro recibo.
        """
        self._transition(effect_id, ("SETTLED", receipt_sha256))

    def mark_unknown(self, effect_id: str) -> None:
        """Raises: SyntheticContextRequired: Ticket ausente ou já liquidado."""
        self._transition(effect_id, ("UNKNOWN", None))

    def seal(self, now: datetime) -> None:
        """Sela a admissão ACTIVE→SEALED por CAS, insistindo contra admissões concorrentes.

        Raises: SyntheticContextRequired: Control ausente, inválido ou de outro owner.
        """
        sort_key = control_key(self._binding.owner_sha256)
        for _ in range(_SEAL_ATTEMPTS):
            text = self._store.payload(sort_key)
            if text is None:
                raise refuse("control_missing")
            control = parse_control(text)
            if control.owner_sha256 != self._binding.owner_sha256:
                raise refuse("control_mismatch")
            if control.admission_state == "SEALED":
                return
            sealed = parse_mapping(text, "control") | {"admission_state": "SEALED",
                                                      "sealed_at": stamp(now)}
            if self._store.write(self._store.control_put(sort_key, canonical_text(sealed), text)):
                return
        raise refuse("ledger_contended")

    def _ticket_text(self, effect_id: str) -> str | None:
        return self._store.payload(ticket_key(self._binding.owner_sha256, effect_id))

    def _ticket(self, effect_id: str) -> SyntheticEffectTicket | None:
        text = self._ticket_text(effect_id)
        return None if text is None else parse_ticket(text)

    def _transition(self, effect_id: str, change: tuple[str, str | None]) -> None:
        for _ in range(ATTEMPTS):
            ticket = (effect_id, self._ticket_text(effect_id))
            request = transition(self._store.table, self._binding, ticket, change)
            if request is None or self._store.write(request):
                return
        raise refuse("ledger_contended")

    def _admit(self, record: ModeCanaryRecord, effect: Effect, now: datetime) -> str:
        identifier = effect_id(self._binding.record_sha256, effect.kind, effect.request_sha256)
        owner = self._binding.owner_sha256
        for _ in range(ATTEMPTS):
            control = self._store.payload(control_key(owner))
            require_control(control, record)
            state = LedgerState(self._binding, record, control, self._ticket_text(identifier))
            plan = admission(self._store.table, state, effect, now)
            if plan.items is None or self._store.transact(plan.items):
                return plan.effect_id
        raise refuse("ledger_contended")

    def _record(self) -> ModeCanaryRecord:
        binding = self._binding
        text = self._store.payload(record_key(binding.owner_sha256, binding.phase))
        if text is None:
            raise refuse("record_missing")
        if digest(text) != binding.record_sha256:
            raise refuse("record_hash_mismatch")
        mapping = parse_mapping(text, "record")
        try:
            record = ModeCanaryRecord.from_mapping(mapping)
        except ValueError as error:
            raise refuse("record_invalid") from error
        if (
            record.owner_sha256 != binding.owner_sha256
            or record.phase != binding.phase
            or record.context_kind != binding.context_kind
            or binding.tenant_id != f"{TENANT_PREFIX}{record.canary_id}"
        ):
            raise refuse("record_mismatch")
        return record

    def _control(self, record: ModeCanaryRecord) -> ModeCanaryControl:
        return require_control(self._store.payload(control_key(self._binding.owner_sha256)),
                               record)


__all__ = [
    "GENESIS_LEDGER_SHA256",
    "DispatchReader",
    "ModeCanaryContext",
    "RuntimeBounds",
    "canonical_text",
    "initialize_control",
    "require_seeded_control",
    "start_effect_id",
]
