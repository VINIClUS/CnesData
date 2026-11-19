"""Ledger de tickets, seal, checagem viva antes do start e facade do control plane."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.control_plane.environment_gate import PermitHolder
from cnes_domain.orchestration.mode_canary import ModeCanaryBinding, SyntheticContextRequired
from cnes_infra.aws.mode_canary import (
    GENESIS_LEDGER_SHA256,
    ModeCanaryContext,
    canonical_text,
    initialize_control,
    start_effect_id,
)
from cnes_infra.control_plane.dynamodb_keys import run_partition, tenant_partition
from packages.cnes_domain.tests.orchestration.test_mode_canary_records import (
    CANARY_ID,
    _control,
    _promotion,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    _create_table,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

_TENANT = f"prod-canary-{CANARY_ID}"
_RUN = "unit-aaaaaaaaaaaaaaaa"
_DISPATCH = "0123456789abcdef"
_HOLDER = PermitHolder(_TENANT, _RUN, _DISPATCH)
_NOW = datetime(2026, 11, 12, 12, 10, tzinfo=UTC)
_OWNER = "a" * 64
_UNITS = ("u1", "u2")


@dataclass
class _Dispatches:
    dispatch_id: str = _DISPATCH
    unit_ids: tuple[str, ...] = _UNITS

    def get_run_dispatch(self, tenant_id: str, run_id: str) -> object:
        return SimpleNamespace(dispatch_id=self.dispatch_id, unit_ids=self.unit_ids)


@dataclass
class _Ledger:
    client: Any
    dispatches: _Dispatches = field(default_factory=_Dispatches)
    record: dict[str, Any] = field(default_factory=_promotion)

    def seed(self, **control: Any) -> None:
        self._put(f"PROD_CANARY#{_OWNER}#unit", canonical_text(self.record))
        self._put(f"PROD_CANARY_CONTROL#{_OWNER}", canonical_text(_control() | control))

    def _put(self, sort_key: str, text: str) -> None:
        self.client.put_item(TableName=_TABLE_NAME, Item={
            "pk": {"S": tenant_partition(_TENANT)}, "sk": {"S": sort_key},
            "entity": {"S": "MODECANARY"}, "payload": {"S": text},
        })

    @property
    def record_sha(self) -> str:
        return sha256(canonical_text(self.record).encode()).hexdigest()

    def context(self) -> ModeCanaryContext:
        binding = ModeCanaryBinding(_TENANT, (_RUN,), _OWNER, self.record_sha, "unit", "promotion")
        return ModeCanaryContext(self.client, _TABLE_NAME, binding, self.dispatches)

    def item(self, pk: str, sk: str) -> dict[str, Any] | None:
        found = self.client.get_item(
            TableName=_TABLE_NAME, Key={"pk": {"S": pk}, "sk": {"S": sk}}, ConsistentRead=True,
        ).get("Item")
        return None if found is None else json.loads(found["payload"]["S"])

    def control(self) -> dict[str, Any]:
        return cast("dict[str, Any]", self.item(
            tenant_partition(_TENANT), f"PROD_CANARY_CONTROL#{_OWNER}"))

    def ticket(self, effect_id: str) -> dict[str, Any] | None:
        return self.item(tenant_partition(_TENANT), f"PROD_CANARY_EFFECT#{_OWNER}#{effect_id}")


@pytest.fixture
def ledger() -> Iterator[_Ledger]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        yield _Ledger(client)


def _effect(ledger: _Ledger) -> str:
    return start_effect_id(ledger.record_sha, _HOLDER, _UNITS)


def test_admissao_grava_ticket_binding_e_contador_na_mesma_transacao(ledger: _Ledger) -> None:
    ledger.seed()

    ledger.context().binding_for(_HOLDER, _NOW)

    ticket = cast("dict[str, Any]", ledger.ticket(_effect(ledger)))
    assert ticket["state"] == "PENDING"
    assert ticket["effect_kind"] == "start-execution"
    assert ticket["receipt_sha256"] is None
    control = ledger.control()
    assert control["admitted_counts"]["unit_tasks"] == len(_UNITS)
    expected = sha256(canonical_text({
        "previous_ledger_sha256": _control()["ledger_sha256"],
        "ticket_sha256": sha256(canonical_text(ticket).encode()).hexdigest(),
    }).encode()).hexdigest()
    assert control["ledger_sha256"] == expected
    binding = ledger.item(run_partition(_TENANT, _RUN), f"PROD_CANARY#{_DISPATCH}")
    assert binding == {
        "context_kind": "promotion", "dispatch_id": _DISPATCH, "owner_sha256": _OWNER,
        "phase": "unit", "record_sha256": ledger.record_sha, "run_id": _RUN,
        "schema": "cnesdata.mode-canary-binding/v1", "tenant_id": _TENANT,
    }


def test_reentrada_reutiliza_ticket_sem_gastar_contador(ledger: _Ledger) -> None:
    ledger.seed()
    context = ledger.context()
    context.binding_for(_HOLDER, _NOW)
    before = ledger.control()

    context.binding_for(_HOLDER, _NOW + timedelta(seconds=5))

    assert ledger.control() == before


def test_reentrada_com_ticket_existente_apos_o_prazo_recusa(ledger: _Ledger) -> None:
    ledger.seed()
    context = ledger.context()
    context.binding_for(_HOLDER, _NOW)
    before = ledger.control()

    with pytest.raises(SyntheticContextRequired, match="reason=context_expired"):
        context.binding_for(_HOLDER, datetime(2026, 11, 12, 12, 30, tzinfo=UTC))

    assert ledger.control() == before


def test_seal_recusa_relogio_sem_fuso_utc(ledger: _Ledger) -> None:
    ledger.seed()

    with pytest.raises(SyntheticContextRequired, match="reason=clock_not_utc"):
        ledger.context().seal(datetime(2026, 11, 12, 12, 10))  # noqa: DTZ001

    assert ledger.control()["admission_state"] == "ACTIVE"


def test_contador_esgotado_recusa_antes_de_qualquer_ticket(ledger: _Ledger) -> None:
    ledger.seed(admitted_counts={"unit_tasks": 7, "recovery_resumes": 0, "recovery_hangs": 0})

    with pytest.raises(SyntheticContextRequired, match="reason=budget_exhausted"):
        ledger.context().binding_for(_HOLDER, _NOW)

    assert ledger.ticket(_effect(ledger)) is None


def test_dispatch_divergente_do_holder_recusa(ledger: _Ledger) -> None:
    ledger.seed()
    ledger.dispatches.dispatch_id = "fedcba9876543210"

    with pytest.raises(SyntheticContextRequired, match="reason=dispatch_mismatch"):
        ledger.context().binding_for(_HOLDER, _NOW)


def test_seal_antes_release_rejeita_novo_ticket_e_preserva_pending(ledger: _Ledger) -> None:
    ledger.seed()
    context = ledger.context()
    context.binding_for(_HOLDER, _NOW)

    context.seal(_NOW)
    context.seal(_NOW + timedelta(seconds=1))

    control = ledger.control()
    assert control["admission_state"] == "SEALED"
    assert control["sealed_at"] == "2026-11-12T12:10:00+00:00"
    assert cast("dict[str, Any]", ledger.ticket(_effect(ledger)))["state"] == "PENDING"
    ledger.dispatches.dispatch_id = "fedcba9876543210"
    with pytest.raises(SyntheticContextRequired, match="reason=control_sealed"):
        context.binding_for(PermitHolder(_TENANT, _RUN, "fedcba9876543210"), _NOW)
    context.settle(_effect(ledger), "c" * 64)
    settled = cast("dict[str, Any]", ledger.ticket(_effect(ledger)))
    assert (settled["state"], settled["receipt_sha256"]) == ("SETTLED", "c" * 64)
    context.settle(_effect(ledger), "c" * 64)
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_settled"):
        context.settle(_effect(ledger), "d" * 64)


def test_callback_unit_tardio_nao_starta_apos_seal(ledger: _Ledger) -> None:
    ledger.seed()
    context = ledger.context()
    context.binding_for(_HOLDER, _NOW)
    context.require_live(_effect(ledger), _NOW)

    context.seal(_NOW)

    with pytest.raises(SyntheticContextRequired, match="reason=control_sealed"):
        context.require_live(_effect(ledger), _NOW)


def test_require_live_recusa_ticket_ausente_vencido_ou_liquidado(ledger: _Ledger) -> None:
    ledger.seed()
    context = ledger.context()
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_missing"):
        context.require_live(_effect(ledger), _NOW)
    context.binding_for(_HOLDER, _NOW)
    with pytest.raises(SyntheticContextRequired, match="reason=context_expired"):
        context.require_live(_effect(ledger), datetime(2026, 11, 12, 12, 30, tzinfo=UTC))
    context.settle(_effect(ledger), "c" * 64)
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_settled"):
        context.require_live(_effect(ledger), _NOW)


def test_ticket_pendente_marcado_unknown_continua_no_ledger(ledger: _Ledger) -> None:
    ledger.seed()
    context = ledger.context()
    context.binding_for(_HOLDER, _NOW)

    context.mark_unknown(_effect(ledger))

    assert cast("dict[str, Any]", ledger.ticket(_effect(ledger)))["state"] == "UNKNOWN"
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_unknown"):
        context.require_live(_effect(ledger), _NOW)


class _Contended:
    def __init__(self, client: Any, failures: int) -> None:
        self._client = client
        self.failures = failures

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def transact_write_items(self, **kwargs: Any) -> Any:
        if self.failures:
            self.failures -= 1
            error = {"Error": {"Code": "TransactionCanceledException", "Message": "x"}}
            raise ClientError(cast("Any", error), "TransactWriteItems")
        return self._client.transact_write_items(**kwargs)


def test_corrida_no_control_reintenta_e_desiste_sem_ticket(ledger: _Ledger) -> None:
    ledger.seed()
    ledger.client = _Contended(ledger.client, 1)
    ledger.context().binding_for(_HOLDER, _NOW)
    assert ledger.ticket(_effect(ledger)) is not None
    other = PermitHolder(_TENANT, _RUN, "fedcba9876543210")
    ledger.dispatches.dispatch_id = "fedcba9876543210"
    ledger.client = _Contended(ledger.client, 3)

    with pytest.raises(SyntheticContextRequired, match="reason=ledger_contended"):
        ledger.context().binding_for(other, _NOW)


def test_initialize_control_e_insert_only_com_replay_identico(ledger: _Ledger) -> None:
    control = _control() | {"ledger_sha256": GENESIS_LEDGER_SHA256}
    initialize_control(ledger.client, _TABLE_NAME, _TENANT, control)
    initialize_control(ledger.client, _TABLE_NAME, _TENANT, control)

    assert ledger.control() == control
    assert sha256(b"[]\n").hexdigest() == GENESIS_LEDGER_SHA256
    with pytest.raises(SyntheticContextRequired, match="reason=control_conflict"):
        initialize_control(ledger.client, _TABLE_NAME, _TENANT, control | {"generation": 4})
    counted = {"unit_tasks": 1, "recovery_resumes": 0, "recovery_hangs": 0}
    for broken in (control | {"extra": 1}, control | {"admitted_counts": counted},
                   control | {"ledger_sha256": "c" * 64},
                   control | {"admission_state": "SEALED", "sealed_at": "2026-11-12T12:05:00Z"}):
        with pytest.raises(SyntheticContextRequired, match="reason=control_invalid"):
            initialize_control(ledger.client, _TABLE_NAME, _TENANT, broken)
    with pytest.raises(SyntheticContextRequired, match="reason=tenant_invalid"):
        initialize_control(ledger.client, _TABLE_NAME, "354130", control)


def test_efeitos_do_lancador_consomem_o_contador_do_tipo(ledger: _Ledger) -> None:
    ledger.seed()
    context = ledger.context()

    recovery = context.admit_effect("run-recovery", "c" * 64, "d" * 64, _NOW)
    context.admit_effect("run-recovery", "c" * 64, "d" * 64, _NOW)
    context.admit_effect("native-mutation", "e" * 64, "d" * 64, _NOW)

    counts = ledger.control()["admitted_counts"]
    assert counts == {"unit_tasks": 0, "recovery_resumes": 1, "recovery_hangs": 0}
    assert cast("dict[str, Any]", ledger.ticket(recovery))["effect_kind"] == "run-recovery"
    context.admit_effect("run-hang", "f" * 64, "d" * 64, _NOW)
    with pytest.raises(SyntheticContextRequired, match="reason=budget_exhausted"):
        context.admit_effect("run-hang", "0" * 64, "d" * 64, _NOW)
    with pytest.raises(SyntheticContextRequired, match="reason=effect_kind_invalid"):
        context.admit_effect("shell", "1" * 64, "d" * 64, _NOW)
    assert context.binding.tenant_id == _TENANT


def test_ticket_adulterado_na_mesma_chave_e_recusado(ledger: _Ledger) -> None:
    ledger.seed()
    context = ledger.context()
    effect = context.admit_effect("native-mutation", "c" * 64, "d" * 64, _NOW)
    key = f"PROD_CANARY_EFFECT#{_OWNER}#{effect}"
    stored = cast("dict[str, Any]", ledger.ticket(effect))
    ledger._put(key, canonical_text(stored | {"request_sha256": "9" * 64}))
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_conflict"):
        context.admit_effect("native-mutation", "c" * 64, "d" * 64, _NOW)
    ledger._put(key, json.dumps(stored))
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_not_canonical"):
        context.settle(effect, "c" * 64)
    ledger._put(key, canonical_text(stored | {"state": "LOST"}))
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_invalid"):
        context.require_live(effect, _NOW)
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_missing"):
        context.mark_unknown("0" * 64)


def test_seal_sem_control_ou_de_outro_owner_falha_fechado(ledger: _Ledger) -> None:
    with pytest.raises(SyntheticContextRequired, match="reason=control_missing"):
        ledger.context().seal(_NOW)
    ledger._put(f"PROD_CANARY_CONTROL#{_OWNER}", canonical_text(_control() | {
        "owner_sha256": "c" * 64}))
    with pytest.raises(SyntheticContextRequired, match="reason=control_mismatch"):
        ledger.context().seal(_NOW)


class _Racing:
    def __init__(self, client: Any, code: str) -> None:
        self._client = client
        self.code = code

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def _fail(self, operation: str) -> Any:
        raise ClientError(cast("Any", {"Error": {"Code": self.code, "Message": "x"}}), operation)

    def put_item(self, **kwargs: Any) -> Any:
        if "ConditionExpression" in kwargs:
            return self._fail("PutItem")
        return self._client.put_item(**kwargs)

    def transact_write_items(self, **kwargs: Any) -> Any:
        return self._fail("TransactWriteItems")


def test_cas_perdido_desiste_e_erro_de_servico_propaga(ledger: _Ledger) -> None:
    ledger.seed()
    context = ledger.context()
    effect = context.admit_effect("native-mutation", "c" * 64, "d" * 64, _NOW)
    raw = ledger.client
    ledger.client = _Racing(raw, "ConditionalCheckFailedException")
    with pytest.raises(SyntheticContextRequired, match="reason=ledger_contended"):
        ledger.context().seal(_NOW)
    with pytest.raises(SyntheticContextRequired, match="reason=ledger_contended"):
        ledger.context().settle(effect, "c" * 64)
    ledger.client = _Racing(raw, "AccessDeniedException")
    with pytest.raises(ClientError):
        ledger.context().seal(_NOW)
    with pytest.raises(ClientError):
        ledger.context().admit_effect("native-mutation", "e" * 64, "d" * 64, _NOW)


def test_runtime_bounds_devolve_menor_prazo_e_fault_do_record_ativo(ledger: _Ledger) -> None:
    ledger.record = ledger.record | {"fault": "hang_no_dispatch", "scenario": "recovery_deadline"}
    ledger.seed()
    context = ledger.context()

    bounds = context.runtime_bounds()

    assert bounds.deadline == datetime(2026, 11, 12, 12, 30, tzinfo=UTC)
    assert bounds.fault == "hang_no_dispatch"
    context.seal(_NOW)
    with pytest.raises(SyntheticContextRequired, match="reason=control_sealed"):
        context.runtime_bounds()


class _LosingSeal:
    def __init__(self, client: Any, losses: int) -> None:
        self._client = client
        self.losses = losses

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def put_item(self, **kwargs: Any) -> Any:
        if self.losses and "ConditionExpression" in kwargs:
            self.losses -= 1
            error = {"Error": {"Code": "ConditionalCheckFailedException", "Message": "x"}}
            raise ClientError(cast("Any", error), "PutItem")
        return self._client.put_item(**kwargs)


def test_seal_insiste_enquanto_admissoes_concorrentes_mudam_o_control(ledger: _Ledger) -> None:
    ledger.seed()
    ledger.client = _LosingSeal(ledger.client, 12)

    ledger.context().seal(_NOW)

    assert ledger.control()["admission_state"] == "SEALED"


class _ConcurrentWriter:
    def __init__(self, ledger: _Ledger) -> None:
        self._ledger = ledger
        self._client = ledger.client
        self.pending = True

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def transact_write_items(self, **kwargs: Any) -> Any:
        if self.pending:
            self.pending = False
            control = self._ledger.control()
            control["admitted_counts"]["recovery_resumes"] += 1
            control["ledger_sha256"] = "c" * 64
            self._ledger._put(f"PROD_CANARY_CONTROL#{_OWNER}", canonical_text(control))
        return self._client.transact_write_items(**kwargs)


def test_admissao_nao_sobrescreve_control_alterado_por_outra_task(ledger: _Ledger) -> None:
    ledger.seed()
    ledger.client = _ConcurrentWriter(ledger)

    effect = ledger.context().admit_effect("run-recovery", "c" * 64, "d" * 64, _NOW)

    control = ledger.control()
    assert control["admitted_counts"]["recovery_resumes"] == 2
    ticket = canonical_text(cast("dict[str, Any]", ledger.ticket(effect)))
    assert control["ledger_sha256"] == sha256(canonical_text({
        "previous_ledger_sha256": "c" * 64,
        "ticket_sha256": sha256(ticket.encode()).hexdigest(),
    }).encode()).hexdigest()
