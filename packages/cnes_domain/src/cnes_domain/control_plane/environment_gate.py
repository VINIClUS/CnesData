"""Fence de promoção e semáforo de unidade do ambiente: entidades, comandos e transições puras."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from typing import TypeGuard

_ENVIRONMENT = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
_DISPATCH_ID = re.compile(r"^[0-9a-f]{16}$")


class FenceState(StrEnum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class AcquisitionStatus(StrEnum):
    ACQUIRED = "ACQUIRED"
    REENTERED = "REENTERED"
    FENCE_CLOSED = "FENCE_CLOSED"
    HELD = "HELD"


def require_environment(value: object) -> str:
    """Valida o nome do ambiente, que entra em texto puro nas chaves do gate.

    Args: value: Nome do ambiente.
    Returns: O nome validado.
    Raises: ValueError: Nome fora de `^[a-z][a-z0-9-]{0,31}$`.
    """
    if not isinstance(value, str) or not _ENVIRONMENT.fullmatch(value):
        raise ValueError("reason=invalid_environment field=environment")
    return value


def _require_text(value: object, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"reason=blank_value field={name}")


def _optional_text(value: object, name: str) -> None:
    if value is not None:
        _require_text(value, name)


def _require_utc(value: object, name: str) -> None:
    if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
        raise ValueError(f"reason=datetime_not_utc field={name}")


def _optional_utc(value: object, name: str) -> None:
    if value is not None:
        _require_utc(value, name)


def _require_at_least(value: object, name: str, minimum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"reason=out_of_range field={name}")


def _require_environment_match(expected: str, actual: str) -> None:
    if expected != actual:
        raise ValueError("reason=environment_mismatch")


def _require_token_command(environment: str, token: int, now: datetime) -> None:
    require_environment(environment)
    _require_at_least(token, "token", 1)
    _require_utc(now, "now")


def _require_fence_command(environment: str, promotion_id: str, now: datetime) -> None:
    require_environment(environment)
    _require_text(promotion_id, "promotion_id")
    _require_utc(now, "now")


@dataclass(frozen=True, slots=True)
class PromotionFence:
    """Fence de promoção; item ausente equivale a aberto na versão 0."""

    environment: str
    state: FenceState
    promotion_id: str | None
    version: int
    changed_at: datetime | None

    def __post_init__(self) -> None:
        require_environment(self.environment)
        _optional_text(self.promotion_id, "promotion_id")
        _require_at_least(self.version, "version", 0)
        _optional_utc(self.changed_at, "changed_at")
        if self.state is FenceState.CLOSED and self.promotion_id is None:
            raise ValueError("reason=closed_fence_requires_promotion")


@dataclass(frozen=True, slots=True)
class PermitHolder:
    """Dispatch dono do permit; o dispatch_id é o nome determinístico da execução."""

    tenant_id: str
    run_id: str
    dispatch_id: str

    def __post_init__(self) -> None:
        _require_text(self.tenant_id, "tenant_id")
        _require_text(self.run_id, "run_id")
        if not _DISPATCH_ID.fullmatch(self.dispatch_id):
            raise ValueError("reason=invalid_hex16 field=dispatch_id")


@dataclass(frozen=True, slots=True)
class UnitPermit:
    """Permit da única unidade do ambiente; o token, não o lease, é a autoridade."""

    environment: str
    token: int
    holder: PermitHolder
    execution_ref: str | None
    acquired_at: datetime
    lease_until: datetime
    canary_promotion_id: str | None

    def __post_init__(self) -> None:
        require_environment(self.environment)
        _require_at_least(self.token, "token", 1)
        _optional_text(self.execution_ref, "execution_ref")
        _require_utc(self.acquired_at, "acquired_at")
        _require_utc(self.lease_until, "lease_until")
        _optional_text(self.canary_promotion_id, "canary_promotion_id")


@dataclass(frozen=True, slots=True)
class EnvironmentGateState:
    """Leitura fortemente consistente do fence e do semáforo do ambiente."""

    environment: str
    fence: PromotionFence
    permit: UnitPermit | None

    def __post_init__(self) -> None:
        require_environment(self.environment)
        _require_environment_match(self.environment, self.fence.environment)
        if self.permit is not None:
            _require_environment_match(self.environment, self.permit.environment)


@dataclass(frozen=True, slots=True)
class PermitAcquisition:
    """Resultado do acquire; fora de ACQUIRED/REENTERED, o permit é o do detentor atual."""

    status: AcquisitionStatus
    permit: UnitPermit | None

    def __post_init__(self) -> None:
        if self.permit is None and self.status is not AcquisitionStatus.FENCE_CLOSED:
            raise ValueError("reason=acquisition_requires_permit")


@dataclass(frozen=True, slots=True)
class AcquireUnitPermit:
    environment: str
    holder: PermitHolder
    now: datetime
    lease_seconds: int
    canary_promotion_id: str | None = None

    def __post_init__(self) -> None:
        require_environment(self.environment)
        _require_utc(self.now, "now")
        _require_at_least(self.lease_seconds, "lease_seconds", 1)
        _optional_text(self.canary_promotion_id, "canary_promotion_id")

    @property
    def lease_until(self) -> datetime:
        return self.now + timedelta(seconds=self.lease_seconds)


@dataclass(frozen=True, slots=True)
class BindUnitPermit:
    environment: str
    token: int
    execution_ref: str
    now: datetime

    def __post_init__(self) -> None:
        _require_token_command(self.environment, self.token, self.now)
        _require_text(self.execution_ref, "execution_ref")


@dataclass(frozen=True, slots=True)
class RenewUnitPermit:
    environment: str
    token: int
    now: datetime
    lease_seconds: int

    def __post_init__(self) -> None:
        _require_token_command(self.environment, self.token, self.now)
        _require_at_least(self.lease_seconds, "lease_seconds", 1)

    @property
    def lease_until(self) -> datetime:
        return self.now + timedelta(seconds=self.lease_seconds)


@dataclass(frozen=True, slots=True)
class ReanchorUnitPermit:
    """Reinicia a carência da prova de ausência no permit do token ainda sem bind."""

    environment: str
    token: int
    now: datetime

    def __post_init__(self) -> None:
        _require_token_command(self.environment, self.token, self.now)


@dataclass(frozen=True, slots=True)
class ReleaseUnitPermit:
    """Libera o permit do token; com anchor, só o permit sem bind ainda ancorado nele."""

    environment: str
    token: int
    now: datetime
    anchor: datetime | None = None

    def __post_init__(self) -> None:
        _require_token_command(self.environment, self.token, self.now)
        _optional_utc(self.anchor, "anchor")


@dataclass(frozen=True, slots=True)
class ClosePromotionFence:
    environment: str
    promotion_id: str
    now: datetime

    def __post_init__(self) -> None:
        _require_fence_command(self.environment, self.promotion_id, self.now)


@dataclass(frozen=True, slots=True)
class ReopenPromotionFence:
    environment: str
    promotion_id: str
    now: datetime

    def __post_init__(self) -> None:
        _require_fence_command(self.environment, self.promotion_id, self.now)


def open_fence(environment: str) -> PromotionFence:
    """Retorna o fence de um ambiente sem item: aberto, sem promoção, versão 0."""
    return PromotionFence(environment, FenceState.OPEN, None, 0, None)


def decide_acquisition(
    state: EnvironmentGateState, command: AcquireUnitPermit,
) -> AcquisitionStatus:
    """Decide o acquire; o lease vencido nunca autoriza takeover do permit.

    Args: state: Leitura forte do gate; command: Acquire do dispatch.
    Returns: REENTERED, FENCE_CLOSED, HELD ou ACQUIRED, nessa precedência.
    Raises: ValueError: Comando de outro ambiente.
    """
    _require_environment_match(state.environment, command.environment)
    permit = state.permit
    if permit is not None and permit.holder == command.holder:
        return AcquisitionStatus.REENTERED
    fence = state.fence
    if fence.state is FenceState.CLOSED and command.canary_promotion_id != fence.promotion_id:
        return AcquisitionStatus.FENCE_CLOSED
    if permit is not None:
        return AcquisitionStatus.HELD
    return AcquisitionStatus.ACQUIRED


def issue_permit(command: AcquireUnitPermit, token: int) -> UnitPermit:
    """Emite o permit do acquire admitido com o token gerado pelo armazenamento."""
    return UnitPermit(
        environment=command.environment, token=token, holder=command.holder,
        execution_ref=None, acquired_at=command.now, lease_until=command.lease_until,
        canary_promotion_id=command.canary_promotion_id,
    )


def _holds(permit: UnitPermit | None, environment: str, token: int) -> TypeGuard[UnitPermit]:
    if permit is None:
        return False
    _require_environment_match(permit.environment, environment)
    return permit.token == token


def bind_permit(permit: UnitPermit | None, command: BindUnitPermit) -> UnitPermit | None:
    """Vincula a execução ao permit do token; replay com a mesma referência é idempotente.

    Returns: Permit vinculado, ou None com token divergente ou outra execução já vinculada.
    Raises: ValueError: Permit de outro ambiente.
    """
    if not _holds(permit, command.environment, command.token):
        return None
    if permit.execution_ref not in {None, command.execution_ref}:
        return None
    return replace(permit, execution_ref=command.execution_ref)


def renew_permit(permit: UnitPermit | None, command: RenewUnitPermit) -> UnitPermit | None:
    """Renova o lease do permit do token.

    Returns: Permit renovado, ou None com token divergente.
    Raises: ValueError: Permit de outro ambiente.
    """
    if not _holds(permit, command.environment, command.token):
        return None
    return replace(permit, lease_until=command.lease_until)


def reanchor_permit(permit: UnitPermit | None, command: ReanchorUnitPermit) -> UnitPermit | None:
    """Reancora no instante do comando a carência do permit do token ainda sem bind.

    Returns: Permit reancorado, ou None com token divergente ou execução já vinculada.
    Raises: ValueError: Permit de outro ambiente.
    """
    if not _holds(permit, command.environment, command.token) or permit.execution_ref is not None:
        return None
    return replace(permit, acquired_at=command.now)


def release_permit(permit: UnitPermit | None, command: ReleaseUnitPermit) -> bool:
    """Indica se o token detém o permit e pode liberá-lo; o anchor exige o permit sem bind.

    Raises: ValueError: Permit de outro ambiente.
    """
    if not _holds(permit, command.environment, command.token):
        return False
    if command.anchor is None:
        return True
    return permit.execution_ref is None and permit.acquired_at == command.anchor


def close_fence(fence: PromotionFence, command: ClosePromotionFence) -> PromotionFence | None:
    """Fecha o fence para a promoção; replay da mesma promoção é idempotente.

    Returns: Fence resultante, ou None se outra promoção o mantém fechado ou se a
        promoção já o reabriu.
    Raises: ValueError: Comando de outro ambiente.
    """
    _require_environment_match(fence.environment, command.environment)
    if fence.promotion_id == command.promotion_id:
        return fence if fence.state is FenceState.CLOSED else None
    if fence.state is FenceState.CLOSED:
        return None
    return PromotionFence(
        fence.environment, FenceState.CLOSED, command.promotion_id, fence.version + 1, command.now,
    )


def reopen_fence(fence: PromotionFence, command: ReopenPromotionFence) -> PromotionFence | None:
    """Reabre o fence só pela promoção que o fechou; o replay é idempotente.

    Returns: Fence aberto, ou None se a promoção não corresponde.
    Raises: ValueError: Comando de outro ambiente.
    """
    _require_environment_match(fence.environment, command.environment)
    if fence.promotion_id != command.promotion_id:
        return None
    if fence.state is FenceState.OPEN:
        return fence
    return PromotionFence(
        fence.environment, FenceState.OPEN, command.promotion_id, fence.version + 1, command.now,
    )
