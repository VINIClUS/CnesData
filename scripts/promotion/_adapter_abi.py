"""Tabela fechada do adapter ABI2 e codecs dos insumos originais entregues pelo keeper."""

from __future__ import annotations

import json
from typing import Any, Final, NoReturn, cast

from scripts.promotion._api_static_records import canonical, sha

ABI_VERSION: Final = 2
INPUTS: Final[dict[str, frozenset[str]]] = {
    "inventory": frozenset({"candidate", "dashboard-artifact", "archive", "static-baseline"}),
    "publish-static": frozenset(
        {"candidate", "dashboard-artifact", "static-plan", "inventory", "host-receipt"}
    ),
    "rollback-api-static": frozenset(
        {"static-plan", "inventory", "publication", "host-recovery-receipt", "recovery-decision"}
    ),
    "close-drain": frozenset({"activation", "expirations"}),
    "closed-ready": frozenset({"activation", "close-drain", "phase-b", "membership"}),
    "reopen": frozenset({"activation", "routing", "canaries", "membership"}),
    "terminal-readback": frozenset(
        {"routing", "membership", "host-receipt", "static-receipt", "decision"}
    ),
}
OBSERVATIONS: Final[dict[str, str]] = {
    "inventory": "inventory", "publish-static": "publication", "rollback-api-static": "rollback",
    "close-drain": "close-drain", "closed-ready": "closed-ready", "reopen": "routing",
    "terminal-readback": "terminal",
}
STAGES: Final[dict[str, frozenset[str]]] = {
    "inventory": frozenset({"application-prepare"}), "publish-static": frozenset({"static"}),
    "rollback-api-static": frozenset({"recovery"}),
    "close-drain": frozenset({"application-prepare"}), "closed-ready": frozenset({"api"}),
    "reopen": frozenset({"final", "recovery"}),
    "terminal-readback": frozenset({"application-prepare", "final", "recovery"}),
}
MEMBER_IDENTITY: Final = (
    "dispatch_id", "request_sha256", "generation", "initial_state", "execution_ref", "resolution",
)
MEMBER_RESOLVED: Final = frozenset({"TERMINAL", "ABSENT_PROVEN", "CANCEL_BEFORE_SEND"})
FROZEN_MEMBER_FIELDS: Final = frozenset(
    {*MEMBER_IDENTITY, "proof_sha256", "observed_at", "send_disabled_at"}
)
MEMBERSHIP_FIELDS: Final = frozenset({"frozen_membership_sha256", "members"})
ROUTING_FIELDS: Final = frozenset({"version", "kind", "fence", "routes_match"})
TERMINAL_READBACK_FIELDS: Final = frozenset({
    "version", "kind", "members", "quiescent", "state_clean", "pending", "unknown", "observed_at",
})
CLOSED_READY_FIELDS: Final = frozenset({
    "version", "kind", "common", "close_drain_sha256", "phase_b_sha256", "fence", "permit",
    "members", "proofs", "observed_at", "exclusion", "stage", "decision",
})
MODE_OPERATIONS: Final[dict[str, tuple[str, str]]] = {
    "unit-seed": ("unit", "unit"), "unit-start": ("unit", "unit"),
    "unit-observe": ("unit", "unit"),
    "recovery-seed": ("recovery", "phase-c-recovery"),
    "recovery-start": ("recovery", "phase-c-recovery"),
    "recovery-observe": ("recovery", "phase-c-recovery"),
    "audit-start": ("audit", "phase-c-audit"), "audit-observe": ("audit", "phase-c-audit"),
}
MODE_OBSERVATION_FIELDS: Final = frozenset({"version", "kind", "accepted", "checks", "commitments"})
MODE_OBSERVATION_KIND: Final = "mode"
_SCHEDULED_INPUTS: Final = frozenset({"activation", "network", "close-drain", "phase-b", "phase-c",
                                      "mode-result", "readbacks"})
MODE_INPUTS: Final[dict[str, frozenset[str]]] = {
    "unit-observe": frozenset({"activation", "close-drain", "record", "mode-result", "readbacks",
                               "observed"}),
    "recovery-observe": _SCHEDULED_INPUTS,
    "audit-observe": _SCHEDULED_INPUTS | {"recovery-result", "recovery-readbacks"},
}
MODE_STAGES: Final[dict[str, frozenset[str]]] = {
    name: frozenset({MODE_OPERATIONS[name][1]}) for name in MODE_INPUTS
}


def _constant(token: str) -> NoReturn:
    raise ValueError(f"json=noninteger value={token}")


def document(raw: bytes, name: str) -> Any:
    """Returns: Valor JSON dos bytes originais canônicos (UTF-8, LF, sem número não inteiro).

    Raises: ValueError: Bytes inválidos ou fora da forma canônica.
    """
    try:
        value = json.loads(raw.decode("utf-8"), parse_constant=_constant, parse_float=_constant)
    except ValueError as error:
        raise ValueError(f"input=encoding name={name}") from error
    if canonical(value) != raw:
        raise ValueError(f"input=canonical name={name}")
    return value


def closed(value: Any, fields: frozenset[str], prefix: str, name: str) -> dict[str, Any]:
    """Returns: Objeto com exatamente os campos fechados. Raises: ValueError: Campo a mais/menos."""
    if not isinstance(value, dict) or set(cast("dict[str, Any]", value)) != fields:
        raise ValueError(f"{prefix}=fields name={name}")
    return cast("dict[str, Any]", value)


def versioned(raw: bytes, name: str, fields: frozenset[str], prefix: str) -> dict[str, Any]:
    """Returns: Documento `version` 1 do kind `name`. Raises: ValueError: Forma ou kind."""
    value = closed(document(raw, name), fields, prefix, name)
    if type(value["version"]) is not int or value["version"] != 1 or value["kind"] != name:
        raise ValueError(f"{prefix}=kind name={name}")
    return value


def frozen_members(raw: bytes, prefix: str) -> list[dict[str, Any]]:
    """Returns: Membros congelados com o compromisso `frozen_membership_sha256` conferido.

    Raises: ValueError: Campos fechados, membro ou compromisso divergente.
    """
    value = closed(document(raw, "membership"), MEMBERSHIP_FIELDS, prefix, "membership")
    members = value["members"]
    if not isinstance(members, list) or value["frozen_membership_sha256"] != sha(
        canonical(members)
    ):
        raise ValueError(f"{prefix}=membership_commitment")
    return [closed(member, FROZEN_MEMBER_FIELDS, prefix, "member")
            for member in cast("list[Any]", members)]


def stage_deadline(stage: dict[str, Any]) -> int:
    """Returns: Prazo epoch do estágio: preparação antes do close, janela depois dele."""
    return stage["preparation_deadline"] if stage["close"] is None else stage["deadline"]


__all__ = [
    "ABI_VERSION",
    "CLOSED_READY_FIELDS",
    "INPUTS",
    "MEMBER_IDENTITY",
    "MODE_INPUTS",
    "MODE_OBSERVATION_FIELDS",
    "MODE_OBSERVATION_KIND",
    "MODE_OPERATIONS",
    "MODE_STAGES",
    "OBSERVATIONS",
    "ROUTING_FIELDS",
    "STAGES",
    "TERMINAL_READBACK_FIELDS",
    "closed",
    "document",
    "frozen_members",
    "stage_deadline",
    "versioned",
]
