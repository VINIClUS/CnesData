"""Namespace reservado e binding imutável do canary de modo."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

TENANT_PREFIX: Final = "prod-canary-"
MAX_SELECTED_RUNS: Final = 2
PHASES: Final = frozenset({"unit", "recovery", "audit", "readiness"})
CONTEXT_KINDS: Final = frozenset({"promotion", "readiness"})
_CANONICAL_TENANT: Final = re.compile(r"prod-canary-[0-9a-f]{32}")
_SHA256: Final = re.compile(r"[0-9a-f]{64}")


class SyntheticContextRequired(ValueError):
    """Tenant do namespace sintético sem contexto autorizado instalado."""


def is_mode_canary_namespace(tenant_id: str) -> bool:
    """Returns: True se o tenant usa o prefixo reservado, em qualquer caixa ou sufixo."""
    return tenant_id.lower().startswith(TENANT_PREFIX)


def is_canonical_mode_canary_tenant(tenant_id: str) -> bool:
    """Returns: True somente para a forma `prod-canary-<32 hex minúsculos>`."""
    return _CANONICAL_TENANT.fullmatch(tenant_id) is not None


def _reject(field: str) -> ValueError:
    return ValueError(f"reason=invalid field={field}")


@dataclass(frozen=True, slots=True)
class ModeCanaryBinding:
    """Locator validado do contexto sintético; não autoriza efeitos sozinho."""

    tenant_id: str
    run_ids: tuple[str, ...]
    owner_sha256: str
    record_sha256: str
    phase: str
    context_kind: str

    def __post_init__(self) -> None:
        """Raises: ValueError: Campo fora do formato fechado, `field=<nome>`."""
        if not is_canonical_mode_canary_tenant(self.tenant_id):
            raise _reject("tenant_id")
        if not self._valid_run_ids():
            raise _reject("run_ids")
        for field in ("owner_sha256", "record_sha256"):
            if _SHA256.fullmatch(getattr(self, field)) is None:
                raise _reject(field)
        if self.phase not in PHASES:
            raise _reject("phase")
        if self.context_kind not in CONTEXT_KINDS:
            raise _reject("context_kind")

    def _valid_run_ids(self) -> bool:
        ids = self.run_ids
        return (
            type(ids) is tuple
            and 1 <= len(ids) <= MAX_SELECTED_RUNS
            and len(set(ids)) == len(ids)
            and all(type(run_id) is str and run_id for run_id in ids)
        )

    def permits(self, tenant_id: str, run_id: str) -> bool:
        """Returns: True se o par tenant/run consta do binding."""
        return tenant_id == self.tenant_id and run_id in self.run_ids


__all__ = [
    "CONTEXT_KINDS",
    "MAX_SELECTED_RUNS",
    "PHASES",
    "TENANT_PREFIX",
    "ModeCanaryBinding",
    "SyntheticContextRequired",
    "is_canonical_mode_canary_tenant",
    "is_mode_canary_namespace",
]
