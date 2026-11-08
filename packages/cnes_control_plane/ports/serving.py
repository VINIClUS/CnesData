"""Port de acesso ao serving consumido pelo plano de controle."""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from cnes_contracts.serving import (  # noqa: TC001 - Runtime annotation inspection.
    ServingGrant,
    ServingRequest,
)


@runtime_checkable
class ServingAccessPort(Protocol):
    def authorize(self, request: ServingRequest) -> ServingGrant: ...
