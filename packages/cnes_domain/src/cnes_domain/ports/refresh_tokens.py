"""Porta de criação e importação de refresh."""

from typing import Protocol

from cnes_domain.refresh_tokens import RefreshTokenInput, RefreshTokenRecord


class RefreshTokenStore(Protocol):
    def create(self, token_input: RefreshTokenInput) -> str: ...  # pragma: no cover - Protocol

    def import_hashed(self, record: RefreshTokenRecord) -> None: ...  # pragma: no cover - Protocol
