"""Porta de criação e importação de refresh."""

from typing import Protocol

from cnes_domain.refresh_tokens import (
    RefreshTokenContext,
    RefreshTokenInput,
    RefreshTokenMutation,
    RefreshTokenRecord,
)


class RefreshTokenStore(Protocol):
    def create(self, token_input: RefreshTokenInput) -> str: ...  # pragma: no cover - Protocol

    def import_hashed(self, record: RefreshTokenRecord) -> None: ...  # pragma: no cover - Protocol

    def validate(
        self, token: str, context: RefreshTokenContext,
    ) -> RefreshTokenRecord | None: ...  # pragma: no cover - Protocol

    def mark_used(
        self, token: str, mutation: RefreshTokenMutation,
    ) -> bool: ...  # pragma: no cover - Protocol

    def revoke(
        self, token: str, mutation: RefreshTokenMutation,
    ) -> bool: ...  # pragma: no cover - Protocol
