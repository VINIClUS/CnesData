"""Conflitos de escrita no registro DynamoDB de agentes Edge."""

from datetime import UTC, datetime

import pytest

from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.control_plane.edge_registration import DynamoEdgeRegistrationMixin

NOW = datetime(2026, 9, 26, tzinfo=UTC)


class _FakeDynamoRegistration(DynamoEdgeRegistrationMixin):
    _table_name = "raw-test"

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.attempts = 0

    def _get_item(self, _key: tuple[str, str]) -> None:
        return None

    def _transact(self, _actions: object) -> None:
        self.attempts += 1
        if self.attempts <= self.failures:
            raise Conflict(ErrorCode.TRANSACTION_CONFLICT)


def test_repete_conflito_de_registro_ate_vencer() -> None:
    adapter = _FakeDynamoRegistration(failures=2)

    agent = adapter.register_edge_agent("354130", "agent-1", "a" * 64, NOW)

    assert adapter.attempts == 3
    assert agent.certificate_fingerprint == "a" * 64


def test_rejeita_registro_apos_tres_conflitos() -> None:
    adapter = _FakeDynamoRegistration(failures=3)

    with pytest.raises(Conflict):
        adapter.register_edge_agent("354130", "agent-1", "a" * 64, NOW)

    assert adapter.attempts == 3
