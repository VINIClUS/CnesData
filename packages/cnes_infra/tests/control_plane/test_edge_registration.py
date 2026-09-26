from datetime import UTC, datetime

import pytest

from cnes_domain.control_plane.enums import AgentState
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane

NOW = datetime(2026, 9, 26, tzinfo=UTC)


def test_registra_agente_e_atualiza_fingerprint_sem_reativar_revogado(tmp_path) -> None:
    control = SQLiteControlPlane(tmp_path / "raw.db", lambda: NOW)
    control.initialize()

    first = control.register_edge_agent("354130", "agent-1", "a" * 64, NOW)
    rotated = control.register_edge_agent("354130", "agent-1", "b" * 64, NOW)
    assert first.certificate_fingerprint == "a" * 64
    assert rotated.certificate_fingerprint == "b" * 64

    control.put_agent(rotated.model_copy(update={"state": AgentState.REVOKED}))
    with pytest.raises(Conflict):
        control.register_edge_agent("354130", "agent-1", "c" * 64, NOW)
    assert control.get_agent("354130", "agent-1").state is AgentState.REVOKED
