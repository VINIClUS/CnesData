"""Contratos de implementação das conexões SQLite."""

import pytest

from cnes_data.storage.sqlite import SQLiteConnectionsPort


@pytest.mark.parametrize("provided", ["read_connection", "write_transaction"])
def test_rejeita_adapter_nominal_sem_uma_das_operacoes(provided: str) -> None:
    incomplete = type("IncompleteConnections", (SQLiteConnectionsPort,), {provided: lambda _: None})

    with pytest.raises(TypeError, match="abstract"):
        incomplete()
