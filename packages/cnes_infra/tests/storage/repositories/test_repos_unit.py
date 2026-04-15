"""Testes unitarios dos repositorios segregados."""

from unittest.mock import MagicMock

import pytest

from cnes_domain.tenant import set_tenant_id
from cnes_infra.storage.repositories.estabelecimento_repo import (
    EstabelecimentoRepository,
)
from cnes_infra.storage.repositories.profissional_repo import (
    ProfissionalRepository,
)
from cnes_infra.storage.repositories.unit_of_work import PostgresUnitOfWork
from cnes_infra.storage.repositories.vinculo_repo import VinculoRepository

TENANT = "355030"


@pytest.fixture(autouse=True)
def _set_tenant():
    set_tenant_id(TENANT)


@pytest.fixture
def mock_con():
    return MagicMock()


class TestProfissionalRepository:
    def test_gravar_executa_upsert(self, mock_con):
        repo = ProfissionalRepository(mock_con)
        rows = [{"tenant_id": TENANT, "cpf": "12345678901"}]

        repo.gravar(rows)

        mock_con.execute.assert_called_once()
        stmt = mock_con.execute.call_args[0][0]
        assert "dim_profissional" in str(stmt)

    def test_gravar_lista_vazia_retorna_zero(self, mock_con):
        repo = ProfissionalRepository(mock_con)

        result = repo.gravar([])

        assert result == 0
        mock_con.execute.assert_not_called()

    def test_gravar_retorna_contagem(self, mock_con):
        repo = ProfissionalRepository(mock_con)
        rows = [
            {"tenant_id": TENANT, "cpf": "11111111111"},
            {"tenant_id": TENANT, "cpf": "22222222222"},
        ]

        result = repo.gravar(rows)

        assert result == 2


class TestVinculoRepository:
    def test_snapshot_replace_executa_delete_e_insert(self, mock_con):
        repo = VinculoRepository(mock_con)
        rows = [
            {
                "tenant_id": TENANT,
                "competencia": "2024-01",
                "cpf": "12345678901",
                "cnes": "1234567",
                "cbo": "225142",
            },
        ]

        repo.snapshot_replace("2024-01", "local", rows)

        assert mock_con.execute.call_count == 2
        delete_clause = mock_con.execute.call_args_list[0][0][0]
        assert "DELETE" in delete_clause.text

    def test_snapshot_replace_retorna_contagem(self, mock_con):
        repo = VinculoRepository(mock_con)
        rows = [
            {
                "tenant_id": TENANT,
                "competencia": "2024-01",
                "cpf": "12345678901",
                "cnes": "1234567",
                "cbo": "225142",
            },
            {
                "tenant_id": TENANT,
                "competencia": "2024-01",
                "cpf": "99999999999",
                "cnes": "7654321",
                "cbo": "225142",
            },
        ]

        result = repo.snapshot_replace("2024-01", "local", rows)

        assert result == 2


class TestEstabelecimentoRepository:
    def test_gravar_executa_upsert(self, mock_con):
        repo = EstabelecimentoRepository(mock_con)
        rows = [{"tenant_id": TENANT, "cnes": "1234567"}]

        repo.gravar(rows)

        mock_con.execute.assert_called_once()
        stmt = mock_con.execute.call_args[0][0]
        assert "dim_estabelecimento" in str(stmt)

    def test_gravar_lista_vazia_retorna_zero(self, mock_con):
        repo = EstabelecimentoRepository(mock_con)

        result = repo.gravar([])

        assert result == 0
        mock_con.execute.assert_not_called()


class TestPostgresUnitOfWork:
    def test_commit_sem_erro(self):
        engine = MagicMock()
        con = MagicMock()
        tx = MagicMock()
        engine.connect.return_value = con
        con.begin.return_value = tx

        with PostgresUnitOfWork(engine) as _uow:
            pass

        tx.commit.assert_called_once()
        tx.rollback.assert_not_called()
        con.close.assert_called_once()

    def test_rollback_com_erro(self):
        engine = MagicMock()
        con = MagicMock()
        tx = MagicMock()
        engine.connect.return_value = con
        con.begin.return_value = tx

        with pytest.raises(ValueError):
            with PostgresUnitOfWork(engine) as _uow:
                raise ValueError("boom")

        tx.rollback.assert_called_once()
        tx.commit.assert_not_called()
        con.close.assert_called_once()

    def test_expoe_tres_repositorios(self):
        engine = MagicMock()
        con = MagicMock()
        tx = MagicMock()
        engine.connect.return_value = con
        con.begin.return_value = tx

        with PostgresUnitOfWork(engine) as uow:
            assert isinstance(uow.profissionais, ProfissionalRepository)
            assert isinstance(
                uow.estabelecimentos, EstabelecimentoRepository,
            )
            assert isinstance(uow.vinculos, VinculoRepository)
