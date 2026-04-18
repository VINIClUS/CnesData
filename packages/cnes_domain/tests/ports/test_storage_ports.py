"""Testes dos Null* ports de persistência."""

from cnes_domain.ports.storage import (
    NullEstabelecimentoStorage,
    NullProfissionalStorage,
    NullUnitOfWork,
    NullVinculoStorage,
)


class TestNullProfissionalStorage:

    def test_gravar_retorna_zero(self):
        assert NullProfissionalStorage().gravar([{"cpf": "1"}]) == 0

    def test_gravar_lista_vazia_retorna_zero(self):
        assert NullProfissionalStorage().gravar([]) == 0


class TestNullEstabelecimentoStorage:

    def test_gravar_retorna_zero(self):
        assert NullEstabelecimentoStorage().gravar([{"cnes": "0985333"}]) == 0

    def test_gravar_lista_vazia_retorna_zero(self):
        assert NullEstabelecimentoStorage().gravar([]) == 0


class TestNullVinculoStorage:

    def test_snapshot_replace_retorna_zero(self):
        storage = NullVinculoStorage()
        assert storage.snapshot_replace("2025-01", "LOCAL", [{}]) == 0

    def test_snapshot_replace_lista_vazia_retorna_zero(self):
        storage = NullVinculoStorage()
        assert storage.snapshot_replace("2025-01", "LOCAL", []) == 0


class TestNullUnitOfWork:

    def test_atributos_instanciados(self):
        uow = NullUnitOfWork()
        assert isinstance(uow.profissionais, NullProfissionalStorage)
        assert isinstance(uow.estabelecimentos, NullEstabelecimentoStorage)
        assert isinstance(uow.vinculos, NullVinculoStorage)

    def test_context_manager_retorna_self(self):
        uow = NullUnitOfWork()
        with uow as ctx:
            assert ctx is uow

    def test_context_manager_exit_nao_levanta(self):
        with NullUnitOfWork():
            pass
