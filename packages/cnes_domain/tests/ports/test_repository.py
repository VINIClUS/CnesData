"""Testes dos protocolos de repositório — conformidade estrutural."""

from cnes_domain.ports.repository import (
    EquipeRepository,
    EstabelecimentoRepository,
    ProfissionalRepository,
)


def _make_estab():
    class _FakeEstab:
        def listar_estabelecimentos(self, competencia=None):
            return [{"CNES": "0985333"}]

    return _FakeEstab()


def _make_prof():
    class _FakeProf:
        def listar_profissionais(self, competencia=None):
            return [{"CPF": "11111111111"}]

    return _FakeProf()


def _make_equipe():
    class _FakeEquipe:
        def listar_equipes(self, competencia=None):
            return [{"INE": "0000111111"}]

    return _FakeEquipe()


class TestEstabelecimentoRepositoryProtocol:

    def test_implementacao_satisfaz_protocolo(self):
        assert isinstance(_make_estab(), EstabelecimentoRepository)

    def test_listar_sem_competencia_retorna_iterable(self):
        result = list(_make_estab().listar_estabelecimentos())
        assert result[0]["CNES"] == "0985333"

    def test_listar_com_competencia_retorna_iterable(self):
        result = list(_make_estab().listar_estabelecimentos((2025, 1)))
        assert len(result) == 1


class TestProfissionalRepositoryProtocol:

    def test_implementacao_satisfaz_protocolo(self):
        assert isinstance(_make_prof(), ProfissionalRepository)

    def test_listar_sem_competencia_retorna_iterable(self):
        result = list(_make_prof().listar_profissionais())
        assert result[0]["CPF"] == "11111111111"


class TestEquipeRepositoryProtocol:

    def test_implementacao_satisfaz_protocolo(self):
        assert isinstance(_make_equipe(), EquipeRepository)

    def test_listar_equipes_retorna_iterable(self):
        result = list(_make_equipe().listar_equipes())
        assert result[0]["INE"] == "0000111111"
