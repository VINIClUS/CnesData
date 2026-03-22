"""test_base.py — Conformidade estrutural dos adapters com os Protocols."""

from unittest.mock import MagicMock


from ingestion.base import EstabelecimentoRepository, ProfissionalRepository, EquipeRepository
from ingestion.cnes_local_adapter import CnesLocalAdapter
from ingestion.cnes_nacional_adapter import CnesNacionalAdapter


class TestCnesLocalAdapterProtocols:

    def test_implementa_profissional_repository(self):
        adapter = CnesLocalAdapter(MagicMock())
        assert isinstance(adapter, ProfissionalRepository)

    def test_implementa_estabelecimento_repository(self):
        adapter = CnesLocalAdapter(MagicMock())
        assert isinstance(adapter, EstabelecimentoRepository)

    def test_implementa_equipe_repository(self):
        adapter = CnesLocalAdapter(MagicMock())
        assert isinstance(adapter, EquipeRepository)


class TestCnesNacionalAdapterProtocols:

    def test_implementa_profissional_repository(self):
        adapter = CnesNacionalAdapter("project-id", "3541307")
        assert isinstance(adapter, ProfissionalRepository)

    def test_implementa_estabelecimento_repository(self):
        adapter = CnesNacionalAdapter("project-id", "3541307")
        assert isinstance(adapter, EstabelecimentoRepository)
