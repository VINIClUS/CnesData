"""Testes do registry de extractors."""

from cnes_domain.models.extraction import ExtractionIntent
from dump_agent.extractors.cnes_extractor import CnesExtractor
from dump_agent.extractors.protocol import Extractor
from dump_agent.extractors.registry import REGISTRY
from dump_agent.extractors.sihd_extractor import SihdExtractor


class TestRegistry:
    def test_todos_os_intents_mapeados(self):
        for intent in ExtractionIntent:
            assert intent in REGISTRY, f"missing={intent}"

    def test_sem_intents_extras(self):
        assert len(REGISTRY) == len(ExtractionIntent)

    def test_todos_implementam_protocol(self):
        for intent, extractor in REGISTRY.items():
            assert isinstance(extractor, Extractor), (
                f"intent={intent} nao implementa Extractor"
            )

    def test_cnes_intents_mapeiam_para_cnes_extractor(self):
        for intent in (
            ExtractionIntent.PROFISSIONAIS,
            ExtractionIntent.ESTABELECIMENTOS,
            ExtractionIntent.EQUIPES,
        ):
            assert isinstance(REGISTRY[intent], CnesExtractor)

    def test_sihd_mapeia_para_sihd_extractor(self):
        assert isinstance(
            REGISTRY[ExtractionIntent.SIHD_PRODUCAO],
            SihdExtractor,
        )
