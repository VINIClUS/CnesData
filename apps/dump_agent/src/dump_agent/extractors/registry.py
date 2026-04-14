"""Registry — mapeia ExtractionIntent para Extractor concreto."""

from cnes_domain.models.extraction import ExtractionIntent
from dump_agent.extractors.cnes_extractor import CnesExtractor
from dump_agent.extractors.protocol import Extractor
from dump_agent.extractors.sihd_extractor import SihdExtractor

REGISTRY: dict[ExtractionIntent, Extractor] = {
    ExtractionIntent.PROFISSIONAIS: CnesExtractor(),
    ExtractionIntent.ESTABELECIMENTOS: CnesExtractor(),
    ExtractionIntent.EQUIPES: CnesExtractor(),
    ExtractionIntent.SIHD_PRODUCAO: SihdExtractor(),
}
