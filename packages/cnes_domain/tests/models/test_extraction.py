import pytest
from pydantic import ValidationError

from cnes_domain.models.extraction import ExtractionIntent, ExtractionParams


class TestExtractionIntent:
    def test_valores_validos(self):
        assert ExtractionIntent.PROFISSIONAIS == "profissionais"
        assert ExtractionIntent.ESTABELECIMENTOS == "estabelecimentos"
        assert ExtractionIntent.EQUIPES == "equipes"
        assert ExtractionIntent.SIHD_PRODUCAO == "sihd_producao"

    def test_total_intents(self):
        assert len(ExtractionIntent) == 4


class TestExtractionParams:
    def test_params_validos(self):
        p = ExtractionParams(
            intent=ExtractionIntent.PROFISSIONAIS,
            competencia="2026-03",
            cod_municipio="354130",
        )
        assert p.intent == ExtractionIntent.PROFISSIONAIS
        assert p.competencia == "2026-03"
        assert p.cod_municipio == "354130"

    def test_rejeita_intent_invalido(self):
        with pytest.raises(ValidationError):
            ExtractionParams(
                intent="inexistente",
                competencia="2026-03",
                cod_municipio="354130",
            )

    def test_rejeita_campo_extra(self):
        with pytest.raises(ValidationError, match="extra"):
            ExtractionParams(
                intent="profissionais",
                competencia="2026-03",
                cod_municipio="354130",
                sql="DROP TABLE users",
            )

    def test_rejeita_competencia_formato_invalido(self):
        with pytest.raises(ValidationError):
            ExtractionParams(
                intent="profissionais",
                competencia="202603",
                cod_municipio="354130",
            )

    def test_rejeita_cod_municipio_formato_invalido(self):
        with pytest.raises(ValidationError):
            ExtractionParams(
                intent="profissionais",
                competencia="2026-03",
                cod_municipio="35",
            )

    def test_aceita_todos_os_intents(self):
        for intent in ExtractionIntent:
            p = ExtractionParams(
                intent=intent,
                competencia="2026-01",
                cod_municipio="354130",
            )
            assert p.intent == intent
