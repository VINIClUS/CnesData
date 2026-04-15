import pytest
from pydantic import ValidationError

from cnes_domain.models.extraction import ExtractionIntent, ExtractionParams


class TestJobValidation:
    def test_rejeita_payload_com_campo_extra(self):
        with pytest.raises(ValidationError):
            ExtractionParams.model_validate({
                "intent": "profissionais",
                "competencia": "2026-03",
                "cod_municipio": "354130",
                "sql": "SELECT 1",
            })

    def test_rejeita_intent_desconhecido(self):
        with pytest.raises(ValidationError):
            ExtractionParams.model_validate({
                "intent": "desconhecido",
                "competencia": "2026-03",
                "cod_municipio": "354130",
            })

    def test_aceita_payload_valido(self):
        p = ExtractionParams.model_validate({
            "intent": "profissionais",
            "competencia": "2026-03",
            "cod_municipio": "354130",
        })
        assert p.intent == ExtractionIntent.PROFISSIONAIS
