"""Testes Gate 1: validacao ExtractionParams."""

import pytest
from pydantic import ValidationError

from cnes_domain.models.extraction import ExtractionParams


class TestGate1Validation:
    def test_rejeita_payload_com_sql(self):
        with pytest.raises(ValidationError):
            ExtractionParams(
                intent="profissionais",
                competencia="2026-03",
                cod_municipio="354130",
                sql="DROP TABLE",
            )

    def test_aceita_payload_valido(self):
        p = ExtractionParams(
            intent="profissionais",
            competencia="2026-03",
            cod_municipio="354130",
        )
        assert p.intent.value == "profissionais"

    def test_rejeita_intent_invalido(self):
        with pytest.raises(ValidationError):
            ExtractionParams(
                intent="invalido",
                competencia="2026-03",
                cod_municipio="354130",
            )

    def test_rejeita_competencia_formato_invalido(self):
        with pytest.raises(ValidationError):
            ExtractionParams(
                intent="profissionais",
                competencia="2026-13",
                cod_municipio="354130",
            )

    def test_rejeita_cod_municipio_curto(self):
        with pytest.raises(ValidationError):
            ExtractionParams(
                intent="profissionais",
                competencia="2026-03",
                cod_municipio="354",
            )


class TestCompleteUploadValidation:

    def test_rejeita_body_sem_size_bytes(self):
        from cnes_domain.models.api import CompleteUploadRequest
        with pytest.raises(ValidationError):
            CompleteUploadRequest(machine_id="m", object_key="k")

    def test_aceita_body_com_size_bytes_zero(self):
        from cnes_domain.models.api import CompleteUploadRequest
        m = CompleteUploadRequest(
            machine_id="m", object_key="k", size_bytes=0,
        )
        assert m.size_bytes == 0
