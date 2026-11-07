"""Contratos de extração."""

from enum import StrEnum

from pydantic import BaseModel, Field


class ExtractionIntent(StrEnum):
    PROFISSIONAIS = "profissionais"
    ESTABELECIMENTOS = "estabelecimentos"
    EQUIPES = "equipes"
    SIHD_PRODUCAO = "sihd_producao"


class ExtractionParams(BaseModel):
    """Payload validado para jobs de extração."""

    model_config = {"extra": "forbid"}

    intent: ExtractionIntent
    competencia: str = Field(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")
    cod_municipio: str = Field(pattern=r"^\d{6}$")
