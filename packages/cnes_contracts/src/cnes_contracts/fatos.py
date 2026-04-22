"""Gold v2 fato contracts."""
from __future__ import annotations

from datetime import date, datetime  # noqa: TC003
from typing import Literal
from uuid import UUID  # noqa: TC003

from pydantic import BaseModel, ConfigDict, Field


class VinculoCNES(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    sk_profissional: int = Field(gt=0)
    sk_estabelecimento: int = Field(gt=0)
    sk_cbo: int = Field(gt=0)
    sk_competencia: int = Field(gt=0)
    carga_horaria_sem: int | None = Field(default=None, ge=0, le=168)
    ind_vinc: str | None = Field(default=None, pattern=r"^\w{1,6}$")
    sk_equipe: int | None = Field(default=None, gt=0)
    job_id: UUID
    fonte_sistema: Literal["CNES_LOCAL", "CNES_NACIONAL"]
    extracao_ts: datetime


class ProducaoAmbulatorial(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    sk_profissional: int = Field(gt=0)
    sk_estabelecimento: int = Field(gt=0)
    sk_procedimento: int = Field(gt=0)
    sk_competencia: int = Field(gt=0)
    sk_cid_principal: int | None = Field(default=None, gt=0)
    qtd: int = Field(gt=0)
    valor_aprov_cents: int = Field(ge=0)
    dt_atendimento: date | None = None
    job_id: UUID
    fonte_sistema: Literal["SIA_APA", "SIA_BPI", "BPA_C", "BPA_I"]
    extracao_ts: datetime
    fontes_reportadas: dict[str, dict] | None = None


class Internacao(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    num_aih: str = Field(pattern=r"^\d{13}$")
    sk_profissional_solicit: int | None = Field(default=None, gt=0)
    sk_estabelecimento: int = Field(gt=0)
    sk_competencia: int = Field(gt=0)
    sk_cid_principal: int | None = Field(default=None, gt=0)
    dt_internacao: date
    dt_saida: date | None = None
    valor_total_cents: int | None = Field(default=None, ge=0)
    job_id: UUID
    fonte_sistema: Literal["SIHD"] = "SIHD"
    extracao_ts: datetime


class ProcedimentoAIH(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    sk_aih: int = Field(gt=0)
    sk_procedimento: int = Field(gt=0)
    sk_profissional_exec: int | None = Field(default=None, gt=0)
    sk_competencia: int = Field(gt=0)
    qtd: int = Field(gt=0, default=1)
    valor_cents: int | None = Field(default=None, ge=0)
    job_id: UUID
    extracao_ts: datetime
