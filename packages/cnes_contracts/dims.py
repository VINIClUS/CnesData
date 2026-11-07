"""Gold v2 dim contracts."""
from __future__ import annotations

from datetime import datetime  # noqa: TC003
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class Profissional(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    sk_profissional: int = Field(gt=0)
    cpf_hash: str = Field(pattern=r"^[a-f0-9]{11}$")
    nome: str = Field(min_length=1, max_length=120)
    cns: str | None = Field(default=None, pattern=r"^\d{15}$")
    sk_cbo_principal: int | None = Field(default=None, gt=0)
    fontes: dict[str, bool] = Field(default_factory=dict)
    criado_em: datetime
    atualizado_em: datetime


class Estabelecimento(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    sk_estabelecimento: int = Field(gt=0)
    cnes: str = Field(pattern=r"^\d{7}$")
    nome: str = Field(min_length=1, max_length=120)
    cnpj_mantenedora: str | None = Field(default=None, pattern=r"^\d{14}$")
    tp_unid: int = Field(ge=0, le=999)
    sk_municipio: int = Field(gt=0)
    fontes: dict[str, bool] = Field(default_factory=dict)
    criado_em: datetime
    atualizado_em: datetime


class ProcedimentoSUS(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    sk_procedimento: int = Field(gt=0)
    cod_sigtap: str = Field(pattern=r"^\d{10}$")
    descricao: str = Field(min_length=1)
    complexidade: Literal[1, 2, 3] | None = None
    financiamento: Literal["MAC", "FAE", "PAB", "VISA"] | None = None
    modalidade: Literal["AMB", "HOSP", "APAC"] | None = None
    competencia_vigencia_ini: int | None = Field(default=None, ge=200001, le=209912)
    competencia_vigencia_fim: int | None = Field(default=None, ge=200001, le=209912)


class CBO(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    sk_cbo: int = Field(gt=0)
    cod_cbo: str = Field(pattern=r"^\d{6}$")
    descricao: str = Field(min_length=1)


class CID10(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    sk_cid: int = Field(gt=0)
    cod_cid: str = Field(pattern=r"^[A-Z]\d{2,3}$")
    descricao: str = Field(min_length=1)
    capitulo: int = Field(ge=1, le=22)


class Municipio(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    sk_municipio: int = Field(gt=0)
    ibge6: str = Field(pattern=r"^\d{6}$")
    ibge7: str = Field(pattern=r"^\d{7}$")
    nome: str = Field(min_length=1)
    uf: str = Field(pattern=r"^[A-Z]{2}$")
    populacao_estimada: int | None = Field(default=None, ge=0)
    teto_pab_cents: int | None = Field(default=None, ge=0)


class Competencia(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    sk_competencia: int = Field(gt=0)
    competencia: int = Field(ge=200001, le=209912)
    ano: int = Field(ge=2020, le=2040)
    mes: int = Field(ge=1, le=12)
    qtd_dias_uteis: int | None = Field(default=None, ge=0, le=31)
