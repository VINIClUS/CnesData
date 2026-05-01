"""Tests for vinculo_repo_v2."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text

from cnes_contracts.fatos import VinculoCNES
from cnes_infra.storage.dim_lookup import (
    upsert_dim_cbo,
    upsert_dim_estabelecimento,
    upsert_dim_municipio,
    upsert_dim_profissional,
)
from cnes_infra.storage.repositories.vinculo_repo_v2 import gravar

pytestmark = pytest.mark.postgres


@pytest.fixture
def dim_seed(pg_conn):
    """Seed dims mínimas + retorna surrogate keys."""
    mun_sk = upsert_dim_municipio(
        pg_conn,
        {"ibge6": "354130", "ibge7": "3541308", "nome": "PE", "uf": "SP"},
    )
    estab_sk = upsert_dim_estabelecimento(
        pg_conn,
        {
            "cnes": "1234567",
            "nome": "UBS Teste",
            "cnpj_mantenedora": None,
            "tp_unid": 1,
            "sk_municipio": mun_sk,
            "fontes": {},
        },
    )
    prof_sk = upsert_dim_profissional(
        pg_conn,
        {"cpf_hash": "aaa11111111", "nome": "Prof Teste", "cns": None, "fontes": {}},
    )
    cbo_sk = upsert_dim_cbo(pg_conn, {"cod_cbo": "225125", "descricao": "Medico"})
    comp_sk = pg_conn.execute(
        text("SELECT sk_competencia FROM gold.dim_competencia WHERE competencia = 202601"),
    ).scalar()
    return {
        "sk_municipio": mun_sk,
        "sk_estabelecimento": estab_sk,
        "sk_profissional": prof_sk,
        "sk_cbo": cbo_sk,
        "sk_competencia": comp_sk,
    }


def test_gravar_produz_linha(pg_conn, dim_seed):
    v = VinculoCNES(
        sk_profissional=dim_seed["sk_profissional"],
        sk_estabelecimento=dim_seed["sk_estabelecimento"],
        sk_cbo=dim_seed["sk_cbo"],
        sk_competencia=dim_seed["sk_competencia"],
        carga_horaria_sem=40,
        ind_vinc="SUS",
        sk_equipe=None,
        job_id=uuid4(),
        fonte_sistema="CNES_LOCAL",
        extracao_ts=datetime.now(UTC),
    )
    gravar(pg_conn, v)

    count = pg_conn.execute(
        text("SELECT COUNT(*) FROM gold.fato_vinculo_cnes WHERE sk_profissional = :p"),
        {"p": v.sk_profissional},
    ).scalar()
    assert count == 1


def test_multi_source_cria_linhas_separadas(pg_conn, dim_seed):
    """Gold v2 semantic: different fonte_sistema -> separate rows (no JSONB merge)."""
    base = {
        "sk_profissional": dim_seed["sk_profissional"],
        "sk_estabelecimento": dim_seed["sk_estabelecimento"],
        "sk_cbo": dim_seed["sk_cbo"],
        "sk_competencia": dim_seed["sk_competencia"],
        "carga_horaria_sem": 40,
        "ind_vinc": "SUS",
        "sk_equipe": None,
        "extracao_ts": datetime.now(UTC),
    }
    v_local = VinculoCNES(**base, job_id=uuid4(), fonte_sistema="CNES_LOCAL")
    v_nac = VinculoCNES(**base, job_id=uuid4(), fonte_sistema="CNES_NACIONAL")
    gravar(pg_conn, v_local)
    gravar(pg_conn, v_nac)

    rows = pg_conn.execute(
        text("""
            SELECT fonte_sistema FROM gold.fato_vinculo_cnes
            WHERE sk_profissional = :p
            ORDER BY fonte_sistema
        """),
        {"p": v_local.sk_profissional},
    ).all()
    assert [r[0] for r in rows] == ["CNES_LOCAL", "CNES_NACIONAL"]
