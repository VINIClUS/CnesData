"""PostgresDimLookup + upsert helper tests."""
from __future__ import annotations

import pytest

from cnes_infra.storage.dim_lookup import (
    PostgresDimLookup,
    upsert_dim_cbo,
    upsert_dim_cid10,
    upsert_dim_estabelecimento,
    upsert_dim_municipio,
    upsert_dim_procedimento_sus,
    upsert_dim_profissional,
)

pytestmark = pytest.mark.postgres


def test_lookup_retorna_none_quando_nao_existe(pg_conn):
    lookup = PostgresDimLookup(pg_conn)
    assert lookup.sk_profissional_por_cpf_hash("notahash12") is None
    assert lookup.sk_estabelecimento_por_cnes("0000000") is None
    assert lookup.sk_cbo_por_codigo("999999") is None
    assert lookup.sk_competencia_por_yyyymm(199001) is None


def test_upsert_profissional_retorna_sk(pg_conn):
    sk = upsert_dim_profissional(
        pg_conn,
        {
            "cpf_hash": "abc12345678",
            "nome": "Teste",
            "cns": None,
            "fontes": {"LOCAL": True},
        },
    )
    assert sk > 0


def test_upsert_profissional_idempotente_mesmo_cpf(pg_conn):
    sk1 = upsert_dim_profissional(
        pg_conn,
        {
            "cpf_hash": "aaa11111111",
            "nome": "X",
            "cns": None,
            "fontes": {"LOCAL": True},
        },
    )
    sk2 = upsert_dim_profissional(
        pg_conn,
        {
            "cpf_hash": "aaa11111111",
            "nome": "Y",
            "cns": None,
            "fontes": {"NACIONAL": True},
        },
    )
    assert sk1 == sk2


def test_cache_hit_evita_segunda_query(pg_conn, monkeypatch):
    upsert_dim_profissional(
        pg_conn,
        {
            "cpf_hash": "ccc33333333",
            "nome": "Z",
            "cns": None,
            "fontes": {},
        },
    )
    lookup = PostgresDimLookup(pg_conn)
    sk1 = lookup.sk_profissional_por_cpf_hash("ccc33333333")
    assert sk1 is not None

    call_count = [0]
    orig_execute = pg_conn.execute

    def counting_execute(*args, **kwargs):
        call_count[0] += 1
        return orig_execute(*args, **kwargs)

    monkeypatch.setattr(pg_conn, "execute", counting_execute)
    sk2 = lookup.sk_profissional_por_cpf_hash("ccc33333333")
    assert sk1 == sk2
    assert call_count[0] == 0, "cached lookup should not hit DB"


def test_upsert_estabelecimento(pg_conn):
    mun_sk = upsert_dim_municipio(
        pg_conn,
        {
            "ibge6": "354130",
            "ibge7": "3541308",
            "nome": "Presidente Epitácio",
            "uf": "SP",
        },
    )
    sk = upsert_dim_estabelecimento(
        pg_conn,
        {
            "cnes": "7654321",
            "nome": "UBS X",
            "cnpj_mantenedora": None,
            "tp_unid": 1,
            "sk_municipio": mun_sk,
            "fontes": {},
        },
    )
    assert sk > 0


def test_upsert_cbo_idempotente(pg_conn):
    sk1 = upsert_dim_cbo(
        pg_conn, {"cod_cbo": "225125", "descricao": "Médico"},
    )
    sk2 = upsert_dim_cbo(
        pg_conn,
        {"cod_cbo": "225125", "descricao": "Médico (atualizado)"},
    )
    assert sk1 == sk2


def test_upsert_cid10(pg_conn):
    sk = upsert_dim_cid10(
        pg_conn,
        {"cod_cid": "A00", "descricao": "Cholera", "capitulo": 1},
    )
    assert sk > 0


def test_upsert_municipio_idempotente(pg_conn):
    sk1 = upsert_dim_municipio(
        pg_conn,
        {"ibge6": "354130", "ibge7": "3541308", "nome": "PE", "uf": "SP"},
    )
    sk2 = upsert_dim_municipio(
        pg_conn,
        {
            "ibge6": "354130",
            "ibge7": "3541308",
            "nome": "Presidente Epitácio",
            "uf": "SP",
        },
    )
    assert sk1 == sk2


def test_upsert_procedimento_sus(pg_conn):
    sk = upsert_dim_procedimento_sus(
        pg_conn,
        {"cod_sigtap": "0101010010", "descricao": "Consulta médica"},
    )
    assert sk > 0


def test_lookup_competencia_dim_prepopulada(pg_conn):
    """dim_competencia seeded by migration 010 with 252 rows (2020-01..2040-12)."""
    lookup = PostgresDimLookup(pg_conn)
    sk = lookup.sk_competencia_por_yyyymm(202601)
    assert sk is not None
    assert sk > 0


def test_cache_hit_competencia_evita_segunda_query(pg_conn, monkeypatch):
    lookup = PostgresDimLookup(pg_conn)
    sk1 = lookup.sk_competencia_por_yyyymm(202602)
    assert sk1 is not None

    call_count = [0]
    orig_execute = pg_conn.execute

    def counting_execute(*args, **kwargs):
        call_count[0] += 1
        return orig_execute(*args, **kwargs)

    monkeypatch.setattr(pg_conn, "execute", counting_execute)
    sk2 = lookup.sk_competencia_por_yyyymm(202602)
    assert sk1 == sk2
    assert call_count[0] == 0
