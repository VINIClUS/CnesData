"""Testes de integração para PostgresAdapter contra banco PostgreSQL real."""

import polars as pl
import pytest
from sqlalchemy import text


def _df_prof(cpf="11111111111", cnes="1234567", fonte="LOCAL", sus="S"):
    return pl.DataFrame([{
        "CPF": cpf, "CNS": "123456789012345", "NOME_PROFISSIONAL": "Test Prof",
        "SEXO": "M", "CBO": "515105", "CNES": cnes, "TIPO_VINCULO": "010101",
        "SUS": sus, "CH_TOTAL": 40, "CH_AMBULATORIAL": 40,
        "CH_OUTRAS": 0, "CH_HOSPITALAR": 0, "FONTE": fonte,
    }])


def _df_estab(cnes="1234567", fonte="LOCAL", vinculo_sus="S"):
    return pl.DataFrame([{
        "CNES": cnes, "NOME_FANTASIA": "UBS Test", "TIPO_UNIDADE": "01",
        "CNPJ_MANTENEDORA": "55293427000117", "NATUREZA_JURIDICA": "1244",
        "COD_MUNICIPIO": "355030", "VINCULO_SUS": vinculo_sus, "FONTE": fonte,
    }])


@pytest.mark.integration
def test_gravar_profissionais_insere_dim_profissional(adapter, pg_engine):
    adapter.gravar_profissionais("2026-01", _df_prof())
    with pg_engine.connect() as con:
        rows = con.execute(text("SELECT cpf FROM gold.dim_profissional")).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "11111111111"


@pytest.mark.integration
def test_gravar_profissionais_insere_fato_vinculo(adapter, pg_engine):
    df = pl.concat([_df_prof("11111111111"), _df_prof("22222222222")])
    adapter.gravar_estabelecimentos("2026-01", _df_estab())
    adapter.gravar_profissionais("2026-01", df)
    with pg_engine.connect() as con:
        count = con.execute(text("SELECT COUNT(*) FROM gold.fato_vinculo")).scalar()
    assert count == 2


@pytest.mark.integration
def test_gravar_profissionais_upsert_nao_duplica_rows(adapter, pg_engine):
    adapter.gravar_profissionais("2026-01", _df_prof())
    adapter.gravar_profissionais("2026-01", _df_prof())
    with pg_engine.connect() as con:
        count = con.execute(text("SELECT COUNT(*) FROM gold.dim_profissional")).scalar()
    assert count == 1


@pytest.mark.integration
def test_sus_s_convertido_para_true(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab())
    adapter.gravar_profissionais("2026-01", _df_prof(sus="S"))
    with pg_engine.connect() as con:
        sus = con.execute(text("SELECT sus FROM gold.fato_vinculo")).scalar()
    assert sus is True


@pytest.mark.integration
def test_sus_n_convertido_para_false(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab())
    adapter.gravar_profissionais("2026-01", _df_prof(sus="N"))
    with pg_engine.connect() as con:
        sus = con.execute(text("SELECT sus FROM gold.fato_vinculo")).scalar()
    assert sus is False


@pytest.mark.integration
def test_fontes_jsonb_merge_em_conflito(adapter, pg_engine):
    adapter.gravar_profissionais("2026-01", _df_prof(fonte="LOCAL"))
    adapter.gravar_profissionais("2026-01", _df_prof(fonte="NACIONAL"))
    with pg_engine.connect() as con:
        fontes = con.execute(text("SELECT fontes FROM gold.dim_profissional")).scalar()
    assert fontes.get("LOCAL") is True
    assert fontes.get("NACIONAL") is True


@pytest.mark.integration
def test_gravar_estabelecimentos_insere_dim_estabelecimento(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab())
    with pg_engine.connect() as con:
        rows = con.execute(text("SELECT cnes FROM gold.dim_estabelecimento")).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "1234567"


@pytest.mark.integration
def test_gravar_estabelecimentos_upsert_preserva_existente(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab(fonte="LOCAL"))
    adapter.gravar_estabelecimentos("2026-01", _df_estab(fonte="NACIONAL"))
    with pg_engine.connect() as con:
        count = con.execute(text("SELECT COUNT(*) FROM gold.dim_estabelecimento")).scalar()
        fontes = con.execute(text("SELECT fontes FROM gold.dim_estabelecimento")).scalar()
    assert count == 1
    assert fontes.get("LOCAL") is True
    assert fontes.get("NACIONAL") is True


@pytest.mark.integration
def test_vinculo_sus_booleano(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab(vinculo_sus="S"))
    with pg_engine.connect() as con:
        v = con.execute(text("SELECT vinculo_sus FROM gold.dim_estabelecimento")).scalar()
    assert v is True


@pytest.mark.integration
def test_tenant_id_injetado_em_todos_os_registros(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab())
    adapter.gravar_profissionais("2026-01", _df_prof())
    with pg_engine.connect() as con:
        t1 = con.execute(text("SELECT DISTINCT tenant_id FROM gold.dim_estabelecimento")).scalar()
        t2 = con.execute(text("SELECT DISTINCT tenant_id FROM gold.dim_profissional")).scalar()
    assert t1 == "355030"
    assert t2 == "355030"


@pytest.mark.integration
def test_constraint_cnes_formato_invalido_levanta_erro(adapter, pg_engine):
    with pytest.raises(Exception):
        adapter.gravar_estabelecimentos("2026-01", _df_estab(cnes="ABC"))


@pytest.mark.integration
def test_constraint_cpf_formato_invalido_levanta_erro(adapter, pg_engine):
    with pytest.raises(Exception):
        adapter.gravar_profissionais("2026-01", _df_prof(cpf="ABCDE"))
