"""Verifica que a migração 010 cria Gold v2 + landing.extractions."""

import pytest
from sqlalchemy import text


@pytest.mark.postgres
def test_010_dim_competencia_populada_2020_a_2040(pg_engine):
    with pg_engine.connect() as con:
        count = con.execute(
            text("SELECT COUNT(*) FROM gold.dim_competencia")
        ).scalar()
    assert count == 252


@pytest.mark.postgres
def test_010_cria_sete_dimensoes_em_gold(pg_engine):
    with pg_engine.connect() as con:
        rows = con.execute(text(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname = 'gold' AND tablename LIKE 'dim_%' "
            "ORDER BY tablename"
        )).all()
    nomes = [r.tablename for r in rows]
    assert nomes == [
        "dim_cbo", "dim_cid10", "dim_competencia", "dim_estabelecimento",
        "dim_municipio", "dim_procedimento_sus", "dim_profissional",
    ]


@pytest.mark.postgres
def test_010_cria_quatro_fatos_particionados_com_particao_2026(pg_engine):
    with pg_engine.connect() as con:
        rows = con.execute(text(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname = 'gold' AND tablename LIKE 'fato_%_2026' "
            "ORDER BY tablename"
        )).all()
    nomes = [r.tablename for r in rows]
    assert nomes == [
        "fato_internacao_2026",
        "fato_procedimento_aih_2026",
        "fato_producao_ambulatorial_2026",
        "fato_vinculo_cnes_2026",
    ]


@pytest.mark.postgres
def test_010_cria_materialized_view_auditoria_producao(pg_engine):
    with pg_engine.connect() as con:
        existe = con.execute(text(
            "SELECT 1 FROM pg_matviews "
            "WHERE schemaname = 'gold' AND matviewname = 'view_auditoria_producao'"
        )).scalar()
    assert existe == 1


@pytest.mark.postgres
def test_010_cria_landing_extractions_com_rls(pg_engine):
    with pg_engine.connect() as con:
        rls_ativo = con.execute(text(
            "SELECT relrowsecurity FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'landing' AND c.relname = 'extractions'"
        )).scalar()
    assert rls_ativo is True


@pytest.mark.postgres
def test_010_remove_queue_schema(pg_engine):
    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM pg_namespace WHERE nspname = 'queue'"
        )).scalar()
    assert count == 0


@pytest.mark.postgres
def test_010_check_constraint_competencia_yyyymm(pg_engine):
    with pg_engine.connect() as con:
        definicao = con.execute(text(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conname = 'chk_competencia_yyyymm'"
        )).scalar()
    assert "200001" in definicao
    assert "209912" in definicao
    assert "% 100" in definicao


@pytest.mark.postgres
def test_010_landing_extractions_tem_unique_source_comp(pg_engine):
    with pg_engine.connect() as con:
        definicao = con.execute(text(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conname = 'uniq_source_comp'"
        )).scalar()
    assert "fonte_sistema" in definicao
    assert "tenant_id" in definicao
    assert "competencia" in definicao
    assert "tipo_extracao" in definicao
