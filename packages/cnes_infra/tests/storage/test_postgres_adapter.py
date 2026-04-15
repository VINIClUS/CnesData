"""Testes de integracao para repositorios + UoW contra PostgreSQL real."""

import polars as pl
import pytest
from sqlalchemy import text

from cnes_domain.processing.row_mapper import (
    extrair_fonte,
    mapear_estabelecimentos,
    mapear_profissionais,
    mapear_vinculos,
)


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


def _gravar_profissionais(uow, competencia, df):
    fonte = extrair_fonte(df)
    prof_rows = mapear_profissionais(df)
    vinculo_rows = mapear_vinculos(competencia, df)
    with uow:
        uow.profissionais.gravar(prof_rows)
        uow.vinculos.snapshot_replace(
            competencia, fonte, vinculo_rows,
        )


def _gravar_estabelecimentos(uow, df):
    estab_rows = mapear_estabelecimentos(df)
    with uow:
        uow.estabelecimentos.gravar(estab_rows)


@pytest.mark.integration
def test_gravar_profissionais_insere_dim_profissional(uow, pg_engine):
    _gravar_profissionais(uow, "2026-01", _df_prof())
    with pg_engine.connect() as con:
        rows = con.execute(text(
            "SELECT cpf FROM gold.dim_profissional"
        )).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "11111111111"


@pytest.mark.integration
def test_gravar_profissionais_insere_fato_vinculo(uow, pg_engine):
    df = pl.concat([
        _df_prof("11111111111"), _df_prof("22222222222"),
    ])
    _gravar_estabelecimentos(uow, _df_estab())
    _gravar_profissionais(uow, "2026-01", df)
    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo"
        )).scalar()
    assert count == 2


@pytest.mark.integration
def test_gravar_profissionais_upsert_nao_duplica_rows(uow, pg_engine):
    _gravar_profissionais(uow, "2026-01", _df_prof())
    _gravar_profissionais(uow, "2026-01", _df_prof())
    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.dim_profissional"
        )).scalar()
    assert count == 1


@pytest.mark.integration
def test_sus_s_convertido_para_true(uow, pg_engine):
    _gravar_estabelecimentos(uow, _df_estab())
    _gravar_profissionais(uow, "2026-01", _df_prof(sus="S"))
    with pg_engine.connect() as con:
        sus = con.execute(text(
            "SELECT sus FROM gold.fato_vinculo"
        )).scalar()
    assert sus is True


@pytest.mark.integration
def test_sus_n_convertido_para_false(uow, pg_engine):
    _gravar_estabelecimentos(uow, _df_estab())
    _gravar_profissionais(uow, "2026-01", _df_prof(sus="N"))
    with pg_engine.connect() as con:
        sus = con.execute(text(
            "SELECT sus FROM gold.fato_vinculo"
        )).scalar()
    assert sus is False


@pytest.mark.integration
def test_fontes_jsonb_merge_em_conflito(uow, pg_engine):
    _gravar_profissionais(uow, "2026-01", _df_prof(fonte="LOCAL"))
    _gravar_profissionais(uow, "2026-01", _df_prof(fonte="NACIONAL"))
    with pg_engine.connect() as con:
        fontes = con.execute(text(
            "SELECT fontes FROM gold.dim_profissional"
        )).scalar()
    assert fontes.get("LOCAL") is True
    assert fontes.get("NACIONAL") is True


@pytest.mark.integration
def test_gravar_estabelecimentos_insere_dim_estabelecimento(
    uow, pg_engine,
):
    _gravar_estabelecimentos(uow, _df_estab())
    with pg_engine.connect() as con:
        rows = con.execute(text(
            "SELECT cnes FROM gold.dim_estabelecimento"
        )).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "1234567"


@pytest.mark.integration
def test_gravar_estabelecimentos_upsert_preserva_existente(
    uow, pg_engine,
):
    _gravar_estabelecimentos(uow, _df_estab(fonte="LOCAL"))
    _gravar_estabelecimentos(uow, _df_estab(fonte="NACIONAL"))
    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.dim_estabelecimento"
        )).scalar()
        fontes = con.execute(text(
            "SELECT fontes FROM gold.dim_estabelecimento"
        )).scalar()
    assert count == 1
    assert fontes.get("LOCAL") is True
    assert fontes.get("NACIONAL") is True


@pytest.mark.integration
def test_vinculo_sus_booleano(uow, pg_engine):
    _gravar_estabelecimentos(uow, _df_estab(vinculo_sus="S"))
    with pg_engine.connect() as con:
        v = con.execute(text(
            "SELECT vinculo_sus FROM gold.dim_estabelecimento"
        )).scalar()
    assert v is True


@pytest.mark.integration
def test_tenant_id_injetado_em_todos_os_registros(uow, pg_engine):
    _gravar_estabelecimentos(uow, _df_estab())
    _gravar_profissionais(uow, "2026-01", _df_prof())
    with pg_engine.connect() as con:
        t1 = con.execute(text(
            "SELECT DISTINCT tenant_id "
            "FROM gold.dim_estabelecimento"
        )).scalar()
        t2 = con.execute(text(
            "SELECT DISTINCT tenant_id "
            "FROM gold.dim_profissional"
        )).scalar()
    assert t1 == "355030"
    assert t2 == "355030"


@pytest.mark.integration
def test_constraint_cnes_formato_invalido_levanta_erro(uow, pg_engine):
    with pytest.raises(Exception):
        _gravar_estabelecimentos(uow, _df_estab(cnes="ABC"))


@pytest.mark.integration
def test_constraint_cpf_formato_invalido_levanta_erro(uow, pg_engine):
    with pytest.raises(Exception):
        _gravar_profissionais(uow, "2026-01", _df_prof(cpf="ABCDE"))


@pytest.mark.integration
def test_reclassificacao_cbo_nao_cria_fantasma(uow, pg_engine):
    _gravar_estabelecimentos(uow, _df_estab())

    df_acs = _df_prof(cpf="11111111111", cnes="1234567")
    df_acs = df_acs.with_columns(pl.lit("515105").alias("CBO"))
    _gravar_profissionais(uow, "2026-01", df_acs)

    df_ace = _df_prof(cpf="11111111111", cnes="1234567")
    df_ace = df_ace.with_columns(pl.lit("515110").alias("CBO"))
    _gravar_profissionais(uow, "2026-01", df_ace)

    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE cpf = '11111111111' "
            "AND competencia = '2026-01'"
        )).scalar()
    assert count == 1, (
        f"expected 1 vinculo after CBO reclassification, got {count}"
    )


@pytest.mark.integration
def test_profissional_multiplos_cbos_legitimos_preservados(
    uow, pg_engine,
):
    _gravar_estabelecimentos(uow, _df_estab())

    df_medico = _df_prof(cpf="11111111111", cnes="1234567")
    df_medico = df_medico.with_columns(
        pl.lit("225125").alias("CBO"),
    )
    df_professor = _df_prof(cpf="11111111111", cnes="1234567")
    df_professor = df_professor.with_columns(
        pl.lit("234110").alias("CBO"),
    )
    df_both = pl.concat([df_medico, df_professor])
    _gravar_profissionais(uow, "2026-01", df_both)

    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE cpf = '11111111111' "
            "AND competencia = '2026-01'"
        )).scalar()
    assert count == 2, (
        f"expected 2 legitimate CBO vinculos, got {count}"
    )


@pytest.mark.integration
def test_snapshot_replace_idempotente(uow, pg_engine):
    _gravar_estabelecimentos(uow, _df_estab())

    df = pl.concat([
        _df_prof(cpf="11111111111"),
        _df_prof(cpf="22222222222"),
    ])
    _gravar_profissionais(uow, "2026-01", df)
    _gravar_profissionais(uow, "2026-01", df)

    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-01'"
        )).scalar()
    assert count == 2, (
        f"expected 2 vinculos after idempotent re-process, got {count}"
    )


@pytest.mark.integration
def test_fonte_local_nao_destroi_nacional(uow, pg_engine):
    _gravar_estabelecimentos(uow, _df_estab())

    df_nac = _df_prof(cpf="11111111111", fonte="NACIONAL")
    _gravar_profissionais(uow, "2026-01", df_nac)

    df_local = _df_prof(cpf="22222222222", fonte="LOCAL")
    _gravar_profissionais(uow, "2026-01", df_local)

    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-01'"
        )).scalar()
        nac = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-01' "
            "AND fontes ? 'NACIONAL'"
        )).scalar()
        loc = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-01' "
            "AND fontes ? 'LOCAL'"
        )).scalar()
    assert count == 2, f"expected 2 total vinculos, got {count}"
    assert nac == 1, f"expected 1 NACIONAL vinculo, got {nac}"
    assert loc == 1, f"expected 1 LOCAL vinculo, got {loc}"


@pytest.mark.integration
def test_fonte_nacional_nao_destroi_local(uow, pg_engine):
    _gravar_estabelecimentos(uow, _df_estab())

    df_local = _df_prof(cpf="11111111111", fonte="LOCAL")
    _gravar_profissionais(uow, "2026-01", df_local)

    df_nac = _df_prof(cpf="22222222222", fonte="NACIONAL")
    _gravar_profissionais(uow, "2026-01", df_nac)

    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-01'"
        )).scalar()
    assert count == 2, f"expected 2 total vinculos, got {count}"


@pytest.mark.integration
def test_delete_insert_atomico_em_falha(uow, pg_engine):
    _gravar_estabelecimentos(uow, _df_estab())
    _gravar_profissionais(uow, "2026-01", _df_prof())

    with pg_engine.connect() as con:
        before = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo"
        )).scalar()
    assert before == 1

    df_bad = _df_prof(cpf="BADCPF00000")
    with pytest.raises(Exception):
        _gravar_profissionais(uow, "2026-01", df_bad)

    with pg_engine.connect() as con:
        after = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo"
        )).scalar()
    assert after == before, (
        f"DELETE should have been rolled back; "
        f"before={before} after={after}"
    )


@pytest.mark.integration
def test_competencia_isolada(uow, pg_engine):
    _gravar_estabelecimentos(uow, _df_estab())

    _gravar_profissionais(uow, "2026-02", _df_prof(cpf="11111111111"))
    _gravar_profissionais(uow, "2026-03", _df_prof(cpf="22222222222"))

    with pg_engine.connect() as con:
        feb = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-02'"
        )).scalar()
        mar = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-03'"
        )).scalar()
    assert feb == 1, f"expected 1 vinculo in 2026-02, got {feb}"
    assert mar == 1, f"expected 1 vinculo in 2026-03, got {mar}"


@pytest.mark.integration
def test_profissional_troca_estabelecimento(uow, pg_engine):
    _gravar_estabelecimentos(uow, _df_estab(cnes="1234567"))
    _gravar_estabelecimentos(uow, _df_estab(cnes="7654321"))

    df_cnes_a = _df_prof(cpf="11111111111", cnes="1234567")
    _gravar_profissionais(uow, "2026-01", df_cnes_a)

    df_cnes_b = _df_prof(cpf="11111111111", cnes="7654321")
    _gravar_profissionais(uow, "2026-01", df_cnes_b)

    with pg_engine.connect() as con:
        rows = con.execute(text(
            "SELECT cnes FROM gold.fato_vinculo "
            "WHERE cpf = '11111111111' "
            "AND competencia = '2026-01'"
        )).fetchall()
    cnes_list = [r[0] for r in rows]
    assert "7654321" in cnes_list, "new CNES should be present"
    assert "1234567" not in cnes_list, (
        "old CNES should have been replaced"
    )
