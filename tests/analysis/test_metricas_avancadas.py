"""Testes para metricas_avancadas."""
import pandas as pd
import pytest

from src.analysis.metricas_avancadas import (
    calcular_anomalias_por_cbo,
    calcular_p90_ch,
    calcular_proporcao_feminina,
    calcular_proporcao_feminina_por_cnes,
    calcular_ranking_cnes,
    calcular_reincidencia,
    calcular_taxa_anomalia,
    calcular_taxa_resolucao,
    calcular_top_glosas,
    calcular_velocidade_regularizacao,
)


def _vinculos(*cpfs) -> pd.DataFrame:
    return pd.DataFrame({"CPF": list(cpfs), "CNS": [None] * len(cpfs)})


def _glosas(*cpfs) -> pd.DataFrame:
    return pd.DataFrame({"cpf": list(cpfs), "cns": [None] * len(cpfs)})


def test_taxa_anomalia_proporcao_correta():
    df_v = _vinculos("111", "222")
    df_g = _glosas("111")
    assert calcular_taxa_anomalia(df_v, df_g) == pytest.approx(0.5)


def test_taxa_anomalia_df_vazio_retorna_zero():
    df_v = pd.DataFrame({"CPF": [], "CNS": []})
    df_g = _glosas("111")
    assert calcular_taxa_anomalia(df_v, df_g) == 0.0


def test_taxa_anomalia_sem_glosas_retorna_zero():
    df_v = _vinculos("111", "222")
    df_g = pd.DataFrame({"cpf": [], "cns": []})
    assert calcular_taxa_anomalia(df_v, df_g) == 0.0


def test_p90_ch_total_correto():
    df = pd.DataFrame({"CH_TOTAL": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]})
    assert calcular_p90_ch(df) == pytest.approx(91.0)


def test_p90_ch_coluna_ausente_retorna_zero():
    df = pd.DataFrame({"OUTRO": [1, 2, 3]})
    assert calcular_p90_ch(df) == 0.0


def test_p90_ch_df_vazio_retorna_zero():
    df = pd.DataFrame({"CH_TOTAL": []})
    assert calcular_p90_ch(df) == 0.0


def test_proporcao_feminina_50_pct():
    df = pd.DataFrame({"SEXO": ["F", "F", "M", "M"]})
    assert calcular_proporcao_feminina(df) == pytest.approx(0.5)


def test_proporcao_feminina_sem_dados():
    df = pd.DataFrame({"SEXO": [None, None]})
    assert calcular_proporcao_feminina(df) == 0.0


def test_proporcao_feminina_coluna_ausente():
    df = pd.DataFrame({"OUTRO": ["F", "M"]})
    assert calcular_proporcao_feminina(df) == 0.0


def test_proporcao_feminina_por_cnes_agrupa_corretamente():
    df = pd.DataFrame({
        "CNES": ["C1", "C1", "C2", "C2"],
        "SEXO": ["F", "M", "F", "F"],
    })
    result = calcular_proporcao_feminina_por_cnes(df)
    by_cnes = {r["cnes"]: r for r in result}
    assert by_cnes["C1"]["proporcao_f"] == pytest.approx(0.5)
    assert by_cnes["C1"]["total"] == 2
    assert by_cnes["C2"]["proporcao_f"] == pytest.approx(1.0)
    assert by_cnes["C2"]["total"] == 2


def test_top_glosas_retorna_top_n():
    df = pd.DataFrame({
        "cpf": ["111", "111", "111", "222", "222", "333"],
        "cns": [None] * 6,
        "nome_profissional": ["Ana"] * 3 + ["Bia"] * 2 + ["Cia"],
    })
    result = calcular_top_glosas(df, n=2)
    assert len(result) == 2
    assert result[0]["total"] == 3
    assert result[0]["cpf"] == "111"
    assert result[1]["total"] == 2


def test_top_glosas_df_vazio():
    df = pd.DataFrame({"cpf": [], "cns": [], "nome_profissional": []})
    assert calcular_top_glosas(df, n=5) == []


def test_calcular_reincidencia_detecta_reincidente():
    df = pd.DataFrame({
        "competencia": ["2026-01", "2026-02"],
        "regra": ["RQ003B", "RQ003B"],
        "cpf": ["111", "111"],
        "cns": [None, None],
    })
    assert calcular_reincidencia(df) == 1


def test_calcular_reincidencia_sem_reincidentes():
    df = pd.DataFrame({
        "competencia": ["2026-01", "2026-02"],
        "regra": ["RQ003B", "RQ003B"],
        "cpf": ["111", "222"],
        "cns": [None, None],
    })
    assert calcular_reincidencia(df) == 0


def test_calcular_reincidencia_usa_cns_quando_cpf_nulo():
    df = pd.DataFrame({
        "competencia": ["2026-01", "2026-02"],
        "regra": ["RQ005", "RQ005"],
        "cpf": [None, None],
        "cns": ["CNS1", "CNS1"],
    })
    assert calcular_reincidencia(df) == 1


def test_taxa_resolucao_50_pct():
    df = pd.DataFrame({
        "competencia": ["2026-01", "2026-01", "2026-02"],
        "regra": ["RQ003B", "RQ005", "RQ003B"],
        "cpf": ["111", "222", "111"],
        "cns": [None, None, None],
    })
    result = calcular_taxa_resolucao("2026-01", "2026-02", df)
    assert result == pytest.approx(0.5)


def test_taxa_resolucao_sem_anterior():
    df = pd.DataFrame({
        "competencia": ["2026-02"],
        "regra": ["RQ003B"],
        "cpf": ["111"],
        "cns": [None],
    })
    assert calcular_taxa_resolucao("2026-01", "2026-02", df) == 0.0


def test_taxa_resolucao_todas_resolvidas():
    df = pd.DataFrame({
        "competencia": ["2026-01", "2026-02"],
        "regra": ["RQ003B", "RQ005"],
        "cpf": ["111", "222"],
        "cns": [None, None],
    })
    result = calcular_taxa_resolucao("2026-01", "2026-02", df)
    assert result == pytest.approx(1.0)


def test_velocidade_regularizacao_media():
    df = pd.DataFrame({
        "competencia": ["2026-01", "2026-02", "2026-03"],
        "regra": ["RQ003B", "RQ003B", "RQ005"],
        "cpf": ["111", "111", "222"],
        "cns": [None, None, None],
    })
    result = calcular_velocidade_regularizacao(df)
    assert result == pytest.approx(1.0)


def test_velocidade_regularizacao_sem_resolvidas_retorna_zero():
    df = pd.DataFrame({
        "competencia": ["2026-01", "2026-02"],
        "regra": ["RQ003B", "RQ003B"],
        "cpf": ["111", "111"],
        "cns": [None, None],
    })
    result = calcular_velocidade_regularizacao(df)
    assert result == 0.0


def test_ranking_cnes_indice_conformidade():
    df_estab = pd.DataFrame({"CNES": ["C1"], "NOME_FANTASIA": ["Hospital A"]})
    df_glosas = pd.DataFrame({"cnes_estabelecimento": ["C1"]})
    df_vinculos = pd.DataFrame({"CNES": ["C1", "C1"]})
    result = calcular_ranking_cnes(df_estab, df_glosas, df_vinculos)
    assert len(result) == 1
    assert result[0]["cnes"] == "C1"
    assert result[0]["indice_conformidade"] == pytest.approx(0.5)
    assert result[0]["total_anomalias"] == 1


def test_calcular_anomalias_por_cbo_taxa_correta():
    df_vinculos = pd.DataFrame({
        "CPF": ["111", "222", "333"],
        "CNS": [None, None, None],
        "CBO": ["225125", "225125", "515105"],
    })
    df_glosas = pd.DataFrame({
        "cpf": ["111"],
        "cns": [None],
        "cnes_estabelecimento": ["C1"],
    })
    cbo_lookup = {"225125": "Médico Clínico", "515105": "Técnico de Enfermagem"}
    result = calcular_anomalias_por_cbo(df_vinculos, df_glosas, cbo_lookup)
    by_cbo = {r["cbo"]: r for r in result}
    assert by_cbo["225125"]["total"] == 1
    assert by_cbo["225125"]["taxa"] == pytest.approx(0.5)
    assert by_cbo["225125"]["descricao"] == "Médico Clínico"
    assert by_cbo["515105"]["total"] == 0
    assert by_cbo["515105"]["taxa"] == pytest.approx(0.0)
