"""Testes de calcular_delta — drift entre df_processado atual e snapshot anterior."""
import pandas as pd
import pytest

from analysis.delta_snapshot import DeltaSnapshot, calcular_delta


def _base() -> pd.DataFrame:
    """DataFrame base com 3 profissionais."""
    return pd.DataFrame({
        "CPF":         ["00000000001", "00000000002", "00000000003"],
        "CNES":        ["1111111",     "1111111",     "2222222"],
        "CBO":         ["515105",      "225125",      "515105"],
        "CH_TOTAL":    [40,            20,            30],
        "TIPO_VINCULO":["30",          "30",          "40"],
    })


class TestCalcularDeltaSemMudancas:
    def test_zero_novos_removidos_alterados(self):
        df = _base()
        delta = calcular_delta(df, df.copy())
        assert delta.n_novos == 0
        assert delta.n_removidos == 0
        assert delta.n_alterados == 0
        assert delta.novos == []
        assert delta.removidos == []
        assert delta.alterados == []


class TestNovosVinculos:
    def test_detecta_novo_profissional(self):
        anterior = _base().iloc[:2].copy()
        atual = _base()
        delta = calcular_delta(atual, anterior)
        assert delta.n_novos == 1
        assert delta.novos[0]["CPF"] == "00000000003"

    def test_novo_estabelecimento_mesmo_cpf(self):
        anterior = _base().copy()
        novo = pd.DataFrame({
            "CPF": ["00000000001"], "CNES": ["9999999"],
            "CBO": ["515105"], "CH_TOTAL": [20], "TIPO_VINCULO": ["30"],
        })
        atual = pd.concat([_base(), novo], ignore_index=True)
        delta = calcular_delta(atual, anterior)
        assert delta.n_novos == 1
        assert delta.novos[0]["CPF"] == "00000000001"
        assert delta.novos[0]["CNES"] == "9999999"


class TestRemovidosVinculos:
    def test_detecta_profissional_removido(self):
        atual = _base().iloc[:2].copy()
        anterior = _base()
        delta = calcular_delta(atual, anterior)
        assert delta.n_removidos == 1
        assert delta.removidos[0]["CPF"] == "00000000003"


class TestAlteracoes:
    def test_detecta_mudanca_cbo(self):
        anterior = _base()
        atual = _base().copy()
        atual.loc[0, "CBO"] = "225125"
        delta = calcular_delta(atual, anterior)
        assert delta.n_alterados == 1
        assert delta.alterados[0]["CBO_anterior"] == "515105"
        assert delta.alterados[0]["CBO_atual"] == "225125"

    def test_detecta_mudanca_ch_total(self):
        anterior = _base()
        atual = _base().copy()
        atual.loc[1, "CH_TOTAL"] = 40
        delta = calcular_delta(atual, anterior)
        assert delta.n_alterados == 1
        assert delta.alterados[0]["CH_TOTAL_anterior"] == 20
        assert delta.alterados[0]["CH_TOTAL_atual"] == 40

    def test_nao_conta_como_alterado_se_igual(self):
        df = _base()
        delta = calcular_delta(df, df.copy())
        assert delta.n_alterados == 0

    def test_colunas_ausentes_ignoradas(self):
        atual = _base().drop(columns=["TIPO_VINCULO"])
        anterior = _base().drop(columns=["TIPO_VINCULO"])
        anterior.loc[0, "CBO"] = "999999"
        delta = calcular_delta(atual, anterior)
        assert delta.n_alterados == 1
