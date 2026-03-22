"""test_cross_check.py — Testes das regras de reconciliação local × nacional (RQ-006 a RQ-011)."""

import pandas as pd
import pytest

from analysis.rules_engine import (
    detectar_estabelecimentos_fantasma,
    detectar_estabelecimentos_ausentes_local,
    detectar_profissionais_fantasma,
    detectar_profissionais_ausentes_local,
    detectar_divergencia_cbo,
    detectar_divergencia_carga_horaria,
)


def _df_estab(cnes_list: list[str], fonte: str) -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": cnes_list,
        "NOME_FANTASIA": [f"ESTAB {c}" for c in cnes_list],
        "FONTE": fonte,
    })


def _df_prof(cns_list: list[str], cnes_list: list[str], cbo_list: list[str],
             ch_list: list[int], fonte: str) -> pd.DataFrame:
    return pd.DataFrame({
        "CNS": cns_list,
        "CNES": cnes_list,
        "CBO": cbo_list,
        "CH_TOTAL": ch_list,
        "FONTE": fonte,
    })


class TestRQ006EstabelecimentosFantasma:

    def test_detecta_cnes_local_sem_correspondencia_nacional(self):
        local = _df_estab(["1111111", "2222222"], "LOCAL")
        nacional = _df_estab(["2222222"], "NACIONAL")
        resultado = detectar_estabelecimentos_fantasma(local, nacional)
        assert list(resultado["CNES"]) == ["1111111"]

    def test_nao_detecta_cnes_presente_em_ambos(self):
        local = _df_estab(["2222222"], "LOCAL")
        nacional = _df_estab(["2222222"], "NACIONAL")
        resultado = detectar_estabelecimentos_fantasma(local, nacional)
        assert resultado.empty

    def test_resultado_vazio_quando_ambas_fontes_vazias(self):
        local = _df_estab([], "LOCAL")
        nacional = _df_estab([], "NACIONAL")
        resultado = detectar_estabelecimentos_fantasma(local, nacional)
        assert resultado.empty

    def test_todos_locais_detectados_quando_nacional_vazio(self):
        local = _df_estab(["1111111", "2222222"], "LOCAL")
        nacional = _df_estab([], "NACIONAL")
        resultado = detectar_estabelecimentos_fantasma(local, nacional)
        assert len(resultado) == 2


class TestRQ007EstabelecimentosAusentesLocal:

    def test_detecta_cnes_nacional_sem_correspondencia_local(self):
        local = _df_estab(["2222222"], "LOCAL")
        nacional = _df_estab(["1111111", "2222222"], "NACIONAL")
        resultado = detectar_estabelecimentos_ausentes_local(local, nacional)
        assert list(resultado["CNES"]) == ["1111111"]

    def test_nao_detecta_cnes_presente_em_ambos(self):
        local = _df_estab(["2222222"], "LOCAL")
        nacional = _df_estab(["2222222"], "NACIONAL")
        resultado = detectar_estabelecimentos_ausentes_local(local, nacional)
        assert resultado.empty

    def test_resultado_vazio_quando_ambas_fontes_vazias(self):
        local = _df_estab([], "LOCAL")
        nacional = _df_estab([], "NACIONAL")
        resultado = detectar_estabelecimentos_ausentes_local(local, nacional)
        assert resultado.empty


class TestRQ008ProfissionaisFantasma:

    def test_detecta_cns_local_ausente_no_nacional(self):
        local = _df_prof(["123456789012345"], ["0985333"], ["515105"], [40], "LOCAL")
        nacional = _df_prof([], [], [], [], "NACIONAL")
        resultado = detectar_profissionais_fantasma(local, nacional)
        assert list(resultado["CNS"]) == ["123456789012345"]

    def test_nao_detecta_cns_presente_em_ambos(self):
        local = _df_prof(["123456789012345"], ["0985333"], ["515105"], [40], "LOCAL")
        nacional = _df_prof(["123456789012345"], ["0985333"], ["515105"], [40], "NACIONAL")
        resultado = detectar_profissionais_fantasma(local, nacional)
        assert resultado.empty

    def test_profissional_com_multiplos_vinculos_retorna_todos_os_registros(self):
        local = _df_prof(
            ["123456789012345", "123456789012345"],
            ["0985333", "0985334"],
            ["515105", "515105"],
            [40, 20],
            "LOCAL",
        )
        nacional = _df_prof([], [], [], [], "NACIONAL")
        resultado = detectar_profissionais_fantasma(local, nacional)
        assert len(resultado) == 2
        assert resultado["CNS"].nunique() == 1

    def test_resultado_vazio_quando_local_sem_cns(self):
        local = pd.DataFrame({
            "CNS": [None, None],
            "CNES": ["0985333", "0985334"],
            "CBO": ["515105", "515105"],
            "CH_TOTAL": [40, 40],
            "FONTE": ["LOCAL", "LOCAL"],
        })
        nacional = _df_prof([], [], [], [], "NACIONAL")
        resultado = detectar_profissionais_fantasma(local, nacional)
        assert resultado.empty


class TestRQ009ProfissionaisAusentesLocal:

    def test_detecta_cns_nacional_ausente_no_local(self):
        local = _df_prof([], [], [], [], "LOCAL")
        nacional = _df_prof(["123456789012345"], ["0985333"], ["515105"], [40], "NACIONAL")
        resultado = detectar_profissionais_ausentes_local(local, nacional)
        assert list(resultado["CNS"]) == ["123456789012345"]

    def test_nao_detecta_cns_presente_em_ambos(self):
        local = _df_prof(["123456789012345"], ["0985333"], ["515105"], [40], "LOCAL")
        nacional = _df_prof(["123456789012345"], ["0985333"], ["515105"], [40], "NACIONAL")
        resultado = detectar_profissionais_ausentes_local(local, nacional)
        assert resultado.empty

    def test_resultado_vazio_quando_nacional_sem_cns(self):
        local = _df_prof([], [], [], [], "LOCAL")
        nacional = pd.DataFrame({
            "CNS": [None],
            "CNES": ["0985333"],
            "CBO": ["515105"],
            "CH_TOTAL": [40],
            "FONTE": ["NACIONAL"],
        })
        resultado = detectar_profissionais_ausentes_local(local, nacional)
        assert resultado.empty


class TestRQ010DivergenciaCbo:

    def test_detecta_mesmo_cns_cnes_com_cbo_diferente(self):
        cns = "123456789012345"
        cnes = "0985333"
        local = _df_prof([cns], [cnes], ["515105"], [40], "LOCAL")
        nacional = _df_prof([cns], [cnes], ["322255"], [40], "NACIONAL")
        resultado = detectar_divergencia_cbo(local, nacional)
        assert len(resultado) == 1
        assert resultado["CBO_LOCAL"].iloc[0] == "515105"
        assert resultado["CBO_NACIONAL"].iloc[0] == "322255"

    def test_nao_detecta_mesmo_cns_cnes_com_cbo_igual(self):
        cns = "123456789012345"
        cnes = "0985333"
        local = _df_prof([cns], [cnes], ["515105"], [40], "LOCAL")
        nacional = _df_prof([cns], [cnes], ["515105"], [40], "NACIONAL")
        resultado = detectar_divergencia_cbo(local, nacional)
        assert resultado.empty

    def test_nao_detecta_quando_cns_so_em_uma_fonte(self):
        local = _df_prof(["111111111111111"], ["0985333"], ["515105"], [40], "LOCAL")
        nacional = _df_prof(["999999999999999"], ["0985333"], ["515105"], [40], "NACIONAL")
        resultado = detectar_divergencia_cbo(local, nacional)
        assert resultado.empty

    def test_retorna_colunas_cbo_local_e_cbo_nacional(self):
        cns = "123456789012345"
        cnes = "0985333"
        local = _df_prof([cns], [cnes], ["515105"], [40], "LOCAL")
        nacional = _df_prof([cns], [cnes], ["322255"], [40], "NACIONAL")
        resultado = detectar_divergencia_cbo(local, nacional)
        assert "CBO_LOCAL" in resultado.columns
        assert "CBO_NACIONAL" in resultado.columns


class TestRQ011DivergenciaCargaHoraria:

    def test_detecta_delta_acima_da_tolerancia(self):
        cns = "123456789012345"
        cnes = "0985333"
        local = _df_prof([cns], [cnes], ["515105"], [40], "LOCAL")
        nacional = _df_prof([cns], [cnes], ["515105"], [20], "NACIONAL")
        resultado = detectar_divergencia_carga_horaria(local, nacional, tolerancia=2)
        assert len(resultado) == 1
        assert resultado["DELTA_CH"].iloc[0] == 20

    def test_nao_detecta_delta_dentro_da_tolerancia(self):
        cns = "123456789012345"
        cnes = "0985333"
        local = _df_prof([cns], [cnes], ["515105"], [40], "LOCAL")
        nacional = _df_prof([cns], [cnes], ["515105"], [39], "NACIONAL")
        resultado = detectar_divergencia_carga_horaria(local, nacional, tolerancia=2)
        assert resultado.empty

    def test_detecta_delta_exatamente_acima_da_tolerancia(self):
        cns = "123456789012345"
        cnes = "0985333"
        local = _df_prof([cns], [cnes], ["515105"], [40], "LOCAL")
        nacional = _df_prof([cns], [cnes], ["515105"], [37], "NACIONAL")
        resultado = detectar_divergencia_carga_horaria(local, nacional, tolerancia=2)
        assert len(resultado) == 1
        assert resultado["DELTA_CH"].iloc[0] == 3

    def test_nao_detecta_quando_cns_so_em_uma_fonte(self):
        local = _df_prof(["111111111111111"], ["0985333"], ["515105"], [40], "LOCAL")
        nacional = _df_prof(["999999999999999"], ["0985333"], ["515105"], [20], "NACIONAL")
        resultado = detectar_divergencia_carga_horaria(local, nacional)
        assert resultado.empty

    def test_retorna_colunas_ch_local_ch_nacional_delta(self):
        cns = "123456789012345"
        cnes = "0985333"
        local = _df_prof([cns], [cnes], ["515105"], [40], "LOCAL")
        nacional = _df_prof([cns], [cnes], ["515105"], [20], "NACIONAL")
        resultado = detectar_divergencia_carga_horaria(local, nacional)
        assert "CH_LOCAL" in resultado.columns
        assert "CH_NACIONAL" in resultado.columns
        assert "DELTA_CH" in resultado.columns

    def test_tolerancia_padrao_e_dois(self):
        cns = "123456789012345"
        cnes = "0985333"
        local = _df_prof([cns], [cnes], ["515105"], [40], "LOCAL")
        nacional = _df_prof([cns], [cnes], ["515105"], [39], "NACIONAL")
        resultado = detectar_divergencia_carga_horaria(local, nacional)
        assert resultado.empty
