"""Testes das funções auxiliares de glosas."""
import pandas as pd

from glosas_helpers import _filtrar_glosas, _mascarar_pii_glosas


def _df_base() -> pd.DataFrame:
    return pd.DataFrame({
        "regra":             ["RQ008",   "RQ009",   "GHOST"],
        "cpf":               ["11111111111", "22222222222", "33333333333"],
        "nome_profissional": ["Ana Silva", "Bruno Costa", "Carla Dias"],
        "cnes_estabelecimento": ["2795001", "2795002", "2795001"],
        "motivo":            ["A", "B", "C"],
    })


class TestFiltrarGlosas:

    def test_sem_filtro_retorna_tudo(self):
        assert len(_filtrar_glosas(_df_base(), [], "")) == 3

    def test_filtro_por_regra(self):
        result = _filtrar_glosas(_df_base(), ["RQ008", "GHOST"], "")
        assert set(result["regra"]) == {"RQ008", "GHOST"}

    def test_busca_por_nome_case_insensitive(self):
        result = _filtrar_glosas(_df_base(), [], "ana")
        assert len(result) == 1
        assert result.iloc[0]["nome_profissional"] == "Ana Silva"

    def test_busca_por_cpf(self):
        result = _filtrar_glosas(_df_base(), [], "222")
        assert len(result) == 1

    def test_regra_e_busca_sem_intersecao(self):
        result = _filtrar_glosas(_df_base(), ["RQ008"], "carla")
        assert len(result) == 0

    def test_df_vazio_retorna_vazio(self):
        df = pd.DataFrame(columns=["regra", "nome_profissional", "cpf"])
        assert _filtrar_glosas(df, ["RQ008"], "").empty


class TestMascaraGlosas:

    def _df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "cpf": ["12345678901"],
            "cns": ["7001234567890123"],
            "nome": ["Ana Silva"],
        })

    def test_mascara_cpf_e_cns(self):
        result = _mascarar_pii_glosas(self._df(), mostrar_completo=False)
        assert result.iloc[0]["cpf"].startswith("***")
        assert result.iloc[0]["cns"].startswith("***")

    def test_sem_mascara_preserva_valores(self):
        result = _mascarar_pii_glosas(self._df(), mostrar_completo=True)
        assert result.iloc[0]["cpf"] == "12345678901"

    def test_nao_muta_df_original(self):
        df = self._df()
        _mascarar_pii_glosas(df, mostrar_completo=False)
        assert df.iloc[0]["cpf"] == "12345678901"

    def test_sem_mascara_nao_muta_df_original(self):
        df = self._df()
        result = _mascarar_pii_glosas(df, mostrar_completo=True)
        result["cpf"] = ["mutated"]
        assert df.iloc[0]["cpf"] == "12345678901"
