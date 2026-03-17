"""
test_transformer.py — Testes Unitários da Função transformar()

Objetivo: Verificar que a transformação dos dados brutos do banco
funciona corretamente, sem precisar de conexão com o banco de dados.

As fixtures `df_profissionais_bruto` e `df_com_equipe` estão em conftest.py
e são injetadas automaticamente pelo pytest pelo nome do parâmetro.

Categorias de teste:
  - Preservação de dados: transformar() não deve perder registros.
  - Limpeza de strings: strip() em colunas de texto.
  - Preenchimento de nulos: LEFT JOIN resulta em None nas colunas de equipe.
  - Imutabilidade: o DataFrame original não deve ser modificado.
  - Estrutura das colunas: todas as colunas esperadas devem existir.
"""

import pandas as pd
import pytest

# conftest.py adicionou src/ ao sys.path
from cnes_exporter import transformar


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 1: Preservação de Dados
# ─────────────────────────────────────────────────────────────────────────────

class TestPreservacaoDeDados:

    def test_transf_nao_perde_registros(self, df_profissionais_bruto):
        """O número de linhas deve ser idêntico antes e depois da transformação."""
        total_antes = len(df_profissionais_bruto)
        resultado = transformar(df_profissionais_bruto)
        assert len(resultado) == total_antes, (
            f"Esperado {total_antes} registros, obtido {len(resultado)}"
        )

    def test_transf_preserva_todas_as_colunas(self, df_profissionais_bruto):
        """Todas as colunas originais devem estar presentes no resultado."""
        resultado = transformar(df_profissionais_bruto)
        colunas_esperadas = set(df_profissionais_bruto.columns)
        colunas_resultado = set(resultado.columns)
        assert colunas_esperadas == colunas_resultado, (
            f"Colunas perdidas: {colunas_esperadas - colunas_resultado}\n"
            f"Colunas extras: {colunas_resultado - colunas_esperadas}"
        )

    def test_transf_preserva_carga_horaria(self, df_profissionais_bruto):
        """CARGA_HORARIA é numérica e não deve ser alterada pela transformação."""
        resultado = transformar(df_profissionais_bruto)
        assert list(resultado["CARGA_HORARIA"]) == list(df_profissionais_bruto["CARGA_HORARIA"])


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 2: Limpeza de Strings
# ─────────────────────────────────────────────────────────────────────────────

class TestLimpezaDeStrings:

    def test_cpf_sem_espacos_extras(self, df_profissionais_bruto):
        """CPF não pode ter espaços antes ou depois (prejudica joins e exibição)."""
        resultado = transformar(df_profissionais_bruto)
        for cpf in resultado["CPF"]:
            assert cpf == cpf.strip(), f"CPF com espaço extra encontrado: '{cpf}'"

    def test_nome_sem_espacos_extras(self, df_profissionais_bruto):
        """NOME_PROFISSIONAL não pode ter espaços antes ou depois."""
        resultado = transformar(df_profissionais_bruto)
        for nome in resultado["NOME_PROFISSIONAL"]:
            assert nome == nome.strip(), f"Nome com espaço extra: '{nome}'"

    def test_cbo_sem_espacos_extras(self, df_profissionais_bruto):
        """CBO não pode ter espaços extras."""
        resultado = transformar(df_profissionais_bruto)
        for cbo in resultado["CBO"]:
            assert cbo == cbo.strip(), f"CBO com espaço extra: '{cbo}'"

    def test_estabelecimento_sem_espacos_extras(self, df_profissionais_bruto):
        """ESTABELECIMENTO não pode ter espaços antes ou depois."""
        resultado = transformar(df_profissionais_bruto)
        for estab in resultado["ESTABELECIMENTO"]:
            assert estab == estab.strip(), f"Estabelecimento com espaço extra: '{estab}'"


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 3: Preenchimento de Nulos (LEFT JOIN → sem equipe)
# ─────────────────────────────────────────────────────────────────────────────

class TestPreenchimentoDeNulos:

    def test_nome_equipe_nulo_vira_sem_equipe(self, df_profissionais_bruto):
        """Profissionais sem equipe devem ter NOME_EQUIPE = 'SEM EQUIPE VINCULADA'."""
        resultado = transformar(df_profissionais_bruto)
        assert resultado["NOME_EQUIPE"].isna().sum() == 0, "Ainda há nulos em NOME_EQUIPE"
        assert all(v == "SEM EQUIPE VINCULADA" for v in resultado["NOME_EQUIPE"]), (
            "NOME_EQUIPE deve ser 'SEM EQUIPE VINCULADA' para todos os sem equipe"
        )

    def test_cod_ine_equipe_nulo_vira_traco(self, df_profissionais_bruto):
        """COD_INE_EQUIPE nulo deve virar '-'."""
        resultado = transformar(df_profissionais_bruto)
        assert resultado["COD_INE_EQUIPE"].isna().sum() == 0
        assert all(v == "-" for v in resultado["COD_INE_EQUIPE"])

    def test_tipo_equipe_nulo_vira_traco(self, df_profissionais_bruto):
        """TIPO_EQUIPE nulo deve virar '-'."""
        resultado = transformar(df_profissionais_bruto)
        assert resultado["TIPO_EQUIPE"].isna().sum() == 0
        assert all(v == "-" for v in resultado["TIPO_EQUIPE"])

    def test_equipe_preenchida_nao_e_alterada(self, df_com_equipe):
        """Quando a equipe já existe (não é nula), não deve ser sobrescrita."""
        resultado = transformar(df_com_equipe)
        assert resultado["NOME_EQUIPE"].iloc[0] == "ESF VILA GERONIMO", (
            "Equipes preenchidas não devem ser sobrescritas pelo fillna"
        )
        assert resultado["TIPO_EQUIPE"].iloc[0] == "01"
        assert resultado["COD_INE_EQUIPE"].iloc[0] == "0001365993"


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 4: Imutabilidade do DataFrame Original
# ─────────────────────────────────────────────────────────────────────────────

class TestImutabilidade:

    def test_dataframe_original_nao_modificado(self, df_profissionais_bruto):
        """
        A função transformar() deve trabalhar em uma CÓPIA do DataFrame.
        O original (df.copy() aplicado internamente) não deve ser alterado.
        """
        # Salva valores originais de NOME_EQUIPE (que são None)
        valores_originais = list(df_profissionais_bruto["NOME_EQUIPE"])

        transformar(df_profissionais_bruto)  # Executa transformação

        # O original deve continuar com None, não com o valor preenchido
        valores_apos = list(df_profissionais_bruto["NOME_EQUIPE"])
        assert valores_originais == valores_apos, (
            "transformar() modificou o DataFrame original — use df.copy() internamente"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 5: Casos de Borda (Edge Cases)
# ─────────────────────────────────────────────────────────────────────────────

class TestCasosDeBorda:

    def test_dataframe_vazio_retorna_vazio(self):
        """transformar() deve aceitar um DataFrame vazio sem erros."""
        df_vazio = pd.DataFrame(columns=[
            "CPF", "NOME_PROFISSIONAL", "CBO", "CARGA_HORARIA",
            "COD_CNES", "ESTABELECIMENTO", "TIPO_ESTAB",
            "COD_INE_EQUIPE", "NOME_EQUIPE", "TIPO_EQUIPE"
        ])
        resultado = transformar(df_vazio)
        assert len(resultado) == 0
        assert set(resultado.columns) == set(df_vazio.columns)

    def test_transformacao_idempotente(self, df_profissionais_bruto):
        """
        Aplicar transformar() duas vezes seguidas deve dar o mesmo resultado
        que aplicar uma vez (idempotência é uma propriedade desejável em ETL).
        """
        resultado_1x = transformar(df_profissionais_bruto)
        resultado_2x = transformar(resultado_1x)

        pd.testing.assert_frame_equal(
            resultado_1x.reset_index(drop=True),
            resultado_2x.reset_index(drop=True),
        )
