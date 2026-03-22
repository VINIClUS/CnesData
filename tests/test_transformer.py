"""
test_transformer.py — Testes Unitários de processing.transformer

Objetivo: verificar que o pipeline de transformação aplica corretamente
todas as regras de qualidade sem conexão com banco de dados.

As fixtures estão em conftest.py e são injetadas pelo pytest via nome.
Nomes de coluna seguem os aliases da Query Master (data_dictionary.md).

Categorias de teste:
  - Preservação: transformar() não deve perder registros válidos nem colunas.
  - Limpeza de strings: strip() em colunas de texto.
  - RQ-002: exclusão de CPFs nulos ou fora de 11 caracteres.
  - RQ-003: flag ALERTA_STATUS_CH para vínculos com carga horária zero.
  - Preenchimento de nulos: colunas opcionais de equipe (LEFT JOIN).
  - Imutabilidade: o DataFrame original não deve ser modificado.
  - Edge cases: DataFrame vazio, idempotência.
"""

import pandas as pd

# conftest.py adicionou src/ ao sys.path
from processing.transformer import (
    transformar,
    ALERTA_ATIVO_SEM_CH,
    ALERTA_CH_OK,
    VALOR_SEM_EQUIPE,
    VALOR_SEM_INE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 1: Preservação de Dados
# ─────────────────────────────────────────────────────────────────────────────

def _df_minimo(cpf="11716723817", ch_total=40) -> pd.DataFrame:
    return pd.DataFrame({
        "CPF":               [cpf],
        "CNS":               ["702002887429583"],
        "NOME_PROFISSIONAL": ["TESTE"],
        "NOME_SOCIAL":       [None],
        "SEXO":              ["F"],
        "DATA_NASCIMENTO":   ["1990-01-01"],
        "CBO":               ["515105"],
        "TIPO_VINCULO":      ["010101"],
        "SUS":               ["S"],
        "CH_TOTAL":          [ch_total],
        "CH_AMBULATORIAL":   [ch_total],
        "CH_OUTRAS":         [0],
        "CH_HOSPITALAR":     [0],
        "CNES":              ["0985333"],
        "ESTABELECIMENTO":   ["UBS TESTE"],
        "TIPO_UNIDADE":      ["02"],
        "COD_MUNICIPIO":     ["354130"],
        "INE":               [None],
        "NOME_EQUIPE":       [None],
        "TIPO_EQUIPE":       [None],
    })


class TestZeroPaddingCPF:

    def test_cpf_10_digitos_recebe_zero_a_esquerda(self):
        resultado = transformar(_df_minimo(cpf="1234567890"))
        assert resultado["CPF"].iloc[0] == "01234567890"

    def test_cpf_9_digitos_recebe_dois_zeros(self):
        resultado = transformar(_df_minimo(cpf="123456789"))
        assert resultado["CPF"].iloc[0] == "00123456789"

    def test_cpf_11_digitos_nao_muda(self):
        resultado = transformar(_df_minimo(cpf="11716723817"))
        assert resultado["CPF"].iloc[0] == "11716723817"

    def test_cpf_nulo_nao_recebe_zfill_e_excluido(self):
        resultado = transformar(_df_minimo(cpf=None))
        assert len(resultado) == 0


class TestPreservacaoDeDados:

    def test_nao_perde_registros_com_cpf_valido(self, df_profissionais_bruto):
        """CPFs válidos: nenhum registro deve ser removido."""
        total_antes = len(df_profissionais_bruto)
        resultado = transformar(df_profissionais_bruto)
        assert len(resultado) == total_antes

    def test_preserva_colunas_de_entrada(self, df_profissionais_bruto):
        """Todas as colunas da entrada devem existir na saída."""
        resultado = transformar(df_profissionais_bruto)
        colunas_entrada = set(df_profissionais_bruto.columns)
        colunas_saida = set(resultado.columns)
        assert colunas_entrada.issubset(colunas_saida), (
            f"Colunas perdidas na transformação: {colunas_entrada - colunas_saida}"
        )

    def test_adiciona_coluna_alerta_status_ch(self, df_profissionais_bruto):
        """A coluna ALERTA_STATUS_CH deve ser adicionada pelo transformar()."""
        resultado = transformar(df_profissionais_bruto)
        assert "ALERTA_STATUS_CH" in resultado.columns

    def test_preserva_valores_numericos(self, df_profissionais_bruto):
        """CH_TOTAL é numérica e não deve ser alterada."""
        resultado = transformar(df_profissionais_bruto)
        assert list(resultado["CH_TOTAL"]) == list(
            df_profissionais_bruto["CH_TOTAL"]
        )


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 2: Limpeza de Strings
# ─────────────────────────────────────────────────────────────────────────────

class TestLimpezaDeStrings:

    def test_cpf_sem_espacos_extras(self, df_profissionais_bruto):
        """CPF não pode ter espaços antes ou depois após a transformação."""
        resultado = transformar(df_profissionais_bruto)
        for cpf in resultado["CPF"]:
            assert cpf == cpf.strip(), f"CPF com espaço extra: '{cpf}'"

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

    def test_sexo_sem_espacos_extras(self, df_profissionais_bruto):
        """SEXO não pode ter espaços extras (coluna CHAR do Firebird tem padding)."""
        resultado = transformar(df_profissionais_bruto)
        for sexo in resultado["SEXO"]:
            assert sexo == sexo.strip(), f"SEXO com espaço extra: '{sexo}'"


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 3: RQ-002 — Validação de CPF
# ─────────────────────────────────────────────────────────────────────────────

class TestRQ002ValidacaoCpf:

    def test_remove_cpf_nulo(self, df_com_cpf_invalido):
        """CPF None deve ser excluído (RQ-002)."""
        resultado = transformar(df_com_cpf_invalido)
        # Fixture tem 3 linhas: 1 None, 1 curto (9 dígitos), 1 válido (11 dígitos)
        assert len(resultado) == 1

    def test_remove_cpf_comprimento_incorreto(self, df_com_cpf_invalido):
        """CPF com comprimento diferente de 11 deve ser excluído (RQ-002)."""
        resultado = transformar(df_com_cpf_invalido)
        for cpf in resultado["CPF"]:
            assert len(cpf.strip()) == 11, f"CPF inválido no resultado: '{cpf}'"

    def test_mantem_cpf_valido(self, df_com_cpf_invalido):
        """O único registro com CPF válido deve ser mantido."""
        resultado = transformar(df_com_cpf_invalido)
        assert "27943602803" in resultado["CPF"].values

    def test_nao_remove_nada_quando_todos_cpfs_validos(self, df_profissionais_bruto):
        """Nenhum registro deve ser removido quando todos os CPFs são válidos."""
        total_antes = len(df_profissionais_bruto)
        resultado = transformar(df_profissionais_bruto)
        assert len(resultado) == total_antes


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 4: RQ-003 — Flag de Carga Horária Zero
# ─────────────────────────────────────────────────────────────────────────────

class TestRQ003FlagCargaHoraria:

    def test_flag_ativo_sem_ch_quando_carga_zero(self, df_com_carga_horaria_zero):
        """Registro com CH_TOTAL=0 deve receber ALERTA_ATIVO_SEM_CH."""
        resultado = transformar(df_com_carga_horaria_zero)
        flags = resultado["ALERTA_STATUS_CH"].tolist()
        assert flags[0] == ALERTA_ATIVO_SEM_CH, (
            f"Esperado '{ALERTA_ATIVO_SEM_CH}', obtido '{flags[0]}'"
        )

    def test_flag_ok_quando_carga_positiva(self, df_com_carga_horaria_zero):
        """Registro com carga horária positiva deve receber ALERTA_CH_OK."""
        resultado = transformar(df_com_carga_horaria_zero)
        flags = resultado["ALERTA_STATUS_CH"].tolist()
        assert flags[1] == ALERTA_CH_OK, (
            f"Esperado '{ALERTA_CH_OK}', obtido '{flags[1]}'"
        )

    def test_nao_exclui_registro_com_carga_zero(self, df_com_carga_horaria_zero):
        """RQ-003 sinaliza mas NÃO remove registros com carga zero."""
        total_antes = len(df_com_carga_horaria_zero)
        resultado = transformar(df_com_carga_horaria_zero)
        assert len(resultado) == total_antes, (
            "RQ-003 não deve remover registros — apenas sinalizar"
        )

    def test_todos_registros_com_ch_positiva_recebem_ok(self, df_profissionais_bruto):
        """Quando todos têm CH > 0, todas as flags devem ser 'OK'."""
        resultado = transformar(df_profissionais_bruto)
        assert all(f == ALERTA_CH_OK for f in resultado["ALERTA_STATUS_CH"])


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 5: Preenchimento de Nulos (LEFT JOIN → sem equipe)
# ─────────────────────────────────────────────────────────────────────────────

class TestPreenchimentoDeNulos:

    def test_nome_equipe_nulo_vira_sem_equipe(self, df_profissionais_bruto):
        """Profissionais sem equipe devem ter NOME_EQUIPE = VALOR_SEM_EQUIPE."""
        resultado = transformar(df_profissionais_bruto)
        assert resultado["NOME_EQUIPE"].isna().sum() == 0
        assert all(v == VALOR_SEM_EQUIPE for v in resultado["NOME_EQUIPE"])

    def test_ine_nulo_vira_traco(self, df_profissionais_bruto):
        """INE nulo deve virar VALOR_SEM_INE ('-')."""
        resultado = transformar(df_profissionais_bruto)
        assert resultado["INE"].isna().sum() == 0
        assert all(v == VALOR_SEM_INE for v in resultado["INE"])

    def test_tipo_equipe_nulo_vira_traco(self, df_profissionais_bruto):
        """TIPO_EQUIPE nulo deve virar VALOR_SEM_INE ('-')."""
        resultado = transformar(df_profissionais_bruto)
        assert resultado["TIPO_EQUIPE"].isna().sum() == 0
        assert all(v == VALOR_SEM_INE for v in resultado["TIPO_EQUIPE"])

    def test_equipe_preenchida_nao_e_sobrescrita(self, df_com_equipe):
        """Equipe já preenchida (não nula) não deve ser sobrescrita pelo fillna."""
        resultado = transformar(df_com_equipe)
        assert resultado["NOME_EQUIPE"].iloc[0] == "ESF VILA GERONIMO"
        assert resultado["TIPO_EQUIPE"].iloc[0] == "70"
        assert resultado["INE"].iloc[0] == "0001365993"


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 6: Imutabilidade do DataFrame Original
# ─────────────────────────────────────────────────────────────────────────────

class TestImutabilidade:

    def test_dataframe_original_nao_modificado(self, df_profissionais_bruto):
        """
        transformar() deve trabalhar em cópia interna.
        O DataFrame original não deve ser alterado (NOME_EQUIPE deve continuar None).
        """
        valores_originais = list(df_profissionais_bruto["NOME_EQUIPE"])
        transformar(df_profissionais_bruto)
        valores_apos = list(df_profissionais_bruto["NOME_EQUIPE"])
        assert valores_originais == valores_apos, (
            "transformar() modificou o DataFrame original — use df.copy() internamente"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 7: Casos de Borda (Edge Cases)
# ─────────────────────────────────────────────────────────────────────────────

class TestCasosDeBorda:

    def test_dataframe_vazio_retorna_vazio_com_colunas(self):
        """transformar() deve aceitar um DataFrame vazio sem lançar exceção."""
        df_vazio = pd.DataFrame(columns=[
            "CPF", "NOME_PROFISSIONAL", "NOME_SOCIAL", "SEXO", "DATA_NASCIMENTO",
            "CBO", "TIPO_VINCULO", "SUS",
            "CH_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS", "CH_HOSPITALAR",
            "CNES", "ESTABELECIMENTO", "TIPO_UNIDADE", "COD_MUNICIPIO",
            "INE", "NOME_EQUIPE", "TIPO_EQUIPE",
        ])
        resultado = transformar(df_vazio)
        assert len(resultado) == 0
        assert "ALERTA_STATUS_CH" in resultado.columns

    def test_transformacao_idempotente(self, df_profissionais_bruto):
        """
        Aplicar transformar() duas vezes deve dar o mesmo resultado que uma.
        Idempotência é essencial em pipelines de dados (reprocessamento seguro).
        """
        resultado_1x = transformar(df_profissionais_bruto)
        resultado_2x = transformar(resultado_1x)

        pd.testing.assert_frame_equal(
            resultado_1x.reset_index(drop=True),
            resultado_2x.reset_index(drop=True),
        )


class TestCboEnrichment:

    def test_adiciona_descricao_cbo_quando_lookup_fornecido(self):
        lookup = {"515105": "AGENTE COMUNITARIO DE SAUDE"}
        df = _df_minimo(cpf="11716723817")
        resultado = transformar(df, cbo_lookup=lookup)
        assert "DESCRICAO_CBO" in resultado.columns
        assert resultado["DESCRICAO_CBO"].iloc[0] == "AGENTE COMUNITARIO DE SAUDE"

    def test_nao_adiciona_descricao_sem_lookup(self):
        df = _df_minimo(cpf="11716723817")
        resultado = transformar(df)
        assert "DESCRICAO_CBO" not in resultado.columns

    def test_cbo_desconhecido_recebe_fallback(self):
        lookup = {"515105": "AGENTE COMUNITARIO DE SAUDE"}
        df = _df_minimo(cpf="11716723817")
        df = df.copy()
        df["CBO"] = ["999999"]
        resultado = transformar(df, cbo_lookup=lookup)
        assert resultado["DESCRICAO_CBO"].iloc[0] == "CBO NAO CATALOGADO"

    def test_lookup_vazio_todos_recebem_fallback(self):
        df = _df_minimo(cpf="11716723817")
        resultado = transformar(df, cbo_lookup={})
        assert resultado["DESCRICAO_CBO"].iloc[0] == "CBO NAO CATALOGADO"


class TestContratoNormalizacaoCPF:
    """Documenta que transformar() realiza strip antes de chamar _aplicar_rq002_validar_cpf.

    _aplicar_rq002_validar_cpf não faz strip próprio — depende desta pré-condição.
    Estes testes garantem que remover o strip defensivo da função privada é seguro.
    """

    def test_cpf_com_espaco_final_e_stripado_e_preservado(self):
        df = _df_minimo(cpf="11716723817 ")
        resultado = transformar(df)
        assert len(resultado) == 1
        assert resultado["CPF"].iloc[0] == "11716723817"

    def test_cpf_com_espaco_inicial_e_stripado_e_preservado(self):
        df = _df_minimo(cpf=" 11716723817")
        resultado = transformar(df)
        assert len(resultado) == 1
        assert resultado["CPF"].iloc[0] == "11716723817"

    def test_cpf_com_espacos_e_zeros_faltantes_e_normalizado(self):
        df = _df_minimo(cpf=" 1716723817")
        resultado = transformar(df)
        assert len(resultado) == 1
        assert resultado["CPF"].iloc[0] == "01716723817"
