"""test_transformer.py — Testes unitários de processing.transformer."""

import polars as pl
from polars.testing import assert_frame_equal

from processing.transformer import (
    ALERTA_ATIVO_SEM_CH,
    ALERTA_CH_OK,
    VALOR_SEM_EQUIPE,
    VALOR_SEM_INE,
    transformar,
)


def _df_minimo(cpf="11716723817", ch_total=40) -> pl.DataFrame:
    return pl.DataFrame({
        "CPF": [cpf],
        "CNS": ["702002887429583"],
        "NOME_PROFISSIONAL": ["TESTE"],
        "NOME_SOCIAL": [None],
        "SEXO": ["F"],
        "DATA_NASCIMENTO": ["1990-01-01"],
        "CBO": ["515105"],
        "TIPO_VINCULO": ["010101"],
        "SUS": ["S"],
        "CH_TOTAL": [ch_total],
        "CH_AMBULATORIAL": [ch_total],
        "CH_OUTRAS": [0],
        "CH_HOSPITALAR": [0],
        "CNES": ["0985333"],
        "ESTABELECIMENTO": ["UBS TESTE"],
        "TIPO_UNIDADE": ["02"],
        "COD_MUNICIPIO": ["354130"],
        "INE": [None],
        "NOME_EQUIPE": [None],
        "TIPO_EQUIPE": [None],
    })


class TestZeroPaddingCPF:

    def test_cpf_10_digitos_recebe_zero_a_esquerda(self):
        resultado = transformar(_df_minimo(cpf="1234567890"))
        assert resultado["CPF"][0] == "01234567890"

    def test_cpf_9_digitos_recebe_dois_zeros(self):
        resultado = transformar(_df_minimo(cpf="123456789"))
        assert resultado["CPF"][0] == "00123456789"

    def test_cpf_11_digitos_nao_muda(self):
        resultado = transformar(_df_minimo(cpf="11716723817"))
        assert resultado["CPF"][0] == "11716723817"

    def test_cpf_nulo_nao_recebe_zfill_e_excluido(self):
        resultado = transformar(_df_minimo(cpf=None))
        assert len(resultado) == 0


class TestPreservacaoDeDados:

    def test_nao_perde_registros_com_cpf_valido(self, df_profissionais_bruto):
        total_antes = len(df_profissionais_bruto)
        resultado = transformar(df_profissionais_bruto)
        assert len(resultado) == total_antes

    def test_preserva_colunas_de_entrada(self, df_profissionais_bruto):
        resultado = transformar(df_profissionais_bruto)
        colunas_entrada = set(df_profissionais_bruto.columns)
        colunas_saida = set(resultado.columns)
        assert colunas_entrada.issubset(colunas_saida)

    def test_adiciona_coluna_alerta_status_ch(self, df_profissionais_bruto):
        resultado = transformar(df_profissionais_bruto)
        assert "ALERTA_STATUS_CH" in resultado.columns

    def test_preserva_valores_numericos(self, df_profissionais_bruto):
        resultado = transformar(df_profissionais_bruto)
        assert resultado["CH_TOTAL"].to_list() == df_profissionais_bruto["CH_TOTAL"].to_list()


class TestLimpezaDeStrings:

    def test_cpf_sem_espacos_extras(self, df_profissionais_bruto):
        resultado = transformar(df_profissionais_bruto)
        for cpf in resultado["CPF"].to_list():
            assert cpf == cpf.strip()

    def test_nome_sem_espacos_extras(self, df_profissionais_bruto):
        resultado = transformar(df_profissionais_bruto)
        for nome in resultado["NOME_PROFISSIONAL"].to_list():
            assert nome == nome.strip()

    def test_cbo_sem_espacos_extras(self, df_profissionais_bruto):
        resultado = transformar(df_profissionais_bruto)
        for cbo in resultado["CBO"].to_list():
            assert cbo == cbo.strip()

    def test_estabelecimento_sem_espacos_extras(self, df_profissionais_bruto):
        resultado = transformar(df_profissionais_bruto)
        for estab in resultado["ESTABELECIMENTO"].to_list():
            assert estab == estab.strip()

    def test_sexo_sem_espacos_extras(self, df_profissionais_bruto):
        resultado = transformar(df_profissionais_bruto)
        for sexo in resultado["SEXO"].to_list():
            assert sexo == sexo.strip()


class TestRQ002ValidacaoCpf:

    def test_remove_cpf_nulo(self, df_com_cpf_invalido):
        resultado = transformar(df_com_cpf_invalido)
        assert len(resultado) == 1

    def test_remove_cpf_comprimento_incorreto(self, df_com_cpf_invalido):
        resultado = transformar(df_com_cpf_invalido)
        for cpf in resultado["CPF"].to_list():
            assert len(cpf.strip()) == 11

    def test_mantem_cpf_valido(self, df_com_cpf_invalido):
        resultado = transformar(df_com_cpf_invalido)
        assert "27943602803" in resultado["CPF"].to_list()

    def test_nao_remove_nada_quando_todos_cpfs_validos(self, df_profissionais_bruto):
        total_antes = len(df_profissionais_bruto)
        resultado = transformar(df_profissionais_bruto)
        assert len(resultado) == total_antes


class TestRQ003FlagCargaHoraria:

    def test_flag_ativo_sem_ch_quando_carga_zero(self, df_com_carga_horaria_zero):
        resultado = transformar(df_com_carga_horaria_zero)
        flags = resultado["ALERTA_STATUS_CH"].to_list()
        assert flags[0] == ALERTA_ATIVO_SEM_CH

    def test_flag_ok_quando_carga_positiva(self, df_com_carga_horaria_zero):
        resultado = transformar(df_com_carga_horaria_zero)
        flags = resultado["ALERTA_STATUS_CH"].to_list()
        assert flags[1] == ALERTA_CH_OK

    def test_nao_exclui_registro_com_carga_zero(self, df_com_carga_horaria_zero):
        total_antes = len(df_com_carga_horaria_zero)
        resultado = transformar(df_com_carga_horaria_zero)
        assert len(resultado) == total_antes

    def test_todos_registros_com_ch_positiva_recebem_ok(self, df_profissionais_bruto):
        resultado = transformar(df_profissionais_bruto)
        assert all(f == ALERTA_CH_OK for f in resultado["ALERTA_STATUS_CH"].to_list())


class TestPreenchimentoDeNulos:

    def test_nome_equipe_nulo_vira_sem_equipe(self, df_profissionais_bruto):
        resultado = transformar(df_profissionais_bruto)
        assert resultado["NOME_EQUIPE"].null_count() == 0
        assert all(v == VALOR_SEM_EQUIPE for v in resultado["NOME_EQUIPE"].to_list())

    def test_ine_nulo_vira_traco(self, df_profissionais_bruto):
        resultado = transformar(df_profissionais_bruto)
        assert resultado["INE"].null_count() == 0
        assert all(v == VALOR_SEM_INE for v in resultado["INE"].to_list())

    def test_tipo_equipe_nulo_vira_traco(self, df_profissionais_bruto):
        resultado = transformar(df_profissionais_bruto)
        assert resultado["TIPO_EQUIPE"].null_count() == 0
        assert all(v == VALOR_SEM_INE for v in resultado["TIPO_EQUIPE"].to_list())

    def test_equipe_preenchida_nao_e_sobrescrita(self, df_com_equipe):
        resultado = transformar(df_com_equipe)
        assert resultado["NOME_EQUIPE"][0] == "ESF VILA GERONIMO"
        assert resultado["TIPO_EQUIPE"][0] == "70"
        assert resultado["INE"][0] == "0001365993"


class TestImutabilidade:

    def test_dataframe_original_nao_modificado(self, df_profissionais_bruto):
        valores_originais = df_profissionais_bruto["NOME_EQUIPE"].to_list()
        transformar(df_profissionais_bruto)
        valores_apos = df_profissionais_bruto["NOME_EQUIPE"].to_list()
        assert valores_originais == valores_apos


class TestCasosDeBorda:

    def test_dataframe_vazio_retorna_vazio_com_colunas(self):
        df_vazio = pl.DataFrame(
            schema={
                "CPF": pl.Utf8, "NOME_PROFISSIONAL": pl.Utf8,
                "NOME_SOCIAL": pl.Utf8, "SEXO": pl.Utf8,
                "DATA_NASCIMENTO": pl.Utf8, "CBO": pl.Utf8,
                "TIPO_VINCULO": pl.Utf8, "SUS": pl.Utf8,
                "CH_TOTAL": pl.Int64, "CH_AMBULATORIAL": pl.Int64,
                "CH_OUTRAS": pl.Int64, "CH_HOSPITALAR": pl.Int64,
                "CNES": pl.Utf8, "ESTABELECIMENTO": pl.Utf8,
                "TIPO_UNIDADE": pl.Utf8, "COD_MUNICIPIO": pl.Utf8,
                "INE": pl.Utf8, "NOME_EQUIPE": pl.Utf8,
                "TIPO_EQUIPE": pl.Utf8,
            }
        )
        resultado = transformar(df_vazio)
        assert len(resultado) == 0
        assert "ALERTA_STATUS_CH" in resultado.columns

    def test_transformacao_idempotente(self, df_profissionais_bruto):
        resultado_1x = transformar(df_profissionais_bruto)
        resultado_2x = transformar(resultado_1x)
        assert_frame_equal(resultado_1x, resultado_2x)


class TestCboEnrichment:

    def test_adiciona_descricao_cbo_quando_lookup_fornecido(self):
        lookup = {"515105": "AGENTE COMUNITARIO DE SAUDE"}
        df = _df_minimo(cpf="11716723817")
        resultado = transformar(df, cbo_lookup=lookup)
        assert "DESCRICAO_CBO" in resultado.columns
        assert resultado["DESCRICAO_CBO"][0] == "AGENTE COMUNITARIO DE SAUDE"

    def test_nao_adiciona_descricao_sem_lookup(self):
        df = _df_minimo(cpf="11716723817")
        resultado = transformar(df)
        assert "DESCRICAO_CBO" not in resultado.columns

    def test_cbo_desconhecido_recebe_fallback(self):
        lookup = {"515105": "AGENTE COMUNITARIO DE SAUDE"}
        df = _df_minimo(cpf="11716723817")
        df = df.with_columns(pl.lit("999999").alias("CBO"))
        resultado = transformar(df, cbo_lookup=lookup)
        assert resultado["DESCRICAO_CBO"][0] == "CBO NAO CATALOGADO"

    def test_lookup_vazio_todos_recebem_fallback(self):
        df = _df_minimo(cpf="11716723817")
        resultado = transformar(df, cbo_lookup={})
        assert resultado["DESCRICAO_CBO"][0] == "CBO NAO CATALOGADO"


class TestContratoNormalizacaoCPF:

    def test_cpf_com_espaco_final_e_stripado_e_preservado(self):
        df = _df_minimo(cpf="11716723817 ")
        resultado = transformar(df)
        assert len(resultado) == 1
        assert resultado["CPF"][0] == "11716723817"

    def test_cpf_com_espaco_inicial_e_stripado_e_preservado(self):
        df = _df_minimo(cpf=" 11716723817")
        resultado = transformar(df)
        assert len(resultado) == 1
        assert resultado["CPF"][0] == "11716723817"

    def test_cpf_com_espacos_e_zeros_faltantes_e_normalizado(self):
        df = _df_minimo(cpf=" 1716723817")
        resultado = transformar(df)
        assert len(resultado) == 1
        assert resultado["CPF"][0] == "01716723817"
