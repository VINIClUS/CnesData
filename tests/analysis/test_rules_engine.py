"""
test_rules_engine.py — Testes Unitários do Motor de Regras de Auditoria

Objetivo: verificar que as regras RQ-003-B e RQ-005 detectam corretamente
anomalias sem precisar de banco de dados.

Cobertura:
  - RQ-003-B: detecção de profissionais com vínculos em múltiplas unidades.
  - RQ-005 (ACS/TACS): lotação em unidade incorreta.
  - RQ-005 (ACE/TACE): lotação em unidade incorreta.
  - Casos negativos: sem anomalias → retorno de DataFrame vazio.
  - Edge cases: DataFrame vazio, profissional com 3 unidades.

Fonte dos CBOs e TP_UNID_IDs válidos: data_dictionary.md (seção RQ-005).
"""

import pandas as pd
import pytest

# conftest.py (tests/) já adicionou src/ ao sys.path
from analysis.rules_engine import (
    detectar_multiplas_unidades,
    auditar_lotacao_acs_tacs,
    auditar_lotacao_ace_tace,
    CBOS_ACS_TACS,
    CBOS_ACE_TACE,
    TP_UNID_VALIDOS_ACS_TACS,
    TP_UNID_VALIDOS_ACE_TACE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _df_base(cpf: str, cbo: str, cod_tipo_unidade: str, cod_cnes: str = "0985333") -> dict:
    """Retorna um dicionário com as colunas mínimas para testes de auditoria."""
    return {
        "CPF": cpf,
        "NOME_PROFISSIONAL": f"PROF {cpf}",
        "CBO": cbo,
        "COD_TIPO_UNIDADE": cod_tipo_unidade,
        "COD_CNES": cod_cnes,
        "ESTABELECIMENTO": "UBS TESTE",
        "ALERTA_STATUS_CH": "OK",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 1: detectar_multiplas_unidades() — RQ-003-B
# ─────────────────────────────────────────────────────────────────────────────

class TestDetectarMultiplasUnidades:

    def test_detecta_profissional_em_duas_unidades(self):
        """Profissional com vínculos em 2 unidades distintas deve ser detectado."""
        df = pd.DataFrame([
            _df_base("11111111111", "515105", "01", "1111111"),
            _df_base("11111111111", "515105", "02", "2222222"),  # mesmo CPF, outra unidade
            _df_base("99999999999", "225142", "02", "3333333"),  # único
        ])
        resultado = detectar_multiplas_unidades(df)

        assert len(resultado) == 2  # 2 linhas do CPF multi-unidade
        assert "11111111111" in resultado["CPF"].values
        assert "99999999999" not in resultado["CPF"].values

    def test_adiciona_coluna_qtd_unidades(self):
        """O resultado deve ter a coluna QTD_UNIDADES com o valor correto."""
        df = pd.DataFrame([
            _df_base("11111111111", "515105", "01", "1111111"),
            _df_base("11111111111", "515105", "01", "2222222"),
            _df_base("11111111111", "515105", "01", "3333333"),  # 3 unidades
        ])
        resultado = detectar_multiplas_unidades(df)

        assert "QTD_UNIDADES" in resultado.columns
        assert resultado["QTD_UNIDADES"].iloc[0] == 3

    def test_retorna_vazio_quando_todos_em_unidade_unica(self):
        """Profissionais com vínculo único não devem aparecer no resultado."""
        df = pd.DataFrame([
            _df_base("11111111111", "515105", "01", "1111111"),
            _df_base("22222222222", "322255", "02", "2222222"),
        ])
        resultado = detectar_multiplas_unidades(df)
        assert resultado.empty

    def test_retorna_vazio_para_dataframe_vazio(self):
        """DataFrame vazio não deve causar exceção."""
        df = pd.DataFrame(columns=["CPF", "CBO", "COD_CNES", "COD_TIPO_UNIDADE",
                                    "NOME_PROFISSIONAL", "ESTABELECIMENTO", "ALERTA_STATUS_CH"])
        resultado = detectar_multiplas_unidades(df)
        assert resultado.empty


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 2: auditar_lotacao_acs_tacs() — RQ-005 ACS/TACS
# ─────────────────────────────────────────────────────────────────────────────

class TestAuditarLotacaoAcsTacs:

    def test_detecta_acs_em_unidade_incorreta(self):
        """ACS (515105) em TP_UNID_ID fora de {'01','02','15'} deve ser detectado."""
        df = pd.DataFrame([
            _df_base("11111111111", "515105", "69"),  # ACE unit — incorreto para ACS
        ])
        resultado = auditar_lotacao_acs_tacs(df)
        assert len(resultado) == 1
        assert resultado["CPF"].iloc[0] == "11111111111"

    def test_detecta_tacs_em_unidade_incorreta(self):
        """TACS (322255) em TP_UNID_ID fora do conjunto válido deve ser detectado."""
        df = pd.DataFrame([
            _df_base("22222222222", "322255", "70"),  # CAPS — incorreto para TACS
        ])
        resultado = auditar_lotacao_acs_tacs(df)
        assert len(resultado) == 1

    def test_nao_detecta_acs_em_unidade_correta(self):
        """ACS em TP_UNID_ID válido ('01', '02', '15') NÃO deve aparecer."""
        for tp_unid in TP_UNID_VALIDOS_ACS_TACS:
            df = pd.DataFrame([_df_base("11111111111", "515105", tp_unid)])
            resultado = auditar_lotacao_acs_tacs(df)
            assert resultado.empty, (
                f"ACS em TP_UNID_ID='{tp_unid}' não deveria ser anomalia"
            )

    def test_ignora_cbos_nao_acs_tacs(self):
        """CBOs que não são ACS/TACS não devem ser incluídos no resultado."""
        df = pd.DataFrame([
            _df_base("33333333333", "225142", "99"),  # Médico em TP incomum — não é ACS
            _df_base("44444444444", "515140", "99"),  # ACE — não é ACS/TACS
        ])
        resultado = auditar_lotacao_acs_tacs(df)
        assert resultado.empty

    def test_retorna_vazio_para_dataframe_vazio(self):
        """DataFrame vazio não deve causar exceção."""
        df = pd.DataFrame(columns=["CPF", "CBO", "COD_TIPO_UNIDADE",
                                    "NOME_PROFISSIONAL", "COD_CNES",
                                    "ESTABELECIMENTO", "ALERTA_STATUS_CH"])
        resultado = auditar_lotacao_acs_tacs(df)
        assert resultado.empty


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 3: auditar_lotacao_ace_tace() — RQ-005 ACE/TACE
# ─────────────────────────────────────────────────────────────────────────────

class TestAuditarLotacaoAceTace:

    def test_detecta_ace_legado_em_unidade_incorreta(self):
        """ACE legado (515140) em TP_UNID_ID fora de {'02','69','22','15'} deve ser detectado."""
        df = pd.DataFrame([
            _df_base("11111111111", "515140", "01"),  # UBS — incorreto para ACE
        ])
        resultado = auditar_lotacao_ace_tace(df)
        assert len(resultado) == 1

    def test_detecta_tace_em_unidade_incorreta(self):
        """TACE atual (322260) em TP_UNID_ID incorreto deve ser detectado."""
        df = pd.DataFrame([
            _df_base("22222222222", "322260", "70"),  # CAPS — incorreto para TACE
        ])
        resultado = auditar_lotacao_ace_tace(df)
        assert len(resultado) == 1

    def test_nao_detecta_ace_em_unidade_correta(self):
        """ACE em TP_UNID_ID válido ('02', '69', '22', '15') NÃO deve aparecer."""
        for tp_unid in TP_UNID_VALIDOS_ACE_TACE:
            df = pd.DataFrame([_df_base("11111111111", "515140", tp_unid)])
            resultado = auditar_lotacao_ace_tace(df)
            assert resultado.empty, (
                f"ACE em TP_UNID_ID='{tp_unid}' não deveria ser anomalia"
            )

    def test_ignora_cbos_nao_ace_tace(self):
        """CBOs que não são ACE/TACE (ex: ACS 515105) não devem aparecer."""
        df = pd.DataFrame([
            _df_base("33333333333", "515105", "99"),  # ACS fora de unidade — não é ACE
        ])
        resultado = auditar_lotacao_ace_tace(df)
        assert resultado.empty

    def test_detecta_todos_os_cbos_do_grupo_ace(self):
        """Os três CBOs do grupo ACE/TACE devem ser detectados quando fora da lotação."""
        cbos_fora_lotacao = [
            ("11111111111", "515140"),  # ACE legado
            ("22222222222", "322210"),  # Técnico ACE legado
            ("33333333333", "322260"),  # TACE atual
        ]
        df = pd.DataFrame([
            _df_base(cpf, cbo, "01")  # UBS — incorreto para ACE
            for cpf, cbo in cbos_fora_lotacao
        ])
        resultado = auditar_lotacao_ace_tace(df)
        assert len(resultado) == 3


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 4: Validação dos Domínios (constantes)
# ─────────────────────────────────────────────────────────────────────────────

class TestDominiosCbo:
    """
    Verifica que os CBOs e TP_UNID_IDs estão corretos conforme data_dictionary.md.
    Estes testes protegem contra regressão acidental nas constantes de domínio.
    """

    def test_cbos_acs_tacs_contem_515105_e_322255(self):
        assert "515105" in CBOS_ACS_TACS  # ACS
        assert "322255" in CBOS_ACS_TACS  # TACS

    def test_cbos_ace_tace_contem_tres_cbos(self):
        assert "515140" in CBOS_ACE_TACE  # ACE legado
        assert "322210" in CBOS_ACE_TACE  # Técnico ACE legado
        assert "322260" in CBOS_ACE_TACE  # TACE atual

    def test_tp_unid_validos_acs_tacs(self):
        assert TP_UNID_VALIDOS_ACS_TACS == frozenset({"01", "02", "15"})

    def test_tp_unid_validos_ace_tace(self):
        assert TP_UNID_VALIDOS_ACE_TACE == frozenset({"02", "69", "22", "15"})

    def test_cbo_516220_nao_esta_em_nenhum_grupo_ace_acs(self):
        """
        516220 (Cuidador em Saúde) foi erroneamente associado a ACS/ACE em versões
        anteriores (data_dictionary.md nota ERRO CRÍTICO). Deve estar ausente dos grupos.
        """
        assert "516220" not in CBOS_ACS_TACS
        assert "516220" not in CBOS_ACE_TACE
