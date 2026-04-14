"""test_sihd_local_adapter.py -- Testes do adapter SIHD2 -> schema AIH."""

from unittest.mock import MagicMock, patch

import polars as pl

from cnes_domain.contracts.sihd_columns import SCHEMA_AIH, SCHEMA_PROCEDIMENTO_AIH
from cnes_infra.ingestion.sihd_local_adapter import SihdLocalAdapter

_DF_HAIH = pl.DataFrame({
    "AH_NUM_AIH":                 [" 3541301234567"],
    "AH_CNES":                    [" 985333 "],
    "AH_CMPT":                    ["202601"],
    "AH_PACIENTE_NOME":           ["JOAO DA SILVA"],
    "AH_PACIENTE_NUMERO_CNS":     [" 702002887429583"],
    "AH_PACIENTE_SEXO":           ["M"],
    "AH_PACIENTE_DT_NASCIMENTO":  ["19750412"],
    "AH_PACIENTE_MUN_ORIGEM":     ["354130"],
    "AH_DIAG_PRI":                ["J189"],
    "AH_DIAG_SEC":                ["J441"],
    "AH_PROC_SOLICITADO":         ["0303060204"],
    "AH_PROC_REALIZADO":          ["0303060204"],
    "AH_DT_INTERNACAO":           ["20260101"],
    "AH_DT_SAIDA":                ["20260115"],
    "AH_MOT_SAIDA":               ["11"],
    "AH_CAR_INTERNACAO":          ["01"],
    "AH_ESPECIALIDADE":           ["03"],
    "AH_SITUACAO":                ["0"],
    "AH_MED_SOL_DOC":             ["11716723817"],
    "AH_MED_RESP_DOC":            ["11716723817"],
    "AH_OE_GESTOR":               ["3541300001"],
    "AH_SEQ":                     [1],
})

_DF_HPA = pl.DataFrame({
    "PA_NUM_AIH":           [" 3541301234567"],
    "PA_CMPT":              ["202601"],
    "PA_PROCEDIMENTO":      ["0303060204"],
    "PA_PROCEDIMENTO_QTD":  [1],
    "PA_VALOR":             [1250.50],
    "PA_PF_CBO":            ["225142"],
    "PA_EXEC_DOC":          ["11716723817"],
})

_COMP = "202601"


def _adapter_com_mock(
    df_aihs=None, df_procs=None,
) -> tuple[SihdLocalAdapter, MagicMock]:
    con = MagicMock()
    adapter = SihdLocalAdapter(con)
    adapter._cache_aihs = df_aihs if df_aihs is not None else _DF_HAIH
    adapter._cache_procs = df_procs if df_procs is not None else _DF_HPA
    return adapter, con


class TestListarAihs:

    def test_retorna_colunas_do_schema(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_aihs(_COMP)
        assert resultado.columns == list(SCHEMA_AIH)

    def test_renomeia_ah_num_aih_para_num_aih(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_aihs(_COMP)
        assert "NUM_AIH" in resultado.columns
        assert "AH_NUM_AIH" not in resultado.columns

    def test_renomeia_ah_cnes_para_cnes(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_aihs(_COMP)
        assert "CNES" in resultado.columns
        assert "AH_CNES" not in resultado.columns

    def test_adiciona_fonte_local(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_aihs(_COMP)
        assert (resultado["FONTE"] == "LOCAL").all()

    def test_strip_num_aih(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_aihs(_COMP)
        assert resultado["NUM_AIH"][0] == "3541301234567"

    def test_strip_cnes(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_aihs(_COMP)
        assert resultado["CNES"][0] == "0985333"

    def test_strip_paciente_cns(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_aihs(_COMP)
        assert resultado["PACIENTE_CNS"][0] == "702002887429583"

    def test_cnes_zfill_7(self):
        df = _DF_HAIH.clone()
        df = df.with_columns(pl.lit("985333").alias("AH_CNES"))
        adapter, _ = _adapter_com_mock(df_aihs=df)
        resultado = adapter.listar_aihs(_COMP)
        assert resultado["CNES"][0] == "0985333"

    def test_nao_muta_dataframe_original(self):
        adapter, _ = _adapter_com_mock()
        original_cols = list(_DF_HAIH.columns)
        adapter.listar_aihs(_COMP)
        assert list(_DF_HAIH.columns) == original_cols


class TestListarProcedimentos:

    def test_retorna_colunas_do_schema(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_procedimentos(_COMP)
        assert resultado.columns == list(SCHEMA_PROCEDIMENTO_AIH)

    def test_renomeia_pa_procedimento(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_procedimentos(_COMP)
        assert "PROCEDIMENTO" in resultado.columns
        assert "PA_PROCEDIMENTO" not in resultado.columns

    def test_renomeia_pa_valor(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_procedimentos(_COMP)
        assert "VALOR" in resultado.columns

    def test_adiciona_fonte_local(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_procedimentos(_COMP)
        assert (resultado["FONTE"] == "LOCAL").all()

    def test_strip_num_aih(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_procedimentos(_COMP)
        assert resultado["NUM_AIH"][0] == "3541301234567"

    def test_valor_preservado(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_procedimentos(_COMP)
        assert resultado["VALOR"][0] == 1250.50


class TestCacheInterno:

    def test_extrair_aihs_chamado_uma_vez(self):
        con = MagicMock()
        with patch(
            "cnes_infra.ingestion.sihd_local_adapter.sihd_client.extrair_aihs",
            return_value=_DF_HAIH,
        ) as mock_extrair:
            adapter = SihdLocalAdapter(con)
            adapter.listar_aihs(_COMP)
            adapter.listar_aihs(_COMP)
            mock_extrair.assert_called_once()

    def test_extrair_procedimentos_chamado_uma_vez(self):
        con = MagicMock()
        with patch(
            "cnes_infra.ingestion.sihd_local_adapter.sihd_client.extrair_procedimentos",
            return_value=_DF_HPA,
        ) as mock_extrair:
            adapter = SihdLocalAdapter(con)
            adapter.listar_procedimentos(_COMP)
            adapter.listar_procedimentos(_COMP)
            mock_extrair.assert_called_once()
