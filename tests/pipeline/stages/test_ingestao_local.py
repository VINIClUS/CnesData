from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline.state import PipelineState
from pipeline.stages.ingestao_local import IngestaoLocalStage


def _state() -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
    )


def _df_prof() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS": ["123456789012345"],
        "CPF": ["12345678901"],
        "NOME_PROFISSIONAL": ["Ana Silva"],
        "CBO": ["515105"],
        "CNES": ["1234567"],
        "TIPO_VINCULO": ["30"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [20],
        "CH_OUTRAS": [10],
        "CH_HOSPITALAR": [10],
        "FONTE": ["LOCAL"],
    })


def _df_estab() -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": ["1234567"],
        "NOME_FANTASIA": ["UBS Centro"],
        "TIPO_UNIDADE": ["01"],
        "CNPJ_MANTENEDORA": ["55293427000117"],
        "NATUREZA_JURIDICA": ["1023"],
        "COD_MUNICIPIO": ["354130"],
        "VINCULO_SUS": ["S"],
        "FONTE": ["LOCAL"],
    })


@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_popula_state_com_dados_locais(
    mock_estab_contract,
    mock_prof_contract,
    mock_adapter_cls,
    mock_cbo,
    mock_conectar,
):
    mock_con = MagicMock()
    mock_conectar.return_value = mock_con
    mock_cbo.return_value = {"515105": "Agente Comunitário"}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state()
    IngestaoLocalStage().execute(state)

    assert state.con is mock_con
    assert state.cbo_lookup == {"515105": "Agente Comunitário"}
    assert len(state.df_prof_local) == 1
    assert len(state.df_estab_local) == 1


@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_valida_contratos_apos_ingestao(
    mock_estab_contract,
    mock_prof_contract,
    mock_adapter_cls,
    mock_cbo,
    mock_conectar,
):
    mock_conectar.return_value = MagicMock()
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    IngestaoLocalStage().execute(_state())

    mock_prof_contract.validate.assert_called_once()
    mock_estab_contract.validate.assert_called_once()


@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_falha_conexao_propaga_excecao(
    mock_estab_contract,
    mock_prof_contract,
    mock_adapter_cls,
    mock_cbo,
    mock_conectar,
):
    mock_conectar.side_effect = RuntimeError("BD indisponivel")

    with pytest.raises(RuntimeError, match="BD indisponivel"):
        IngestaoLocalStage().execute(_state())
