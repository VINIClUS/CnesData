"""Testes para IngestaoLocalStage — Firebird direto, sem snapshots."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from pipeline.state import PipelineState
from pipeline.stages.ingestao_local import IngestaoLocalStage


def _state() -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
    )


def _df_prof() -> pl.DataFrame:
    return pl.DataFrame({
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


def _df_estab() -> pl.DataFrame:
    return pl.DataFrame({
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
    mock_estab_contract, mock_prof_contract,
    mock_adapter_cls, mock_cbo, mock_conectar,
):
    mock_conectar.return_value = MagicMock()
    mock_cbo.return_value = {"515105": "Agente Comunitário"}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state()
    IngestaoLocalStage().execute(state)

    assert state.cbo_lookup == {"515105": "Agente Comunitário"}
    assert len(state.df_prof_local) == 1
    assert len(state.df_estab_local) == 1


@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_valida_contratos_apos_ingestao(
    mock_estab_contract, mock_prof_contract,
    mock_adapter_cls, mock_cbo, mock_conectar,
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
def test_falha_conexao_propaga_excecao(mock_conectar):
    mock_conectar.side_effect = RuntimeError("BD indisponivel")

    with pytest.raises(RuntimeError, match="BD indisponivel"):
        IngestaoLocalStage().execute(_state())


@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_usa_firebird_sempre(
    mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar,
):
    mock_conectar.return_value = MagicMock()
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    IngestaoLocalStage().execute(_state())

    mock_conectar.assert_called_once()


def test_early_return_quando_target_source_nacional():
    state = _state()
    state.target_source = "NACIONAL"
    with patch("pipeline.stages.ingestao_local.conectar") as mock_con:
        IngestaoLocalStage().execute(state)
        mock_con.assert_not_called()
    assert state.df_prof_local.is_empty()
