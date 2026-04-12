"""Testes para IngestaoLocalStage sem DatabaseLoader."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline.orchestrator import StageSkipError
from pipeline.state import PipelineState
from pipeline.stages.ingestao_local import IngestaoLocalStage
from storage.snapshot_local import SnapshotLocal, salvar_snapshot


def _dump_side_effect(df_prof_fn):
    def _side(con, output_dir, competencia):
        path = output_dir / f"firebird_dump_{competencia}.parquet"
        output_dir.mkdir(parents=True, exist_ok=True)
        df_prof_fn().to_parquet(path, index=False)
        return path
    return _side


def _state(force: bool = False) -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        force_reingestao=force,
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


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
@patch("pipeline.stages.ingestao_local.dump_vinculos_para_parquet")
def test_popula_state_com_dados_locais(
    mock_dump, mock_estab_contract, mock_prof_contract,
    mock_adapter_cls, mock_cbo, mock_conectar, mock_periodo, tmp_path,
):
    mock_dump.side_effect = _dump_side_effect(_df_prof)
    mock_conectar.return_value = MagicMock()
    mock_cbo.return_value = {"515105": "Agente Comunitário"}
    mock_adapter = MagicMock()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state()
    IngestaoLocalStage(tmp_path).execute(state)

    assert state.cbo_lookup == {"515105": "Agente Comunitário"}
    assert len(state.df_prof_local) == 1
    assert len(state.df_estab_local) == 1


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
@patch("pipeline.stages.ingestao_local.dump_vinculos_para_parquet")
def test_valida_contratos_apos_ingestao(
    mock_dump, mock_estab_contract, mock_prof_contract,
    mock_adapter_cls, mock_cbo, mock_conectar, mock_periodo, tmp_path,
):
    mock_dump.side_effect = _dump_side_effect(_df_prof)
    mock_conectar.return_value = MagicMock()
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    IngestaoLocalStage(tmp_path).execute(_state())

    mock_prof_contract.validate.assert_called_once()
    mock_estab_contract.validate.assert_called_once()


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
def test_falha_conexao_propaga_excecao(mock_conectar, mock_periodo, tmp_path):
    mock_conectar.side_effect = RuntimeError("BD indisponivel")

    with pytest.raises(RuntimeError, match="BD indisponivel"):
        IngestaoLocalStage(tmp_path).execute(_state())


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
def test_usa_snapshot_quando_existe_e_sem_force(mock_conectar, mock_periodo, tmp_path):
    snap = SnapshotLocal(
        df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={"515105": "ACS"}
    )
    salvar_snapshot("2024-12", tmp_path, snap)

    state = _state(force=False)
    IngestaoLocalStage(tmp_path).execute(state)

    mock_conectar.assert_not_called()
    assert len(state.df_prof_local) == 1
    assert state.cbo_lookup == {"515105": "ACS"}


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
@patch("pipeline.stages.ingestao_local.dump_vinculos_para_parquet")
def test_usa_firebird_quando_force_reingestao(
    mock_dump, mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, mock_periodo, tmp_path
):
    mock_dump.side_effect = _dump_side_effect(_df_prof)
    snap = SnapshotLocal(df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={})
    salvar_snapshot("2024-12", tmp_path, snap)
    mock_conectar.return_value = MagicMock()
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    IngestaoLocalStage(tmp_path).execute(_state(force=True))

    mock_conectar.assert_called_once()


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
@patch("pipeline.stages.ingestao_local.dump_vinculos_para_parquet")
def test_usa_firebird_quando_sem_snapshot(
    mock_dump, mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, mock_periodo, tmp_path
):
    mock_dump.side_effect = _dump_side_effect(_df_prof)
    mock_conectar.return_value = MagicMock()
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    IngestaoLocalStage(tmp_path).execute(_state(force=False))

    mock_conectar.assert_called_once()


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2026-04")
def test_levanta_stage_skip_para_periodo_passado_sem_dados(mock_periodo, tmp_path):
    state = _state()
    with pytest.raises(StageSkipError, match="dados_locais_indisponiveis"):
        IngestaoLocalStage(tmp_path).execute(state)


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
@patch("pipeline.stages.ingestao_local.dump_vinculos_para_parquet")
def test_force_reingestao_usa_firebird_no_periodo_atual(
    mock_dump, mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, mock_periodo, tmp_path
):
    mock_dump.side_effect = _dump_side_effect(_df_prof)
    mock_conectar.return_value = MagicMock()
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    IngestaoLocalStage(tmp_path).execute(_state(force=True))

    mock_conectar.assert_called_once()


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2026-04")
def test_force_reingestao_ignora_firebird_para_periodo_passado(mock_periodo, tmp_path):
    snap = SnapshotLocal(df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={})
    salvar_snapshot("2024-12", tmp_path, snap)

    state = _state(force=True)
    with patch("pipeline.stages.ingestao_local.conectar") as mock_con:
        IngestaoLocalStage(tmp_path).execute(state)
        mock_con.assert_not_called()
    assert len(state.df_prof_local) == 1


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
def test_early_return_quando_target_source_nacional(mock_periodo, tmp_path):
    state = _state()
    state.target_source = "NACIONAL"
    with patch("pipeline.stages.ingestao_local.conectar") as mock_con:
        IngestaoLocalStage(tmp_path).execute(state)
        mock_con.assert_not_called()
    assert state.df_prof_local.empty
