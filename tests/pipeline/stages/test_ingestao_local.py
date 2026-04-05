from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline.state import PipelineState
from pipeline.stages.ingestao_local import IngestaoLocalStage
from storage.database_loader import DatabaseLoader
from storage.snapshot_local import SnapshotLocal, salvar_snapshot


def _state() -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
    )


def _state_com_force(force: bool = False) -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
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
def test_popula_state_com_dados_locais(
    mock_estab_contract,
    mock_prof_contract,
    mock_adapter_cls,
    mock_cbo,
    mock_conectar,
    mock_periodo,
    tmp_path,
):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    mock_con = MagicMock()
    mock_conectar.return_value = mock_con
    mock_cbo.return_value = {"515105": "Agente Comunitário"}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state()
    IngestaoLocalStage(tmp_path, loader).execute(state)

    assert state.con is mock_con
    assert state.cbo_lookup == {"515105": "Agente Comunitário"}
    assert len(state.df_prof_local) == 1
    assert len(state.df_estab_local) == 1


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
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
    mock_periodo,
    tmp_path,
):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    mock_conectar.return_value = MagicMock()
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    IngestaoLocalStage(tmp_path, loader).execute(_state())

    mock_prof_contract.validate.assert_called_once()
    mock_estab_contract.validate.assert_called_once()


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
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
    mock_periodo,
    tmp_path,
):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    mock_conectar.side_effect = RuntimeError("BD indisponivel")

    with pytest.raises(RuntimeError, match="BD indisponivel"):
        IngestaoLocalStage(tmp_path, loader).execute(_state())


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_usa_snapshot_quando_existe_e_sem_force(
    mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, mock_periodo, tmp_path
):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    snap = SnapshotLocal(
        df_prof=_df_prof_new(), df_estab=_df_estab_new(), cbo_lookup={"515105": "ACS"}
    )
    salvar_snapshot("2024-12", tmp_path, snap)

    state = _state_com_force(force=False)
    IngestaoLocalStage(tmp_path, loader).execute(state)

    mock_conectar.assert_not_called()
    assert state.snapshot_carregado is True
    assert len(state.df_prof_local) == 1
    assert state.cbo_lookup == {"515105": "ACS"}


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_usa_firebird_quando_force_reingestao(
    mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, mock_periodo, tmp_path
):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    snap = SnapshotLocal(df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={})
    salvar_snapshot("2024-12", tmp_path, snap)

    mock_con = MagicMock()
    mock_conectar.return_value = mock_con
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state_com_force(force=True)
    IngestaoLocalStage(tmp_path, loader).execute(state)

    mock_conectar.assert_called_once()
    assert state.snapshot_carregado is False


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_usa_firebird_quando_sem_snapshot(
    mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, mock_periodo, tmp_path
):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    mock_con = MagicMock()
    mock_conectar.return_value = mock_con
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state_com_force(force=False)
    IngestaoLocalStage(tmp_path, loader).execute(state)

    mock_conectar.assert_called_once()
    assert state.snapshot_carregado is False


def _df_prof_new() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS": ["123456789012345"],
        "CPF": ["12345678901"],
        "NOME_PROFISSIONAL": ["Ana Silva"],
        "SEXO": ["F"],
        "CBO": ["515105"],
        "CNES": ["2795001"],
        "TIPO_VINCULO": ["30"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [20],
        "CH_OUTRAS": [10],
        "CH_HOSPITALAR": [10],
        "FONTE": ["LOCAL"],
        "ALERTA_STATUS_CH": ["OK"],
        "DESCRICAO_CBO": ["ACS"],
    })


def _df_estab_new() -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": ["2795001"],
        "NOME_FANTASIA": ["UBS Centro"],
        "TIPO_UNIDADE": ["01"],
        "CNPJ_MANTENEDORA": ["55293427000117"],
        "NATUREZA_JURIDICA": ["1023"],
        "COD_MUNICIPIO": ["354130"],
        "VINCULO_SUS": ["S"],
        "FONTE": ["LOCAL"],
    })


def _state_basico(force: bool = False) -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
        force_reingestao=force,
    )


def test_carrega_do_duckdb_quando_dados_existem(tmp_path):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    loader.gravar_profissionais("2024-12", _df_prof_new())
    loader.gravar_estabelecimentos("2024-12", _df_estab_new())
    loader.gravar_cbo_lookup("2024-12", {"515105": "ACS"})

    state = _state_basico()
    IngestaoLocalStage(tmp_path, loader).execute(state)

    assert state.snapshot_carregado is True
    assert state.local_disponivel is True
    assert len(state.df_prof_local) == 1
    assert state.cbo_lookup == {"515105": "ACS"}


def test_backfill_do_parquet_quando_duckdb_vazio(tmp_path):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    snap = SnapshotLocal(
        df_prof=_df_prof_new(), df_estab=_df_estab_new(), cbo_lookup={"515105": "X"}
    )
    salvar_snapshot("2024-12", tmp_path, snap)

    state = _state_basico()
    IngestaoLocalStage(tmp_path, loader).execute(state)

    assert state.snapshot_carregado is True
    assert state.local_disponivel is True
    assert loader.profissional_existe("2024-12")


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_consulta_firebird_para_periodo_atual_sem_dados(
    mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, mock_periodo, tmp_path
):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    mock_con = MagicMock()
    mock_conectar.return_value = mock_con
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof_new()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab_new()
    mock_adapter_cls.return_value = mock_adapter

    state = _state_basico()
    IngestaoLocalStage(tmp_path, loader).execute(state)

    mock_conectar.assert_called_once()
    assert state.local_disponivel is True
    assert state.snapshot_carregado is False


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2026-04")
def test_marca_indisponivel_para_periodo_passado_sem_dados(mock_periodo, tmp_path):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()

    state = _state_basico()
    IngestaoLocalStage(tmp_path, loader).execute(state)

    assert state.local_disponivel is False


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_force_reingestao_usa_firebird_no_periodo_atual(
    mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, mock_periodo, tmp_path
):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    loader.gravar_profissionais("2024-12", _df_prof_new())
    mock_con = MagicMock()
    mock_conectar.return_value = mock_con
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof_new()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab_new()
    mock_adapter_cls.return_value = mock_adapter

    state = _state_basico(force=True)
    IngestaoLocalStage(tmp_path, loader).execute(state)

    mock_conectar.assert_called_once()


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2026-04")
def test_force_reingestao_ignora_firebird_para_periodo_passado(mock_periodo, tmp_path):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    loader.gravar_profissionais("2024-12", _df_prof_new())

    state = _state_basico(force=True)
    with patch("pipeline.stages.ingestao_local.conectar") as mock_con:
        IngestaoLocalStage(tmp_path, loader).execute(state)
        mock_con.assert_not_called()
        assert state.snapshot_carregado is True
        assert state.local_disponivel is True
