"""Testes do SnapshotLocalStage — persistência de snapshot pós-processamento."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline.state import PipelineState
from pipeline.stages.snapshot_local import SnapshotLocalStage
from storage.database_loader import DatabaseLoader
from storage.snapshot_local import SnapshotLocal, salvar_snapshot, snapshot_existe


def _df_processado() -> pd.DataFrame:
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
        "DESCRICAO_CBO": ["Agente Comunitário"],
    })


def _df_estab() -> pd.DataFrame:
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


def _state(snapshot_carregado: bool = False, force: bool = False) -> PipelineState:
    s = PipelineState(
        competencia_ano=2026,
        competencia_mes=3,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=False,
        executar_hr=False,
        force_reingestao=force,
        snapshot_carregado=snapshot_carregado,
    )
    s.df_processado = _df_processado()
    s.df_estab_local = _df_estab()
    s.cbo_lookup = {"515105": "Agente Comunitário"}
    return s


class TestSnapshotLocalStage:
    def test_salva_snapshot_em_primeira_rodada(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state(snapshot_carregado=False)
        SnapshotLocalStage(tmp_path, loader).execute(state)
        assert snapshot_existe("2026-03", tmp_path)

    def test_nao_salva_quando_carregado_do_snapshot(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state(snapshot_carregado=True)
        SnapshotLocalStage(tmp_path, loader).execute(state)
        assert not snapshot_existe("2026-03", tmp_path)

    def test_sobrescreve_snapshot_com_force(self, tmp_path):
        snap_antigo = SnapshotLocal(
            df_prof=_df_processado(), df_estab=_df_estab(), cbo_lookup={"515105": "v1"}
        )
        salvar_snapshot("2026-03", tmp_path, snap_antigo)

        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state(snapshot_carregado=False, force=True)
        SnapshotLocalStage(tmp_path, loader).execute(state)

        from storage.snapshot_local import carregar_snapshot
        loaded = carregar_snapshot("2026-03", tmp_path)
        assert loaded.cbo_lookup["515105"] == "Agente Comunitário"

    def test_calcula_delta_quando_force_e_snapshot_existe(self, tmp_path):
        df_anterior = _df_processado().copy()
        df_anterior.loc[0, "CBO"] = "999999"
        snap_antigo = SnapshotLocal(df_prof=df_anterior, df_estab=_df_estab(), cbo_lookup={})
        salvar_snapshot("2026-03", tmp_path, snap_antigo)

        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state(snapshot_carregado=False, force=True)
        SnapshotLocalStage(tmp_path, loader).execute(state)

        assert state.delta_local["n_alterados"] == 1

    def test_sem_delta_quando_primeira_rodada(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state(snapshot_carregado=False, force=False)
        SnapshotLocalStage(tmp_path, loader).execute(state)
        assert state.delta_local == {}


def _state_com_db(tmp_path, snapshot_carregado: bool = False, force: bool = False) -> PipelineState:
    s = PipelineState(
        competencia_ano=2026,
        competencia_mes=3,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=False,
        executar_hr=False,
        force_reingestao=force,
        snapshot_carregado=snapshot_carregado,
    )
    s.df_processado = _df_processado()
    s.df_estab_local = _df_estab()
    s.cbo_lookup = {"515105": "Agente Comunitário"}
    return s


class TestSnapshotLocalStageDuckDB:
    def test_grava_profissionais_no_duckdb(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state_com_db(tmp_path)
        SnapshotLocalStage(tmp_path, loader).execute(state)
        assert loader.profissional_existe("2026-03")

    def test_grava_estabelecimentos_no_duckdb(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state_com_db(tmp_path)
        SnapshotLocalStage(tmp_path, loader).execute(state)
        df = loader.carregar_estabelecimentos("2026-03")
        assert len(df) == 1

    def test_grava_cbo_lookup_no_duckdb(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state_com_db(tmp_path)
        SnapshotLocalStage(tmp_path, loader).execute(state)
        assert loader.carregar_cbo_lookup("2026-03") == {"515105": "Agente Comunitário"}

    def test_nao_grava_quando_snapshot_carregado(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state_com_db(tmp_path, snapshot_carregado=True)
        SnapshotLocalStage(tmp_path, loader).execute(state)
        assert not loader.profissional_existe("2026-03")
