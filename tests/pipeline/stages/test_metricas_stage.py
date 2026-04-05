"""Testes para MetricasStage."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.metricas import MetricasStage
from storage.database_loader import DatabaseLoader
from storage.historico_reader import HistoricoReader

_GLOSAS_COLS = [
    "competencia", "regra", "cpf", "cns", "nome_profissional",
    "sexo", "cnes_estabelecimento", "motivo",
    "criado_em_firebird", "criado_em_pipeline", "atualizado_em_pipeline",
]

_GLOSAS_HISTORICO_COLS = [
    "competencia", "regra", "cpf", "cns", "nome_profissional",
    "sexo", "cnes_estabelecimento", "motivo",
    "criado_em_firebird", "criado_em_pipeline", "atualizado_em_pipeline",
]


def _state() -> PipelineState:
    state = PipelineState(
        competencia_ano=2026, competencia_mes=3,
        output_path=Path("data/processed/r.csv"),
        executar_nacional=False, executar_hr=False,
    )
    state.df_processado = pd.DataFrame({
        "CPF": ["12345678901"], "CNS": ["123456789012345"],
        "CBO": ["515105"], "CNES": ["1234567"],
        "CH_TOTAL": [40], "SEXO": ["F"],
    })
    state.df_estab_local = pd.DataFrame({
        "CNES": ["1234567"], "NOME_FANTASIA": ["UBS Centro"],
    })
    state.cbo_lookup = {"515105": "ACS"}
    return state


def _make_stage() -> tuple[MetricasStage, MagicMock, MagicMock]:
    db = MagicMock(spec=DatabaseLoader)
    reader = MagicMock(spec=HistoricoReader)
    reader.carregar_glosas_historicas.return_value = pd.DataFrame(columns=_GLOSAS_HISTORICO_COLS)
    reader.listar_competencias.return_value = ["2026-03"]
    return MetricasStage(db, reader), db, reader


_METRICAS_KEYS = {
    "taxa_anomalia_geral",
    "p90_ch_total",
    "proporcao_feminina_geral",
    "n_reincidentes",
    "taxa_resolucao",
    "velocidade_regularizacao_media",
    "top_glosas_json",
    "anomalias_por_cbo_json",
    "proporcao_feminina_por_cnes_json",
    "ranking_cnes_json",
}

_EMPTY_GLOSAS = pd.DataFrame(columns=_GLOSAS_COLS)


def test_execute_popula_state_metricas_avancadas():
    stage, db, reader = _make_stage()
    state = _state()

    with patch("pipeline.stages.metricas.construir_glosas", return_value=_EMPTY_GLOSAS):
        stage.execute(state)

    assert isinstance(state.metricas_avancadas, dict)
    assert len(state.metricas_avancadas) > 0


def test_gravar_glosas_chamado_por_regra():
    stage, db, reader = _make_stage()
    state = _state()
    state.df_ghost = pd.DataFrame({
        "CPF": ["12345678901"], "CNS": ["123456789012345"],
        "NOME_PROFISSIONAL": ["Fulano"], "SEXO": ["M"],
        "CNES": ["1234567"], "MOTIVO_GHOST": ["ausente CNES"],
    })

    df_glosas_ghost = pd.DataFrame({
        "competencia": ["2026-03"],
        "regra": ["GHOST"],
        "cpf": ["12345678901"], "cns": ["123456789012345"],
        "nome_profissional": ["Fulano"], "sexo": ["M"],
        "cnes_estabelecimento": ["1234567"], "motivo": ["ausente CNES"],
        "criado_em_firebird": [None],
        "criado_em_pipeline": [None], "atualizado_em_pipeline": [None],
    })

    with patch("pipeline.stages.metricas.construir_glosas", return_value=df_glosas_ghost):
        stage.execute(state)

    calls = db.gravar_glosas.call_args_list
    regras_gravadas = [c.args[1] for c in calls]
    assert "GHOST" in regras_gravadas


def test_gravar_metricas_avancadas_chamado():
    stage, db, reader = _make_stage()
    state = _state()

    with patch("pipeline.stages.metricas.construir_glosas", return_value=_EMPTY_GLOSAS):
        stage.execute(state)

    db.gravar_metricas_avancadas.assert_called_once()
    assert db.gravar_metricas_avancadas.call_args.args[0] == state.competencia_str


def test_metricas_contem_chaves_esperadas():
    stage, db, reader = _make_stage()
    state = _state()

    with patch("pipeline.stages.metricas.construir_glosas", return_value=_EMPTY_GLOSAS):
        stage.execute(state)

    assert _METRICAS_KEYS.issubset(state.metricas_avancadas.keys())


def test_taxa_resolucao_zero_sem_competencia_anterior():
    stage, db, reader = _make_stage()
    reader.listar_competencias.return_value = ["2026-03"]
    state = _state()

    with patch("pipeline.stages.metricas.construir_glosas", return_value=_EMPTY_GLOSAS):
        stage.execute(state)

    assert state.metricas_avancadas["taxa_resolucao"] == 0.0


def test_skip_quando_local_indisponivel():
    state = PipelineState(
        competencia_ano=2024, competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=False, executar_hr=False,
        local_disponivel=False,
    )
    db = MagicMock(spec=DatabaseLoader)
    reader = MagicMock(spec=HistoricoReader)
    MetricasStage(db, reader).execute(state)
    db.gravar_metricas_avancadas.assert_not_called()
