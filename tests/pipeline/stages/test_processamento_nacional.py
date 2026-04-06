"""Testes para ProcessamentoNacionalStage."""
from pathlib import Path

import pandas as pd

from pipeline.stages.processamento_nacional import ProcessamentoNacionalStage
from pipeline.state import PipelineState


def _df_nacional() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS": ["123456789012345", "234567890123456"],
        "NOME_PROFISSIONAL": ["  Ana Silva  ", "  João Costa  "],
        "CBO": ["515105", "223505"],
        "CNES": ["2795001", "2795001"],
        "TIPO_VINCULO": ["30", "30"],
        "SUS": ["S", "S"],
        "CH_TOTAL": [40, 0],
        "SEXO": ["F", "M"],
        "CPF": [None, None],
    })


def _state_nacional_only() -> PipelineState:
    state = PipelineState(
        competencia_ano=2026,
        competencia_mes=3,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
        local_disponivel=False,
    )
    state.nacional_disponivel = True
    state.df_prof_nacional = _df_nacional()
    state.cbo_lookup = {"515105": "Agente Comunitário"}
    return state


class TestProcessamentoNacionalStage:
    def test_nao_executa_quando_local_disponivel(self):
        state = _state_nacional_only()
        state.local_disponivel = True
        ProcessamentoNacionalStage().execute(state)
        assert state.df_processado.empty

    def test_nao_executa_quando_nacional_indisponivel(self):
        state = _state_nacional_only()
        state.nacional_disponivel = False
        ProcessamentoNacionalStage().execute(state)
        assert state.df_processado.empty

    def test_nao_executa_quando_df_nacional_vazio(self):
        state = _state_nacional_only()
        state.df_prof_nacional = pd.DataFrame()
        ProcessamentoNacionalStage().execute(state)
        assert state.df_processado.empty

    def test_popula_df_processado_com_dados_nacionais(self):
        state = _state_nacional_only()
        ProcessamentoNacionalStage().execute(state)
        assert not state.df_processado.empty
        assert len(state.df_processado) == 2

    def test_strips_espacos_em_colunas_de_texto(self):
        state = _state_nacional_only()
        ProcessamentoNacionalStage().execute(state)
        assert "Ana Silva" in state.df_processado["NOME_PROFISSIONAL"].values

    def test_adiciona_alerta_status_ch_ok_para_ch_normal(self):
        state = _state_nacional_only()
        ProcessamentoNacionalStage().execute(state)
        df = state.df_processado
        assert df[df["CNS"] == "123456789012345"]["ALERTA_STATUS_CH"].iloc[0] == "OK"

    def test_adiciona_alerta_ativo_sem_ch_para_ch_zero(self):
        state = _state_nacional_only()
        ProcessamentoNacionalStage().execute(state)
        df = state.df_processado
        assert df[df["CNS"] == "234567890123456"]["ALERTA_STATUS_CH"].iloc[0] == "ATIVO_SEM_CH"

    def test_adiciona_descricao_cbo_do_lookup(self):
        state = _state_nacional_only()
        ProcessamentoNacionalStage().execute(state)
        df = state.df_processado
        assert df[df["CBO"] == "515105"]["DESCRICAO_CBO"].iloc[0] == "Agente Comunitário"

    def test_cbo_nao_catalogado_quando_ausente_no_lookup(self):
        state = _state_nacional_only()
        ProcessamentoNacionalStage().execute(state)
        df = state.df_processado
        assert df[df["CBO"] == "223505"]["DESCRICAO_CBO"].iloc[0] == "CBO NAO CATALOGADO"

    def test_nao_remove_registros_sem_cpf(self):
        state = _state_nacional_only()
        ProcessamentoNacionalStage().execute(state)
        assert len(state.df_processado) == 2
