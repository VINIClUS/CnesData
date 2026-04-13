"""Testes da resiliência do PipelineOrchestrator com critico flag."""
import pytest
from cnes_domain.pipeline.orchestrator import PipelineOrchestrator, StageFatalError, StageSkipError
from cnes_domain.pipeline.state import PipelineState


def _make_state() -> PipelineState:
    return PipelineState(
        competencia_ano=2026,
        competencia_mes=1,
        output_path=None,
    )


class _StageOk:
    nome = "ok"
    critico = False

    def execute(self, state):
        state.cbo_lookup["ok"] = True


class _StageSkip:
    nome = "skip"
    critico = False

    def execute(self, state):
        raise StageSkipError("sem_dados")


class _StageErroNaoCritico:
    nome = "erro_nao_critico"
    critico = False

    def execute(self, state):
        raise ValueError("erro_simulado")


class _StageErroCritico:
    nome = "erro_critico"
    critico = True

    def execute(self, state):
        raise RuntimeError("falha_fatal")


class _StageErroCriticoFatal:
    nome = "erro_critico_fatal"
    critico = True

    def execute(self, state):
        raise StageFatalError("fatal_explicito")


def test_stage_ok_executa_normalmente():
    state = _make_state()
    orch = PipelineOrchestrator([_StageOk()])
    orch.executar(state)
    assert state.cbo_lookup.get("ok") is True


def test_stage_skip_nao_interrompe_pipeline():
    state = _make_state()
    orch = PipelineOrchestrator([_StageSkip(), _StageOk()])
    orch.executar(state)
    assert state.cbo_lookup.get("ok") is True


def test_stage_nao_critico_erro_continua_pipeline():
    state = _make_state()
    orch = PipelineOrchestrator([_StageErroNaoCritico(), _StageOk()])
    orch.executar(state)
    assert state.cbo_lookup.get("ok") is True


def test_stage_critico_erro_para_pipeline():
    state = _make_state()
    orch = PipelineOrchestrator([_StageErroCritico(), _StageOk()])
    with pytest.raises(RuntimeError, match="falha_fatal"):
        orch.executar(state)
    assert state.cbo_lookup.get("ok") is None


def test_stage_fatal_error_para_pipeline():
    state = _make_state()
    orch = PipelineOrchestrator([_StageErroCriticoFatal()])
    with pytest.raises(StageFatalError):
        orch.executar(state)


def test_multiplos_stages_nao_criticos_com_erros_continuam():
    state = _make_state()
    orch = PipelineOrchestrator([
        _StageErroNaoCritico(),
        _StageErroNaoCritico(),
        _StageOk(),
    ])
    orch.executar(state)
    assert state.cbo_lookup.get("ok") is True
