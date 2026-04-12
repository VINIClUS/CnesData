from pathlib import Path

import pytest

from pipeline.orchestrator import PipelineOrchestrator
from pipeline.state import PipelineState


def _make_state() -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        
    )


def test_chama_stages_na_ordem():
    order = []

    class StageA:
        nome = "A"
        def execute(self, state): order.append("A")

    class StageB:
        nome = "B"
        def execute(self, state): order.append("B")

    orchestrator = PipelineOrchestrator([StageA(), StageB()])
    orchestrator.executar(_make_state())

    assert order == ["A", "B"]


def test_hard_fail_propaga_excecao():
    class StageRuim:
        nome = "ruim"
        def execute(self, state): raise RuntimeError("falha critica")

    orchestrator = PipelineOrchestrator([StageRuim()])

    with pytest.raises(RuntimeError, match="falha critica"):
        orchestrator.executar(_make_state())


def test_stage_seguinte_nao_executa_apos_falha():
    executou = []

    class StageFalha:
        nome = "falha"
        def execute(self, state): raise RuntimeError("boom")

    class StageDepois:
        nome = "depois"
        def execute(self, state): executou.append(True)

    orchestrator = PipelineOrchestrator([StageFalha(), StageDepois()])

    with pytest.raises(RuntimeError):
        orchestrator.executar(_make_state())

    assert executou == []


def test_sem_stages_nao_levanta():
    orchestrator = PipelineOrchestrator([])
    orchestrator.executar(_make_state())
