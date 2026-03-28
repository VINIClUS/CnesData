"""Stage Protocol e PipelineOrchestrator."""
import logging
from typing import Protocol

from pipeline.state import PipelineState

logger = logging.getLogger(__name__)


class Stage(Protocol):
    """Contrato de um estágio do pipeline."""

    nome: str

    def execute(self, state: PipelineState) -> None: ...


class PipelineOrchestrator:
    """Executa stages em sequência sobre um PipelineState compartilhado."""

    def __init__(self, stages: list[Stage]) -> None:
        self._stages = stages

    def executar(self, state: PipelineState) -> None:
        """Executa cada stage em ordem. Exceções propagam imediatamente."""
        for stage in self._stages:
            logger.info("stage_inicio nome=%s", stage.nome)
            stage.execute(state)
            logger.info("stage_fim nome=%s", stage.nome)
