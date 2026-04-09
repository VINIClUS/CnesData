"""Stage Protocol e PipelineOrchestrator."""
import logging
from typing import Protocol

from pipeline.state import PipelineState

logger = logging.getLogger(__name__)


class StageSkipError(Exception):
    """Raised by a stage to signal graceful skip (sem dados ou condição não atendida)."""


class StageFatalError(Exception):
    """Raised by a critical stage on unrecoverable failure."""


class Stage(Protocol):
    """Contrato de um estágio do pipeline."""

    nome: str
    critico: bool

    def execute(self, state: PipelineState) -> None: ...


class PipelineOrchestrator:
    """Executa stages em sequência sobre um PipelineState compartilhado."""

    def __init__(self, stages: list[Stage]) -> None:
        self._stages = stages

    def executar(self, state: PipelineState) -> None:
        """Executa cada stage com recuperação por estágio.

        Stages críticos propagam exceções imediatamente.
        Stages não-críticos registram o erro e continuam.
        StageSkipError é sempre silenciosa (skip intencional).
        """
        for stage in self._stages:
            critico = getattr(stage, "critico", True)
            logger.info("stage_inicio nome=%s critico=%s", stage.nome, critico)
            try:
                stage.execute(state)
                logger.info("stage_fim nome=%s", stage.nome)
            except StageSkipError as exc:
                logger.warning("stage_skip nome=%s motivo=%s", stage.nome, exc)
            except StageFatalError as exc:
                logger.error("stage_fatal nome=%s err=%s", stage.nome, exc)
                raise
            except Exception as exc:
                logger.error("stage_erro nome=%s err=%s", stage.nome, exc)
                if critico:
                    raise
                logger.warning("stage_continuando_apos_erro nome=%s", stage.nome)
