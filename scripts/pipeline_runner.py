"""pipeline_runner — spawns main.py subprocess and streams its output."""
import subprocess
import sys
from datetime import date
from pathlib import Path

_MAIN = Path(__file__).parent.parent / "src" / "main.py"


def competencia_atual() -> tuple[int, int]:
    hoje = date.today()
    return hoje.year, hoje.month


def iniciar_pipeline(
    competencia: str,
    skip_nacional: bool,
    skip_hr: bool,
) -> subprocess.Popen:
    """Inicia src/main.py como subprocesso com as flags CLI fornecidas.

    Args:
        competencia: Período no formato YYYY-MM.
        skip_nacional: Se True, passa --skip-nacional.
        skip_hr: Se True, passa --skip-hr.
    Returns:
        Processo iniciado com stdout+stderr unificados em PIPE texto.
    """
    cmd = [sys.executable, str(_MAIN), "-c", competencia]
    if skip_nacional:
        cmd.append("--skip-nacional")
    if skip_hr:
        cmd.append("--skip-hr")
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
