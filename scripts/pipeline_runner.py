"""pipeline_runner — spawns main.py subprocess and streams its output."""
import queue
import subprocess
import sys
import threading
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


def iniciar_leitor(proc: subprocess.Popen) -> queue.Queue:
    """Inicia thread daemon que lê proc.stdout linha a linha para fila.
    Args:
        proc: Processo iniciado com stdout em PIPE.
    Returns:
        Fila que recebe linhas do stdout do processo.
    """
    q: queue.Queue = queue.Queue()

    def _ler() -> None:
        for linha in proc.stdout:
            q.put(linha)

    t = threading.Thread(target=_ler, name="pipeline-leitor", daemon=True)
    t.start()
    return q
