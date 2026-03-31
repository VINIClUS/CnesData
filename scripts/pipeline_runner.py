"""pipeline_runner — spawns main.py subprocess and streams its output."""
from datetime import date


def competencia_atual() -> tuple[int, int]:
    hoje = date.today()
    return hoje.year, hoje.month
