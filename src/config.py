"""
config.py — Módulo de Configuração Centralizada do Projeto CnesData

Princípio: Single Source of Truth (Uma fonte única de verdade).
Todos os outros módulos importam as constantes daqui, nunca definem
caminhos ou credenciais diretamente (evitando o anti-pattern "hardcode").

Como funciona:
  - O arquivo `.env` na raiz do projeto armazena os valores sensíveis/locais.
  - A biblioteca `python-dotenv` lê esse arquivo e popula as variáveis de ambiente.
  - As funções `os.getenv()` leem essas variáveis, com valores padrão seguros.
  - Se uma variável obrigatória estiver ausente, uma exceção clara é levantada.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Localização do Projeto ─────────────────────────────────────────────────
# Path(__file__) é o caminho para este próprio arquivo (src/config.py).
# .parent sobe um nível → src/
# .parent.parent sobe mais um → raiz do projeto (CnesData/)
RAIZ_PROJETO = Path(__file__).parent.parent

# Carrega o .env da raiz do projeto explicitamente.
# override=False garante que variáveis já definidas no sistema operacional
# têm prioridade sobre o .env (boa prática para deploys em produção).
load_dotenv(RAIZ_PROJETO / ".env", override=False)


def _exigir(nome: str) -> str:
    """
    Lê uma variável de ambiente. Levanta um erro explicativo se ela não existir.
    Isso falha rápido (fail-fast) antes de a aplicação começar a rodar.
    """
    valor = os.getenv(nome)
    if not valor:
        raise EnvironmentError(
            f"Variável de ambiente obrigatória '{nome}' não encontrada. "
            f"Verifique o arquivo .env na raiz do projeto."
        )
    return valor


# ── Banco de Dados Firebird ────────────────────────────────────────────────
DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PATH: str = _exigir("DB_PATH")
DB_USER: str = os.getenv("DB_USER", "SYSDBA")
DB_PASSWORD: str = _exigir("DB_PASSWORD")

# String de conexão no formato DSN: host:caminho_do_arquivo
DB_DSN: str = f"{DB_HOST}:{DB_PATH}"

# ── Driver Firebird 64-bits ────────────────────────────────────────────────
FIREBIRD_DLL: str = _exigir("FIREBIRD_DLL")

# ── Filtros do Município ───────────────────────────────────────────────────
COD_MUN_IBGE: str = _exigir("COD_MUN_IBGE")           # 6 dígitos — usado no Firebird local
ID_MUNICIPIO_IBGE7: str = _exigir("ID_MUNICIPIO_IBGE7")  # 7 dígitos — usado no BigQuery
CNPJ_MANTENEDORA: str = _exigir("CNPJ_MANTENEDORA")

# ── Snapshots Históricos ──────────────────────────────────────────────────
SNAPSHOTS_DIR: Path = RAIZ_PROJETO / os.getenv("SNAPSHOTS_DIR", "data/snapshots")

# ── Saída de Dados ─────────────────────────────────────────────────────────
_output_dir = os.getenv("OUTPUT_DIR", "data/processed")
_output_filename = os.getenv("OUTPUT_FILENAME", "Relatorio_Profissionais_CNES.csv")

# Usa a raiz do projeto para garantir que o caminho é absoluto,
# independente de onde o script for chamado.
OUTPUT_PATH: Path = RAIZ_PROJETO / _output_dir / _output_filename

# ── Google Cloud / BigQuery ────────────────────────────────────────────────
GCP_PROJECT_ID: str = _exigir("GCP_PROJECT_ID")

# ── Competência da Base Nacional (BigQuery) ───────────────────────────────
# Usada como parâmetro de partição nas queries ao basedosdados.
COMPETENCIA_ANO: int = int(os.getenv("COMPETENCIA_ANO", "2026"))
COMPETENCIA_MES: int = int(os.getenv("COMPETENCIA_MES", "01"))

# ── RH / Folha de Pagamento (opcional) ────────────────────────────────────
# Se não configurado, o cross-check CNES × RH é ignorado silenciosamente.
_folha_hr_path = os.getenv("FOLHA_HR_PATH")
FOLHA_HR_PATH: Path | None = Path(_folha_hr_path) if _folha_hr_path else None

# ── Logs ───────────────────────────────────────────────────────────────────
LOGS_DIR: Path = RAIZ_PROJETO / "logs"
LOG_FILE: Path = LOGS_DIR / "cnes_exporter.log"
