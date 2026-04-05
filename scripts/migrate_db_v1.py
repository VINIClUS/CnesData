"""One-time DB cleanup: remove invalid keys, stale runs, backfill pipeline_run entries."""
import os
import sys
from pathlib import Path

import duckdb

_WORKTREE_ROOT = Path(__file__).parent.parent.resolve()
_GIT_COMMON_DIR = Path(
    __import__("subprocess").check_output(
        ["git", "rev-parse", "--git-common-dir"],
        cwd=_WORKTREE_ROOT,
        text=True,
    ).strip()
)
_MAIN_REPO_ROOT = _GIT_COMMON_DIR.parent
_SHARED_DB = _MAIN_REPO_ROOT / "data" / "cnesdata.duckdb"
os.environ.setdefault("DUCKDB_PATH", str(_SHARED_DB))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(_MAIN_REPO_ROOT / ".env", override=False)

sys.path.insert(0, str(_WORKTREE_ROOT / "src"))
import config  # noqa: E402

db = duckdb.connect(str(config.DUCKDB_PATH))

db.execute(
    "DELETE FROM gold.auditoria_resultados "
    "WHERE NOT regexp_matches(data_competencia, '^\\d{4}-\\d{2}$')"
)
db.execute(
    "DELETE FROM gold.evolucao_metricas_mensais "
    "WHERE NOT regexp_matches(data_competencia, '^\\d{4}-\\d{2}$')"
)

db.execute(
    "DELETE FROM gold.pipeline_runs "
    "WHERE status IN ('sem_dados', 'sem_dados_locais')"
)

db.execute("""
    INSERT OR IGNORE INTO gold.pipeline_runs
        (competencia, local_disponivel, nacional_disponivel,
         hr_disponivel, status, iniciado_em, concluido_em)
    SELECT DISTINCT
        ar.data_competencia,
        TRUE,
        FALSE,
        FALSE,
        'parcial',
        NULL,
        NULL
    FROM gold.auditoria_resultados ar
    LEFT JOIN gold.pipeline_runs pr ON pr.competencia = ar.data_competencia
    WHERE pr.competencia IS NULL
      AND regexp_matches(ar.data_competencia, '^\\d{4}-\\d{2}$')
""")

db.execute("DELETE FROM gold.auditoria_resultados WHERE regra = 'RQ005'")

db.close()
print("migration complete")
