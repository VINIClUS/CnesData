# Strict ELT V3 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure CnesData monorepo into Strict ELT — dump_agent becomes a zero-transform extractor with Intent Whitelist and I/O circuit breakers; adapters migrate to data_processor for server-side transformation.

**Architecture:** Domain contracts (`ExtractionIntent`, `ExtractionParams`) live in `cnes_domain`. dump_agent owns Firebird extractors + io_guard. data_processor owns adapters (column mapping, NFKD, FONTE). central_api validates intents at Gate 1, dump_agent re-validates at Gate 2.

**Tech Stack:** Python 3.13, Pydantic v2, Polars, fdb, StrEnum, tempfile, shutil.disk_usage

---

## File Structure

### New files

| File | Package | Responsibility |
|---|---|---|
| `packages/cnes_domain/src/cnes_domain/models/extraction.py` | cnes_domain | `ExtractionIntent` StrEnum + `ExtractionParams` Pydantic model |
| `apps/dump_agent/src/dump_agent/extractors/__init__.py` | dump_agent | Package init |
| `apps/dump_agent/src/dump_agent/extractors/protocol.py` | dump_agent | `Extractor` Protocol |
| `apps/dump_agent/src/dump_agent/extractors/cnes_extractor.py` | dump_agent | Firebird CNES extraction (3 intents) |
| `apps/dump_agent/src/dump_agent/extractors/sihd_extractor.py` | dump_agent | Firebird SIHD extraction |
| `apps/dump_agent/src/dump_agent/extractors/registry.py` | dump_agent | `REGISTRY: dict[ExtractionIntent, Extractor]` |
| `apps/dump_agent/src/dump_agent/io_guard.py` | dump_agent | Pre-flight, spool limit, cleanup |
| `apps/data_processor/src/data_processor/adapters/__init__.py` | data_processor | Package init |
| `apps/data_processor/src/data_processor/adapters/cnes_local_adapter.py` | data_processor | Firebird raw → canonical schema |
| `apps/data_processor/src/data_processor/adapters/cnes_nacional_adapter.py` | data_processor | BigQuery raw → canonical schema |
| `apps/data_processor/src/data_processor/adapters/sihd_local_adapter.py` | data_processor | SIHD raw → canonical schema |

### Modified files

| File | Change |
|---|---|
| `apps/dump_agent/pyproject.toml` | Replace `cnes-infra[etl]` with `cnes-domain` + `fdb` + `httpx` |
| `apps/dump_agent/src/dump_agent/worker/consumer.py` | Use registry + ExtractionParams validation |
| `apps/dump_agent/src/dump_agent/worker/streaming_executor.py` | Use Extractor protocol + io_guard |
| `apps/central_api/src/central_api/routes/jobs.py` | Validate ExtractionParams on POST acquire |
| `apps/data_processor/src/data_processor/processor.py` | Use local adapters before `transformar()` |
| `apps/data_processor/pyproject.toml` | Add `cnes-domain` to sources |

### Deleted files (after migration)

| File | Reason |
|---|---|
| `packages/cnes_infra/src/cnes_infra/ingestion/cnes_client.py` | Migrated to dump_agent/extractors |
| `packages/cnes_infra/src/cnes_infra/ingestion/sihd_client.py` | Migrated to dump_agent/extractors |
| `packages/cnes_infra/src/cnes_infra/ingestion/cnes_local_adapter.py` | Migrated to data_processor/adapters |
| `packages/cnes_infra/src/cnes_infra/ingestion/sihd_local_adapter.py` | Migrated to data_processor/adapters |
| `packages/cnes_infra/src/cnes_infra/ingestion/cnes_nacional_adapter.py` | Migrated to data_processor/adapters |
| `packages/cnes_infra/src/cnes_infra/ingestion/config_sihd.py` | Migrated to dump_agent config |

---

## Task 1: Domain Contracts — ExtractionIntent + ExtractionParams

**Files:**
- Create: `packages/cnes_domain/src/cnes_domain/models/extraction.py`
- Test: `packages/cnes_domain/tests/models/test_extraction.py`

- [ ] **Step 1: Write failing tests for ExtractionIntent and ExtractionParams**

```python
# packages/cnes_domain/tests/models/test_extraction.py
import pytest
from pydantic import ValidationError

from cnes_domain.models.extraction import ExtractionIntent, ExtractionParams


class TestExtractionIntent:
    def test_valores_validos(self):
        assert ExtractionIntent.PROFISSIONAIS == "profissionais"
        assert ExtractionIntent.ESTABELECIMENTOS == "estabelecimentos"
        assert ExtractionIntent.EQUIPES == "equipes"
        assert ExtractionIntent.SIHD_PRODUCAO == "sihd_producao"

    def test_total_intents(self):
        assert len(ExtractionIntent) == 4


class TestExtractionParams:
    def test_params_validos(self):
        p = ExtractionParams(
            intent=ExtractionIntent.PROFISSIONAIS,
            competencia="2026-03",
            cod_municipio="354130",
        )
        assert p.intent == ExtractionIntent.PROFISSIONAIS
        assert p.competencia == "2026-03"
        assert p.cod_municipio == "354130"

    def test_rejeita_intent_invalido(self):
        with pytest.raises(ValidationError):
            ExtractionParams(
                intent="inexistente",
                competencia="2026-03",
                cod_municipio="354130",
            )

    def test_rejeita_campo_extra(self):
        with pytest.raises(ValidationError, match="extra"):
            ExtractionParams(
                intent="profissionais",
                competencia="2026-03",
                cod_municipio="354130",
                sql="DROP TABLE users",
            )

    def test_rejeita_competencia_formato_invalido(self):
        with pytest.raises(ValidationError):
            ExtractionParams(
                intent="profissionais",
                competencia="202603",
                cod_municipio="354130",
            )

    def test_rejeita_cod_municipio_formato_invalido(self):
        with pytest.raises(ValidationError):
            ExtractionParams(
                intent="profissionais",
                competencia="2026-03",
                cod_municipio="35",
            )

    def test_aceita_todos_os_intents(self):
        for intent in ExtractionIntent:
            p = ExtractionParams(
                intent=intent,
                competencia="2026-01",
                cod_municipio="354130",
            )
            assert p.intent == intent
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./venv/Scripts/python.exe -m pytest packages/cnes_domain/tests/models/test_extraction.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cnes_domain.models.extraction'`

- [ ] **Step 3: Implement ExtractionIntent and ExtractionParams**

```python
# packages/cnes_domain/src/cnes_domain/models/extraction.py
"""Contratos de extração — intents e parâmetros validados."""

from enum import StrEnum

from pydantic import BaseModel, Field


class ExtractionIntent(StrEnum):
    PROFISSIONAIS = "profissionais"
    ESTABELECIMENTOS = "estabelecimentos"
    EQUIPES = "equipes"
    SIHD_PRODUCAO = "sihd_producao"


class ExtractionParams(BaseModel):
    """Payload validado para jobs de extração."""

    model_config = {"extra": "forbid"}

    intent: ExtractionIntent
    competencia: str = Field(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")
    cod_municipio: str = Field(pattern=r"^\d{6}$")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./venv/Scripts/python.exe -m pytest packages/cnes_domain/tests/models/test_extraction.py -v`
Expected: 7 PASSED

- [ ] **Step 5: Lint**

Run: `./venv/Scripts/ruff.exe check packages/cnes_domain/src/cnes_domain/models/extraction.py packages/cnes_domain/tests/models/test_extraction.py`
Expected: All checks passed

- [ ] **Step 6: Commit**

```bash
git add packages/cnes_domain/src/cnes_domain/models/extraction.py packages/cnes_domain/tests/models/test_extraction.py
git commit -m "feat(domain): add ExtractionIntent StrEnum and ExtractionParams model"
```

---

## Task 2: I/O Guard — Circuit Breakers

**Files:**
- Create: `apps/dump_agent/src/dump_agent/io_guard.py`
- Test: `apps/dump_agent/tests/test_io_guard.py`

- [ ] **Step 1: Write failing tests for io_guard**

```python
# apps/dump_agent/tests/test_io_guard.py
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from dump_agent.io_guard import (
    InsufficientDiskError,
    SpoolLimitExceeded,
    SpoolGuard,
    pre_flight_check,
)


class TestPreFlightCheck:
    def test_aceita_disco_com_espaco_suficiente(self, tmp_path):
        pre_flight_check(tmp_path, min_free_mb=1)

    def test_rejeita_disco_sem_espaco(self, tmp_path):
        with pytest.raises(InsufficientDiskError, match="free_mb"):
            pre_flight_check(tmp_path, min_free_mb=999_999_999)


class TestSpoolGuard:
    def test_aceita_escrita_dentro_do_limite(self):
        guard = SpoolGuard(max_bytes=1000)
        guard.track(500)
        guard.track(400)

    def test_rejeita_escrita_acima_do_limite(self):
        guard = SpoolGuard(max_bytes=1000)
        guard.track(600)
        with pytest.raises(SpoolLimitExceeded, match="1000"):
            guard.track(500)

    def test_acumula_bytes_corretamente(self):
        guard = SpoolGuard(max_bytes=100)
        guard.track(30)
        guard.track(30)
        guard.track(30)
        assert guard.total_bytes == 90

    def test_limite_exato_nao_dispara(self):
        guard = SpoolGuard(max_bytes=100)
        guard.track(100)

    def test_reset_zera_contagem(self):
        guard = SpoolGuard(max_bytes=100)
        guard.track(90)
        guard.reset()
        assert guard.total_bytes == 0
        guard.track(90)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./venv/Scripts/python.exe -m pytest apps/dump_agent/tests/test_io_guard.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'dump_agent.io_guard'`

- [ ] **Step 3: Implement io_guard**

```python
# apps/dump_agent/src/dump_agent/io_guard.py
"""Circuit breakers para I/O em máquinas municipais."""

import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


class InsufficientDiskError(RuntimeError):
    pass


class SpoolLimitExceeded(RuntimeError):
    pass


def pre_flight_check(
    target_dir: Path, min_free_mb: int = 500,
) -> None:
    """Verifica espaço livre em disco antes de iniciar extração.

    Args:
        target_dir: Diretório onde os temporários serão escritos.
        min_free_mb: Mínimo de MB livres exigidos.

    Raises:
        InsufficientDiskError: Se o disco não tiver espaço suficiente.
    """
    usage = shutil.disk_usage(target_dir)
    free_mb = usage.free // (1024 * 1024)
    if free_mb < min_free_mb:
        raise InsufficientDiskError(
            f"free_mb={free_mb} min_required={min_free_mb} "
            f"path={target_dir}"
        )
    logger.info("pre_flight_ok free_mb=%d min=%d", free_mb, min_free_mb)


class SpoolGuard:
    """Monitora bytes escritos e aborta se exceder limite."""

    def __init__(self, max_bytes: int) -> None:
        self._max = max_bytes
        self._total = 0

    @property
    def total_bytes(self) -> int:
        return self._total

    def track(self, n_bytes: int) -> None:
        self._total += n_bytes
        if self._total > self._max:
            raise SpoolLimitExceeded(
                f"spool_limit_exceeded written={self._total} "
                f"max={self._max}"
            )

    def reset(self) -> None:
        self._total = 0
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./venv/Scripts/python.exe -m pytest apps/dump_agent/tests/test_io_guard.py -v`
Expected: 6 PASSED

- [ ] **Step 5: Lint**

Run: `./venv/Scripts/ruff.exe check apps/dump_agent/src/dump_agent/io_guard.py apps/dump_agent/tests/test_io_guard.py`
Expected: All checks passed

- [ ] **Step 6: Commit**

```bash
git add apps/dump_agent/src/dump_agent/io_guard.py apps/dump_agent/tests/test_io_guard.py
git commit -m "feat(dump-agent): add I/O guard circuit breakers"
```

---

## Task 3: Extractor Protocol + CNES Extractor

**Files:**
- Create: `apps/dump_agent/src/dump_agent/extractors/__init__.py`
- Create: `apps/dump_agent/src/dump_agent/extractors/protocol.py`
- Create: `apps/dump_agent/src/dump_agent/extractors/cnes_extractor.py`
- Test: `apps/dump_agent/tests/extractors/test_cnes_extractor.py`

**Reference:** Current SQL and cursor logic is in `packages/cnes_infra/src/cnes_infra/ingestion/cnes_client.py`.

- [ ] **Step 1: Write failing tests**

```python
# apps/dump_agent/tests/extractors/__init__.py
```

```python
# apps/dump_agent/tests/extractors/test_cnes_extractor.py
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from cnes_domain.models.extraction import ExtractionIntent, ExtractionParams
from dump_agent.extractors.cnes_extractor import CnesExtractor
from dump_agent.extractors.protocol import Extractor
from dump_agent.io_guard import SpoolGuard


class TestExtractorProtocol:
    def test_cnes_extractor_implementa_protocol(self):
        assert isinstance(CnesExtractor(), Extractor)


class TestCnesExtractor:
    def _make_params(self, intent: str = "profissionais") -> ExtractionParams:
        return ExtractionParams(
            intent=intent,
            competencia="2026-03",
            cod_municipio="354130",
        )

    def _mock_cursor(self, rows, columns):
        cur = MagicMock()
        cur.description = [(c,) for c in columns]
        cur.fetchmany = MagicMock(side_effect=[rows, []])
        cur.close = MagicMock()
        return cur

    def test_extract_profissionais_gera_parquet(self, tmp_path):
        columns = [
            "CPF", "CNS", "NOME_PROF", "NO_SOCIAL", "SEXO",
            "DATA_NASC", "COD_CBO", "IND_VINC", "TP_SUS_NAO_SUS",
            "CARGA_HORARIA_TOTAL", "CG_HORAAMB", "CGHORAOUTR",
            "CGHORAHOSP", "CNES", "NOME_FANTA", "TP_UNID_ID",
            "CODMUNGEST",
        ]
        rows = [
            ("12345678901", "123456", "JOAO", None, "M",
             "1990-01-01", "225125", "1", "S",
             40, 20, 10, 10, "1234567", "UBS CENTRAL", "1",
             "354130"),
        ]
        mock_con = MagicMock()
        mock_cur = self._mock_cursor(rows, columns)
        mock_con.cursor.return_value = mock_cur

        extractor = CnesExtractor()
        guard = SpoolGuard(max_bytes=50 * 1024 * 1024)
        path = extractor.extract(
            self._make_params("profissionais"),
            mock_con,
            tmp_path,
            guard,
        )

        assert path.exists()
        assert path.suffix == ".parquet"
        df = pl.read_parquet(path)
        assert len(df) == 1
        assert "CPF" in df.columns

    def test_extract_retorna_colunas_raw_sem_rename(self, tmp_path):
        columns = ["CPF", "CNS", "NOME_PROF"]
        rows = [("111", "222", "MARIA")]
        mock_con = MagicMock()
        mock_cur = self._mock_cursor(rows, columns)
        mock_con.cursor.return_value = mock_cur

        extractor = CnesExtractor()
        guard = SpoolGuard(max_bytes=50 * 1024 * 1024)
        path = extractor.extract(
            self._make_params("profissionais"),
            mock_con,
            tmp_path,
            guard,
        )
        df = pl.read_parquet(path)
        assert "CPF" in df.columns
        assert "NOME_PROFISSIONAL" not in df.columns
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./venv/Scripts/python.exe -m pytest apps/dump_agent/tests/extractors/test_cnes_extractor.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement Extractor protocol**

```python
# apps/dump_agent/src/dump_agent/extractors/__init__.py
```

```python
# apps/dump_agent/src/dump_agent/extractors/protocol.py
"""Protocol para extractors de dados."""

from pathlib import Path
from typing import Protocol, runtime_checkable

from cnes_domain.models.extraction import ExtractionParams
from dump_agent.io_guard import SpoolGuard


@runtime_checkable
class Extractor(Protocol):
    def extract(
        self,
        params: ExtractionParams,
        con: object,
        tmp_dir: Path,
        guard: SpoolGuard,
    ) -> Path: ...
```

- [ ] **Step 4: Implement CnesExtractor**

The extractor takes raw SQL (hardcoded), runs cursor.fetchmany in batches, writes raw Parquet chunks, and tracks bytes via SpoolGuard. No column renaming, no NFKD, no FONTE — purely raw extraction.

```python
# apps/dump_agent/src/dump_agent/extractors/cnes_extractor.py
"""Extractor CNES — Firebird cursor → raw Parquet."""

import logging
from pathlib import Path

import polars as pl

from cnes_domain.models.extraction import ExtractionIntent, ExtractionParams
from dump_agent.io_guard import SpoolGuard

logger = logging.getLogger(__name__)

_SQL_PROFISSIONAIS: str = """
    SELECT
        prof.CPF_PROF, prof.COD_CNS, prof.NOME_PROF,
        prof.NO_SOCIAL, prof.SEXO, prof.DATA_NASC,
        vinc.COD_CBO, vinc.IND_VINC, vinc.TP_SUS_NAO_SUS,
        (COALESCE(vinc.CG_HORAAMB, 0)
         + COALESCE(vinc.CGHORAOUTR, 0)
         + COALESCE(vinc.CGHORAHOSP, 0)) AS CARGA_HORARIA_TOTAL,
        COALESCE(vinc.CG_HORAAMB, 0) AS CG_HORAAMB,
        COALESCE(vinc.CGHORAOUTR, 0) AS CGHORAOUTR,
        COALESCE(vinc.CGHORAHOSP, 0) AS CGHORAHOSP,
        est.CNES, est.NOME_FANTA, est.TP_UNID_ID,
        est.CODMUNGEST
    FROM       LFCES021 vinc
    INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
    INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
    WHERE est.CODMUNGEST = ?
    ORDER BY prof.NOME_PROF, vinc.COD_CBO
"""

_SQL_ESTABELECIMENTOS: str = """
    SELECT
        est.CNES, est.NOME_FANTA, est.TP_UNID_ID,
        est.CODMUNGEST, est.CNPJ_MANT
    FROM LFCES004 est
    WHERE est.CODMUNGEST = ?
"""

_SQL_EQUIPES: str = """
    SELECT
        eq.SEQ_EQUIPE, eq.INE, eq.DS_AREA,
        eq.TP_EQUIPE, eq.COD_MUN
    FROM LFCES060 eq
    WHERE eq.COD_MUN = ?
"""

_INTENT_SQL: dict[ExtractionIntent, str] = {
    ExtractionIntent.PROFISSIONAIS: _SQL_PROFISSIONAIS,
    ExtractionIntent.ESTABELECIMENTOS: _SQL_ESTABELECIMENTOS,
    ExtractionIntent.EQUIPES: _SQL_EQUIPES,
}


class CnesExtractor:
    """Extrai dados CNES do Firebird em Parquet raw."""

    def extract(
        self,
        params: ExtractionParams,
        con: object,
        tmp_dir: Path,
        guard: SpoolGuard,
        batch_size: int = 5000,
    ) -> Path:
        sql = _INTENT_SQL[params.intent]
        output = tmp_dir / f"{params.intent.value}.parquet"

        cur = con.cursor()
        try:
            cur.execute(sql, (params.cod_municipio,))
            columns: list[str] = [d[0] for d in cur.description]
            frames: list[pl.DataFrame] = []

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                batch_df = pl.DataFrame(
                    rows, schema=columns, orient="row",
                )
                frames.append(batch_df)
                guard.track(batch_df.estimated_size())
        finally:
            cur.close()

        if not frames:
            empty = pl.DataFrame(
                schema=dict.fromkeys(columns, pl.Utf8),
            )
            empty.write_parquet(output)
        else:
            combined = pl.concat(frames)
            combined.write_parquet(output)

        logger.info(
            "extract_done intent=%s rows=%d path=%s",
            params.intent.value,
            sum(len(f) for f in frames),
            output,
        )
        return output
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `./venv/Scripts/python.exe -m pytest apps/dump_agent/tests/extractors/test_cnes_extractor.py -v`
Expected: 3 PASSED

- [ ] **Step 6: Lint**

Run: `./venv/Scripts/ruff.exe check apps/dump_agent/src/dump_agent/extractors/ apps/dump_agent/tests/extractors/`
Expected: All checks passed

- [ ] **Step 7: Commit**

```bash
git add apps/dump_agent/src/dump_agent/extractors/ apps/dump_agent/tests/extractors/
git commit -m "feat(dump-agent): add Extractor protocol and CnesExtractor"
```

---

## Task 4: SIHD Extractor

**Files:**
- Create: `apps/dump_agent/src/dump_agent/extractors/sihd_extractor.py`
- Test: `apps/dump_agent/tests/extractors/test_sihd_extractor.py`

**Reference:** Current SQL in `packages/cnes_infra/src/cnes_infra/ingestion/sihd_client.py`.

- [ ] **Step 1: Write failing tests**

```python
# apps/dump_agent/tests/extractors/test_sihd_extractor.py
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from cnes_domain.models.extraction import ExtractionParams
from dump_agent.extractors.protocol import Extractor
from dump_agent.extractors.sihd_extractor import SihdExtractor
from dump_agent.io_guard import SpoolGuard


class TestSihdExtractor:
    def _make_params(self) -> ExtractionParams:
        return ExtractionParams(
            intent="sihd_producao",
            competencia="2026-03",
            cod_municipio="354130",
        )

    def _mock_cursor(self, rows, columns):
        cur = MagicMock()
        cur.description = [(c,) for c in columns]
        cur.fetchmany = MagicMock(side_effect=[rows, []])
        cur.close = MagicMock()
        return cur

    def test_implementa_protocol(self):
        assert isinstance(SihdExtractor(), Extractor)

    def test_extract_gera_parquet_raw(self, tmp_path):
        columns = [
            "AH_NUM_AIH", "AH_CNES", "AH_CMPT",
            "AH_PACIENTE_NOME", "AH_PACIENTE_NUMERO_CNS",
        ]
        rows = [("12345", "1234567", "202603", "JOAO", "123456")]
        mock_con = MagicMock()
        mock_con.cursor.return_value = self._mock_cursor(rows, columns)

        extractor = SihdExtractor()
        guard = SpoolGuard(max_bytes=50 * 1024 * 1024)
        path = extractor.extract(
            self._make_params(), mock_con, tmp_path, guard,
        )

        assert path.exists()
        df = pl.read_parquet(path)
        assert len(df) == 1
        assert "AH_NUM_AIH" in df.columns
        assert "NUM_AIH" not in df.columns
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./venv/Scripts/python.exe -m pytest apps/dump_agent/tests/extractors/test_sihd_extractor.py -v`
Expected: FAIL

- [ ] **Step 3: Implement SihdExtractor**

```python
# apps/dump_agent/src/dump_agent/extractors/sihd_extractor.py
"""Extractor SIHD — Firebird cursor → raw Parquet."""

import logging
from pathlib import Path

import polars as pl

from cnes_domain.models.extraction import ExtractionParams
from dump_agent.io_guard import SpoolGuard

logger = logging.getLogger(__name__)

_SQL_SIHD_PRODUCAO: str = """
    SELECT
        AH_NUM_AIH, AH_CNES, AH_CMPT,
        AH_PACIENTE_NOME, AH_PACIENTE_NUMERO_CNS,
        AH_PACIENTE_SEXO, AH_PACIENTE_DT_NASCIMENTO,
        AH_PACIENTE_MUN_ORIGEM,
        AH_DIAG_PRI, AH_DIAG_SEC,
        AH_PROC_SOLICITADO, AH_PROC_REALIZADO,
        AH_DT_INTERNACAO, AH_DT_SAIDA,
        AH_MOT_SAIDA, AH_CAR_INTERNACAO,
        AH_ESPECIALIDADE, AH_SITUACAO,
        AH_MED_SOL_DOC, AH_MED_RESP_DOC,
        AH_OE_GESTOR, AH_SEQ
    FROM TB_HAIH
    WHERE AH_CMPT = ?
    ORDER BY AH_NUM_AIH
"""


class SihdExtractor:
    """Extrai dados SIHD do Firebird em Parquet raw."""

    def extract(
        self,
        params: ExtractionParams,
        con: object,
        tmp_dir: Path,
        guard: SpoolGuard,
        batch_size: int = 5000,
    ) -> Path:
        competencia_aaaamm = params.competencia.replace("-", "")
        output = tmp_dir / f"{params.intent.value}.parquet"

        cur = con.cursor()
        try:
            cur.execute(_SQL_SIHD_PRODUCAO, (competencia_aaaamm,))
            columns: list[str] = [d[0] for d in cur.description]
            frames: list[pl.DataFrame] = []

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                batch_df = pl.DataFrame(
                    rows, schema=columns, orient="row",
                )
                frames.append(batch_df)
                guard.track(batch_df.estimated_size())
        finally:
            cur.close()

        if not frames:
            empty = pl.DataFrame(
                schema=dict.fromkeys(columns, pl.Utf8),
            )
            empty.write_parquet(output)
        else:
            combined = pl.concat(frames)
            combined.write_parquet(output)

        logger.info(
            "extract_done intent=%s rows=%d",
            params.intent.value,
            sum(len(f) for f in frames),
        )
        return output
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./venv/Scripts/python.exe -m pytest apps/dump_agent/tests/extractors/test_sihd_extractor.py -v`
Expected: 2 PASSED

- [ ] **Step 5: Commit**

```bash
git add apps/dump_agent/src/dump_agent/extractors/sihd_extractor.py apps/dump_agent/tests/extractors/test_sihd_extractor.py
git commit -m "feat(dump-agent): add SihdExtractor"
```

---

## Task 5: Extractor Registry

**Files:**
- Create: `apps/dump_agent/src/dump_agent/extractors/registry.py`
- Test: `apps/dump_agent/tests/extractors/test_registry.py`

- [ ] **Step 1: Write failing tests**

```python
# apps/dump_agent/tests/extractors/test_registry.py
import pytest

from cnes_domain.models.extraction import ExtractionIntent
from dump_agent.extractors.cnes_extractor import CnesExtractor
from dump_agent.extractors.protocol import Extractor
from dump_agent.extractors.registry import REGISTRY
from dump_agent.extractors.sihd_extractor import SihdExtractor


class TestRegistry:
    def test_todos_os_intents_mapeados(self):
        for intent in ExtractionIntent:
            assert intent in REGISTRY, f"missing={intent}"

    def test_sem_intents_extras(self):
        assert len(REGISTRY) == len(ExtractionIntent)

    def test_todos_implementam_protocol(self):
        for intent, extractor in REGISTRY.items():
            assert isinstance(extractor, Extractor), (
                f"intent={intent} nao implementa Extractor"
            )

    def test_cnes_intents_mapeiam_para_cnes_extractor(self):
        for intent in (
            ExtractionIntent.PROFISSIONAIS,
            ExtractionIntent.ESTABELECIMENTOS,
            ExtractionIntent.EQUIPES,
        ):
            assert isinstance(REGISTRY[intent], CnesExtractor)

    def test_sihd_mapeia_para_sihd_extractor(self):
        assert isinstance(
            REGISTRY[ExtractionIntent.SIHD_PRODUCAO],
            SihdExtractor,
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./venv/Scripts/python.exe -m pytest apps/dump_agent/tests/extractors/test_registry.py -v`
Expected: FAIL

- [ ] **Step 3: Implement registry**

```python
# apps/dump_agent/src/dump_agent/extractors/registry.py
"""Registry — mapeia ExtractionIntent para Extractor concreto."""

from cnes_domain.models.extraction import ExtractionIntent
from dump_agent.extractors.cnes_extractor import CnesExtractor
from dump_agent.extractors.protocol import Extractor
from dump_agent.extractors.sihd_extractor import SihdExtractor

REGISTRY: dict[ExtractionIntent, Extractor] = {
    ExtractionIntent.PROFISSIONAIS: CnesExtractor(),
    ExtractionIntent.ESTABELECIMENTOS: CnesExtractor(),
    ExtractionIntent.EQUIPES: CnesExtractor(),
    ExtractionIntent.SIHD_PRODUCAO: SihdExtractor(),
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./venv/Scripts/python.exe -m pytest apps/dump_agent/tests/extractors/test_registry.py -v`
Expected: 5 PASSED

- [ ] **Step 5: Commit**

```bash
git add apps/dump_agent/src/dump_agent/extractors/registry.py apps/dump_agent/tests/extractors/test_registry.py
git commit -m "feat(dump-agent): add Extractor registry"
```

---

## Task 6: Refactor consumer.py + streaming_executor.py

**Files:**
- Modify: `apps/dump_agent/src/dump_agent/worker/consumer.py`
- Modify: `apps/dump_agent/src/dump_agent/worker/streaming_executor.py`
- Test: `apps/dump_agent/tests/worker/test_consumer_v3.py`

This is the core refactoring: consumer validates ExtractionParams (Gate 2), uses registry to dispatch, and integrates io_guard.

- [ ] **Step 1: Write failing tests for the new consumer flow**

```python
# apps/dump_agent/tests/worker/test_consumer_v3.py
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from cnes_domain.models.extraction import ExtractionIntent


class TestJobValidation:
    def test_rejeita_payload_com_campo_extra(self):
        from pydantic import ValidationError
        from cnes_domain.models.extraction import ExtractionParams

        with pytest.raises(ValidationError):
            ExtractionParams.model_validate({
                "intent": "profissionais",
                "competencia": "2026-03",
                "cod_municipio": "354130",
                "sql": "SELECT 1",
            })

    def test_rejeita_intent_desconhecido(self):
        from pydantic import ValidationError
        from cnes_domain.models.extraction import ExtractionParams

        with pytest.raises(ValidationError):
            ExtractionParams.model_validate({
                "intent": "desconhecido",
                "competencia": "2026-03",
                "cod_municipio": "354130",
            })

    def test_aceita_payload_valido(self):
        from cnes_domain.models.extraction import ExtractionParams

        p = ExtractionParams.model_validate({
            "intent": "profissionais",
            "competencia": "2026-03",
            "cod_municipio": "354130",
        })
        assert p.intent == ExtractionIntent.PROFISSIONAIS
```

- [ ] **Step 2: Run tests to verify they pass** (these use only domain contracts)

Run: `./venv/Scripts/python.exe -m pytest apps/dump_agent/tests/worker/test_consumer_v3.py -v`
Expected: 3 PASSED

- [ ] **Step 3: Create connection helper**

The dump_agent needs its own Firebird connection function (previously from cnes_infra.ingestion.cnes_client):

```python
# apps/dump_agent/src/dump_agent/worker/connection.py
"""Firebird connection helper for dump_agent."""

import logging
import os
from pathlib import Path

import fdb

logger = logging.getLogger(__name__)


def conectar_firebird() -> fdb.Connection:
    """Abre conexão com o banco CNES Firebird local."""
    dll_path = Path(os.environ["FIREBIRD_DLL"])
    if not dll_path.exists():
        raise FileNotFoundError(f"dll_path={dll_path}")
    fdb.load_api(str(dll_path))

    db_host = os.getenv("DB_HOST", "localhost")
    db_path = os.environ["DB_PATH"]
    dsn = f"{db_host}:{db_path}"

    con = fdb.connect(
        dsn=dsn,
        user=os.getenv("DB_USER", "SYSDBA"),
        password=os.environ["DB_PASSWORD"],
        charset="WIN1252",
    )
    logger.info("firebird_connected dsn=%s", dsn)
    return con
```

- [ ] **Step 5: Rewrite streaming_executor.py**

Replace the current `stream_to_storage` that takes raw SQL with a new version that uses registry + io_guard:

```python
# apps/dump_agent/src/dump_agent/worker/streaming_executor.py
"""Streaming executor — registry + io_guard → gzip → PUT upload."""

import gzip
import logging
import os
import tempfile
from pathlib import Path

import httpx

from cnes_domain.models.extraction import ExtractionParams
from dump_agent.extractors.registry import REGISTRY
from dump_agent.io_guard import SpoolGuard, pre_flight_check

logger = logging.getLogger(__name__)

_MIN_FREE_MB = int(os.getenv("DUMP_MIN_FREE_DISK_MB", "500"))
_MAX_SPOOL_MB = int(os.getenv("DUMP_MAX_SPOOL_MB", "200"))
_MAX_SPOOL_BYTES = _MAX_SPOOL_MB * 1024 * 1024


def stream_to_storage(
    con: object,
    params: ExtractionParams,
    upload_url: str,
) -> int:
    """Extrai via registry, comprime e envia via PUT."""
    extractor = REGISTRY[params.intent]

    with tempfile.TemporaryDirectory(
        prefix="dump_agent_",
    ) as tmp_str:
        tmp_dir = Path(tmp_str)
        pre_flight_check(tmp_dir, _MIN_FREE_MB)

        guard = SpoolGuard(max_bytes=_MAX_SPOOL_BYTES)
        parquet_path = extractor.extract(
            params, con, tmp_dir, guard,
        )

        compressed = _compress_file(parquet_path)
        _upload_payload(upload_url, compressed)

        size = parquet_path.stat().st_size
        logger.info(
            "stream_done intent=%s parquet_bytes=%d "
            "compressed_bytes=%d",
            params.intent.value, size, len(compressed),
        )
        return size


def _compress_file(path: Path) -> bytes:
    return gzip.compress(path.read_bytes())


def _upload_payload(url: str, data: bytes) -> None:
    if url.startswith(("null://", "placeholder://")):
        logger.warning("upload_skipped url=%s", url[:60])
        return
    resp = httpx.put(
        url,
        content=data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=300.0,
    )
    resp.raise_for_status()
```

- [ ] **Step 6: Rewrite consumer.py**

Replace the `_execute_streaming_job` that imports `cnes_infra.ingestion.cnes_client` with a version that validates `ExtractionParams` and delegates to the new `stream_to_storage`:

```python
# apps/dump_agent/src/dump_agent/worker/consumer.py
"""Consumer — loop streaming com heartbeat e upload via pre-signed URL."""

import asyncio
import logging
import os
import random
import signal
from functools import partial

import httpx
from pydantic import ValidationError

from cnes_domain.models.extraction import ExtractionParams
from cnes_domain.tenant import set_tenant_id

logger = logging.getLogger(__name__)

_POLL_INTERVAL: float = 1.0
_HEARTBEAT_INTERVAL: float = 300.0


async def run_worker(
    api_base_url: str,
    machine_id: str,
    jitter_max: float = 1800.0,
) -> None:
    """Loop streaming — acquire via API, upload direto ao MinIO."""
    loop = asyncio.get_running_loop()
    running = True

    def _shutdown(sig: signal.Signals) -> None:
        nonlocal running
        logger.info("worker_shutdown signal=%s", sig.name)
        running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, partial(_shutdown, sig))
        except NotImplementedError:
            pass

    logger.info(
        "worker_started api=%s machine=%s",
        api_base_url, machine_id,
    )

    while running:
        job_data = await _acquire_job(api_base_url, machine_id)
        if job_data is None:
            jitter = random.uniform(0, jitter_max)
            logger.info("no_jobs jitter_sleep=%.1fs", jitter)
            await asyncio.sleep(jitter)
            continue

        hb_task = asyncio.create_task(
            _heartbeat_loop(
                api_base_url,
                str(job_data["job_id"]),
                machine_id,
            )
        )
        try:
            await _execute_job(
                loop, api_base_url, machine_id, job_data,
            )
        finally:
            hb_task.cancel()
            try:
                await hb_task
            except asyncio.CancelledError:
                pass

        await asyncio.sleep(random.uniform(0, 5))
    logger.info("worker_stopped")


async def _acquire_job(
    api_url: str, machine_id: str,
) -> dict | None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{api_url}/api/v1/jobs/acquire",
            json={"machine_id": machine_id},
        )
    if resp.status_code == 204:
        return None
    resp.raise_for_status()
    return resp.json()


async def _heartbeat_loop(
    api_url: str, job_id: str, machine_id: str,
) -> None:
    while True:
        await asyncio.sleep(_HEARTBEAT_INTERVAL)
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(
                    f"{api_url}/api/v1/jobs/{job_id}/heartbeat",
                    json={"machine_id": machine_id},
                )
            logger.debug("heartbeat_sent job_id=%s", job_id)
        except Exception:
            logger.warning("heartbeat_failed job_id=%s", job_id)


async def _execute_job(
    loop: asyncio.AbstractEventLoop,
    api_url: str,
    machine_id: str,
    job_data: dict,
) -> None:
    job_id = str(job_data["job_id"])
    upload_url = job_data["upload_url"]
    object_key = job_data["object_key"]

    try:
        params = ExtractionParams.model_validate(
            job_data.get("extraction_params", {}),
        )
    except ValidationError:
        logger.exception("invalid_params job_id=%s", job_id)
        return

    set_tenant_id(job_data["tenant_id"])

    try:
        from dump_agent.worker.connection import conectar_firebird

        con = await loop.run_in_executor(None, conectar_firebird)
        try:
            from dump_agent.worker.streaming_executor import (
                stream_to_storage,
            )

            await loop.run_in_executor(
                None, stream_to_storage,
                con, params, upload_url,
            )
        finally:
            con.close()

        async with httpx.AsyncClient(timeout=30.0) as client:
            await client.post(
                f"{api_url}/api/v1/jobs/{job_id}/complete-upload",
                json={
                    "machine_id": machine_id,
                    "object_key": object_key,
                },
            )
        logger.info("job_done job_id=%s", job_id)
    except Exception:
        logger.exception("job_error job_id=%s", job_id)
```

- [ ] **Step 7: Run all dump_agent tests**

Run: `./venv/Scripts/python.exe -m pytest apps/dump_agent/tests/ -v`
Expected: All PASSED

- [ ] **Step 8: Lint**

Run: `./venv/Scripts/ruff.exe check apps/dump_agent/src/dump_agent/worker/`
Expected: All checks passed

- [ ] **Step 9: Commit**

```bash
git add apps/dump_agent/src/dump_agent/worker/
git commit -m "refactor(dump-agent): rewrite consumer and executor for registry + io_guard"
```

---

## Task 7: Update dump_agent Dependencies

**Files:**
- Modify: `apps/dump_agent/pyproject.toml`

- [ ] **Step 1: Update pyproject.toml**

Replace `cnes-infra[etl]` dependency with `cnes-domain` + `fdb` + `httpx`:

```toml
[project]
name = "dump-agent"
version = "0.2.0"
description = "ELT extraction worker — Firebird → raw Parquet → MinIO"
requires-python = ">=3.13"
dependencies = [
    "cnes-domain",
    "fdb>=2.0",
    "httpx>=0.27",
    "polars>=1.20",
    "backoff>=2.2",
    "tenacity>=9.1",
    "loguru>=0.7",
]

[tool.uv.sources]
cnes-domain = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/dump_agent"]
```

- [ ] **Step 2: Sync dependencies**

Run: `uv sync`
Expected: Resolves without error

- [ ] **Step 3: Run dump_agent tests to confirm deps work**

Run: `./venv/Scripts/python.exe -m pytest apps/dump_agent/tests/ -v`
Expected: All PASSED

- [ ] **Step 4: Commit**

```bash
git add apps/dump_agent/pyproject.toml uv.lock
git commit -m "refactor(dump-agent): drop cnes-infra dep, use cnes-domain + fdb directly"
```

---

## Task 8: Migrate Adapters to data_processor

**Files:**
- Create: `apps/data_processor/src/data_processor/adapters/__init__.py`
- Create: `apps/data_processor/src/data_processor/adapters/cnes_local_adapter.py`
- Create: `apps/data_processor/src/data_processor/adapters/cnes_nacional_adapter.py`
- Create: `apps/data_processor/src/data_processor/adapters/sihd_local_adapter.py`
- Test: `apps/data_processor/tests/adapters/test_cnes_local_adapter.py`
- Migrate tests from: `packages/cnes_infra/tests/ingestion/test_cnes_local_adapter.py`

These adapters are **copied** from cnes_infra with import paths adjusted. The key change: they now operate on **raw Parquet DataFrames** (with Firebird column names) instead of calling `cnes_client.extrair_profissionais()` internally.

- [ ] **Step 1: Create adapters package**

```python
# apps/data_processor/src/data_processor/adapters/__init__.py
```

- [ ] **Step 2: Migrate cnes_local_adapter.py**

Copy from `packages/cnes_infra/src/cnes_infra/ingestion/cnes_local_adapter.py` and modify:
- Remove `from cnes_infra.ingestion import cnes_client` import
- Remove `_extrair()` method that calls `cnes_client.extrair_profissionais()`
- Constructor takes `pl.DataFrame` (raw Parquet data) instead of `fdb.Connection`
- Keep all mapping dicts and NFKD normalization intact

```python
# apps/data_processor/src/data_processor/adapters/cnes_local_adapter.py
"""Adapter: Parquet raw (Firebird) → schema padronizado."""

import logging
import unicodedata

import polars as pl

from cnes_domain.contracts.columns import (
    SCHEMA_EQUIPE,
    SCHEMA_ESTABELECIMENTO,
    SCHEMA_PROFISSIONAL,
)

logger = logging.getLogger(__name__)

_FONTE_LOCAL: str = "LOCAL"

_MAP_PROFISSIONAL: dict[str, str] = {
    "COD_CNES": "CNES",
    "COD_VINCULO": "TIPO_VINCULO",
    "SUS_NAO_SUS": "SUS",
    "CARGA_HORARIA_TOTAL": "CH_TOTAL",
}

_MAP_ESTABELECIMENTO: dict[str, str] = {
    "COD_CNES": "CNES",
    "ESTABELECIMENTO": "NOME_FANTASIA",
    "COD_TIPO_UNIDADE": "TIPO_UNIDADE",
    "COD_MUN_GESTOR": "COD_MUNICIPIO",
}

_MAP_EQUIPE: dict[str, str] = {
    "COD_INE_EQUIPE": "INE",
    "COD_TIPO_EQUIPE": "TIPO_EQUIPE",
    "COD_CNES": "CNES",
    "COD_MUN_GESTOR": "COD_MUNICIPIO",
}


def _normalizar_nfkd(s: str | None) -> str | None:
    if s is None:
        return None
    return unicodedata.normalize("NFKD", str(s))


class CnesLocalAdapter:
    """Adapter: raw Parquet (Firebird cols) → schema canônico."""

    def __init__(self, df_raw: pl.DataFrame) -> None:
        self._raw = df_raw

    def listar_profissionais(self) -> pl.DataFrame:
        df = self._raw.rename(_MAP_PROFISSIONAL)
        df = df.with_columns(
            pl.col("CNS").str.strip_chars(),
            pl.col("CPF").str.strip_chars(),
            pl.col("CNES").str.strip_chars().str.pad_start(7, "0"),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        for col in ("NOME_PROFISSIONAL", "NOME_SOCIAL"):
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).map_elements(
                        _normalizar_nfkd, return_dtype=pl.Utf8,
                    )
                )
        logger.debug(
            "listar_profissionais fonte=LOCAL rows=%d", len(df),
        )
        return df.select(list(SCHEMA_PROFISSIONAL))

    def listar_estabelecimentos(self) -> pl.DataFrame:
        estab = self._raw.select(
            list(_MAP_ESTABELECIMENTO.keys()),
        ).rename(_MAP_ESTABELECIMENTO)
        estab = estab.with_columns(
            pl.col("CNES").str.strip_chars().str.pad_start(7, "0"),
            pl.col("NOME_FANTASIA").map_elements(
                _normalizar_nfkd, return_dtype=pl.Utf8,
            ),
        ).unique(subset=["CNES"])
        estab = estab.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("CNPJ_MANTENEDORA"),
            pl.lit(None).cast(pl.Utf8).alias("NATUREZA_JURIDICA"),
            pl.lit(None).cast(pl.Utf8).alias("VINCULO_SUS"),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        return estab.select(list(SCHEMA_ESTABELECIMENTO))

    def listar_equipes(self) -> pl.DataFrame:
        eq = self._raw.select(
            "COD_INE_EQUIPE", "NOME_EQUIPE", "COD_TIPO_EQUIPE",
            "COD_CNES", "COD_MUN_GESTOR",
        ).rename(_MAP_EQUIPE)
        eq = eq.with_columns(pl.col("INE").cast(pl.Utf8))
        eq = eq.drop_nulls(subset=["INE"]).unique(subset=["INE"])
        eq = eq.with_columns(
            pl.col("INE").str.strip_chars(),
            pl.col("CNES").cast(pl.Utf8).str.strip_chars().str.pad_start(
                7, "0",
            ),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        return eq.select(list(SCHEMA_EQUIPE))
```

- [ ] **Step 3: Write test for migrated adapter**

```python
# apps/data_processor/tests/adapters/__init__.py
```

```python
# apps/data_processor/tests/adapters/test_cnes_local_adapter.py
import polars as pl
import pytest

from data_processor.adapters.cnes_local_adapter import CnesLocalAdapter


def _make_raw_df() -> pl.DataFrame:
    return pl.DataFrame({
        "CPF": [" 12345678901 "],
        "CNS": [" 123456 "],
        "NOME_PROFISSIONAL": ["JO\u00c3O"],
        "NOME_SOCIAL": [None],
        "SEXO": ["M"],
        "DATA_NASCIMENTO": ["1990-01-01"],
        "CBO": ["225125"],
        "COD_VINCULO": ["1"],
        "SUS_NAO_SUS": ["S"],
        "CARGA_HORARIA_TOTAL": [40],
        "CH_AMBULATORIAL": [20],
        "CH_OUTRAS": [10],
        "CH_HOSPITALAR": [10],
        "COD_CNES": [" 1234567 "],
        "ESTABELECIMENTO": ["UBS CENTRAL"],
        "COD_TIPO_UNIDADE": ["1"],
        "COD_MUN_GESTOR": ["354130"],
        "COD_INE_EQUIPE": ["12345678"],
        "NOME_EQUIPE": ["ESF 001"],
        "COD_TIPO_EQUIPE": ["70"],
    })


class TestCnesLocalAdapter:
    def test_listar_profissionais_renomeia_colunas(self):
        adapter = CnesLocalAdapter(_make_raw_df())
        df = adapter.listar_profissionais()
        assert "CNES" in df.columns
        assert "COD_CNES" not in df.columns
        assert "TIPO_VINCULO" in df.columns
        assert "FONTE" in df.columns
        assert df["FONTE"][0] == "LOCAL"

    def test_listar_profissionais_strip_cpf(self):
        adapter = CnesLocalAdapter(_make_raw_df())
        df = adapter.listar_profissionais()
        assert df["CPF"][0] == "12345678901"

    def test_listar_profissionais_pad_cnes(self):
        raw = _make_raw_df().with_columns(
            pl.lit("123").alias("COD_CNES"),
        )
        adapter = CnesLocalAdapter(raw)
        df = adapter.listar_profissionais()
        assert df["CNES"][0] == "0000123"

    def test_listar_estabelecimentos_unique(self):
        raw = pl.concat([_make_raw_df(), _make_raw_df()])
        adapter = CnesLocalAdapter(raw)
        df = adapter.listar_estabelecimentos()
        assert len(df) == 1

    def test_listar_equipes_drop_null_ine(self):
        raw = _make_raw_df().with_columns(
            pl.lit(None).alias("COD_INE_EQUIPE"),
        )
        adapter = CnesLocalAdapter(raw)
        df = adapter.listar_equipes()
        assert len(df) == 0
```

- [ ] **Step 4: Run tests**

Run: `./venv/Scripts/python.exe -m pytest apps/data_processor/tests/adapters/test_cnes_local_adapter.py -v`
Expected: 5 PASSED

- [ ] **Step 5: Migrate cnes_nacional_adapter.py and sihd_local_adapter.py**

Copy `cnes_nacional_adapter.py` from cnes_infra — keep the cache/BigQuery logic intact since data_processor runs server-side with access to BigQuery. No changes needed except import path adjustments.

Copy `sihd_local_adapter.py` — change constructor to accept `pl.DataFrame` instead of `fdb.Connection`, remove `sihd_client` import:

```python
# apps/data_processor/src/data_processor/adapters/sihd_local_adapter.py
"""Adapter: Parquet raw (SIHD Firebird) → schema padronizado de AIH."""

import logging

import polars as pl

from cnes_domain.contracts.sihd_columns import (
    SCHEMA_AIH,
    SCHEMA_PROCEDIMENTO_AIH,
)

logger = logging.getLogger(__name__)

_FONTE_LOCAL: str = "LOCAL"

_MAP_AIH: dict[str, str] = {
    "AH_NUM_AIH": "NUM_AIH",
    "AH_CNES": "CNES",
    "AH_CMPT": "COMPETENCIA",
    "AH_PACIENTE_NOME": "PACIENTE_NOME",
    "AH_PACIENTE_NUMERO_CNS": "PACIENTE_CNS",
    "AH_PACIENTE_SEXO": "PACIENTE_SEXO",
    "AH_PACIENTE_DT_NASCIMENTO": "PACIENTE_DT_NASCIMENTO",
    "AH_PACIENTE_MUN_ORIGEM": "PACIENTE_MUN_ORIGEM",
    "AH_DIAG_PRI": "DIAG_PRI",
    "AH_DIAG_SEC": "DIAG_SEC",
    "AH_PROC_SOLICITADO": "PROC_SOLICITADO",
    "AH_PROC_REALIZADO": "PROC_REALIZADO",
    "AH_DT_INTERNACAO": "DT_INTERNACAO",
    "AH_DT_SAIDA": "DT_SAIDA",
    "AH_MOT_SAIDA": "MOT_SAIDA",
    "AH_CAR_INTERNACAO": "CAR_INTERNACAO",
    "AH_ESPECIALIDADE": "ESPECIALIDADE",
    "AH_SITUACAO": "SITUACAO",
    "AH_MED_SOL_DOC": "MED_SOL_DOC",
    "AH_MED_RESP_DOC": "MED_RESP_DOC",
}

_MAP_PROCEDIMENTO: dict[str, str] = {
    "PA_NUM_AIH": "NUM_AIH",
    "PA_CMPT": "COMPETENCIA",
    "PA_PROCEDIMENTO": "PROCEDIMENTO",
    "PA_PROCEDIMENTO_QTD": "QTD",
    "PA_VALOR": "VALOR",
    "PA_PF_CBO": "CBO_EXEC",
    "PA_EXEC_DOC": "DOC_EXEC",
}


class SihdLocalAdapter:
    """Adapter: raw Parquet (SIHD cols) → schema canônico AIH."""

    def __init__(self, df_raw: pl.DataFrame) -> None:
        self._raw = df_raw

    def listar_aihs(self) -> pl.DataFrame:
        df = self._raw.rename(_MAP_AIH)
        df = df.with_columns(
            pl.col("CNES").str.strip_chars().str.pad_start(7, "0"),
            pl.col("NUM_AIH").str.strip_chars(),
            pl.col("PACIENTE_CNS").str.strip_chars(),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        return df.select(list(SCHEMA_AIH))

    def listar_procedimentos(self) -> pl.DataFrame:
        df = self._raw.rename(_MAP_PROCEDIMENTO)
        df = df.with_columns(
            pl.col("NUM_AIH").str.strip_chars(),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        return df.select(list(SCHEMA_PROCEDIMENTO_AIH))
```

- [ ] **Step 6: Copy cnes_nacional_adapter.py with adjusted imports**

```bash
cp packages/cnes_infra/src/cnes_infra/ingestion/cnes_nacional_adapter.py apps/data_processor/src/data_processor/adapters/cnes_nacional_adapter.py
```

Then update the import from `cnes_infra.ingestion.web_client` to `cnes_infra.ingestion.web_client` (this import stays the same — data_processor still depends on cnes_infra for web_client access).

- [ ] **Step 7: Run all data_processor tests**

Run: `./venv/Scripts/python.exe -m pytest apps/data_processor/tests/ -v`
Expected: All PASSED

- [ ] **Step 8: Commit**

```bash
git add apps/data_processor/src/data_processor/adapters/ apps/data_processor/tests/adapters/
git commit -m "feat(data-processor): migrate adapters from cnes_infra"
```

---

## Task 9: Refactor data_processor/processor.py

**Files:**
- Modify: `apps/data_processor/src/data_processor/processor.py`

The processor now applies the local adapter to raw Parquet before calling `transformar()`.

- [ ] **Step 1: Update processor.py**

```python
# apps/data_processor/src/data_processor/processor.py
"""Processor — download raw Parquet, adapt, transform, persist to Gold."""
import gzip
import io
import logging

import httpx
import polars as pl
from sqlalchemy import select
from sqlalchemy.engine import Engine

from cnes_domain.ports.object_storage import ObjectStoragePort
from cnes_domain.processing.transformer import transformar
from cnes_infra.storage.job_queue import Job
from cnes_infra.storage.landing import raw_payload
from cnes_infra.storage.postgres_adapter import PostgresAdapter
from data_processor.adapters.cnes_local_adapter import CnesLocalAdapter
from data_processor.adapters.sihd_local_adapter import SihdLocalAdapter
from data_processor.config import MINIO_BUCKET

logger = logging.getLogger(__name__)


def process_job(
    engine: Engine,
    storage: ObjectStoragePort,
    job: Job,
) -> None:
    """Processa um job COMPLETED: MinIO → adapt → transform → Gold."""
    object_key = _get_object_key(engine, job.payload_id)
    if not object_key:
        raise ValueError(
            f"object_key_missing payload_id={job.payload_id}"
        )

    download_url = storage.get_presigned_download_url(
        MINIO_BUCKET, object_key,
    )
    df_raw = _download_parquet(download_url)
    logger.info(
        "downloaded rows=%d job_id=%s", len(df_raw), job.id,
    )

    competencia = _get_competencia(engine, job.payload_id)
    adapter = PostgresAdapter(engine)

    if job.source_system in (
        "cnes_profissional", "profissionais",
    ):
        local = CnesLocalAdapter(df_raw)
        df = local.listar_profissionais()
        df = transformar(df)
        adapter.gravar_profissionais(competencia, df)
    elif job.source_system in (
        "cnes_estabelecimento", "estabelecimentos",
    ):
        local = CnesLocalAdapter(df_raw)
        df = local.listar_estabelecimentos()
        adapter.gravar_estabelecimentos(competencia, df)
    elif job.source_system == "sihd_producao":
        local = SihdLocalAdapter(df_raw)
        df = local.listar_aihs()
        adapter.gravar_profissionais(competencia, df)

    logger.info(
        "processed job_id=%s source=%s rows=%d",
        job.id, job.source_system, len(df_raw),
    )


def _get_object_key(
    engine: Engine, payload_id: object,
) -> str | None:
    with engine.connect() as con:
        row = con.execute(
            select(raw_payload.c.object_key)
            .where(raw_payload.c.id == payload_id)
        ).first()
    return row.object_key if row else None


def _get_competencia(
    engine: Engine, payload_id: object,
) -> str:
    with engine.connect() as con:
        row = con.execute(
            select(raw_payload.c.competencia)
            .where(raw_payload.c.id == payload_id)
        ).first()
    if row is None:
        raise ValueError(f"payload_not_found id={payload_id}")
    return row.competencia


def _download_parquet(url: str) -> pl.DataFrame:
    if url.startswith("null://"):
        raise ValueError("null_storage url_not_downloadable")
    resp = httpx.get(url, timeout=120.0)
    resp.raise_for_status()
    raw = resp.content
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return pl.read_parquet(io.BytesIO(raw))
```

- [ ] **Step 2: Run data_processor tests**

Run: `./venv/Scripts/python.exe -m pytest apps/data_processor/tests/ -v`
Expected: All PASSED

- [ ] **Step 3: Commit**

```bash
git add apps/data_processor/src/data_processor/processor.py
git commit -m "refactor(data-processor): apply adapter before transform on raw Parquet"
```

---

## Task 10: Gate 1 — central_api ExtractionParams Validation

**Files:**
- Modify: `apps/central_api/src/central_api/routes/jobs.py`
- Test: `apps/central_api/tests/routes/test_jobs_validation.py`

- [ ] **Step 1: Write failing test**

```python
# apps/central_api/tests/routes/test_jobs_validation.py
import pytest
from pydantic import ValidationError

from cnes_domain.models.extraction import ExtractionParams


class TestGate1Validation:
    def test_rejeita_payload_com_sql(self):
        with pytest.raises(ValidationError):
            ExtractionParams(
                intent="profissionais",
                competencia="2026-03",
                cod_municipio="354130",
                sql="DROP TABLE",
            )

    def test_aceita_payload_valido(self):
        p = ExtractionParams(
            intent="profissionais",
            competencia="2026-03",
            cod_municipio="354130",
        )
        assert p.intent.value == "profissionais"
```

- [ ] **Step 2: Run test**

Run: `./venv/Scripts/python.exe -m pytest apps/central_api/tests/routes/test_jobs_validation.py -v`
Expected: 2 PASSED (these test domain contracts, not route changes yet)

- [ ] **Step 3: Add ExtractionParams import to jobs.py acquire route**

Add validation of `extraction_params` field in the acquire endpoint. The API should accept an optional `extraction_params` in the job creation flow. For backwards compatibility during migration, make it optional:

```python
# In apps/central_api/src/central_api/routes/jobs.py
# Add to imports:
from cnes_domain.models.extraction import ExtractionParams
```

Add a new route for creating jobs with validated params:

```python
@router.post("/jobs/create")
def create_job_with_params(
    body: ExtractionParams,
    engine: Engine = Depends(get_engine),
) -> Response:
    """Gate 1: valida ExtractionParams antes de enfileirar."""
    from cnes_infra.storage.job_queue import enqueue_job

    enqueue_job(
        engine,
        source_system=body.intent.value,
        tenant_id=body.cod_municipio,
        extraction_params=body.model_dump(),
    )
    return Response(status_code=201)
```

- [ ] **Step 4: Commit**

```bash
git add apps/central_api/src/central_api/routes/jobs.py apps/central_api/tests/routes/
git commit -m "feat(central-api): add Gate 1 ExtractionParams validation on job create"
```

---

## Task 11: Remove Migrated Files from cnes_infra

**Files:**
- Delete: `packages/cnes_infra/src/cnes_infra/ingestion/cnes_client.py`
- Delete: `packages/cnes_infra/src/cnes_infra/ingestion/sihd_client.py`
- Delete: `packages/cnes_infra/src/cnes_infra/ingestion/cnes_local_adapter.py`
- Delete: `packages/cnes_infra/src/cnes_infra/ingestion/sihd_local_adapter.py`
- Delete: `packages/cnes_infra/src/cnes_infra/ingestion/cnes_nacional_adapter.py`
- Delete: `packages/cnes_infra/src/cnes_infra/ingestion/config_sihd.py`
- Delete: `packages/cnes_infra/tests/ingestion/test_cnes_client.py`
- Delete: `packages/cnes_infra/tests/ingestion/test_cnes_local_adapter.py`
- Delete: `packages/cnes_infra/tests/ingestion/test_sihd_local_adapter.py`
- Delete: `packages/cnes_infra/tests/ingestion/test_cnes_nacional_adapter.py`

- [ ] **Step 1: Verify no remaining imports of deleted modules**

Run: `grep -r "cnes_infra.ingestion.cnes_client" apps/ packages/ --include="*.py" -l`
Run: `grep -r "cnes_infra.ingestion.sihd_client" apps/ packages/ --include="*.py" -l`
Run: `grep -r "cnes_infra.ingestion.cnes_local_adapter" apps/ packages/ --include="*.py" -l`
Run: `grep -r "cnes_infra.ingestion.sihd_local_adapter" apps/ packages/ --include="*.py" -l`
Run: `grep -r "cnes_infra.ingestion.cnes_nacional_adapter" apps/ packages/ --include="*.py" -l`

Expected: No results (all imports have been replaced in prior tasks)

If any files still reference these modules, update the imports first.

- [ ] **Step 2: Delete source files**

```bash
rm packages/cnes_infra/src/cnes_infra/ingestion/cnes_client.py
rm packages/cnes_infra/src/cnes_infra/ingestion/sihd_client.py
rm packages/cnes_infra/src/cnes_infra/ingestion/cnes_local_adapter.py
rm packages/cnes_infra/src/cnes_infra/ingestion/sihd_local_adapter.py
rm packages/cnes_infra/src/cnes_infra/ingestion/cnes_nacional_adapter.py
rm packages/cnes_infra/src/cnes_infra/ingestion/config_sihd.py
```

- [ ] **Step 3: Delete test files**

```bash
rm packages/cnes_infra/tests/ingestion/test_cnes_client.py
rm packages/cnes_infra/tests/ingestion/test_cnes_local_adapter.py
rm packages/cnes_infra/tests/ingestion/test_sihd_local_adapter.py
rm packages/cnes_infra/tests/ingestion/test_cnes_nacional_adapter.py
```

- [ ] **Step 4: Run full test suite to confirm nothing breaks**

Run: `./venv/Scripts/python.exe -m pytest packages/ apps/ -v --tb=short`
Expected: All PASSED

- [ ] **Step 5: Lint entire codebase**

Run: `./venv/Scripts/ruff.exe check packages/ apps/`
Expected: All checks passed

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(cnes-infra): remove migrated clients and adapters"
```

---

## Task 12: Final Integration Test + Cleanup

- [ ] **Step 1: Run full test suite**

Run: `./venv/Scripts/python.exe -m pytest packages/ apps/ -q --tb=short`
Expected: All PASSED

- [ ] **Step 2: Run full lint**

Run: `./venv/Scripts/ruff.exe check packages/ apps/`
Expected: All checks passed

- [ ] **Step 3: Verify dependency isolation**

Confirm dump_agent no longer imports cnes_infra:

Run: `grep -r "cnes_infra" apps/dump_agent/ --include="*.py" -l`
Expected: No results

Confirm data_processor imports cnes_infra only for storage (not ingestion clients):

Run: `grep -r "cnes_infra.ingestion.cnes_client\|cnes_infra.ingestion.sihd_client" apps/data_processor/ --include="*.py" -l`
Expected: No results

- [ ] **Step 4: Commit final state**

```bash
git add -A
git commit -m "chore: verify Strict ELT V3 integration — all tests pass"
```
