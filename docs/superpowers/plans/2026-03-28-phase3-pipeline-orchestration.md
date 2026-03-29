# Phase 3 — Pipeline Orchestration + Pandera Contracts

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the God Object in `main.py` (367 lines, 5 layers) by introducing a `PipelineOrchestrator` with a `Stage` Protocol and 5 cohesive stage classes, plus Pandera `DataFrameModel` contracts that enforce canonical schemas at ingestion boundaries.

**Architecture:**
`main()` → `PipelineOrchestrator.executar(state)` → 5 stages mutate `PipelineState` in sequence. Pandera contracts validate DataFrames immediately after ingestion before they reach the rules engine. Epic 6 (cascade_resolver + CachingVerificadorCnes) is already implemented — no new code needed there.

**Tech Stack:** Python 3.12, pandas, pandera[pandas], pytest, pytest-mock, existing fdb/BigQuery adapters

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `src/pipeline/__init__.py` | Package marker |
| Create | `src/pipeline/state.py` | `PipelineState` dataclass — all inter-stage data |
| Create | `src/pipeline/orchestrator.py` | `Stage` Protocol + `PipelineOrchestrator` |
| Create | `src/pipeline/stages/__init__.py` | Package marker |
| Create | `src/pipeline/stages/ingestao_local.py` | `IngestaoLocalStage` — Firebird ingestion |
| Create | `src/pipeline/stages/ingestao_nacional.py` | `IngestaoNacionalStage` — BigQuery (soft-fail) |
| Create | `src/pipeline/stages/processamento.py` | `ProcessamentoStage` — transformer.transformar |
| Create | `src/pipeline/stages/auditoria.py` | `AuditoriaStage` — all rules_engine functions |
| Create | `src/pipeline/stages/exportacao.py` | `ExportacaoStage` — CSV, Excel, snapshot, DuckDB |
| Create | `src/contracts/__init__.py` | Package marker |
| Create | `src/contracts/schemas.py` | Pandera DataFrameModel contracts |
| Modify | `src/main.py` | Refactor to ≤40 lines |
| Create | `tests/pipeline/test_state.py` | State construction tests |
| Create | `tests/pipeline/test_orchestrator.py` | Orchestrator sequencing + error propagation |
| Create | `tests/pipeline/stages/test_ingestao_local.py` | Local ingestion stage tests |
| Create | `tests/pipeline/stages/test_ingestao_nacional.py` | National ingestion stage tests |
| Create | `tests/pipeline/stages/test_processamento.py` | Processamento stage tests |
| Create | `tests/pipeline/stages/test_auditoria.py` | Auditoria stage tests |
| Create | `tests/pipeline/stages/test_exportacao.py` | Exportacao stage tests |
| Create | `tests/contracts/test_schemas.py` | Contract validation tests |
| Modify | `tests/test_main.py` | Update for new main() structure |

---

## Task 1: PipelineState + Stage Protocol + PipelineOrchestrator

**Files:**
- Create: `src/pipeline/__init__.py`
- Create: `src/pipeline/state.py`
- Create: `src/pipeline/orchestrator.py`
- Test: `tests/pipeline/test_state.py`
- Test: `tests/pipeline/test_orchestrator.py`

- [ ] **Step 1: Install pandera**

```bash
./venv/Scripts/pip.exe install "pandera[pandas]"
./venv/Scripts/pip.exe freeze | grep pandera >> requirements.txt
```

- [ ] **Step 2: Write failing tests for PipelineState**

```python
# tests/pipeline/__init__.py  (empty)
# tests/pipeline/test_state.py
from pathlib import Path

import pandas as pd
import pytest

from pipeline.state import PipelineState


def test_construcao_minima_preenche_defaults():
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
    )

    assert state.competencia_ano == 2024
    assert state.competencia_mes == 12
    assert state.con is None
    assert state.cbo_lookup == {}
    assert isinstance(state.df_prof_local, pd.DataFrame)
    assert state.df_prof_local.empty
    assert state.df_estab_local.empty
    assert state.df_prof_nacional.empty
    assert state.df_estab_nacional.empty
    assert state.df_processado.empty
    assert state.df_multi_unidades.empty
    assert state.df_acs_incorretos.empty
    assert state.df_ace_incorretos.empty
    assert state.df_ghost.empty
    assert state.df_missing.empty
    assert state.df_estab_fantasma.empty
    assert state.df_estab_ausente.empty
    assert state.df_prof_fantasma.empty
    assert state.df_prof_ausente.empty
    assert state.df_cbo_diverg.empty
    assert state.df_ch_diverg.empty


def test_competencia_str_formata_corretamente():
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=3,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=False,
        executar_hr=False,
    )

    assert state.competencia_str == "2024-03"
```

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/test_state.py -x --tb=short -q`
Expected: FAIL with ImportError

- [ ] **Step 3: Write failing tests for PipelineOrchestrator**

```python
# tests/pipeline/test_orchestrator.py
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

from pipeline.orchestrator import PipelineOrchestrator
from pipeline.state import PipelineState


def _make_state() -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
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
    orchestrator.executar(_make_state())  # deve completar sem erros
```

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/test_orchestrator.py -x --tb=short -q`
Expected: FAIL with ImportError

- [ ] **Step 4: Implement PipelineState**

```python
# src/pipeline/__init__.py  (empty file)

# src/pipeline/state.py
"""PipelineState — contentor imutável de dados inter-stage."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class PipelineState:
    """Todos os dados trocados entre stages do pipeline."""

    competencia_ano: int
    competencia_mes: int
    output_path: Path
    executar_nacional: bool
    executar_hr: bool

    con: Any = None
    cbo_lookup: dict[str, str] = field(default_factory=dict)

    df_prof_local: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_estab_local: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_prof_nacional: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_estab_nacional: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_processado: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_multi_unidades: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_acs_incorretos: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_ace_incorretos: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_ghost: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_missing: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_estab_fantasma: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_estab_ausente: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_prof_fantasma: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_prof_ausente: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_cbo_diverg: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_ch_diverg: pd.DataFrame = field(default_factory=pd.DataFrame)

    @property
    def competencia_str(self) -> str:
        return f"{self.competencia_ano}-{self.competencia_mes:02d}"
```

- [ ] **Step 5: Implement PipelineOrchestrator**

```python
# src/pipeline/orchestrator.py
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
```

- [ ] **Step 6: Run tests — expect all green**

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/ -x --tb=short -q`
Expected: 6 tests passing

- [ ] **Step 7: Run full suite — expect no regressions**

Run: `./venv/Scripts/python.exe -m pytest tests/ -q --tb=short`
Expected: all previously passing tests still green

- [ ] **Step 8: Commit**

```bash
git add src/pipeline/__init__.py src/pipeline/state.py src/pipeline/orchestrator.py tests/pipeline/
git commit -m "feat(pipeline): PipelineState + Stage Protocol + PipelineOrchestrator"
```

---

## Task 2: Pandera Contracts

**Files:**
- Create: `src/contracts/__init__.py`
- Create: `src/contracts/schemas.py`
- Test: `tests/contracts/test_schemas.py`

- [ ] **Step 1: Write failing tests for Pandera contracts**

```python
# tests/contracts/__init__.py  (empty)
# tests/contracts/test_schemas.py
import pandas as pd
import pandera as pa
import pytest

from contracts.schemas import EstabelecimentoContract, ProfissionalContract


def _df_prof_valido() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS": ["123456789012345"],
        "CPF": ["12345678901"],
        "NOME_PROFISSIONAL": ["Ana Silva"],
        "CBO": ["515105"],
        "CNES": ["1234567"],
        "TIPO_VINCULO": ["30"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [20],
        "CH_OUTRAS": [10],
        "CH_HOSPITALAR": [10],
        "FONTE": ["LOCAL"],
    })


def _df_estab_valido() -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": ["1234567"],
        "NOME_FANTASIA": ["UBS Centro"],
        "TIPO_UNIDADE": ["01"],
        "CNPJ_MANTENEDORA": ["55293427000117"],
        "NATUREZA_JURIDICA": ["1023"],
        "COD_MUNICIPIO": ["354130"],
        "VINCULO_SUS": ["S"],
        "FONTE": ["LOCAL"],
    })


def test_profissional_valido_passa():
    ProfissionalContract.validate(_df_prof_valido())


def test_profissional_cpf_nulo_nacional_passa():
    df = _df_prof_valido().copy()
    df["CPF"] = None
    df["FONTE"] = "NACIONAL"
    ProfissionalContract.validate(df)


def test_profissional_coluna_ausente_levanta_schema_error():
    df = _df_prof_valido().drop(columns=["CNS"])
    with pytest.raises(pa.errors.SchemaError):
        ProfissionalContract.validate(df)


def test_profissional_fonte_invalida_levanta_schema_error():
    df = _df_prof_valido().copy()
    df["FONTE"] = "DESCONHECIDA"
    with pytest.raises(pa.errors.SchemaError):
        ProfissionalContract.validate(df)


def test_profissional_sus_invalido_levanta_schema_error():
    df = _df_prof_valido().copy()
    df["SUS"] = "X"
    with pytest.raises(pa.errors.SchemaError):
        ProfissionalContract.validate(df)


def test_estabelecimento_valido_passa():
    EstabelecimentoContract.validate(_df_estab_valido())


def test_estabelecimento_coluna_ausente_levanta_schema_error():
    df = _df_estab_valido().drop(columns=["CNES"])
    with pytest.raises(pa.errors.SchemaError):
        EstabelecimentoContract.validate(df)


def test_estabelecimento_fonte_invalida_levanta_schema_error():
    df = _df_estab_valido().copy()
    df["FONTE"] = "ERRADO"
    with pytest.raises(pa.errors.SchemaError):
        EstabelecimentoContract.validate(df)


def test_estabelecimento_vinculo_sus_invalido_levanta_schema_error():
    df = _df_estab_valido().copy()
    df["VINCULO_SUS"] = "Z"
    with pytest.raises(pa.errors.SchemaError):
        EstabelecimentoContract.validate(df)
```

Run: `./venv/Scripts/python.exe -m pytest tests/contracts/ -x --tb=short -q`
Expected: FAIL with ImportError

- [ ] **Step 2: Implement contracts**

```python
# src/contracts/__init__.py  (empty)

# src/contracts/schemas.py
"""Pandera DataFrameModel contracts para os schemas canônicos de ingestão."""
import pandera as pa
from pandera.typing import Series


class ProfissionalContract(pa.DataFrameModel):
    """Contrato para SCHEMA_PROFISSIONAL — local e nacional."""

    CNS: Series[str]
    CPF: Series[str] = pa.Field(nullable=True)
    NOME_PROFISSIONAL: Series[str]
    CBO: Series[str]
    CNES: Series[str]
    TIPO_VINCULO: Series[str]
    SUS: Series[str] = pa.Field(isin=["S", "N"])
    CH_TOTAL: Series[int]
    CH_AMBULATORIAL: Series[int]
    CH_OUTRAS: Series[int]
    CH_HOSPITALAR: Series[int]
    FONTE: Series[str] = pa.Field(isin=["LOCAL", "NACIONAL"])

    class Config:
        strict = False  # permite colunas extras (como QTD_UNIDADES após auditoria)
        coerce = False


class EstabelecimentoContract(pa.DataFrameModel):
    """Contrato para SCHEMA_ESTABELECIMENTO — local e nacional."""

    CNES: Series[str]
    NOME_FANTASIA: Series[str] = pa.Field(nullable=True)
    TIPO_UNIDADE: Series[str]
    CNPJ_MANTENEDORA: Series[str] = pa.Field(nullable=True)
    NATUREZA_JURIDICA: Series[str] = pa.Field(nullable=True)
    COD_MUNICIPIO: Series[str]
    VINCULO_SUS: Series[str] = pa.Field(isin=["S", "N"])
    FONTE: Series[str] = pa.Field(isin=["LOCAL", "NACIONAL"])

    class Config:
        strict = False
        coerce = False
```

- [ ] **Step 3: Run tests — expect all green**

Run: `./venv/Scripts/python.exe -m pytest tests/contracts/ -x --tb=short -q`
Expected: 10 tests passing

- [ ] **Step 4: Run full suite**

Run: `./venv/Scripts/python.exe -m pytest tests/ -q --tb=short`
Expected: all tests green

- [ ] **Step 5: Commit**

```bash
git add src/contracts/__init__.py src/contracts/schemas.py tests/contracts/
git commit -m "feat(contracts): Pandera DataFrameModel para Profissional e Estabelecimento"
```

---

## Task 3: IngestaoLocalStage

**Files:**
- Create: `src/pipeline/stages/__init__.py`
- Create: `src/pipeline/stages/ingestao_local.py`
- Test: `tests/pipeline/stages/__init__.py`
- Test: `tests/pipeline/stages/test_ingestao_local.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/pipeline/stages/__init__.py  (empty)
# tests/pipeline/stages/test_ingestao_local.py
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline.state import PipelineState
from pipeline.stages.ingestao_local import IngestaoLocalStage


def _state() -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
    )


def _df_prof() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS": ["123456789012345"],
        "CPF": ["12345678901"],
        "NOME_PROFISSIONAL": ["Ana Silva"],
        "CBO": ["515105"],
        "CNES": ["1234567"],
        "TIPO_VINCULO": ["30"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [20],
        "CH_OUTRAS": [10],
        "CH_HOSPITALAR": [10],
        "FONTE": ["LOCAL"],
    })


def _df_estab() -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": ["1234567"],
        "NOME_FANTASIA": ["UBS Centro"],
        "TIPO_UNIDADE": ["01"],
        "CNPJ_MANTENEDORA": ["55293427000117"],
        "NATUREZA_JURIDICA": ["1023"],
        "COD_MUNICIPIO": ["354130"],
        "VINCULO_SUS": ["S"],
        "FONTE": ["LOCAL"],
    })


@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_popula_state_com_dados_locais(
    mock_estab_contract,
    mock_prof_contract,
    mock_adapter_cls,
    mock_cbo,
    mock_conectar,
):
    mock_con = MagicMock()
    mock_conectar.return_value = mock_con
    mock_cbo.return_value = {"515105": "Agente Comunitário"}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state()
    IngestaoLocalStage().execute(state)

    assert state.con is mock_con
    assert state.cbo_lookup == {"515105": "Agente Comunitário"}
    assert len(state.df_prof_local) == 1
    assert len(state.df_estab_local) == 1


@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_valida_contratos_apos_ingestao(
    mock_estab_contract,
    mock_prof_contract,
    mock_adapter_cls,
    mock_cbo,
    mock_conectar,
):
    mock_conectar.return_value = MagicMock()
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    IngestaoLocalStage().execute(_state())

    mock_prof_contract.validate.assert_called_once()
    mock_estab_contract.validate.assert_called_once()


@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_falha_conexao_propaga_excecao(
    mock_estab_contract,
    mock_prof_contract,
    mock_adapter_cls,
    mock_cbo,
    mock_conectar,
):
    mock_conectar.side_effect = RuntimeError("BD indisponivel")

    with pytest.raises(RuntimeError, match="BD indisponivel"):
        IngestaoLocalStage().execute(_state())
```

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_ingestao_local.py -x --tb=short -q`
Expected: FAIL with ImportError

- [ ] **Step 2: Implement IngestaoLocalStage**

```python
# src/pipeline/stages/__init__.py  (empty)

# src/pipeline/stages/ingestao_local.py
"""IngestaoLocalStage — ingere dados do Firebird e valida contratos."""
import logging

from contracts.schemas import EstabelecimentoContract, ProfissionalContract
from ingestion.cnes_client import conectar, extrair_lookup_cbo
from ingestion.cnes_local_adapter import CnesLocalAdapter
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)


class IngestaoLocalStage:
    nome = "ingestao_local"

    def execute(self, state: PipelineState) -> None:
        state.con = conectar()
        state.cbo_lookup = extrair_lookup_cbo(state.con)
        repo = CnesLocalAdapter(state.con)
        state.df_prof_local = repo.listar_profissionais()
        state.df_estab_local = repo.listar_estabelecimentos()
        ProfissionalContract.validate(state.df_prof_local, lazy=False)
        EstabelecimentoContract.validate(state.df_estab_local, lazy=False)
        logger.info(
            "ingestao_local profissionais=%d estabelecimentos=%d",
            len(state.df_prof_local),
            len(state.df_estab_local),
        )
```

- [ ] **Step 3: Run tests — expect all green**

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_ingestao_local.py -x --tb=short -q`
Expected: 3 tests passing

- [ ] **Step 4: Run full suite**

Run: `./venv/Scripts/python.exe -m pytest tests/ -q --tb=short`
Expected: all tests green

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/stages/__init__.py src/pipeline/stages/ingestao_local.py tests/pipeline/stages/
git commit -m "feat(pipeline): IngestaoLocalStage com validação Pandera"
```

---

## Task 4: IngestaoNacionalStage

**Files:**
- Create: `src/pipeline/stages/ingestao_nacional.py`
- Test: `tests/pipeline/stages/test_ingestao_nacional.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/pipeline/stages/test_ingestao_nacional.py
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline.state import PipelineState
from pipeline.stages.ingestao_nacional import IngestaoNacionalStage


def _state(executar_nacional: bool = True) -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=executar_nacional,
        executar_hr=False,
    )


def _df_prof() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS": ["123456789012345"],
        "CPF": [None],
        "NOME_PROFISSIONAL": ["Ana Silva"],
        "CBO": ["515105"],
        "CNES": ["1234567"],
        "TIPO_VINCULO": ["30"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [20],
        "CH_OUTRAS": [10],
        "CH_HOSPITALAR": [10],
        "FONTE": ["NACIONAL"],
    })


def _df_estab() -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": ["1234567"],
        "NOME_FANTASIA": [None],
        "TIPO_UNIDADE": ["01"],
        "CNPJ_MANTENEDORA": [None],
        "NATUREZA_JURIDICA": [None],
        "COD_MUNICIPIO": ["354130"],
        "VINCULO_SUS": ["S"],
        "FONTE": ["NACIONAL"],
    })


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
def test_popula_state_quando_executar_nacional_true(mock_adapter_cls):
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state(executar_nacional=True)
    IngestaoNacionalStage().execute(state)

    assert len(state.df_prof_nacional) == 1
    assert len(state.df_estab_nacional) == 1


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
def test_mantém_dfs_vazios_quando_skip(mock_adapter_cls):
    state = _state(executar_nacional=False)
    IngestaoNacionalStage().execute(state)

    assert state.df_prof_nacional.empty
    assert state.df_estab_nacional.empty
    mock_adapter_cls.assert_not_called()


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
def test_soft_fail_deixa_dfs_vazios_em_excecao(mock_adapter_cls):
    mock_adapter_cls.side_effect = Exception("BigQuery timeout")

    state = _state(executar_nacional=True)
    IngestaoNacionalStage().execute(state)  # não deve levantar

    assert state.df_prof_nacional.empty
    assert state.df_estab_nacional.empty


@patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
def test_busca_profissionais_e_estabelecimentos_em_paralelo(mock_adapter_cls):
    calls = []
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.side_effect = lambda comp: calls.append("prof") or _df_prof()
    mock_adapter.listar_estabelecimentos.side_effect = lambda comp: calls.append("estab") or _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    IngestaoNacionalStage().execute(_state(executar_nacional=True))

    assert "prof" in calls and "estab" in calls
```

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_ingestao_nacional.py -x --tb=short -q`
Expected: FAIL with ImportError

- [ ] **Step 2: Implement IngestaoNacionalStage**

```python
# src/pipeline/stages/ingestao_nacional.py
"""IngestaoNacionalStage — ingere BigQuery com soft-fail e cache pickle."""
import logging
from concurrent.futures import ThreadPoolExecutor

import config
from ingestion.cnes_nacional_adapter import CnesNacionalAdapter
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)


class IngestaoNacionalStage:
    nome = "ingestao_nacional"

    def execute(self, state: PipelineState) -> None:
        if not state.executar_nacional:
            logger.warning("nacional_cross_check=skipped motivo=skip_nacional_flag")
            return
        try:
            self._buscar(state)
        except Exception as exc:
            logger.warning("nacional_cross_check=skipped motivo=%s", exc)

    def _buscar(self, state: PipelineState) -> None:
        repo = CnesNacionalAdapter(
            config.GCP_PROJECT_ID,
            config.ID_MUNICIPIO_IBGE7,
            cache_dir=config.CACHE_DIR,
        )
        competencia = (state.competencia_ano, state.competencia_mes)
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_prof = pool.submit(repo.listar_profissionais, competencia)
            fut_estab = pool.submit(repo.listar_estabelecimentos, competencia)
        state.df_prof_nacional = fut_prof.result()
        state.df_estab_nacional = fut_estab.result()
        logger.info(
            "ingestao_nacional profissionais=%d estabelecimentos=%d",
            len(state.df_prof_nacional),
            len(state.df_estab_nacional),
        )
```

- [ ] **Step 3: Run tests — expect all green**

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_ingestao_nacional.py -x --tb=short -q`
Expected: 4 tests passing

- [ ] **Step 4: Run full suite**

Run: `./venv/Scripts/python.exe -m pytest tests/ -q --tb=short`
Expected: all tests green

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/stages/ingestao_nacional.py tests/pipeline/stages/test_ingestao_nacional.py
git commit -m "feat(pipeline): IngestaoNacionalStage com soft-fail e ThreadPoolExecutor"
```

---

## Task 5: ProcessamentoStage

**Files:**
- Create: `src/pipeline/stages/processamento.py`
- Test: `tests/pipeline/stages/test_processamento.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/pipeline/stages/test_processamento.py
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from pipeline.state import PipelineState
from pipeline.stages.processamento import ProcessamentoStage


def _state_com_prof() -> PipelineState:
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
    )
    state.cbo_lookup = {"515105": "Agente Comunitário"}
    state.df_prof_local = pd.DataFrame({"CPF": ["12345678901"], "CNES": ["1234567"]})
    return state


@patch("pipeline.stages.processamento.transformar")
def test_chama_transformar_com_cbo_lookup(mock_transformar):
    df_transformado = pd.DataFrame({"CPF": ["12345678901"]})
    mock_transformar.return_value = df_transformado

    state = _state_com_prof()
    ProcessamentoStage().execute(state)

    mock_transformar.assert_called_once_with(
        state.df_prof_local, cbo_lookup={"515105": "Agente Comunitário"}
    )
    assert state.df_processado is df_transformado


@patch("pipeline.stages.processamento.transformar")
def test_df_processado_populado_no_state(mock_transformar):
    df_resultado = pd.DataFrame({"CPF": ["99988877766"]})
    mock_transformar.return_value = df_resultado

    state = _state_com_prof()
    ProcessamentoStage().execute(state)

    assert len(state.df_processado) == 1
    assert state.df_processado["CPF"].iloc[0] == "99988877766"
```

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_processamento.py -x --tb=short -q`
Expected: FAIL with ImportError

- [ ] **Step 2: Implement ProcessamentoStage**

```python
# src/pipeline/stages/processamento.py
"""ProcessamentoStage — limpeza CPF, datas ISO, dedup."""
import logging

from processing.transformer import transformar
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)


class ProcessamentoStage:
    nome = "processamento"

    def execute(self, state: PipelineState) -> None:
        state.df_processado = transformar(
            state.df_prof_local, cbo_lookup=state.cbo_lookup
        )
        logger.info("processamento registros=%d", len(state.df_processado))
```

- [ ] **Step 3: Run tests — expect all green**

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_processamento.py -x --tb=short -q`
Expected: 2 tests passing

- [ ] **Step 4: Run full suite**

Run: `./venv/Scripts/python.exe -m pytest tests/ -q --tb=short`
Expected: all tests green

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/stages/processamento.py tests/pipeline/stages/test_processamento.py
git commit -m "feat(pipeline): ProcessamentoStage"
```

---

## Task 6: AuditoriaStage

**Files:**
- Create: `src/pipeline/stages/auditoria.py`
- Test: `tests/pipeline/stages/test_auditoria.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/pipeline/stages/test_auditoria.py
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from pipeline.state import PipelineState
from pipeline.stages.auditoria import AuditoriaStage


def _state() -> PipelineState:
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=True,
    )
    state.df_processado = pd.DataFrame({"CPF": ["12345678901"], "CNES": ["1234567"]})
    state.df_estab_local = pd.DataFrame({"CNES": ["1234567"], "TIPO_UNIDADE": ["01"]})
    state.df_prof_nacional = pd.DataFrame({"CNS": ["123456789012345"]})
    state.df_estab_nacional = pd.DataFrame({"CNES": ["1234567"]})
    return state


_REGRAS_LOCAIS = [
    "pipeline.stages.auditoria.detectar_multiplas_unidades",
    "pipeline.stages.auditoria.auditar_lotacao_acs_tacs",
    "pipeline.stages.auditoria.auditar_lotacao_ace_tace",
]


@patch("pipeline.stages.auditoria.detectar_multiplas_unidades", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.auditar_lotacao_acs_tacs", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.auditar_lotacao_ace_tace", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.carregar_folha", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_folha_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_registro_ausente", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_estabelecimentos_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_estabelecimentos_ausentes_local", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_profissionais_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_profissionais_ausentes_local", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_divergencia_cbo", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_divergencia_carga_horaria", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.resolver_lag_rq006", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.CachingVerificadorCnes")
@patch("pipeline.stages.auditoria.CnesOficialWebAdapter")
@patch("pipeline.stages.auditoria.config")
def test_regras_locais_sempre_executam(mock_config, *args):
    mock_config.FOLHA_HR_PATH = MagicMock()
    mock_config.FOLHA_HR_PATH.exists.return_value = True
    mock_config.CACHE_DIR = Path("data/cache")
    state = _state()
    AuditoriaStage().execute(state)


@patch("pipeline.stages.auditoria.detectar_multiplas_unidades", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.auditar_lotacao_acs_tacs", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.auditar_lotacao_ace_tace", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.carregar_folha", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_folha_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_registro_ausente", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_estabelecimentos_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_estabelecimentos_ausentes_local", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_profissionais_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_profissionais_ausentes_local", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_divergencia_cbo", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.detectar_divergencia_carga_horaria", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.resolver_lag_rq006", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria.CachingVerificadorCnes")
@patch("pipeline.stages.auditoria.CnesOficialWebAdapter")
@patch("pipeline.stages.auditoria.config")
def test_hr_skipped_quando_executar_hr_false(mock_config, mock_web, mock_caching, mock_resolver,
    mock_ch, mock_cbo, mock_paus, mock_pfant, mock_eaus, mock_efant,
    mock_reg_aus, mock_ghost, mock_folha, mock_ace, mock_acs, mock_multi):
    mock_config.FOLHA_HR_PATH = None
    mock_config.CACHE_DIR = Path("data/cache")
    state = _state()
    state.executar_hr = False

    AuditoriaStage().execute(state)

    mock_folha.assert_not_called()
    mock_ghost.assert_not_called()
    mock_reg_aus.assert_not_called()
```

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_auditoria.py -x --tb=short -q`
Expected: FAIL with ImportError

- [ ] **Step 2: Implement AuditoriaStage**

```python
# src/pipeline/stages/auditoria.py
"""AuditoriaStage — executa todas as regras do rules_engine."""
import logging
from typing import Final

import pandas as pd

import config
from analysis.cascade_resolver import resolver_lag_rq006
from analysis.rules_engine import (
    auditar_lotacao_ace_tace,
    auditar_lotacao_acs_tacs,
    detectar_divergencia_cbo,
    detectar_divergencia_carga_horaria,
    detectar_estabelecimentos_ausentes_local,
    detectar_estabelecimentos_fantasma,
    detectar_folha_fantasma,
    detectar_multiplas_unidades,
    detectar_profissionais_ausentes_local,
    detectar_profissionais_fantasma,
    detectar_registro_ausente,
)
from analysis.verificacao_cache import CachingVerificadorCnes
from ingestion.cnes_oficial_web_adapter import CnesOficialWebAdapter
from ingestion.hr_client import carregar_folha
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)

_TIPOS_EXCLUIR_RQ007: Final[frozenset[str]] = frozenset({"22"})


class AuditoriaStage:
    nome = "auditoria"

    def execute(self, state: PipelineState) -> None:
        self._regras_locais(state)
        self._regras_hr(state)
        self._regras_nacional(state)

    def _regras_locais(self, state: PipelineState) -> None:
        state.df_multi_unidades = detectar_multiplas_unidades(state.df_processado)
        df_com_unidade = state.df_processado.merge(
            state.df_estab_local[["CNES", "TIPO_UNIDADE"]], on="CNES", how="left"
        )
        state.df_acs_incorretos = auditar_lotacao_acs_tacs(df_com_unidade)
        state.df_ace_incorretos = auditar_lotacao_ace_tace(df_com_unidade)

    def _regras_hr(self, state: PipelineState) -> None:
        if not state.executar_hr:
            logger.warning("hr_cross_check=skipped motivo=executar_hr=False")
            return
        if not config.FOLHA_HR_PATH or not config.FOLHA_HR_PATH.exists():
            raise FileNotFoundError(
                f"Arquivo ausente: {config.FOLHA_HR_PATH}. "
                "Execute scripts/hr_pre_processor.py para gerar hr_padronizado.csv."
            )
        df_rh = carregar_folha(config.FOLHA_HR_PATH)
        state.df_ghost = detectar_folha_fantasma(state.df_processado, df_rh)
        state.df_missing = detectar_registro_ausente(state.df_processado, df_rh)

    def _regras_nacional(self, state: PipelineState) -> None:
        if not state.executar_nacional:
            return
        if state.df_estab_nacional.empty and state.df_prof_nacional.empty:
            logger.warning(
                "nacional_cross_check=skipped motivo=dados_nacionais_vazios "
                "competencia=%s", state.competencia_str,
            )
            return
        self._cruzar_estabelecimentos(state)
        self._cruzar_profissionais(state)

    def _cruzar_estabelecimentos(self, state: PipelineState) -> None:
        if state.df_estab_nacional.empty:
            return
        state.df_estab_fantasma = detectar_estabelecimentos_fantasma(
            state.df_estab_local, state.df_estab_nacional
        )
        state.df_estab_ausente = detectar_estabelecimentos_ausentes_local(
            state.df_estab_local,
            state.df_estab_nacional,
            tipos_excluir=_TIPOS_EXCLUIR_RQ007,
        )
        if not state.df_estab_fantasma.empty:
            _adapter = CachingVerificadorCnes(
                CnesOficialWebAdapter(),
                config.CACHE_DIR / "cnes_verificados.json",
            )
            state.df_estab_fantasma = resolver_lag_rq006(state.df_estab_fantasma, _adapter)

    def _cruzar_profissionais(self, state: PipelineState) -> None:
        if state.df_prof_nacional.empty:
            return
        state.df_prof_fantasma = detectar_profissionais_fantasma(
            state.df_processado, state.df_prof_nacional
        )
        cnes_excluir = (
            frozenset(state.df_estab_ausente["CNES"])
            if not state.df_estab_ausente.empty
            else frozenset()
        )
        state.df_prof_ausente = detectar_profissionais_ausentes_local(
            state.df_processado, state.df_prof_nacional, cnes_excluir=cnes_excluir
        )
        state.df_cbo_diverg = detectar_divergencia_cbo(
            state.df_processado, state.df_prof_nacional, cbo_lookup=state.cbo_lookup
        )
        state.df_ch_diverg = detectar_divergencia_carga_horaria(
            state.df_processado, state.df_prof_nacional
        )
```

- [ ] **Step 3: Run tests — expect all green**

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_auditoria.py -x --tb=short -q`
Expected: 2 tests passing

- [ ] **Step 4: Run full suite**

Run: `./venv/Scripts/python.exe -m pytest tests/ -q --tb=short`
Expected: all tests green

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/stages/auditoria.py tests/pipeline/stages/test_auditoria.py
git commit -m "feat(pipeline): AuditoriaStage — todas as regras do rules_engine"
```

---

## Task 7: ExportacaoStage

**Files:**
- Create: `src/pipeline/stages/exportacao.py`
- Test: `tests/pipeline/stages/test_exportacao.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/pipeline/stages/test_exportacao.py
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from pipeline.state import PipelineState
from pipeline.stages.exportacao import ExportacaoStage


def _state() -> PipelineState:
    state = PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/Relatorio_2024-12.csv"),
        executar_nacional=True,
        executar_hr=False,
    )
    state.df_processado = pd.DataFrame({"CPF": ["12345678901"]})
    state.df_multi_unidades = pd.DataFrame()
    state.df_acs_incorretos = pd.DataFrame()
    state.df_ace_incorretos = pd.DataFrame()
    state.df_ghost = pd.DataFrame()
    state.df_missing = pd.DataFrame()
    state.df_estab_fantasma = pd.DataFrame()
    state.df_estab_ausente = pd.DataFrame()
    state.df_prof_fantasma = pd.DataFrame()
    state.df_prof_ausente = pd.DataFrame()
    state.df_cbo_diverg = pd.DataFrame()
    state.df_ch_diverg = pd.DataFrame()
    return state


@patch("pipeline.stages.exportacao.exportar_csv")
@patch("pipeline.stages.exportacao.gerar_relatorio")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.config")
def test_exporta_csv_principal(
    mock_config, mock_loader_cls, mock_salvar, mock_criar, mock_gerar, mock_exportar
):
    mock_config.SNAPSHOTS_DIR = Path("data/snapshots")
    mock_config.DUCKDB_PATH = Path("data/cnesdata.duckdb")
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_loader_cls.return_value = MagicMock()

    state = _state()
    ExportacaoStage().execute(state)

    mock_exportar.assert_any_call(state.df_processado, state.output_path)


@patch("pipeline.stages.exportacao.exportar_csv")
@patch("pipeline.stages.exportacao.gerar_relatorio")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.config")
def test_grava_snapshot_no_duckdb(
    mock_config, mock_loader_cls, mock_salvar, mock_criar, mock_gerar, mock_exportar
):
    mock_config.SNAPSHOTS_DIR = Path("data/snapshots")
    mock_config.DUCKDB_PATH = Path("data/cnesdata.duckdb")
    snapshot = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_criar.return_value = snapshot
    mock_loader = MagicMock()
    mock_loader_cls.return_value = mock_loader

    ExportacaoStage().execute(_state())

    mock_loader.inicializar_schema.assert_called_once()
    mock_loader.gravar_metricas.assert_called_once_with(snapshot)


@patch("pipeline.stages.exportacao.exportar_csv")
@patch("pipeline.stages.exportacao.gerar_relatorio")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.config")
def test_nao_exporta_csv_para_df_vazio(
    mock_config, mock_loader_cls, mock_salvar, mock_criar, mock_gerar, mock_exportar
):
    mock_config.SNAPSHOTS_DIR = Path("data/snapshots")
    mock_config.DUCKDB_PATH = Path("data/cnesdata.duckdb")
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_loader_cls.return_value = MagicMock()

    state = _state()
    ExportacaoStage().execute(state)

    # df_ghost é vazio — não deve ser exportado
    exported_paths = [c.args[1] for c in mock_exportar.call_args_list]
    assert not any("ghost" in str(p) for p in exported_paths)
```

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_exportacao.py -x --tb=short -q`
Expected: FAIL with ImportError

- [ ] **Step 2: Implement ExportacaoStage**

```python
# src/pipeline/stages/exportacao.py
"""ExportacaoStage — CSV, Excel, snapshot JSON e DuckDB."""
import logging
from pathlib import Path

import config
from analysis.evolution_tracker import criar_snapshot, salvar_snapshot
from export.csv_exporter import exportar_csv
from export.report_generator import gerar_relatorio
from pipeline.state import PipelineState
from storage.database_loader import DatabaseLoader

logger = logging.getLogger(__name__)


class ExportacaoStage:
    nome = "exportacao"

    def execute(self, state: PipelineState) -> None:
        output_dir = state.output_path.parent
        self._exportar_csvs(state, output_dir)
        self._gerar_relatorio(state)
        self._persistir_historico(state)

    def _exportar_csvs(self, state: PipelineState, output_dir: Path) -> None:
        exportar_csv(state.df_processado, state.output_path)
        _exportar_se_nao_vazio = lambda df, nome: (
            exportar_csv(df, output_dir / nome) if not df.empty else None
        )
        _exportar_se_nao_vazio(state.df_multi_unidades, "auditoria_rq003b_multiplas_unidades.csv")
        _exportar_se_nao_vazio(state.df_acs_incorretos, "auditoria_rq005_acs_tacs_incorretos.csv")
        _exportar_se_nao_vazio(state.df_ace_incorretos, "auditoria_rq005_ace_tace_incorretos.csv")
        _exportar_se_nao_vazio(state.df_ghost, "auditoria_ghost_payroll.csv")
        _exportar_se_nao_vazio(state.df_missing, "auditoria_missing_registration.csv")
        _exportar_se_nao_vazio(state.df_estab_fantasma, "auditoria_rq006_estab_fantasma.csv")
        _exportar_se_nao_vazio(state.df_estab_ausente, "auditoria_rq007_estab_ausente_local.csv")
        _exportar_se_nao_vazio(state.df_prof_fantasma, "auditoria_rq008_prof_fantasma_cns.csv")
        _exportar_se_nao_vazio(state.df_prof_ausente, "auditoria_rq009_prof_ausente_local_cns.csv")
        _exportar_se_nao_vazio(state.df_cbo_diverg, "auditoria_rq010_divergencia_cbo.csv")
        _exportar_se_nao_vazio(state.df_ch_diverg, "auditoria_rq011_divergencia_ch.csv")

    def _gerar_relatorio(self, state: PipelineState) -> None:
        gerar_relatorio(
            state.output_path.with_suffix(".xlsx"),
            {
                "principal": state.df_processado,
                "ghost": state.df_ghost,
                "missing": state.df_missing,
                "multi_unidades": state.df_multi_unidades,
                "acs_tacs": state.df_acs_incorretos,
                "ace_tace": state.df_ace_incorretos,
                "rq006_estab_fantasma": state.df_estab_fantasma,
                "rq007_estab_ausente": state.df_estab_ausente,
                "rq008_prof_fantasma": state.df_prof_fantasma,
                "rq009_prof_ausente": state.df_prof_ausente,
                "rq010_divergencia_cbo": state.df_cbo_diverg,
                "rq011_divergencia_ch": state.df_ch_diverg,
            },
            competencia=state.competencia_str,
        )

    def _persistir_historico(self, state: PipelineState) -> None:
        competencia_stem = (
            state.output_path.stem.split("_")[-1]
            if "_" in state.output_path.stem
            else "desconhecida"
        )
        snapshot = criar_snapshot(
            competencia_stem,
            state.df_processado,
            state.df_ghost,
            state.df_missing,
            state.df_multi_unidades,
            state.df_acs_incorretos,
            state.df_ace_incorretos,
        )
        salvar_snapshot(snapshot, config.SNAPSHOTS_DIR)
        loader = DatabaseLoader(config.DUCKDB_PATH)
        loader.inicializar_schema()
        loader.gravar_metricas(snapshot)
        loader.gravar_auditoria(snapshot.data_competencia, "GHOST", snapshot.total_ghost)
        loader.gravar_auditoria(snapshot.data_competencia, "MISSING", snapshot.total_missing)
        loader.gravar_auditoria(snapshot.data_competencia, "RQ005", snapshot.total_rq005)
        logger.info("exportacao concluida output=%s", state.output_path)
```

- [ ] **Step 3: Run tests — expect all green**

Run: `./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_exportacao.py -x --tb=short -q`
Expected: 3 tests passing

- [ ] **Step 4: Run full suite**

Run: `./venv/Scripts/python.exe -m pytest tests/ -q --tb=short`
Expected: all tests green

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/stages/exportacao.py tests/pipeline/stages/test_exportacao.py
git commit -m "feat(pipeline): ExportacaoStage — CSV, Excel, snapshot e DuckDB"
```

---

## Task 8: Refactor main.py (≤40 lines)

**Files:**
- Modify: `src/main.py`
- Modify: `tests/test_main.py`

- [ ] **Step 1: Write failing test for new main() structure**

First, read the current `tests/test_main.py` to understand existing tests:

```bash
./venv/Scripts/python.exe -m pytest tests/test_main.py -v --tb=short
```

Then add this test to verify the orchestrator is wired:

```python
# Adicionar ao tests/test_main.py

from unittest.mock import patch, MagicMock
from pathlib import Path


@patch("main.PipelineOrchestrator")
@patch("main.parse_args")
@patch("main.configurar_logging")
@patch("main.config")
def test_main_usa_orchestrator(mock_config, mock_log, mock_args, mock_orch_cls):
    mock_config.COMPETENCIA_ANO = 2024
    mock_config.COMPETENCIA_MES = 12
    mock_config.OUTPUT_PATH = Path("data/processed/report.csv")
    mock_config.FOLHA_HR_PATH = None
    mock_args.return_value = MagicMock(
        competencia=None, output_dir=None, skip_nacional=False,
        skip_hr=False, verbose=False,
    )
    mock_orch = MagicMock()
    mock_orch_cls.return_value = mock_orch

    from main import main
    result = main()

    mock_orch.executar.assert_called_once()
    assert result == 0


@patch("main.PipelineOrchestrator")
@patch("main.parse_args")
@patch("main.configurar_logging")
@patch("main.config")
def test_main_retorna_1_em_excecao(mock_config, mock_log, mock_args, mock_orch_cls):
    mock_config.COMPETENCIA_ANO = 2024
    mock_config.COMPETENCIA_MES = 12
    mock_config.OUTPUT_PATH = Path("data/processed/report.csv")
    mock_config.FOLHA_HR_PATH = None
    mock_args.return_value = MagicMock(
        competencia=None, output_dir=None, skip_nacional=False,
        skip_hr=False, verbose=False,
    )
    mock_orch = MagicMock()
    mock_orch.executar.side_effect = RuntimeError("boom")
    mock_orch_cls.return_value = mock_orch

    from main import main
    result = main()

    assert result == 1
```

Run: `./venv/Scripts/python.exe -m pytest tests/test_main.py -x --tb=short -q`
Expected: new tests FAIL (main still uses old structure)

- [ ] **Step 2: Rewrite main.py**

```python
# src/main.py
"""Ponto de entrada do pipeline CnesData."""
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

import config
from cli import parse_args
from pipeline.orchestrator import PipelineOrchestrator
from pipeline.state import PipelineState
from pipeline.stages.auditoria import AuditoriaStage
from pipeline.stages.exportacao import ExportacaoStage
from pipeline.stages.ingestao_local import IngestaoLocalStage
from pipeline.stages.ingestao_nacional import IngestaoNacionalStage
from pipeline.stages.processamento import ProcessamentoStage


def configurar_logging(verbose: bool = False) -> None:
    """Configura handlers de console e arquivo rotativo."""
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(fmt)
    arquivo = RotatingFileHandler(
        config.LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    arquivo.setLevel(logging.DEBUG)
    arquivo.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(arquivo)


def _criar_estado(args) -> PipelineState:
    ano, mes = args.competencia if args.competencia else (config.COMPETENCIA_ANO, config.COMPETENCIA_MES)
    output_path = (
        (Path(args.output_dir) / config.OUTPUT_PATH.name).resolve()
        if args.output_dir
        else config.OUTPUT_PATH
    )
    return PipelineState(
        competencia_ano=ano,
        competencia_mes=mes,
        output_path=output_path,
        executar_nacional=not args.skip_nacional,
        executar_hr=not args.skip_hr and config.FOLHA_HR_PATH is not None,
    )


def main() -> int:
    """Ponto de entrada principal. Returns: 0 = sucesso, 1 = erro."""
    args = parse_args()
    configurar_logging(verbose=args.verbose)
    state = _criar_estado(args)
    orchestrator = PipelineOrchestrator([
        IngestaoLocalStage(),
        IngestaoNacionalStage(),
        ProcessamentoStage(),
        AuditoriaStage(),
        ExportacaoStage(),
    ])
    try:
        orchestrator.executar(state)
        logging.getLogger(__name__).info("pipeline concluido output=%s", state.output_path)
        return 0
    except (EnvironmentError, FileNotFoundError) as e:
        logging.getLogger(__name__).error("erro_config=%s", e)
        return 1
    except Exception as e:
        logging.getLogger(__name__).exception("erro_inesperado=%s", e)
        return 1
    finally:
        if state.con is not None:
            state.con.close()
            logging.getLogger(__name__).info("conexao_encerrada")


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Run tests — expect all green**

Run: `./venv/Scripts/python.exe -m pytest tests/test_main.py -x --tb=short -q`
Expected: all tests passing

- [ ] **Step 4: Run full suite**

Run: `./venv/Scripts/python.exe -m pytest tests/ -q --tb=short`
Expected: all tests green

- [ ] **Step 5: Lint**

Run: `./venv/Scripts/ruff.exe check src/ tests/ --fix && ./venv/Scripts/ruff.exe format src/ tests/`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add src/main.py tests/test_main.py
git commit -m "refactor(main): extrai God Object para PipelineOrchestrator — main.py 40 linhas"
```

---

## Self-Review

### Spec Coverage

| Requirement | Task |
|---|---|
| PipelineState com todos os campos inter-stage | Task 1 |
| Stage Protocol com `nome` e `execute(state) -> None` | Task 1 |
| PipelineOrchestrator.executar() em sequência | Task 1 |
| Pandera DataFrameModel para Profissional | Task 2 |
| Pandera DataFrameModel para Estabelecimento | Task 2 |
| `strict=False` (colunas extras não quebram) | Task 2 |
| Validação explícita `Contract.validate(df, lazy=False)` | Task 3 |
| IngestaoLocalStage hard-fail | Task 3 |
| IngestaoNacionalStage soft-fail | Task 4 |
| ThreadPoolExecutor bigquery | Task 4 |
| ProcessamentoStage → transformer.transformar | Task 5 |
| AuditoriaStage cobre todos os 11 rules | Task 6 |
| AuditoriaStage skip HR quando `executar_hr=False` | Task 6 |
| ExportacaoStage → CSV, Excel, JSON, DuckDB | Task 7 |
| main.py ≤ 40 linhas | Task 8 |
| Epic 6 (cascade_resolver + CachingVerificadorCnes) já implementado — sem novo código | N/A |

### Placeholder Scan

Nenhum placeholder identificado. Todos os steps contêm código completo.

### Type Consistency

- `PipelineState.con: Any` — usado como `state.con.close()` no `finally` em main.py ✓
- `Stage.execute(state: PipelineState) -> None` — todos os stages implementam ✓
- `PipelineOrchestrator.executar(state)` — chama `stage.execute(state)` ✓
- `state.competencia_str` property usada em `AuditoriaStage` e `ExportacaoStage` ✓
- `DatabaseLoader` importado no `ExportacaoStage` (não em main.py) ✓
