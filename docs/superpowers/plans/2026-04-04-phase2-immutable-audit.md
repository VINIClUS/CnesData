# Phase 2 — Immutable Audit Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace live Firebird queries on re-runs with immutable per-competency snapshots, add skip logic, compute drift between runs, and expose drift metrics in the dashboard.

**Architecture:** On the first pipeline run for a competency, local Firebird data (post-processing) is saved as parquet snapshots under `historico/YYYY-MM/`. Subsequent runs for the same competency load from snapshot and skip Firebird entirely. Running with `--force-reingestao` bypasses the snapshot, queries Firebird, computes a delta (diff between current and stored data), and overwrites the snapshot. Delta KPIs are persisted in `gold.delta_local_snapshot` (DuckDB) and exposed in a new dashboard page.

**Tech Stack:** pandas, pyarrow (parquet), DuckDB, Streamlit, dataclasses

---

## File Map

| Action | File |
|--------|------|
| Create | `src/storage/snapshot_local.py` |
| Create | `src/analysis/delta_snapshot.py` |
| Create | `src/pipeline/stages/snapshot_local.py` |
| Create | `scripts/pages/6_Delta.py` |
| Modify | `src/pipeline/stages/ingestao_local.py` |
| Modify | `src/pipeline/state.py` |
| Modify | `src/cli.py` |
| Modify | `src/main.py` |
| Modify | `src/storage/database_loader.py` |
| Modify | `src/storage/historico_reader.py` |
| Modify | `src/pipeline/stages/metricas.py` |
| Create | `tests/storage/test_snapshot_local.py` |
| Create | `tests/analysis/test_delta_snapshot.py` |
| Create | `tests/pipeline/stages/test_snapshot_local_stage.py` |
| Modify | `tests/pipeline/stages/test_ingestao_local.py` |
| Modify | `tests/storage/test_database_loader.py` |

---

### Task 1: `src/storage/snapshot_local.py` — Snapshot CRUD

**Files:**
- Create: `src/storage/snapshot_local.py`
- Create: `tests/storage/test_snapshot_local.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/storage/test_snapshot_local.py`:

```python
"""Testes de snapshot_local — persistência de snapshots parquet por competência."""
import json
from pathlib import Path

import pandas as pd
import pytest

from storage.snapshot_local import (
    SnapshotLocal,
    carregar_snapshot,
    salvar_snapshot,
    snapshot_existe,
)


def _df_prof() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS": ["123456789012345"],
        "CPF": ["12345678901"],
        "NOME_PROFISSIONAL": ["Ana Silva"],
        "SEXO": ["F"],
        "CBO": ["515105"],
        "CNES": ["2795001"],
        "TIPO_VINCULO": ["30"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [20],
        "CH_OUTRAS": [10],
        "CH_HOSPITALAR": [10],
        "FONTE": ["LOCAL"],
        "ALERTA_STATUS_CH": ["OK"],
        "DESCRICAO_CBO": ["Agente Comunitário"],
    })


def _df_estab() -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": ["2795001"],
        "NOME_FANTASIA": ["UBS Centro"],
        "TIPO_UNIDADE": ["01"],
        "CNPJ_MANTENEDORA": ["55293427000117"],
        "NATUREZA_JURIDICA": ["1023"],
        "COD_MUNICIPIO": ["354130"],
        "VINCULO_SUS": ["S"],
        "FONTE": ["LOCAL"],
    })


class TestSnapshotExiste:
    def test_falso_quando_sem_arquivos(self, tmp_path):
        assert not snapshot_existe("2026-03", tmp_path)

    def test_verdadeiro_apos_salvar(self, tmp_path):
        snap = SnapshotLocal(df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={})
        salvar_snapshot("2026-03", tmp_path, snap)
        assert snapshot_existe("2026-03", tmp_path)

    def test_falso_para_competencia_diferente(self, tmp_path):
        snap = SnapshotLocal(df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={})
        salvar_snapshot("2026-03", tmp_path, snap)
        assert not snapshot_existe("2026-04", tmp_path)


class TestSalvarCarregarSnapshot:
    def test_roundtrip_df_prof(self, tmp_path):
        original = _df_prof()
        salvar_snapshot("2026-03", tmp_path, SnapshotLocal(original, _df_estab(), {}))
        loaded = carregar_snapshot("2026-03", tmp_path)
        pd.testing.assert_frame_equal(loaded.df_prof.reset_index(drop=True), original.reset_index(drop=True))

    def test_roundtrip_df_estab(self, tmp_path):
        original = _df_estab()
        salvar_snapshot("2026-03", tmp_path, SnapshotLocal(_df_prof(), original, {}))
        loaded = carregar_snapshot("2026-03", tmp_path)
        pd.testing.assert_frame_equal(loaded.df_estab.reset_index(drop=True), original.reset_index(drop=True))

    def test_roundtrip_cbo_lookup(self, tmp_path):
        lookup = {"515105": "Agente Comunitário", "225125": "Médico"}
        salvar_snapshot("2026-03", tmp_path, SnapshotLocal(_df_prof(), _df_estab(), lookup))
        loaded = carregar_snapshot("2026-03", tmp_path)
        assert loaded.cbo_lookup == lookup

    def test_cria_diretorio_competencia_se_ausente(self, tmp_path):
        snap = SnapshotLocal(df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={})
        salvar_snapshot("2026-03", tmp_path, snap)
        assert (tmp_path / "2026-03" / "snapshot_local_prof.parquet").exists()
        assert (tmp_path / "2026-03" / "snapshot_local_estab.parquet").exists()
        assert (tmp_path / "2026-03" / "snapshot_cbo_lookup.json").exists()

    def test_sobrescreve_snapshot_existente(self, tmp_path):
        snap_v1 = SnapshotLocal(_df_prof(), _df_estab(), {"515105": "v1"})
        salvar_snapshot("2026-03", tmp_path, snap_v1)
        snap_v2 = SnapshotLocal(_df_prof(), _df_estab(), {"515105": "v2"})
        salvar_snapshot("2026-03", tmp_path, snap_v2)
        loaded = carregar_snapshot("2026-03", tmp_path)
        assert loaded.cbo_lookup["515105"] == "v2"
```

- [ ] **Step 2: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_snapshot_local.py -v
```
Expected: `ModuleNotFoundError: No module named 'storage.snapshot_local'`

- [ ] **Step 3: Implement `src/storage/snapshot_local.py`**

```python
"""snapshot_local.py — Persistência parquet de snapshots locais por competência."""
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class SnapshotLocal:
    """Snapshot imutável de profissionais e estabelecimentos de uma competência."""

    df_prof: pd.DataFrame
    df_estab: pd.DataFrame
    cbo_lookup: dict[str, str]


def snapshot_existe(competencia: str, historico_dir: Path) -> bool:
    """Retorna True se o snapshot de profissionais existir para a competência.

    Args:
        competencia: Competência no formato YYYY-MM.
        historico_dir: Diretório raiz do histórico (config.HISTORICO_DIR).

    Returns:
        True quando o arquivo parquet de profissionais existir.
    """
    return (historico_dir / competencia / "snapshot_local_prof.parquet").exists()


def salvar_snapshot(
    competencia: str, historico_dir: Path, snapshot: SnapshotLocal
) -> None:
    """Persiste profissionais, estabelecimentos e cbo_lookup como arquivos locais.

    Args:
        competencia: Competência no formato YYYY-MM.
        historico_dir: Diretório raiz do histórico (config.HISTORICO_DIR).
        snapshot: SnapshotLocal a persistir.
    """
    pasta = historico_dir / competencia
    pasta.mkdir(parents=True, exist_ok=True)
    snapshot.df_prof.to_parquet(pasta / "snapshot_local_prof.parquet", index=False)
    snapshot.df_estab.to_parquet(pasta / "snapshot_local_estab.parquet", index=False)
    (pasta / "snapshot_cbo_lookup.json").write_text(
        json.dumps(snapshot.cbo_lookup, ensure_ascii=False), encoding="utf-8"
    )


def carregar_snapshot(competencia: str, historico_dir: Path) -> SnapshotLocal:
    """Lê snapshot de profissionais, estabelecimentos e cbo_lookup do disco.

    Args:
        competencia: Competência no formato YYYY-MM.
        historico_dir: Diretório raiz do histórico (config.HISTORICO_DIR).

    Returns:
        SnapshotLocal carregado do disco.

    Raises:
        FileNotFoundError: Se algum arquivo do snapshot não existir.
    """
    pasta = historico_dir / competencia
    df_prof = pd.read_parquet(pasta / "snapshot_local_prof.parquet")
    df_estab = pd.read_parquet(pasta / "snapshot_local_estab.parquet")
    cbo_lookup: dict[str, str] = json.loads(
        (pasta / "snapshot_cbo_lookup.json").read_text(encoding="utf-8")
    )
    return SnapshotLocal(df_prof=df_prof, df_estab=df_estab, cbo_lookup=cbo_lookup)
```

- [ ] **Step 4: Run tests to verify PASS**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_snapshot_local.py -v
```
Expected: all 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/storage/snapshot_local.py tests/storage/test_snapshot_local.py
git commit -m "feat(storage): SnapshotLocal — persistencia parquet de snapshots locais"
```

---

### Task 2: `src/analysis/delta_snapshot.py` — Delta entre runs

**Files:**
- Create: `src/analysis/delta_snapshot.py`
- Create: `tests/analysis/test_delta_snapshot.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/analysis/test_delta_snapshot.py`:

```python
"""Testes de calcular_delta — drift entre df_processado atual e snapshot anterior."""
import pandas as pd
import pytest

from analysis.delta_snapshot import DeltaSnapshot, calcular_delta


def _base() -> pd.DataFrame:
    """DataFrame base com 3 profissionais."""
    return pd.DataFrame({
        "CPF":         ["00000000001", "00000000002", "00000000003"],
        "CNES":        ["1111111",     "1111111",     "2222222"],
        "CBO":         ["515105",      "225125",      "515105"],
        "CH_TOTAL":    [40,            20,            30],
        "TIPO_VINCULO":["30",          "30",          "40"],
    })


class TestCalcularDeltaSemMudancas:
    def test_zero_novos_removidos_alterados(self):
        df = _base()
        delta = calcular_delta(df, df.copy())
        assert delta.n_novos == 0
        assert delta.n_removidos == 0
        assert delta.n_alterados == 0
        assert delta.novos == []
        assert delta.removidos == []
        assert delta.alterados == []


class TestNovosVinculos:
    def test_detecta_novo_profissional(self):
        anterior = _base().iloc[:2].copy()
        atual = _base()
        delta = calcular_delta(atual, anterior)
        assert delta.n_novos == 1
        assert delta.novos[0]["CPF"] == "00000000003"

    def test_novo_estabelecimento_mesmo_cpf(self):
        anterior = _base().copy()
        novo = pd.DataFrame({
            "CPF": ["00000000001"], "CNES": ["9999999"],
            "CBO": ["515105"], "CH_TOTAL": [20], "TIPO_VINCULO": ["30"],
        })
        atual = pd.concat([_base(), novo], ignore_index=True)
        delta = calcular_delta(atual, anterior)
        assert delta.n_novos == 1
        assert delta.novos[0]["CPF"] == "00000000001"
        assert delta.novos[0]["CNES"] == "9999999"


class TestRemovidosVinculos:
    def test_detecta_profissional_removido(self):
        atual = _base().iloc[:2].copy()
        anterior = _base()
        delta = calcular_delta(atual, anterior)
        assert delta.n_removidos == 1
        assert delta.removidos[0]["CPF"] == "00000000003"


class TestAlteracoes:
    def test_detecta_mudanca_cbo(self):
        anterior = _base()
        atual = _base().copy()
        atual.loc[0, "CBO"] = "225125"
        delta = calcular_delta(atual, anterior)
        assert delta.n_alterados == 1
        assert delta.alterados[0]["CBO_anterior"] == "515105"
        assert delta.alterados[0]["CBO_atual"] == "225125"

    def test_detecta_mudanca_ch_total(self):
        anterior = _base()
        atual = _base().copy()
        atual.loc[1, "CH_TOTAL"] = 40
        delta = calcular_delta(atual, anterior)
        assert delta.n_alterados == 1
        assert delta.alterados[0]["CH_TOTAL_anterior"] == 20
        assert delta.alterados[0]["CH_TOTAL_atual"] == 40

    def test_nao_conta_como_alterado_se_igual(self):
        df = _base()
        delta = calcular_delta(df, df.copy())
        assert delta.n_alterados == 0

    def test_colunas_ausentes_ignoradas(self):
        atual = _base().drop(columns=["TIPO_VINCULO"])
        anterior = _base().drop(columns=["TIPO_VINCULO"])
        anterior.loc[0, "CBO"] = "999999"
        delta = calcular_delta(atual, anterior)
        assert delta.n_alterados == 1
```

- [ ] **Step 2: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/analysis/test_delta_snapshot.py -v
```
Expected: `ModuleNotFoundError: No module named 'analysis.delta_snapshot'`

- [ ] **Step 3: Implement `src/analysis/delta_snapshot.py`**

```python
"""delta_snapshot.py — Drift entre df_processado atual e snapshot anterior."""
from dataclasses import dataclass, field

import pandas as pd

_CHAVE = ("CPF", "CNES")
_ATRIBUTOS = ("CBO", "CH_TOTAL", "TIPO_VINCULO")


@dataclass
class DeltaSnapshot:
    """Diferença entre dois DataFrames de profissionais indexados por CPF+CNES."""

    n_novos: int
    n_removidos: int
    n_alterados: int
    novos: list[dict] = field(default_factory=list)
    removidos: list[dict] = field(default_factory=list)
    alterados: list[dict] = field(default_factory=list)


def calcular_delta(df_atual: pd.DataFrame, df_anterior: pd.DataFrame) -> DeltaSnapshot:
    """Compara profissionais atuais com snapshot anterior por CPF+CNES.

    Args:
        df_atual: DataFrame processado da rodada atual (Firebird).
        df_anterior: DataFrame processado do snapshot salvo.

    Returns:
        DeltaSnapshot com contagens e detalhes dos registros modificados.
    """
    chave = list(_CHAVE)
    atributos = [c for c in _ATRIBUTOS if c in df_atual.columns and c in df_anterior.columns]

    mapa_atual = _para_mapa(df_atual, chave, atributos)
    mapa_anterior = _para_mapa(df_anterior, chave, atributos)

    keys_atual = set(mapa_atual)
    keys_anterior = set(mapa_anterior)

    novos_keys = keys_atual - keys_anterior
    removidos_keys = keys_anterior - keys_atual
    comuns_keys = keys_atual & keys_anterior
    alterados_keys = {k for k in comuns_keys if mapa_atual[k] != mapa_anterior[k]}

    return DeltaSnapshot(
        n_novos=len(novos_keys),
        n_removidos=len(removidos_keys),
        n_alterados=len(alterados_keys),
        novos=_registros_simples(mapa_atual, novos_keys, atributos),
        removidos=_registros_simples(mapa_anterior, removidos_keys, atributos),
        alterados=_registros_diff(mapa_atual, mapa_anterior, alterados_keys, atributos),
    )


def _para_mapa(
    df: pd.DataFrame, chave: list[str], atributos: list[str]
) -> dict[tuple, dict]:
    cols = chave + atributos
    dedup = df[cols].drop_duplicates(subset=chave)
    result: dict[tuple, dict] = {}
    for _, row in dedup.iterrows():
        key = tuple(str(row[c]) for c in chave)
        result[key] = {a: row[a] for a in atributos}
    return result


def _registros_simples(
    mapa: dict[tuple, dict], keys: set[tuple], atributos: list[str]
) -> list[dict]:
    result = []
    for key in keys:
        entry = {"CPF": key[0], "CNES": key[1]}
        entry.update(mapa[key])
        result.append(entry)
    return result


def _registros_diff(
    mapa_atual: dict[tuple, dict],
    mapa_anterior: dict[tuple, dict],
    keys: set[tuple],
    atributos: list[str],
) -> list[dict]:
    result = []
    for key in keys:
        entry: dict = {"CPF": key[0], "CNES": key[1]}
        for attr in atributos:
            entry[f"{attr}_anterior"] = mapa_anterior[key][attr]
            entry[f"{attr}_atual"] = mapa_atual[key][attr]
        result.append(entry)
    return result
```

- [ ] **Step 4: Run tests to verify PASS**

```
./venv/Scripts/python.exe -m pytest tests/analysis/test_delta_snapshot.py -v
```
Expected: all 10 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/analysis/delta_snapshot.py tests/analysis/test_delta_snapshot.py
git commit -m "feat(analysis): delta_snapshot — drift entre runs do pipeline por CPF+CNES"
```

---

### Task 3: Pipeline integration — skip logic e snapshot stage

**Context:** This task modifies 5 files and creates 1 new stage. Read each file before editing. The snapshot is saved **after** `ProcessamentoStage` (because it stores `df_processado` + the resolved `cbo_lookup`). The skip check happens in `IngestaoLocalStage` — if a snapshot exists and `force_reingestao=False`, it loads from parquet and sets `state.snapshot_carregado = True`, bypassing Firebird entirely.

**Files:**
- Modify: `src/pipeline/state.py`
- Modify: `src/cli.py`
- Modify: `src/main.py`
- Modify: `src/pipeline/stages/ingestao_local.py`
- Create: `src/pipeline/stages/snapshot_local.py`
- Create: `tests/pipeline/stages/test_snapshot_local_stage.py`
- Modify: `tests/pipeline/stages/test_ingestao_local.py`

- [ ] **Step 1: Write failing tests for `IngestaoLocalStage` skip behaviour**

Add to `tests/pipeline/stages/test_ingestao_local.py`:

```python
from unittest.mock import patch
from storage.snapshot_local import SnapshotLocal, salvar_snapshot


def _state_com_force(force: bool = False) -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
        force_reingestao=force,
    )


@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_usa_snapshot_quando_existe_e_sem_force(
    mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, tmp_path
):
    snap = SnapshotLocal(df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={"515105": "ACS"})
    salvar_snapshot("2024-12", tmp_path, snap)

    state = _state_com_force(force=False)
    IngestaoLocalStage(tmp_path).execute(state)

    mock_conectar.assert_not_called()
    assert state.snapshot_carregado is True
    assert len(state.df_prof_local) == 1
    assert state.cbo_lookup == {"515105": "ACS"}


@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_usa_firebird_quando_force_reingestao(
    mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, tmp_path
):
    snap = SnapshotLocal(df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={})
    salvar_snapshot("2024-12", tmp_path, snap)

    mock_con = MagicMock()
    mock_conectar.return_value = mock_con
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state_com_force(force=True)
    IngestaoLocalStage(tmp_path).execute(state)

    mock_conectar.assert_called_once()
    assert state.snapshot_carregado is False


@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_usa_firebird_quando_sem_snapshot(
    mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, tmp_path
):
    mock_con = MagicMock()
    mock_conectar.return_value = mock_con
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state_com_force(force=False)
    IngestaoLocalStage(tmp_path).execute(state)

    mock_conectar.assert_called_once()
    assert state.snapshot_carregado is False
```

- [ ] **Step 2: Write failing tests for `SnapshotLocalStage`**

Create `tests/pipeline/stages/test_snapshot_local_stage.py`:

```python
"""Testes do SnapshotLocalStage — persistência de snapshot pós-processamento."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipeline.state import PipelineState
from pipeline.stages.snapshot_local import SnapshotLocalStage
from storage.snapshot_local import SnapshotLocal, salvar_snapshot, snapshot_existe


def _df_processado() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS": ["123456789012345"],
        "CPF": ["12345678901"],
        "NOME_PROFISSIONAL": ["Ana Silva"],
        "SEXO": ["F"],
        "CBO": ["515105"],
        "CNES": ["2795001"],
        "TIPO_VINCULO": ["30"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [20],
        "CH_OUTRAS": [10],
        "CH_HOSPITALAR": [10],
        "FONTE": ["LOCAL"],
        "ALERTA_STATUS_CH": ["OK"],
        "DESCRICAO_CBO": ["Agente Comunitário"],
    })


def _df_estab() -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": ["2795001"],
        "NOME_FANTASIA": ["UBS Centro"],
        "TIPO_UNIDADE": ["01"],
        "CNPJ_MANTENEDORA": ["55293427000117"],
        "NATUREZA_JURIDICA": ["1023"],
        "COD_MUNICIPIO": ["354130"],
        "VINCULO_SUS": ["S"],
        "FONTE": ["LOCAL"],
    })


def _state(snapshot_carregado: bool = False, force: bool = False) -> PipelineState:
    s = PipelineState(
        competencia_ano=2026,
        competencia_mes=3,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=False,
        executar_hr=False,
        force_reingestao=force,
        snapshot_carregado=snapshot_carregado,
    )
    s.df_processado = _df_processado()
    s.df_estab_local = _df_estab()
    s.cbo_lookup = {"515105": "Agente Comunitário"}
    return s


class TestSnapshotLocalStage:
    def test_salva_snapshot_em_primeira_rodada(self, tmp_path):
        state = _state(snapshot_carregado=False)
        SnapshotLocalStage(tmp_path).execute(state)
        assert snapshot_existe("2026-03", tmp_path)

    def test_nao_salva_quando_carregado_do_snapshot(self, tmp_path):
        state = _state(snapshot_carregado=True)
        SnapshotLocalStage(tmp_path).execute(state)
        assert not snapshot_existe("2026-03", tmp_path)

    def test_sobrescreve_snapshot_com_force(self, tmp_path):
        snap_antigo = SnapshotLocal(
            df_prof=_df_processado(), df_estab=_df_estab(), cbo_lookup={"515105": "v1"}
        )
        salvar_snapshot("2026-03", tmp_path, snap_antigo)

        state = _state(snapshot_carregado=False, force=True)
        SnapshotLocalStage(tmp_path).execute(state)

        from storage.snapshot_local import carregar_snapshot
        loaded = carregar_snapshot("2026-03", tmp_path)
        assert loaded.cbo_lookup["515105"] == "Agente Comunitário"

    def test_calcula_delta_quando_force_e_snapshot_existe(self, tmp_path):
        df_anterior = _df_processado().copy()
        df_anterior.loc[0, "CBO"] = "999999"
        snap_antigo = SnapshotLocal(df_prof=df_anterior, df_estab=_df_estab(), cbo_lookup={})
        salvar_snapshot("2026-03", tmp_path, snap_antigo)

        state = _state(snapshot_carregado=False, force=True)
        SnapshotLocalStage(tmp_path).execute(state)

        assert state.delta_local["n_alterados"] == 1

    def test_sem_delta_quando_primeira_rodada(self, tmp_path):
        state = _state(snapshot_carregado=False, force=False)
        SnapshotLocalStage(tmp_path).execute(state)
        assert state.delta_local == {}
```

- [ ] **Step 3: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_snapshot_local_stage.py tests/pipeline/stages/test_ingestao_local.py -v
```
Expected: multiple failures (missing fields in PipelineState, missing historico_dir param in IngestaoLocalStage)

- [ ] **Step 4: Modify `src/pipeline/state.py`**

Add three fields to `PipelineState` (after `metricas_avancadas`):

```python
    force_reingestao: bool = False
    snapshot_carregado: bool = False
    delta_local: dict = field(default_factory=dict)
```

- [ ] **Step 5: Modify `src/cli.py`**

In `CliArgs` dataclass, add:
```python
    force_reingestao: bool
```

In `parse_args`, add the argument before `parser.parse_args(argv)`:
```python
    parser.add_argument(
        "--force-reingestao",
        action="store_true",
        default=False,
        help="Força re-ingestão do Firebird mesmo quando snapshot local existir.",
    )
```

In the `return CliArgs(...)` call, add:
```python
        force_reingestao=args.force_reingestao,
```

- [ ] **Step 6: Modify `src/pipeline/stages/ingestao_local.py`**

Replace the entire file content:

```python
"""IngestaoLocalStage — ingere do Firebird ou carrega snapshot local imutável."""
import logging
from pathlib import Path

from contracts.schemas import EstabelecimentoContract, ProfissionalContract
from ingestion.cnes_client import conectar, extrair_lookup_cbo
from ingestion.cnes_local_adapter import CnesLocalAdapter
from pipeline.state import PipelineState
from storage.snapshot_local import carregar_snapshot, snapshot_existe

logger = logging.getLogger(__name__)


class IngestaoLocalStage:
    nome = "ingestao_local"

    def __init__(self, historico_dir: Path) -> None:
        self._historico_dir = historico_dir

    def execute(self, state: PipelineState) -> None:
        if not state.force_reingestao and snapshot_existe(
            state.competencia_str, self._historico_dir
        ):
            snap = carregar_snapshot(state.competencia_str, self._historico_dir)
            state.df_prof_local = snap.df_prof
            state.df_estab_local = snap.df_estab
            state.cbo_lookup = snap.cbo_lookup
            state.snapshot_carregado = True
            logger.info("snapshot_local carregado competencia=%s", state.competencia_str)
            return

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

- [ ] **Step 7: Create `src/pipeline/stages/snapshot_local.py`**

```python
"""SnapshotLocalStage — persiste snapshot pós-processamento e calcula delta."""
import json
import logging
from pathlib import Path

import pandas as pd

from analysis.delta_snapshot import DeltaSnapshot, calcular_delta
from pipeline.state import PipelineState
from storage.snapshot_local import (
    SnapshotLocal,
    carregar_snapshot,
    salvar_snapshot,
    snapshot_existe,
)

logger = logging.getLogger(__name__)


class SnapshotLocalStage:
    nome = "snapshot_local"

    def __init__(self, historico_dir: Path) -> None:
        self._historico_dir = historico_dir

    def execute(self, state: PipelineState) -> None:
        if state.snapshot_carregado:
            return

        competencia = state.competencia_str
        if state.force_reingestao and snapshot_existe(competencia, self._historico_dir):
            snap_anterior = carregar_snapshot(competencia, self._historico_dir)
            delta = calcular_delta(state.df_processado, snap_anterior.df_prof)
            state.delta_local = _delta_para_dict(delta)
            logger.info(
                "delta_snapshot calculado competencia=%s novos=%d removidos=%d alterados=%d",
                competencia, delta.n_novos, delta.n_removidos, delta.n_alterados,
            )

        snap = SnapshotLocal(
            df_prof=state.df_processado,
            df_estab=state.df_estab_local,
            cbo_lookup=state.cbo_lookup,
        )
        salvar_snapshot(competencia, self._historico_dir, snap)
        logger.info("snapshot_local salvo competencia=%s", competencia)


def _delta_para_dict(delta: DeltaSnapshot) -> dict:
    return {
        "n_novos": delta.n_novos,
        "n_removidos": delta.n_removidos,
        "n_alterados": delta.n_alterados,
        "novos_json": json.dumps(delta.novos, ensure_ascii=False, default=str),
        "removidos_json": json.dumps(delta.removidos, ensure_ascii=False, default=str),
        "alterados_json": json.dumps(delta.alterados, ensure_ascii=False, default=str),
    }
```

- [ ] **Step 8: Modify `src/main.py`**

In `_criar_estado`, add `force_reingestao=args.force_reingestao,` to the `PipelineState(...)` call.

In `main()`, change `IngestaoLocalStage()` to `IngestaoLocalStage(config.HISTORICO_DIR)`, and insert `SnapshotLocalStage(config.HISTORICO_DIR)` immediately after `ProcessamentoStage()`:

```python
from pipeline.stages.snapshot_local import SnapshotLocalStage

# ...

    orchestrator = PipelineOrchestrator([
        IngestaoLocalStage(config.HISTORICO_DIR),
        ProcessamentoStage(),
        SnapshotLocalStage(config.HISTORICO_DIR),
        IngestaoNacionalStage(db_loader),
        AuditoriaLocalStage(),
        AuditoriaNacionalStage(),
        MetricasStage(db_loader, historico_reader),
        ExportacaoStage(),
    ])
```

- [ ] **Step 9: Fix existing `test_ingestao_local.py` calls broken by constructor change**

In `tests/pipeline/stages/test_ingestao_local.py`, the three existing tests call `IngestaoLocalStage()` with no args. Replace each call with `IngestaoLocalStage(tmp_path)` and add `tmp_path` as a parameter to each test function. Since `tmp_path` has no snapshot files, the Firebird path is still taken (snapshot_existe returns False).

Change `test_popula_state_com_dados_locais` signature to `(mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, tmp_path)` and body call to `IngestaoLocalStage(tmp_path).execute(state)`.

Change `test_valida_contratos_apos_ingestao` signature to `(..., tmp_path)` and call to `IngestaoLocalStage(tmp_path).execute(_state())`.

Change `test_falha_conexao_propaga_excecao` signature to `(..., tmp_path)` and call to `IngestaoLocalStage(tmp_path).execute(_state())`.

Also update `_state()` helper to use `_state_com_force(False)` or keep as is — both work since `force_reingestao` defaults to `False`.

- [ ] **Step 10: Run full test suite to verify PASS**

```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_snapshot_local_stage.py tests/pipeline/stages/test_ingestao_local.py tests/pipeline/test_state.py tests/test_cli.py -v
```
Expected: all tests PASS

- [ ] **Step 11: Commit**

```bash
git add src/pipeline/state.py src/cli.py src/main.py \
        src/pipeline/stages/ingestao_local.py \
        src/pipeline/stages/snapshot_local.py \
        tests/pipeline/stages/test_snapshot_local_stage.py \
        tests/pipeline/stages/test_ingestao_local.py
git commit -m "feat(pipeline): SnapshotLocalStage — skip Firebird e persistencia pos-processamento"
```

---

### Task 4: Delta persistence — DuckDB DDL, gravar/carregar e MetricasStage

**Context:** `gold.delta_local_snapshot` stores the delta computed by `SnapshotLocalStage`. `MetricasStage` persists it when `state.delta_local` is non-empty. `HistoricoReader` exposes `carregar_delta_snapshot` for the dashboard.

**Files:**
- Modify: `src/storage/database_loader.py`
- Modify: `src/storage/historico_reader.py`
- Modify: `src/pipeline/stages/metricas.py`
- Modify: `tests/storage/test_database_loader.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/storage/test_database_loader.py`:

```python
class TestDeltaLocalSnapshot:
    def test_cria_tabela_delta_local_snapshot(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "delta_local_snapshot" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_gravar_e_ler_delta(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        delta = {
            "n_novos": 3,
            "n_removidos": 1,
            "n_alterados": 2,
            "novos_json": '[{"CPF": "00000000001", "CNES": "1111111"}]',
            "removidos_json": "[]",
            "alterados_json": "[]",
        }
        loader.gravar_delta_snapshot("2026-03", delta)

        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        resultado = reader.carregar_delta_snapshot("2026-03")

        assert resultado is not None
        assert resultado["n_novos"] == 3
        assert resultado["n_removidos"] == 1
        assert resultado["n_alterados"] == 2

    def test_gravar_delta_upsert_idempotente(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        delta_v1 = {"n_novos": 1, "n_removidos": 0, "n_alterados": 0,
                    "novos_json": "[]", "removidos_json": "[]", "alterados_json": "[]"}
        delta_v2 = {"n_novos": 5, "n_removidos": 2, "n_alterados": 1,
                    "novos_json": "[]", "removidos_json": "[]", "alterados_json": "[]"}
        loader.gravar_delta_snapshot("2026-03", delta_v1)
        loader.gravar_delta_snapshot("2026-03", delta_v2)

        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        resultado = reader.carregar_delta_snapshot("2026-03")
        assert resultado["n_novos"] == 5

    def test_carregar_delta_retorna_none_quando_ausente(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        assert reader.carregar_delta_snapshot("2026-03") is None
```

Also add import for `HistoricoReader` at the top of the test file if not already present:
```python
from storage.historico_reader import HistoricoReader
```

- [ ] **Step 2: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py::TestDeltaLocalSnapshot -v
```
Expected: `AttributeError: 'DatabaseLoader' object has no attribute 'gravar_delta_snapshot'`

- [ ] **Step 3: Add DDL constant and `gravar_delta_snapshot` to `src/storage/database_loader.py`**

Add DDL constant after `_DDL_METRICAS_AVANCADAS`:

```python
_DDL_DELTA_SNAPSHOT = """
    CREATE TABLE IF NOT EXISTS gold.delta_local_snapshot (
        competencia    VARCHAR PRIMARY KEY,
        n_novos        INTEGER NOT NULL DEFAULT 0,
        n_removidos    INTEGER NOT NULL DEFAULT 0,
        n_alterados    INTEGER NOT NULL DEFAULT 0,
        novos_json     VARCHAR,
        removidos_json VARCHAR,
        alterados_json VARCHAR,
        gravado_em     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
"""
```

In `inicializar_schema`, add after the last `con.execute(...)` call:
```python
            con.execute(_DDL_DELTA_SNAPSHOT)
```

Add new method after `gravar_metricas_avancadas`:

```python
    def gravar_delta_snapshot(self, competencia: str, delta: dict) -> None:
        """INSERT OR REPLACE do delta de snapshot em gold.delta_local_snapshot.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            delta: Dicionário com chaves n_novos, n_removidos, n_alterados,
                   novos_json, removidos_json, alterados_json.
        """
        with self._conectar() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO gold.delta_local_snapshot
                    (competencia, n_novos, n_removidos, n_alterados,
                     novos_json, removidos_json, alterados_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    competencia,
                    delta.get("n_novos", 0),
                    delta.get("n_removidos", 0),
                    delta.get("n_alterados", 0),
                    delta.get("novos_json"),
                    delta.get("removidos_json"),
                    delta.get("alterados_json"),
                ],
            )
        logger.info("delta_snapshot gravado competencia=%s", competencia)
```

- [ ] **Step 4: Add `carregar_delta_snapshot` to `src/storage/historico_reader.py`**

Add after `carregar_metricas_avancadas`:

```python
    def carregar_delta_snapshot(self, competencia: str) -> dict | None:
        """Retorna o delta de snapshot para uma competência específica.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            Dict com todas as colunas de gold.delta_local_snapshot, ou None se ausente.
        """
        try:
            df = self._ler_df(
                "SELECT * FROM gold.delta_local_snapshot WHERE competencia = ?",
                [competencia],
            )
        except duckdb.CatalogException:
            return None
        if df.empty:
            return None
        return df.iloc[0].to_dict()
```

- [ ] **Step 5: Modify `src/pipeline/stages/metricas.py`**

In `MetricasStage.execute`, after `self._db.gravar_metricas_avancadas(competencia, metricas)`, add:

```python
        if state.delta_local:
            self._db.gravar_delta_snapshot(competencia, state.delta_local)
            logger.info("delta_snapshot gravado competencia=%s", competencia)
```

- [ ] **Step 6: Run tests to verify PASS**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py tests/pipeline/stages/test_metricas_stage.py -v
```
Expected: all tests PASS

- [ ] **Step 7: Commit**

```bash
git add src/storage/database_loader.py src/storage/historico_reader.py \
        src/pipeline/stages/metricas.py \
        tests/storage/test_database_loader.py
git commit -m "feat(storage): delta_local_snapshot — DDL, gravar e carregar delta de snapshot"
```

---

### Task 5: Dashboard — página `6_Delta.py`

**Context:** New Streamlit page. The delta is only present when the pipeline ran with `--force-reingestao`. The page gracefully handles the absent case with a clear instruction to the user.

**Files:**
- Create: `scripts/pages/6_Delta.py`

- [ ] **Step 1: Create `scripts/pages/6_Delta.py`**

```python
"""Página 6 — Delta Snapshot: drift entre rodadas do pipeline por competência."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st

import config
from storage.historico_reader import HistoricoReader

st.title("Delta — Drift do CNES Local")

if "reader" not in st.session_state:
    st.session_state["reader"] = HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

competencia = st.sidebar.selectbox("Competência", options=competencias[::-1], index=0)
raw = reader.carregar_delta_snapshot(competencia)

if raw is None:
    st.info(
        f"Sem delta para {competencia}. "
        "Execute o pipeline com `--force-reingestao` para calcular o drift entre rodadas."
    )
    st.stop()

col1, col2, col3 = st.columns(3)
col1.metric("Novos vínculos", int(raw.get("n_novos", 0)))
col2.metric("Desligamentos", int(raw.get("n_removidos", 0)))
col3.metric("Alterações de atributo", int(raw.get("n_alterados", 0)))


def _parse(val) -> list:
    if not val:
        return []
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return []


novos = _parse(raw.get("novos_json"))
removidos = _parse(raw.get("removidos_json"))
alterados = _parse(raw.get("alterados_json"))

if novos:
    st.subheader("Novos Vínculos")
    st.dataframe(pd.DataFrame(novos), use_container_width=True, hide_index=True)

if removidos:
    st.subheader("Desligamentos")
    st.dataframe(pd.DataFrame(removidos), use_container_width=True, hide_index=True)

if alterados:
    st.subheader("Alterações")
    st.dataframe(pd.DataFrame(alterados), use_container_width=True, hide_index=True)

if not novos and not removidos and not alterados:
    st.success("Nenhuma diferença encontrada entre esta rodada e a anterior.")
```

- [ ] **Step 2: Verify imports resolve correctly**

```
./venv/Scripts/python.exe -c "
import sys; sys.path.insert(0, 'src'); sys.path.insert(0, 'scripts')
from storage.historico_reader import HistoricoReader
print('imports ok')
"
```
Expected: `imports ok`

- [ ] **Step 3: Run full test suite**

```
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```
Expected: all existing tests PASS, no regressions

- [ ] **Step 4: Commit**

```bash
git add scripts/pages/6_Delta.py
git commit -m "feat(dashboard): pagina Delta — drift CNES entre rodadas com --force-reingestao"
```

---

## Post-implementation smoke test

After all tasks are complete, verify end-to-end behaviour manually:

```bash
# First run — saves snapshot, no delta
./venv/Scripts/python.exe src/main.py --skip-nacional --skip-hr -v

# Second run — loads from snapshot, Firebird NOT queried
./venv/Scripts/python.exe src/main.py --skip-nacional --skip-hr -v

# Force run — re-queries Firebird, computes and persists delta
./venv/Scripts/python.exe src/main.py --skip-nacional --skip-hr --force-reingestao -v
```

Verify logs for:
- First run: `snapshot_local salvo competencia=YYYY-MM`
- Second run: `snapshot_local carregado competencia=YYYY-MM`
- Force run: `delta_snapshot calculado competencia=YYYY-MM novos=N removidos=N alterados=N`
