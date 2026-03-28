# Epic 2 — DuckDB Medallion POC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Substituir a gravação de snapshots JSON por um banco analítico DuckDB embarcado com schema Gold, adicionando persistência SQL sem quebrar o pipeline existente.

**Architecture:** Novo package `src/storage/` contém `DatabaseLoader` — única classe responsável pela conexão e escrita no DuckDB. O pipeline principal invoca o loader ao final de cada execução, em paralelo à gravação JSON (que continua funcionando como fallback durante o POC). A evolução_tracker.py permanece inalterada; o loader é uma camada adicional.

**Tech Stack:** `duckdb>=0.10`, `pandas` (já instalado), `pytest` + `tmp_path` para testes isolados (DuckDB in-file na tmp_path).

**Scope POC — o que esta sprint NÃO faz:**
- Bronze/Silver layers (aguardam dados PEC/SIAH)
- Excel lendo da camada Gold (é o próximo epic: Dashboard)
- Remoção dos JSONs (deferred — dual-write neste sprint)

---

## File Map

| Ação | Arquivo | Responsabilidade |
|---|---|---|
| Criar | `src/storage/__init__.py` | Marca package |
| Criar | `src/storage/database_loader.py` | DatabaseLoader: schema, UPSERT, leitura |
| Criar | `tests/storage/__init__.py` | Marca package de testes |
| Criar | `tests/storage/test_database_loader.py` | 10 testes TDD |
| Modificar | `src/config.py` | Adicionar `DUCKDB_PATH` |
| Modificar | `src/main.py` | Chamar DatabaseLoader após salvar_snapshot |
| Modificar | `tests/test_main.py` | Mock do DatabaseLoader |
| Modificar | `ROADMAP.md` | Marcar Epic 2 ✅ |

---

## Task 1 — Instalar duckdb e adicionar DUCKDB_PATH ao config

**Files:**
- Modify: `src/config.py` (após linha `SNAPSHOTS_DIR`)
- Install: `duckdb` via pip

- [ ] **Step 1: Instalar duckdb**

```bash
./venv/Scripts/pip.exe install duckdb
```

Saída esperada: `Successfully installed duckdb-X.Y.Z`

- [ ] **Step 2: Verificar instalação**

```bash
./venv/Scripts/python.exe -c "import duckdb; print(duckdb.__version__)"
```

Saída esperada: número de versão (ex: `0.10.3`).

- [ ] **Step 3: Adicionar DUCKDB_PATH em `src/config.py`**

Abrir `src/config.py`. Localizar a linha com `SNAPSHOTS_DIR` (linha ~89). Adicionar logo após:

```python
DUCKDB_PATH: Path = RAIZ_PROJETO / os.getenv("DUCKDB_PATH", "data/cnesdata.duckdb")
```

- [ ] **Step 4: Verificar que config carrega sem erro**

```bash
./venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); import config; print(config.DUCKDB_PATH)"
```

Saída esperada: caminho absoluto terminando em `data/cnesdata.duckdb`.

- [ ] **Step 5: Commit**

```bash
git add src/config.py
git commit -m "chore(storage): add duckdb dependency + DUCKDB_PATH config"
```

---

## Task 2 — Package src/storage + DatabaseLoader: schema init

**Files:**
- Create: `src/storage/__init__.py`
- Create: `src/storage/database_loader.py`
- Create: `tests/storage/__init__.py`
- Create: `tests/storage/test_database_loader.py`

- [ ] **Step 1: Criar packages**

```bash
mkdir -p src/storage tests/storage
touch src/storage/__init__.py tests/storage/__init__.py
```

- [ ] **Step 2: Escrever teste RED para schema init**

Criar `tests/storage/test_database_loader.py`:

```python
"""Testes do DatabaseLoader — persistência DuckDB Gold layer."""

from pathlib import Path

import duckdb
import pytest

from storage.database_loader import DatabaseLoader


def _tabelas_existentes(caminho_db: Path) -> list[str]:
    con = duckdb.connect(str(caminho_db), read_only=True)
    df = con.execute("SHOW ALL TABLES").df()
    con.close()
    return df["name"].tolist()


class TestInicializarSchema:

    def test_cria_tabela_evolucao_metricas(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")

        # Act
        loader.inicializar_schema()

        # Assert
        assert "evolucao_metricas_mensais" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_tabela_auditoria_resultados(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")

        # Act
        loader.inicializar_schema()

        # Assert
        assert "auditoria_resultados" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_idempotente_chamada_multiplas_vezes(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")

        # Act — não deve levantar exceção
        loader.inicializar_schema()
        loader.inicializar_schema()
```

- [ ] **Step 3: Rodar e confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py -x --tb=short -q
```

Saída esperada: `ImportError: No module named 'storage.database_loader'`

- [ ] **Step 4: Criar `src/storage/database_loader.py` com implementação mínima**

```python
"""DatabaseLoader — persistência DuckDB com schema Gold (Medallion POC)."""

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

_DDL_SCHEMA_GOLD = "CREATE SCHEMA IF NOT EXISTS gold"

_DDL_EVOLUCAO = """
    CREATE TABLE IF NOT EXISTS gold.evolucao_metricas_mensais (
        data_competencia VARCHAR PRIMARY KEY,
        total_vinculos   INTEGER,
        total_ghost      INTEGER,
        total_missing    INTEGER,
        total_rq005      INTEGER,
        gravado_em       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

_DDL_AUDITORIA = """
    CREATE TABLE IF NOT EXISTS gold.auditoria_resultados (
        data_competencia VARCHAR,
        regra            VARCHAR,
        total_anomalias  INTEGER,
        gravado_em       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (data_competencia, regra)
    )
"""


class DatabaseLoader:
    """Gerencia a conexão e persistência no banco DuckDB local."""

    def __init__(self, caminho_db: Path) -> None:
        self._caminho_db = caminho_db

    def inicializar_schema(self) -> None:
        """Cria schemas e tabelas Gold se ainda não existirem."""
        with duckdb.connect(str(self._caminho_db)) as con:
            con.execute(_DDL_SCHEMA_GOLD)
            con.execute(_DDL_EVOLUCAO)
            con.execute(_DDL_AUDITORIA)
        logger.info("schema_gold inicializado db=%s", self._caminho_db)
```

- [ ] **Step 5: Rodar e confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py -x --tb=short -q
```

Saída esperada: `3 passed`

- [ ] **Step 6: Rodar suite completa — sem regressões**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" --tb=short -q
```

Saída esperada: todos os testes existentes passando.

- [ ] **Step 7: Commit**

```bash
git add src/storage/ tests/storage/
git commit -m "test(storage): red+green — DatabaseLoader schema init Gold"
```

---

## Task 3 — DatabaseLoader.gravar_metricas (Snapshot → Gold)

**Files:**
- Modify: `src/storage/database_loader.py` (adicionar método)
- Modify: `tests/storage/test_database_loader.py` (adicionar classe de testes)

O `Snapshot` vem de `analysis.evolution_tracker`. O loader importa e usa o dataclass diretamente.

- [ ] **Step 1: Adicionar testes RED para gravar_metricas**

Adicionar ao `tests/storage/test_database_loader.py` APÓS os imports existentes:

```python
from analysis.evolution_tracker import Snapshot


def _snapshot(competencia: str = "2024-12", vinculos: int = 357) -> Snapshot:
    return Snapshot(
        data_competencia=competencia,
        total_vinculos=vinculos,
        total_ghost=5,
        total_missing=3,
        total_rq005=8,
    )


def _ler_metricas(caminho_db: Path):
    con = duckdb.connect(str(caminho_db), read_only=True)
    df = con.execute("SELECT * FROM gold.evolucao_metricas_mensais").df()
    con.close()
    return df


class TestGravarMetricas:

    def test_insere_snapshot_na_tabela_gold(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        loader.gravar_metricas(_snapshot())

        # Assert
        df = _ler_metricas(tmp_path / "test.duckdb")
        assert len(df) == 1
        assert df["data_competencia"].iloc[0] == "2024-12"
        assert df["total_vinculos"].iloc[0] == 357

    def test_upsert_substitui_competencia_existente(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_metricas(_snapshot(vinculos=100))

        # Act — mesma competência, vinculos diferentes
        loader.gravar_metricas(_snapshot(vinculos=357))

        # Assert — deve haver apenas 1 linha (UPSERT, não INSERT duplo)
        df = _ler_metricas(tmp_path / "test.duckdb")
        assert len(df) == 1
        assert df["total_vinculos"].iloc[0] == 357

    def test_multiplas_competencias_inseridas(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        loader.gravar_metricas(_snapshot("2024-11"))
        loader.gravar_metricas(_snapshot("2024-12"))

        # Assert
        df = _ler_metricas(tmp_path / "test.duckdb")
        assert len(df) == 2
        assert set(df["data_competencia"]) == {"2024-11", "2024-12"}
```

- [ ] **Step 2: Confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py::TestGravarMetricas -x --tb=short -q
```

Saída esperada: `AttributeError: 'DatabaseLoader' object has no attribute 'gravar_metricas'`

- [ ] **Step 3: Implementar gravar_metricas em `src/storage/database_loader.py`**

Adicionar import no topo do arquivo:
```python
from analysis.evolution_tracker import Snapshot
```

Adicionar método à classe `DatabaseLoader`:

```python
def gravar_metricas(self, snapshot: Snapshot) -> None:
    """UPSERT das métricas do snapshot em gold.evolucao_metricas_mensais.

    Args:
        snapshot: Snapshot da competência a persistir.
    """
    with duckdb.connect(str(self._caminho_db)) as con:
        con.execute(
            """
            INSERT OR REPLACE INTO gold.evolucao_metricas_mensais
                (data_competencia, total_vinculos, total_ghost, total_missing, total_rq005)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                snapshot.data_competencia,
                snapshot.total_vinculos,
                snapshot.total_ghost,
                snapshot.total_missing,
                snapshot.total_rq005,
            ],
        )
    logger.info(
        "metricas gravadas competencia=%s vinculos=%d",
        snapshot.data_competencia,
        snapshot.total_vinculos,
    )
```

- [ ] **Step 4: Confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py -x --tb=short -q
```

Saída esperada: `6 passed`

- [ ] **Step 5: Commit**

```bash
git add src/storage/database_loader.py tests/storage/test_database_loader.py
git commit -m "feat(storage): gravar_metricas — UPSERT Snapshot→Gold evolucao_metricas_mensais"
```

---

## Task 4 — DatabaseLoader.gravar_auditoria (contagens por regra → Gold)

**Files:**
- Modify: `src/storage/database_loader.py`
- Modify: `tests/storage/test_database_loader.py`

Esta tabela armazena o total de anomalias por regra por competência. Alimentada com os DataFrames de auditoria ao final do pipeline.

- [ ] **Step 1: Adicionar testes RED**

Adicionar ao `tests/storage/test_database_loader.py`:

```python
def _ler_auditoria(caminho_db: Path):
    con = duckdb.connect(str(caminho_db), read_only=True)
    df = con.execute(
        "SELECT * FROM gold.auditoria_resultados ORDER BY regra"
    ).df()
    con.close()
    return df


class TestGravarAuditoria:

    def test_insere_contagem_por_regra(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        loader.gravar_auditoria("2024-12", "RQ006", 3)

        # Assert
        df = _ler_auditoria(tmp_path / "test.duckdb")
        assert len(df) == 1
        assert df["regra"].iloc[0] == "RQ006"
        assert df["total_anomalias"].iloc[0] == 3

    def test_upsert_atualiza_contagem_existente(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_auditoria("2024-12", "RQ006", 3)

        # Act — reexecução com nova contagem
        loader.gravar_auditoria("2024-12", "RQ006", 5)

        # Assert — linha única com valor atualizado
        df = _ler_auditoria(tmp_path / "test.duckdb")
        assert len(df) == 1
        assert df["total_anomalias"].iloc[0] == 5

    def test_insere_multiplas_regras_mesma_competencia(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        loader.gravar_auditoria("2024-12", "RQ006", 2)
        loader.gravar_auditoria("2024-12", "RQ008", 7)

        # Assert
        df = _ler_auditoria(tmp_path / "test.duckdb")
        assert len(df) == 2
        assert set(df["regra"]) == {"RQ006", "RQ008"}
```

- [ ] **Step 2: Confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py::TestGravarAuditoria -x --tb=short -q
```

Saída esperada: `AttributeError: 'DatabaseLoader' object has no attribute 'gravar_auditoria'`

- [ ] **Step 3: Implementar gravar_auditoria**

Adicionar método à classe `DatabaseLoader` em `src/storage/database_loader.py`:

```python
def gravar_auditoria(self, data_competencia: str, regra: str, total: int) -> None:
    """UPSERT de contagem de anomalias por regra em gold.auditoria_resultados.

    Args:
        data_competencia: Competência no formato 'YYYY-MM'.
        regra: Código da regra de auditoria (ex: 'RQ006').
        total: Total de anomalias detectadas.
    """
    with duckdb.connect(str(self._caminho_db)) as con:
        con.execute(
            """
            INSERT OR REPLACE INTO gold.auditoria_resultados
                (data_competencia, regra, total_anomalias)
            VALUES (?, ?, ?)
            """,
            [data_competencia, regra, total],
        )
    logger.info("auditoria gravada competencia=%s regra=%s total=%d", data_competencia, regra, total)
```

- [ ] **Step 4: Confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py -x --tb=short -q
```

Saída esperada: `9 passed`

- [ ] **Step 5: Commit**

```bash
git add src/storage/database_loader.py tests/storage/test_database_loader.py
git commit -m "feat(storage): gravar_auditoria — UPSERT contagens RQ→Gold auditoria_resultados"
```

---

## Task 5 — DatabaseLoader.carregar_historico (Gold → list[Snapshot])

**Files:**
- Modify: `src/storage/database_loader.py`
- Modify: `tests/storage/test_database_loader.py`

Substitui `evolution_tracker.carregar_snapshots` para leituras futuras (ex: dashboard).

- [ ] **Step 1: Adicionar testes RED**

```python
class TestCarregarHistorico:

    def test_retorna_lista_vazia_quando_banco_sem_dados(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        resultado = loader.carregar_historico()

        # Assert
        assert resultado == []

    def test_retorna_snapshots_em_ordem_cronologica(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_metricas(_snapshot("2024-11", vinculos=300))
        loader.gravar_metricas(_snapshot("2024-12", vinculos=357))

        # Act
        resultado = loader.carregar_historico()

        # Assert
        assert len(resultado) == 2
        assert resultado[0].data_competencia == "2024-11"
        assert resultado[1].data_competencia == "2024-12"
        assert resultado[1].total_vinculos == 357

    def test_snapshot_retornado_tem_todos_os_campos(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_metricas(_snapshot("2024-12"))

        # Act
        resultado = loader.carregar_historico()

        # Assert
        s = resultado[0]
        assert s.data_competencia == "2024-12"
        assert s.total_vinculos == 357
        assert s.total_ghost == 5
        assert s.total_missing == 3
        assert s.total_rq005 == 8
```

- [ ] **Step 2: Confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py::TestCarregarHistorico -x --tb=short -q
```

Saída esperada: `AttributeError: 'DatabaseLoader' object has no attribute 'carregar_historico'`

- [ ] **Step 3: Implementar carregar_historico**

```python
def carregar_historico(self) -> list[Snapshot]:
    """Retorna todos os snapshots do Gold ordenados por competência.

    Returns:
        Lista de Snapshot em ordem cronológica crescente.
    """
    with duckdb.connect(str(self._caminho_db), read_only=True) as con:
        df = con.execute(
            "SELECT * FROM gold.evolucao_metricas_mensais ORDER BY data_competencia"
        ).df()

    return [
        Snapshot(
            data_competencia=row["data_competencia"],
            total_vinculos=int(row["total_vinculos"]),
            total_ghost=int(row["total_ghost"]),
            total_missing=int(row["total_missing"]),
            total_rq005=int(row["total_rq005"]),
        )
        for _, row in df.iterrows()
    ]
```

- [ ] **Step 4: Confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py -x --tb=short -q
```

Saída esperada: `12 passed`

- [ ] **Step 5: Suite completa sem regressões**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" --tb=short -q
```

- [ ] **Step 6: Commit**

```bash
git add src/storage/database_loader.py tests/storage/test_database_loader.py
git commit -m "feat(storage): carregar_historico — leitura Gold→list[Snapshot]"
```

---

## Task 6 — Integrar DatabaseLoader no main.py

**Files:**
- Modify: `src/main.py`
- Modify: `tests/test_main.py`

O loader é chamado após `salvar_snapshot`. Os JSONs continuam sendo escritos (dual-write neste sprint).

- [ ] **Step 1: Adicionar import em main.py**

Abrir `src/main.py`. Localizar o bloco de imports existente (topo do arquivo). Adicionar:

```python
from storage.database_loader import DatabaseLoader
```

- [ ] **Step 2: Adicionar chamada após salvar_snapshot**

Localizar a linha `salvar_snapshot(snapshot, config.SNAPSHOTS_DIR)` (linha ~306). Adicionar logo após:

```python
        _loader = DatabaseLoader(config.DUCKDB_PATH)
        _loader.inicializar_schema()
        _loader.gravar_metricas(snapshot)
        _loader.gravar_auditoria(snapshot.data_competencia, "GHOST", snapshot.total_ghost)
        _loader.gravar_auditoria(snapshot.data_competencia, "MISSING", snapshot.total_missing)
        _loader.gravar_auditoria(snapshot.data_competencia, "RQ005", snapshot.total_rq005)
```

- [ ] **Step 3: Adicionar mock do DatabaseLoader nos testes de main**

Abrir `tests/test_main.py`. Na função `_aplicar_patches`, dentro do bloco de patches, adicionar:

```python
    stack.enter_context(patch("main.DatabaseLoader"))
```

**Onde exatamente adicionar:** logo após a linha `stack.enter_context(patch("main.gerar_relatorio"))`.

- [ ] **Step 4: Rodar testes de main**

```bash
./venv/Scripts/python.exe -m pytest tests/test_main.py --tb=short -q
```

Saída esperada: todos os testes existentes passando.

- [ ] **Step 5: Suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" --tb=short -q
```

- [ ] **Step 6: Lint**

```bash
./venv/Scripts/ruff.exe check src/storage/ src/main.py --fix
```

- [ ] **Step 7: Commit**

```bash
git add src/main.py tests/test_main.py
git commit -m "feat(main): integrar DatabaseLoader — dual-write Gold + JSON por competencia"
```

---

## Task 7 — Atualizar ROADMAP e PROJECT_CONTEXT

**Files:**
- Modify: `ROADMAP.md` — marcar Epic 2 ✅
- Modify: `PROJECT_CONTEXT.md` — atualizar seção 6 (Near-Term Roadmap)

- [ ] **Step 1: Atualizar status da Epic 2 no ROADMAP.md**

No bloco `### Epic 2 — Fundação Data Warehouse`, mudar:

```
**Status:** Planejado
```

Para:

```
**Status:** ✅ POC implementado (Gold layer: evolucao_metricas_mensais + auditoria_resultados)
**Nota (2026-03):** Dual-write com JSON durante POC. Bronze/Silver e remoção de JSON são work packages futuros.
```

- [ ] **Step 2: Atualizar tabela Near-Term Roadmap em PROJECT_CONTEXT.md**

Na seção `## 6. Where It's Headed`, mudar a linha da Epic 2 de `Planned` para `✅ Done (POC)`.

- [ ] **Step 3: Commit final**

```bash
git add ROADMAP.md PROJECT_CONTEXT.md
git commit -m "docs: marcar Epic 2 DuckDB POC como concluido"
```

---

## Self-Review — Spec Coverage

| Critério do PRD | Cobertura no plano |
|---|---|
| Módulo `DatabaseLoader` | ✅ Task 2 cria a classe |
| Schema `Gold` com `evolucao_metricas_mensais` | ✅ Task 2 DDL |
| UPSERT baseado em Competência | ✅ Task 3 + Task 4 (`INSERT OR REPLACE`) |
| Integração no pipeline | ✅ Task 6 |
| Excel lendo do Gold | ⏳ **Deferred** — é o epic "Dashboard de Evolução", não este POC |
| Bronze/Silver layers | ⏳ **Deferred** — aguardam dados PEC/SIAH |

**Nota sobre o critério "Excel lê do Gold":** O PRD v1 inclui esse requisito, mas o próprio PRD também classifica esta epic como "Spike/POC". Mudar `report_generator.py` para ler do DuckDB exigiria refatorar sua assinatura e 25+ testes. Isso é escopo do epic "Dashboard de Evolução no Excel" (Prioridade 4 no Roadmap). O que este plano entrega — `DatabaseLoader` com Gold queryável — é o pré-requisito para aquele epic.

---

## Quick Reference

```bash
# Rodar apenas testes do novo módulo
./venv/Scripts/python.exe -m pytest tests/storage/ -v

# Suite completa sem integração
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q

# Verificar DuckDB criado após execução real
./venv/Scripts/python.exe -c "
import duckdb
con = duckdb.connect('data/cnesdata.duckdb', read_only=True)
print(con.execute('SHOW ALL TABLES').df())
print(con.execute('SELECT * FROM gold.evolucao_metricas_mensais').df())
con.close()
"
```
