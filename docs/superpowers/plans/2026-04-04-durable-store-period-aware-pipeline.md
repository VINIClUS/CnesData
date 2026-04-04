# Durable Audit Store & Period-Aware Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace CSV/XLSX as primary pipeline storage with DuckDB; restrict Firebird access to the current calendar period; enforce the audit period invariant; degrade gracefully when local data is unavailable for a past period.

**Architecture:** `IngestaoLocalStage` loads from DuckDB first, falls back to parquet backfill, then Firebird (current period only), or marks `local_disponivel=False` (past period, no data). `SnapshotLocalStage` writes processed data to DuckDB in addition to parquet. `AuditoriaNacionalStage` runs when either side has data but cross-checks require both. `ExportacaoStage` writes `gold.pipeline_runs` and no longer writes CSV/XLSX. XLSX is generated on-demand from DuckDB via a dashboard download button.

**Tech Stack:** pandas, pyarrow (parquet, kept as file backup), DuckDB, openpyxl, Streamlit, dataclasses

---

## File Map

| Action | File |
|---|---|
| Modify | `src/storage/competencia_utils.py` |
| Modify | `src/pipeline/state.py` |
| Modify | `src/storage/database_loader.py` |
| Modify | `src/storage/historico_reader.py` |
| Modify | `src/pipeline/stages/ingestao_local.py` |
| Modify | `src/pipeline/stages/snapshot_local.py` |
| Modify | `src/pipeline/stages/processamento.py` |
| Modify | `src/pipeline/stages/ingestao_nacional.py` |
| Modify | `src/pipeline/stages/auditoria_local.py` |
| Modify | `src/pipeline/stages/auditoria_nacional.py` |
| Modify | `src/pipeline/stages/metricas.py` |
| Modify | `src/pipeline/stages/exportacao.py` |
| Modify | `src/export/report_generator.py` |
| Modify | `src/main.py` |
| Modify | `scripts/pages/2_Por_Regra.py` |
| Modify | `scripts/pages/1_Tendencias.py` |
| Modify | `scripts/pages/4_Glosas.py` |
| Modify | `scripts/pages/5_Metricas.py` |
| Modify | `tests/storage/test_competencia_utils.py` |
| Modify | `tests/pipeline/test_state.py` |
| Modify | `tests/storage/test_database_loader.py` |
| Modify | `tests/storage/test_historico_reader.py` |
| Modify | `tests/pipeline/stages/test_ingestao_local.py` |
| Modify | `tests/pipeline/stages/test_snapshot_local_stage.py` |
| Modify | `tests/pipeline/stages/test_auditoria_nacional.py` |
| Modify | `tests/pipeline/stages/test_exportacao.py` |
| Modify | `tests/export/test_report_generator.py` |

---

### Task 1: `periodo_atual()` + `PipelineState` new fields

**Files:**
- Modify: `src/storage/competencia_utils.py`
- Modify: `src/pipeline/state.py`
- Modify: `tests/storage/test_competencia_utils.py`
- Modify: `tests/pipeline/test_state.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/storage/test_competencia_utils.py`:

```python
from datetime import date
from unittest.mock import patch

from storage.competencia_utils import periodo_atual


class TestPeriodoAtual:
    def test_formato_yyyy_mm(self):
        resultado = periodo_atual()
        partes = resultado.split("-")
        assert len(partes) == 2
        assert len(partes[0]) == 4
        assert len(partes[1]) == 2

    def test_retorna_mes_atual(self):
        with patch("storage.competencia_utils.date") as mock_date:
            mock_date.today.return_value = date(2026, 4, 4)
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
            assert periodo_atual() == "2026-04"

    def test_retorna_dezembro_correto(self):
        with patch("storage.competencia_utils.date") as mock_date:
            mock_date.today.return_value = date(2025, 12, 1)
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
            assert periodo_atual() == "2025-12"
```

Add to `tests/pipeline/test_state.py`:

```python
def test_state_local_disponivel_default_true():
    state = PipelineState(
        competencia_ano=2024, competencia_mes=12,
        output_path=Path("x.csv"), executar_nacional=False, executar_hr=False,
    )
    assert state.local_disponivel is True


def test_state_nacional_disponivel_default_false():
    state = PipelineState(
        competencia_ano=2024, competencia_mes=12,
        output_path=Path("x.csv"), executar_nacional=False, executar_hr=False,
    )
    assert state.nacional_disponivel is False
```

- [ ] **Step 2: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_competencia_utils.py::TestPeriodoAtual tests/pipeline/test_state.py::test_state_local_disponivel_default_true tests/pipeline/test_state.py::test_state_nacional_disponivel_default_false -v
```

Expected: `AttributeError` / `ImportError`

- [ ] **Step 3: Add `periodo_atual` to `src/storage/competencia_utils.py`**

Add after the existing imports at the top:

```python
from datetime import date, timedelta
```

(already imported — just add `periodo_atual` function at the end of the file)

```python
def periodo_atual() -> str:
    """Retorna competência do mês calendário atual no formato YYYY-MM."""
    hoje = date.today()
    return f"{hoje.year}-{hoje.month:02d}"
```

- [ ] **Step 4: Add fields to `src/pipeline/state.py`**

After `metricas_avancadas`, `force_reingestao`, `snapshot_carregado`, `delta_local`:

```python
    local_disponivel: bool = True
    nacional_disponivel: bool = False
```

- [ ] **Step 5: Run tests to verify PASS**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_competencia_utils.py::TestPeriodoAtual tests/pipeline/test_state.py::test_state_local_disponivel_default_true tests/pipeline/test_state.py::test_state_nacional_disponivel_default_false -v
```

Expected: all PASS

- [ ] **Step 6: Run full suite to verify no regressions**

```
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add src/storage/competencia_utils.py src/pipeline/state.py \
        tests/storage/test_competencia_utils.py tests/pipeline/test_state.py
git commit -m "feat(pipeline): periodo_atual e campos local/nacional_disponivel em PipelineState"
```

---

### Task 2: DatabaseLoader — 4 new tables + write + read methods

**Files:**
- Modify: `src/storage/database_loader.py`
- Modify: `tests/storage/test_database_loader.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/storage/test_database_loader.py` (after existing classes):

```python
import pandas as pd


def _df_prof_sample() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS":              ["123456789012345"],
        "CPF":              ["12345678901"],
        "NOME_PROFISSIONAL":["Ana Silva"],
        "SEXO":             ["F"],
        "CBO":              ["515105"],
        "CNES":             ["2795001"],
        "TIPO_VINCULO":     ["30"],
        "SUS":              ["S"],
        "CH_TOTAL":         [40],
        "CH_AMBULATORIAL":  [20],
        "CH_OUTRAS":        [10],
        "CH_HOSPITALAR":    [10],
        "FONTE":            ["LOCAL"],
        "ALERTA_STATUS_CH": ["OK"],
        "DESCRICAO_CBO":    ["Agente Comunitário"],
    })


def _df_estab_sample() -> pd.DataFrame:
    return pd.DataFrame({
        "CNES":             ["2795001"],
        "NOME_FANTASIA":    ["UBS Centro"],
        "TIPO_UNIDADE":     ["01"],
        "CNPJ_MANTENEDORA": ["55293427000117"],
        "NATUREZA_JURIDICA":["1023"],
        "COD_MUNICIPIO":    ["354130"],
        "VINCULO_SUS":      ["S"],
        "FONTE":            ["LOCAL"],
    })


class TestNovasTabelasDDL:
    def test_cria_profissionais_processados(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "profissionais_processados" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_estabelecimentos(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "estabelecimentos" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_cbo_lookup(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "cbo_lookup" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_pipeline_runs(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "pipeline_runs" in _tabelas_existentes(tmp_path / "test.duckdb")


class TestGravarCarregarProfissionais:
    def test_profissional_existe_falso_quando_vazio(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert not loader.profissional_existe("2026-03")

    def test_profissional_existe_verdadeiro_apos_gravar(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_profissionais("2026-03", _df_prof_sample())
        assert loader.profissional_existe("2026-03")

    def test_roundtrip_profissionais(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        df = _df_prof_sample()
        loader.gravar_profissionais("2026-03", df)
        resultado = loader.carregar_profissionais("2026-03")
        assert list(resultado["CPF"]) == ["12345678901"]
        assert list(resultado["CNES"]) == ["2795001"]

    def test_gravar_profissionais_substitui_existentes(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_profissionais("2026-03", _df_prof_sample())
        df2 = _df_prof_sample().copy()
        df2["CPF"] = ["99999999999"]
        df2["CNES"] = ["9999999"]
        loader.gravar_profissionais("2026-03", df2)
        resultado = loader.carregar_profissionais("2026-03")
        assert len(resultado) == 1
        assert resultado["CPF"].iloc[0] == "99999999999"


class TestGravarCarregarEstabelecimentos:
    def test_roundtrip_estabelecimentos(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        df = _df_estab_sample()
        loader.gravar_estabelecimentos("2026-03", df)
        resultado = loader.carregar_estabelecimentos("2026-03")
        assert list(resultado["CNES"]) == ["2795001"]
        assert list(resultado["NOME_FANTASIA"]) == ["UBS Centro"]


class TestGravarCarregarCboLookup:
    def test_roundtrip_cbo_lookup(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        lookup = {"515105": "Agente Comunitário", "225125": "Médico"}
        loader.gravar_cbo_lookup("2026-03", lookup)
        resultado = loader.carregar_cbo_lookup("2026-03")
        assert resultado == lookup

    def test_lookup_vazio_retorna_dict_vazio(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert loader.carregar_cbo_lookup("2026-03") == {}


class TestGravarPipelineRun:
    def test_gravar_pipeline_run_completo(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_pipeline_run("2026-03", True, True, False, "completo")
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            df = con.execute(
                "SELECT * FROM gold.pipeline_runs WHERE competencia='2026-03'"
            ).df()
        assert len(df) == 1
        assert df["status"].iloc[0] == "completo"
        assert df["local_disponivel"].iloc[0] is True

    def test_gravar_pipeline_run_upsert(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_pipeline_run("2026-03", False, True, False, "sem_dados_locais")
        loader.gravar_pipeline_run("2026-03", True, True, False, "completo")
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            df = con.execute(
                "SELECT status FROM gold.pipeline_runs WHERE competencia='2026-03'"
            ).df()
        assert len(df) == 1
        assert df["status"].iloc[0] == "completo"
```

- [ ] **Step 2: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py::TestNovasTabelasDDL tests/storage/test_database_loader.py::TestGravarCarregarProfissionais -v
```

Expected: `AttributeError: 'DatabaseLoader' object has no attribute 'gravar_profissionais'`

- [ ] **Step 3: Add DDL constants to `src/storage/database_loader.py`**

Add after `_DDL_DELTA_SNAPSHOT`:

```python
_DDL_PROFISSIONAIS_PROCESSADOS = """
    CREATE TABLE IF NOT EXISTS gold.profissionais_processados (
        competencia        VARCHAR NOT NULL,
        cpf                VARCHAR NOT NULL,
        cnes               VARCHAR NOT NULL,
        cns                VARCHAR,
        nome_profissional  VARCHAR,
        sexo               VARCHAR(1),
        cbo                VARCHAR,
        tipo_vinculo       VARCHAR,
        sus                VARCHAR(1),
        ch_total           INTEGER,
        ch_ambulatorial    INTEGER,
        ch_outras          INTEGER,
        ch_hospitalar      INTEGER,
        fonte              VARCHAR,
        alerta_status_ch   VARCHAR,
        descricao_cbo      VARCHAR,
        gravado_em         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (competencia, cpf, cnes)
    )
"""

_DDL_ESTABELECIMENTOS = """
    CREATE TABLE IF NOT EXISTS gold.estabelecimentos (
        competencia        VARCHAR NOT NULL,
        cnes               VARCHAR NOT NULL,
        nome_fantasia      VARCHAR,
        tipo_unidade       VARCHAR,
        cnpj_mantenedora   VARCHAR,
        natureza_juridica  VARCHAR,
        cod_municipio      VARCHAR,
        vinculo_sus        VARCHAR(1),
        fonte              VARCHAR,
        gravado_em         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (competencia, cnes)
    )
"""

_DDL_CBO_LOOKUP = """
    CREATE TABLE IF NOT EXISTS gold.cbo_lookup (
        competencia  VARCHAR NOT NULL,
        codigo_cbo   VARCHAR NOT NULL,
        descricao    VARCHAR,
        PRIMARY KEY (competencia, codigo_cbo)
    )
"""

_DDL_PIPELINE_RUNS = """
    CREATE TABLE IF NOT EXISTS gold.pipeline_runs (
        competencia          VARCHAR PRIMARY KEY,
        local_disponivel     BOOLEAN NOT NULL DEFAULT FALSE,
        nacional_disponivel  BOOLEAN NOT NULL DEFAULT FALSE,
        hr_disponivel        BOOLEAN NOT NULL DEFAULT FALSE,
        status               VARCHAR NOT NULL,
        iniciado_em          TIMESTAMP,
        concluido_em         TIMESTAMP
    )
"""
```

- [ ] **Step 4: Update `inicializar_schema` in `src/storage/database_loader.py`**

Replace the body of `inicializar_schema`:

```python
    def inicializar_schema(self) -> None:
        """Cria schemas e tabelas Gold se ainda não existirem."""
        with self._conectar() as con:
            con.execute(_DDL_SCHEMA_GOLD)
            con.execute(_DDL_EVOLUCAO)
            con.execute(_DDL_AUDITORIA)
            con.execute(_DDL_GLOSAS)
            con.execute(_DDL_CACHE_NACIONAL)
            con.execute(_DDL_METRICAS_AVANCADAS)
            con.execute(_DDL_DELTA_SNAPSHOT)
            con.execute(_DDL_PROFISSIONAIS_PROCESSADOS)
            con.execute(_DDL_ESTABELECIMENTOS)
            con.execute(_DDL_CBO_LOOKUP)
            con.execute(_DDL_PIPELINE_RUNS)
        logger.info("schema_gold inicializado db=%s", self._caminho_db)
```

- [ ] **Step 5: Add write methods to `src/storage/database_loader.py`**

Add after `gravar_delta_snapshot`:

```python
    def gravar_profissionais(self, competencia: str, df: pd.DataFrame) -> None:
        """DELETE + INSERT de profissionais processados para uma competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            df: DataFrame com colunas do SCHEMA_PROFISSIONAL + ALERTA_STATUS_CH + DESCRICAO_CBO.
        """
        df_insert = df.rename(columns=str.lower).copy()
        df_insert.insert(0, "competencia", competencia)
        with self._conectar() as con:
            con.execute(
                "DELETE FROM gold.profissionais_processados WHERE competencia = ?",
                [competencia],
            )
            con.register("_tmp_prof", df_insert)
            con.execute("""
                INSERT INTO gold.profissionais_processados
                    (competencia, cpf, cnes, cns, nome_profissional, sexo, cbo,
                     tipo_vinculo, sus, ch_total, ch_ambulatorial, ch_outras,
                     ch_hospitalar, fonte, alerta_status_ch, descricao_cbo, gravado_em)
                SELECT competencia, cpf, cnes, cns, nome_profissional, sexo, cbo,
                       tipo_vinculo, sus, ch_total, ch_ambulatorial, ch_outras,
                       ch_hospitalar, fonte, alerta_status_ch, descricao_cbo,
                       CURRENT_TIMESTAMP
                FROM _tmp_prof
            """)
        logger.info("profissionais gravados competencia=%s total=%d", competencia, len(df))

    def gravar_estabelecimentos(self, competencia: str, df: pd.DataFrame) -> None:
        """DELETE + INSERT de estabelecimentos para uma competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            df: DataFrame com colunas do SCHEMA_ESTABELECIMENTO.
        """
        df_insert = df.rename(columns=str.lower).copy()
        df_insert.insert(0, "competencia", competencia)
        with self._conectar() as con:
            con.execute(
                "DELETE FROM gold.estabelecimentos WHERE competencia = ?",
                [competencia],
            )
            con.register("_tmp_estab", df_insert)
            con.execute("""
                INSERT INTO gold.estabelecimentos
                    (competencia, cnes, nome_fantasia, tipo_unidade, cnpj_mantenedora,
                     natureza_juridica, cod_municipio, vinculo_sus, fonte, gravado_em)
                SELECT competencia, cnes, nome_fantasia, tipo_unidade, cnpj_mantenedora,
                       natureza_juridica, cod_municipio, vinculo_sus, fonte, CURRENT_TIMESTAMP
                FROM _tmp_estab
            """)
        logger.info("estabelecimentos gravados competencia=%s total=%d", competencia, len(df))

    def gravar_cbo_lookup(self, competencia: str, lookup: dict[str, str]) -> None:
        """DELETE + INSERT do dicionário CBO para uma competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            lookup: Dicionário codigo_cbo → descricao.
        """
        if not lookup:
            return
        df = pd.DataFrame(
            [{"competencia": competencia, "codigo_cbo": k, "descricao": v}
             for k, v in lookup.items()]
        )
        with self._conectar() as con:
            con.execute(
                "DELETE FROM gold.cbo_lookup WHERE competencia = ?",
                [competencia],
            )
            con.register("_tmp_cbo", df)
            con.execute("INSERT INTO gold.cbo_lookup SELECT * FROM _tmp_cbo")
        logger.info("cbo_lookup gravado competencia=%s total=%d", competencia, len(lookup))

    def gravar_pipeline_run(
        self,
        competencia: str,
        local_disponivel: bool,
        nacional_disponivel: bool,
        hr_disponivel: bool,
        status: str,
    ) -> None:
        """INSERT OR REPLACE do status de execução em gold.pipeline_runs.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            local_disponivel: True se dados locais estavam disponíveis.
            nacional_disponivel: True se dados nacionais foram ingeridos.
            hr_disponivel: True se cross-check HR foi executado.
            status: 'completo' | 'parcial' | 'sem_dados_locais' | 'sem_dados'.
        """
        with self._conectar() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO gold.pipeline_runs
                    (competencia, local_disponivel, nacional_disponivel, hr_disponivel,
                     status, iniciado_em, concluido_em)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [competencia, local_disponivel, nacional_disponivel, hr_disponivel, status],
            )
        logger.info("pipeline_run gravado competencia=%s status=%s", competencia, status)
```

- [ ] **Step 6: Add read methods to `src/storage/database_loader.py`**

Add after `gravar_pipeline_run`:

```python
    def profissional_existe(self, competencia: str) -> bool:
        """Retorna True se existirem profissionais processados para a competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.

        Returns:
            True quando pelo menos uma linha existir em gold.profissionais_processados.
        """
        with self._conectar(read_only=True) as con:
            df = con.execute(
                "SELECT COUNT(*) AS n FROM gold.profissionais_processados WHERE competencia = ?",
                [competencia],
            ).df()
        return int(df["n"].iloc[0]) > 0

    def carregar_profissionais(self, competencia: str) -> pd.DataFrame:
        """Carrega profissionais processados de uma competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.

        Returns:
            DataFrame com colunas em maiúsculas (SCHEMA_PROFISSIONAL + ALERTA_STATUS_CH + DESCRICAO_CBO).
        """
        with self._conectar(read_only=True) as con:
            df = con.execute(
                """SELECT cns, cpf, nome_profissional, sexo, cbo, cnes, tipo_vinculo,
                          sus, ch_total, ch_ambulatorial, ch_outras, ch_hospitalar,
                          fonte, alerta_status_ch, descricao_cbo
                   FROM gold.profissionais_processados WHERE competencia = ?""",
                [competencia],
            ).df()
        return df.rename(columns=str.upper)

    def carregar_estabelecimentos(self, competencia: str) -> pd.DataFrame:
        """Carrega estabelecimentos de uma competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.

        Returns:
            DataFrame com colunas em maiúsculas (SCHEMA_ESTABELECIMENTO).
        """
        with self._conectar(read_only=True) as con:
            df = con.execute(
                """SELECT cnes, nome_fantasia, tipo_unidade, cnpj_mantenedora,
                          natureza_juridica, cod_municipio, vinculo_sus, fonte
                   FROM gold.estabelecimentos WHERE competencia = ?""",
                [competencia],
            ).df()
        return df.rename(columns=str.upper)

    def carregar_cbo_lookup(self, competencia: str) -> dict[str, str]:
        """Carrega dicionário CBO de uma competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.

        Returns:
            Dicionário codigo_cbo → descricao.
        """
        with self._conectar(read_only=True) as con:
            df = con.execute(
                "SELECT codigo_cbo, descricao FROM gold.cbo_lookup WHERE competencia = ?",
                [competencia],
            ).df()
        return dict(zip(df["codigo_cbo"], df["descricao"].fillna("")))
```

- [ ] **Step 7: Run tests to verify PASS**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py -v --tb=short
```

Expected: all pass (including new classes)

- [ ] **Step 8: Commit**

```bash
git add src/storage/database_loader.py tests/storage/test_database_loader.py
git commit -m "feat(storage): 4 novas tabelas Gold e metodos gravar/carregar por competencia"
```

---

### Task 3: HistoricoReader — new read methods + REGRAS_AUDITORIA

**Files:**
- Modify: `src/storage/historico_reader.py`
- Modify: `tests/storage/test_historico_reader.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/storage/test_historico_reader.py` (use the `reader` fixture already defined in the file):

```python
import duckdb
import pandas as pd


def _popular_tabelas_novas(path):
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute("""
            CREATE TABLE IF NOT EXISTS gold.profissionais_processados (
                competencia VARCHAR, cpf VARCHAR, cnes VARCHAR,
                cns VARCHAR, nome_profissional VARCHAR, sexo VARCHAR,
                cbo VARCHAR, tipo_vinculo VARCHAR, sus VARCHAR,
                ch_total INTEGER, ch_ambulatorial INTEGER, ch_outras INTEGER,
                ch_hospitalar INTEGER, fonte VARCHAR, alerta_status_ch VARCHAR,
                descricao_cbo VARCHAR, gravado_em TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS gold.estabelecimentos (
                competencia VARCHAR, cnes VARCHAR, nome_fantasia VARCHAR,
                tipo_unidade VARCHAR, cnpj_mantenedora VARCHAR,
                natureza_juridica VARCHAR, cod_municipio VARCHAR,
                vinculo_sus VARCHAR, fonte VARCHAR, gravado_em TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS gold.pipeline_runs (
                competencia VARCHAR PRIMARY KEY, local_disponivel BOOLEAN,
                nacional_disponivel BOOLEAN, hr_disponivel BOOLEAN,
                status VARCHAR, iniciado_em TIMESTAMP, concluido_em TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS gold.glosas_profissional (
                competencia VARCHAR, regra VARCHAR, cpf VARCHAR,
                cns VARCHAR, nome_profissional VARCHAR, sexo VARCHAR,
                cnes_estabelecimento VARCHAR, motivo VARCHAR,
                criado_em_firebird TIMESTAMP, criado_em_pipeline TIMESTAMP,
                atualizado_em_pipeline TIMESTAMP
            )
        """)
        con.execute("""
            INSERT INTO gold.profissionais_processados VALUES
            ('2026-03','12345678901','2795001','123456789012345','Ana Silva','F',
             '515105','30','S',40,20,10,10,'LOCAL','OK','Agente','2026-03-01')
        """)
        con.execute("""
            INSERT INTO gold.estabelecimentos VALUES
            ('2026-03','2795001','UBS Centro','01','55293427000117','1023','354130','S','LOCAL','2026-03-01')
        """)
        con.execute("""
            INSERT INTO gold.pipeline_runs VALUES
            ('2026-03',TRUE,TRUE,FALSE,'completo','2026-03-01','2026-03-01')
        """)
        con.execute("""
            INSERT INTO gold.glosas_profissional VALUES
            ('2026-03','RQ008','12345678901','123456789012345','Ana','F','2795001',
             'motivo','2026-03-01','2026-03-01','2026-03-01')
        """)


class TestNovosMétodosHistoricoReader:
    def test_carregar_profissionais(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        df = reader.carregar_profissionais("2026-03")
        assert len(df) == 1
        assert df["CPF"].iloc[0] == "12345678901"

    def test_carregar_profissionais_vazio(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        df = reader.carregar_profissionais("2025-01")
        assert df.empty

    def test_carregar_estabelecimentos(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        df = reader.carregar_estabelecimentos("2026-03")
        assert list(df["CNES"]) == ["2795001"]

    def test_carregar_pipeline_run(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        resultado = reader.carregar_pipeline_run("2026-03")
        assert resultado is not None
        assert resultado["status"] == "completo"

    def test_carregar_pipeline_run_ausente(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        assert reader.carregar_pipeline_run("2025-01") is None

    def test_carregar_glosas_periodo(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        df = reader.carregar_glosas_periodo("RQ008", "2026-03")
        assert len(df) == 1
        assert df["cpf"].iloc[0] == "12345678901"

    def test_carregar_glosas_periodo_regra_ausente(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        df = reader.carregar_glosas_periodo("RQ010", "2026-03")
        assert df.empty
```

- [ ] **Step 2: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py::TestNovosMétodosHistoricoReader -v
```

Expected: `AttributeError: 'HistoricoReader' object has no attribute 'carregar_profissionais'`

- [ ] **Step 3: Add constant and new methods to `src/storage/historico_reader.py`**

Replace `CSV_MAP` with `REGRAS_AUDITORIA` (keep `CSV_MAP` as alias for now to avoid breaking dashboard before Task 10):

```python
REGRAS_AUDITORIA: tuple[str, ...] = (
    "RQ003B", "RQ005_ACS", "RQ005_ACE", "GHOST", "MISSING",
    "RQ006", "RQ007", "RQ008", "RQ009", "RQ010", "RQ011",
)

# Mantido por compatibilidade — remover em Task 10
CSV_MAP: dict[str, str] = {
    "RQ003B":    "auditoria_rq003b_multiplas_unidades.csv",
    "RQ005_ACS": "auditoria_rq005_acs_tacs_incorretos.csv",
    "RQ005_ACE": "auditoria_rq005_ace_tace_incorretos.csv",
    "GHOST":     "auditoria_ghost_payroll.csv",
    "MISSING":   "auditoria_missing_registration.csv",
    "RQ006":     "auditoria_rq006_estab_fantasma.csv",
    "RQ007":     "auditoria_rq007_estab_ausente_local.csv",
    "RQ008":     "auditoria_rq008_prof_fantasma_cns.csv",
    "RQ009":     "auditoria_rq009_prof_ausente_local_cns.csv",
    "RQ010":     "auditoria_rq010_divergencia_cbo.csv",
    "RQ011":     "auditoria_rq011_divergencia_ch.csv",
}
```

Add methods after `carregar_delta_snapshot`:

```python
    def carregar_profissionais(self, competencia: str) -> pd.DataFrame:
        """Carrega profissionais processados de uma competência.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            DataFrame com colunas em maiúsculas; vazio se ausente.
        """
        try:
            df = self._ler_df(
                """SELECT cns, cpf, nome_profissional, sexo, cbo, cnes, tipo_vinculo,
                          sus, ch_total, ch_ambulatorial, ch_outras, ch_hospitalar,
                          fonte, alerta_status_ch, descricao_cbo
                   FROM gold.profissionais_processados WHERE competencia = ?""",
                [competencia],
            )
        except duckdb.CatalogException:
            return pd.DataFrame()
        return df.rename(columns=str.upper)

    def carregar_estabelecimentos(self, competencia: str) -> pd.DataFrame:
        """Carrega estabelecimentos de uma competência.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            DataFrame com colunas em maiúsculas; vazio se ausente.
        """
        try:
            df = self._ler_df(
                """SELECT cnes, nome_fantasia, tipo_unidade, cnpj_mantenedora,
                          natureza_juridica, cod_municipio, vinculo_sus, fonte
                   FROM gold.estabelecimentos WHERE competencia = ?""",
                [competencia],
            )
        except duckdb.CatalogException:
            return pd.DataFrame()
        return df.rename(columns=str.upper)

    def carregar_pipeline_run(self, competencia: str) -> dict | None:
        """Retorna o registro de execução de pipeline para uma competência.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            Dict com todas as colunas de gold.pipeline_runs, ou None se ausente.
        """
        try:
            df = self._ler_df(
                "SELECT * FROM gold.pipeline_runs WHERE competencia = ?",
                [competencia],
            )
        except duckdb.CatalogException:
            return None
        if df.empty:
            return None
        return df.iloc[0].to_dict()

    def carregar_glosas_periodo(self, regra: str, competencia: str) -> pd.DataFrame:
        """Carrega glosas de uma regra e competência de gold.glosas_profissional.

        Substitui carregar_registros (que lia de CSVs arquivados).

        Args:
            regra: Código da regra (ex: 'RQ008').
            competencia: Competência no formato YYYY-MM.

        Returns:
            DataFrame com registros ou DataFrame vazio se ausente.
        """
        try:
            return self._ler_df(
                "SELECT * FROM gold.glosas_profissional WHERE competencia = ? AND regra = ?",
                [competencia, regra],
            )
        except duckdb.CatalogException:
            return pd.DataFrame()
```

- [ ] **Step 4: Run tests to verify PASS**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py -v --tb=short
```

Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add src/storage/historico_reader.py tests/storage/test_historico_reader.py
git commit -m "feat(storage): HistoricoReader — carregar_profissionais, estabelecimentos, pipeline_run, glosas_periodo"
```

---

### Task 4: IngestaoLocalStage — DuckDB-first + period-aware

**Files:**
- Modify: `src/pipeline/stages/ingestao_local.py`
- Modify: `tests/pipeline/stages/test_ingestao_local.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/pipeline/stages/test_ingestao_local.py` (after existing tests):

```python
from storage.database_loader import DatabaseLoader
from storage.competencia_utils import periodo_atual


def _state_basico(force: bool = False) -> PipelineState:
    return PipelineState(
        competencia_ano=2024,
        competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True,
        executar_hr=False,
        force_reingestao=force,
    )


@pytest.fixture
def db_vazio(tmp_path):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    return loader, tmp_path


def test_carrega_do_duckdb_quando_dados_existem(tmp_path):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    loader.gravar_profissionais("2024-12", _df_prof())
    loader.gravar_estabelecimentos("2024-12", _df_estab())
    loader.gravar_cbo_lookup("2024-12", {"515105": "ACS"})

    state = _state_basico()
    IngestaoLocalStage(tmp_path, loader).execute(state)

    assert state.snapshot_carregado is True
    assert state.local_disponivel is True
    assert len(state.df_prof_local) == 1
    assert state.cbo_lookup == {"515105": "ACS"}


def test_backfill_do_parquet_quando_duckdb_vazio(tmp_path):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    snap = SnapshotLocal(df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={"515105": "X"})
    salvar_snapshot("2024-12", tmp_path, snap)

    state = _state_basico()
    IngestaoLocalStage(tmp_path, loader).execute(state)

    assert state.snapshot_carregado is True
    assert state.local_disponivel is True
    assert loader.profissional_existe("2024-12")


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_consulta_firebird_para_periodo_atual_sem_dados(
    mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, mock_periodo, tmp_path
):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    mock_con = MagicMock()
    mock_conectar.return_value = mock_con
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state_basico()
    IngestaoLocalStage(tmp_path, loader).execute(state)

    mock_conectar.assert_called_once()
    assert state.local_disponivel is True
    assert state.snapshot_carregado is False


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2026-04")
def test_marca_indisponivel_para_periodo_passado_sem_dados(mock_periodo, tmp_path):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()

    state = _state_basico()
    IngestaoLocalStage(tmp_path, loader).execute(state)

    assert state.local_disponivel is False


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
@patch("pipeline.stages.ingestao_local.extrair_lookup_cbo")
@patch("pipeline.stages.ingestao_local.CnesLocalAdapter")
@patch("pipeline.stages.ingestao_local.ProfissionalContract")
@patch("pipeline.stages.ingestao_local.EstabelecimentoContract")
def test_force_reingestao_usa_firebird_no_periodo_atual(
    mock_ec, mock_pc, mock_adapter_cls, mock_cbo, mock_conectar, mock_periodo, tmp_path
):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    loader.gravar_profissionais("2024-12", _df_prof())
    mock_con = MagicMock()
    mock_conectar.return_value = mock_con
    mock_cbo.return_value = {}
    mock_adapter = MagicMock()
    mock_adapter.listar_profissionais.return_value = _df_prof()
    mock_adapter.listar_estabelecimentos.return_value = _df_estab()
    mock_adapter_cls.return_value = mock_adapter

    state = _state_basico(force=True)
    IngestaoLocalStage(tmp_path, loader).execute(state)

    mock_conectar.assert_called_once()


@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2026-04")
def test_force_reingestao_ignora_firebird_para_periodo_passado(mock_periodo, tmp_path):
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    loader.gravar_profissionais("2024-12", _df_prof())

    state = _state_basico(force=True)
    with patch("pipeline.stages.ingestao_local.conectar") as mock_con:
        IngestaoLocalStage(tmp_path, loader).execute(state)
        mock_con.assert_not_called()
```

- [ ] **Step 2: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_ingestao_local.py::test_carrega_do_duckdb_quando_dados_existem tests/pipeline/stages/test_ingestao_local.py::test_marca_indisponivel_para_periodo_passado_sem_dados -v
```

Expected: `TypeError` (constructor mismatch)

- [ ] **Step 3: Replace `src/pipeline/stages/ingestao_local.py` entirely**

```python
"""IngestaoLocalStage — DuckDB-first, parquet backfill, Firebird apenas no período atual."""
import logging
from pathlib import Path

from contracts.schemas import EstabelecimentoContract, ProfissionalContract
from ingestion.cnes_client import conectar, extrair_lookup_cbo
from ingestion.cnes_local_adapter import CnesLocalAdapter
from pipeline.state import PipelineState
from storage.competencia_utils import periodo_atual
from storage.database_loader import DatabaseLoader
from storage.snapshot_local import SnapshotLocal, carregar_snapshot, snapshot_existe

logger = logging.getLogger(__name__)


class IngestaoLocalStage:
    nome = "ingestao_local"

    def __init__(self, historico_dir: Path, db_loader: DatabaseLoader) -> None:
        self._historico_dir = historico_dir
        self._db = db_loader

    def execute(self, state: PipelineState) -> None:
        competencia = state.competencia_str
        eh_periodo_atual = competencia == periodo_atual()

        if state.force_reingestao and eh_periodo_atual:
            self._ingerir_do_firebird(state)
            return

        if state.force_reingestao and not eh_periodo_atual:
            logger.warning("force_reingestao_ignorado_periodo_passado competencia=%s", competencia)

        if self._db.profissional_existe(competencia):
            self._carregar_do_duckdb(state)
            return

        if snapshot_existe(competencia, self._historico_dir):
            self._backfill_do_parquet(state)
            return

        if eh_periodo_atual:
            self._ingerir_do_firebird(state)
            return

        state.local_disponivel = False
        logger.info("dados_locais_indisponiveis competencia=%s", competencia)

    def _carregar_do_duckdb(self, state: PipelineState) -> None:
        competencia = state.competencia_str
        state.df_prof_local = self._db.carregar_profissionais(competencia)
        state.df_estab_local = self._db.carregar_estabelecimentos(competencia)
        state.cbo_lookup = self._db.carregar_cbo_lookup(competencia)
        state.snapshot_carregado = True
        logger.info("local_duckdb carregado competencia=%s", competencia)

    def _backfill_do_parquet(self, state: PipelineState) -> None:
        competencia = state.competencia_str
        snap = carregar_snapshot(competencia, self._historico_dir)
        state.df_prof_local = snap.df_prof
        state.df_estab_local = snap.df_estab
        state.cbo_lookup = snap.cbo_lookup
        self._db.gravar_profissionais(competencia, snap.df_prof)
        self._db.gravar_estabelecimentos(competencia, snap.df_estab)
        self._db.gravar_cbo_lookup(competencia, snap.cbo_lookup)
        state.snapshot_carregado = True
        logger.info("local_parquet_backfill competencia=%s", competencia)

    def _ingerir_do_firebird(self, state: PipelineState) -> None:
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

- [ ] **Step 4: Update existing tests in `test_ingestao_local.py` that use old constructor**

The three existing tests (`test_popula_state_com_dados_locais`, `test_valida_contratos_apos_ingestao`, `test_falha_conexao_propaga_excecao`) use `IngestaoLocalStage(tmp_path)`. Update each to `IngestaoLocalStage(tmp_path, loader)` and add `loader` construction:

For `test_popula_state_com_dados_locais`, add before `IngestaoLocalStage(tmp_path).execute(state)`:
```python
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
    IngestaoLocalStage(tmp_path, loader).execute(state)
```

Apply the same pattern to `test_valida_contratos_apos_ingestao` and `test_falha_conexao_propaga_excecao`. Add `from storage.database_loader import DatabaseLoader` at the top of the test file.

Also patch `periodo_atual` to return `"2024-12"` in those three tests so they exercise the Firebird path:

```python
@patch("pipeline.stages.ingestao_local.periodo_atual", return_value="2024-12")
@patch("pipeline.stages.ingestao_local.conectar")
# ... other patches
def test_popula_state_com_dados_locais(mock_periodo, ..., tmp_path):
```

- [ ] **Step 5: Run all ingestao_local tests**

```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_ingestao_local.py -v --tb=short
```

Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add src/pipeline/stages/ingestao_local.py tests/pipeline/stages/test_ingestao_local.py
git commit -m "feat(pipeline): IngestaoLocalStage — DuckDB-first, backfill parquet, period-aware"
```

---

### Task 5: SnapshotLocalStage — write to DuckDB + updated constructor

**Files:**
- Modify: `src/pipeline/stages/snapshot_local.py`
- Modify: `tests/pipeline/stages/test_snapshot_local_stage.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/pipeline/stages/test_snapshot_local_stage.py`:

```python
from storage.database_loader import DatabaseLoader


def _state_com_db(tmp_path, snapshot_carregado: bool = False, force: bool = False) -> PipelineState:
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


class TestSnapshotLocalStageDuckDB:
    def test_grava_profissionais_no_duckdb(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state_com_db(tmp_path)
        SnapshotLocalStage(tmp_path, loader).execute(state)
        assert loader.profissional_existe("2026-03")

    def test_grava_estabelecimentos_no_duckdb(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state_com_db(tmp_path)
        SnapshotLocalStage(tmp_path, loader).execute(state)
        df = loader.carregar_estabelecimentos("2026-03")
        assert len(df) == 1

    def test_grava_cbo_lookup_no_duckdb(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state_com_db(tmp_path)
        SnapshotLocalStage(tmp_path, loader).execute(state)
        assert loader.carregar_cbo_lookup("2026-03") == {"515105": "Agente Comunitário"}

    def test_nao_grava_quando_snapshot_carregado(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        state = _state_com_db(tmp_path, snapshot_carregado=True)
        SnapshotLocalStage(tmp_path, loader).execute(state)
        assert not loader.profissional_existe("2026-03")
```

- [ ] **Step 2: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_snapshot_local_stage.py::TestSnapshotLocalStageDuckDB -v
```

Expected: `TypeError` (constructor mismatch)

- [ ] **Step 3: Replace `src/pipeline/stages/snapshot_local.py` entirely**

```python
"""SnapshotLocalStage — persiste snapshot pós-processamento em parquet e DuckDB."""
import json
import logging
from pathlib import Path

from analysis.delta_snapshot import DeltaSnapshot, calcular_delta
from pipeline.state import PipelineState
from storage.database_loader import DatabaseLoader
from storage.snapshot_local import (
    SnapshotLocal,
    carregar_snapshot,
    salvar_snapshot,
    snapshot_existe,
)

logger = logging.getLogger(__name__)


class SnapshotLocalStage:
    nome = "snapshot_local"

    def __init__(self, historico_dir: Path, db_loader: DatabaseLoader) -> None:
        self._historico_dir = historico_dir
        self._db = db_loader

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
        self._db.gravar_profissionais(competencia, state.df_processado)
        self._db.gravar_estabelecimentos(competencia, state.df_estab_local)
        self._db.gravar_cbo_lookup(competencia, state.cbo_lookup)
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

- [ ] **Step 4: Update existing `test_snapshot_local_stage.py` tests to use new constructor**

In `TestSnapshotLocalStage` class, add a `loader` fixture to each test:

```python
# At top of file add:
from storage.database_loader import DatabaseLoader

# Update _state() helper to accept tmp_path (already done in tests above)
# Update each TestSnapshotLocalStage test to use:
SnapshotLocalStage(tmp_path, loader).execute(state)
# where loader = DatabaseLoader(tmp_path / "test.duckdb") with inicializar_schema()
```

Specifically: each test in `TestSnapshotLocalStage` should add:
```python
    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()
```
before calling `SnapshotLocalStage(tmp_path, loader).execute(state)`.

- [ ] **Step 5: Run all snapshot_local_stage tests**

```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_snapshot_local_stage.py -v --tb=short
```

Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add src/pipeline/stages/snapshot_local.py tests/pipeline/stages/test_snapshot_local_stage.py
git commit -m "feat(pipeline): SnapshotLocalStage grava profissionais e estabelecimentos no DuckDB"
```

---

### Task 6: ProcessamentoStage + IngestaoNacionalStage minor changes

**Files:**
- Modify: `src/pipeline/stages/processamento.py`
- Modify: `src/pipeline/stages/ingestao_nacional.py`
- Modify: `tests/pipeline/stages/test_processamento.py`
- Modify: `tests/pipeline/stages/test_ingestao_nacional.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/pipeline/stages/test_processamento.py`:

```python
def test_skip_quando_local_indisponivel():
    state = PipelineState(
        competencia_ano=2024, competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=False, executar_hr=False,
        local_disponivel=False,
    )
    ProcessamentoStage().execute(state)
    assert state.df_processado.empty
```

Add to `tests/pipeline/stages/test_ingestao_nacional.py`:

```python
def test_seta_nacional_disponivel_apos_busca(tmp_path):
    # Build a state with local data so fingerprint works
    state = PipelineState(
        competencia_ano=2024, competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True, executar_hr=False,
    )
    state.df_processado = pd.DataFrame({"CPF": ["001"], "CBO": ["515105"], "CNES": ["111"]})

    loader = DatabaseLoader(tmp_path / "test.duckdb")
    loader.inicializar_schema()

    with patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter") as mock_cls, \
         patch("pipeline.stages.ingestao_nacional.config") as mock_cfg:
        mock_cfg.NACIONAL_CACHE_TTL_DIAS = 7
        mock_cfg.GCP_PROJECT_ID = "proj"
        mock_cfg.ID_MUNICIPIO_IBGE7 = "3543105"
        mock_cfg.CACHE_DIR = tmp_path
        mock_adapter = MagicMock()
        mock_adapter.listar_profissionais.return_value = pd.DataFrame({"CNS": ["123456789012345"]})
        mock_adapter.listar_estabelecimentos.return_value = pd.DataFrame({"CNES": ["1234567"]})
        mock_cls.return_value = mock_adapter

        IngestaoNacionalStage(loader).execute(state)

    assert state.nacional_disponivel is True
```

- [ ] **Step 2: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_processamento.py::test_skip_quando_local_indisponivel tests/pipeline/stages/test_ingestao_nacional.py::test_seta_nacional_disponivel_apos_busca -v
```

Expected: FAIL

- [ ] **Step 3: Update `src/pipeline/stages/processamento.py`**

Replace the file entirely:

```python
"""ProcessamentoStage — limpeza CPF, datas ISO, dedup."""
import logging

from processing.transformer import transformar
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)


class ProcessamentoStage:
    nome = "processamento"

    def execute(self, state: PipelineState) -> None:
        if not state.local_disponivel:
            return
        state.df_processado = transformar(
            state.df_prof_local, cbo_lookup=state.cbo_lookup
        )
        logger.info("processamento registros=%d", len(state.df_processado))
```

- [ ] **Step 4: Update `src/pipeline/stages/ingestao_nacional.py`**

In `execute`, after `state.nacional_validado = True`, add:

```python
        state.nacional_disponivel = (
            not state.df_prof_nacional.empty or not state.df_estab_nacional.empty
        )
```

- [ ] **Step 5: Run tests to verify PASS**

```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_processamento.py tests/pipeline/stages/test_ingestao_nacional.py -v --tb=short
```

Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add src/pipeline/stages/processamento.py src/pipeline/stages/ingestao_nacional.py \
        tests/pipeline/stages/test_processamento.py tests/pipeline/stages/test_ingestao_nacional.py
git commit -m "feat(pipeline): ProcessamentoStage e IngestaoNacionalStage — guards de disponibilidade"
```

---

### Task 7: AuditoriaLocalStage + AuditoriaNacionalStage guards

**Files:**
- Modify: `src/pipeline/stages/auditoria_local.py`
- Modify: `src/pipeline/stages/auditoria_nacional.py`
- Modify: `tests/pipeline/stages/test_auditoria_local.py`
- Modify: `tests/pipeline/stages/test_auditoria_nacional.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/pipeline/stages/test_auditoria_local.py`:

```python
def test_skip_quando_local_indisponivel():
    from pipeline.state import PipelineState
    from pipeline.stages.auditoria_local import AuditoriaLocalStage
    state = PipelineState(
        competencia_ano=2024, competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=False, executar_hr=False,
        local_disponivel=False,
    )
    AuditoriaLocalStage().execute(state)
    assert state.df_multi_unidades.empty
    assert state.df_acs_incorretos.empty
```

Add to `tests/pipeline/stages/test_auditoria_nacional.py`:

```python
from pipeline.stages.auditoria_nacional import PeriodoInvariantError


def _state_com_flags(local: bool, nacional: bool) -> PipelineState:
    state = _state()
    state.local_disponivel = local
    state.nacional_disponivel = nacional
    return state


def test_skip_quando_nenhum_disponivel():
    state = _state_com_flags(local=False, nacional=False)
    state.df_estab_nacional = pd.DataFrame()
    state.df_prof_nacional = pd.DataFrame()
    with patch("pipeline.stages.auditoria_nacional.detectar_estabelecimentos_fantasma") as mock:
        AuditoriaNacionalStage().execute(state)
        mock.assert_not_called()


def test_skip_cross_check_quando_so_nacional_disponivel():
    state = _state_com_flags(local=False, nacional=True)
    with patch("pipeline.stages.auditoria_nacional.detectar_profissionais_fantasma") as mock:
        AuditoriaNacionalStage().execute(state)
        mock.assert_not_called()


def test_skip_cross_check_quando_so_local_disponivel():
    state = _state_com_flags(local=True, nacional=False)
    state.df_estab_nacional = pd.DataFrame()
    state.df_prof_nacional = pd.DataFrame()
    with patch("pipeline.stages.auditoria_nacional.detectar_profissionais_fantasma") as mock:
        AuditoriaNacionalStage().execute(state)
        mock.assert_not_called()


def test_periodo_invariant_error_quando_flags_inconsistentes():
    state = _state_com_flags(local=True, nacional=True)
    state.df_estab_nacional = pd.DataFrame()
    state.df_prof_nacional = pd.DataFrame()
    with pytest.raises(PeriodoInvariantError):
        AuditoriaNacionalStage().execute(state)
```

- [ ] **Step 2: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_auditoria_nacional.py::test_skip_quando_nenhum_disponivel tests/pipeline/stages/test_auditoria_nacional.py::test_periodo_invariant_error_quando_flags_inconsistentes -v
```

Expected: FAIL / ImportError

- [ ] **Step 3: Update `src/pipeline/stages/auditoria_local.py`**

Add guard at the start of `execute`:

```python
    def execute(self, state: PipelineState) -> None:
        if not state.local_disponivel:
            logger.info("auditoria_local=skipped motivo=sem_dados_locais competencia=%s", state.competencia_str)
            return
        state.df_multi_unidades = detectar_multiplas_unidades(state.df_processado)
        # ... rest unchanged
```

- [ ] **Step 4: Replace `src/pipeline/stages/auditoria_nacional.py` entirely**

```python
"""AuditoriaNacionalStage — cruzamento com dados nacionais BigQuery."""
import logging
from typing import Final

import config
from analysis.cascade_resolver import resolver_lag_rq006
from analysis.rules_engine import (
    detectar_divergencia_cbo,
    detectar_divergencia_carga_horaria,
    detectar_estabelecimentos_ausentes_local,
    detectar_estabelecimentos_fantasma,
    detectar_profissionais_ausentes_local,
    detectar_profissionais_fantasma,
)
from analysis.verificacao_cache import CachingVerificadorCnes
from ingestion.cnes_oficial_web_adapter import CnesOficialWebAdapter
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)

_TIPOS_EXCLUIR_RQ007: Final[frozenset[str]] = frozenset({"22"})


class PeriodoInvariantError(Exception):
    """Flags de disponibilidade inconsistentes com dados reais no estado do pipeline."""


class AuditoriaNacionalStage:
    nome = "auditoria_nacional"

    def execute(self, state: PipelineState) -> None:
        if not state.local_disponivel and not state.nacional_disponivel:
            logger.warning(
                "auditoria_nacional=skipped motivo=sem_dados competencia=%s",
                state.competencia_str,
            )
            return

        pode_cruzar = state.local_disponivel and state.nacional_disponivel
        if not pode_cruzar:
            logger.info(
                "cross_check=skipped motivo=dados_parciais local=%s nacional=%s competencia=%s",
                state.local_disponivel, state.nacional_disponivel, state.competencia_str,
            )
            return

        if state.df_estab_nacional.empty and state.df_prof_nacional.empty:
            raise PeriodoInvariantError(
                f"nacional_disponivel=True mas dados nacionais vazios competencia={state.competencia_str}"
            )

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

- [ ] **Step 5: Update existing test `test_skip_quando_nacionais_vazios` in `test_auditoria_nacional.py`**

The existing test `test_skip_quando_nacionais_vazios` sets both DataFrames empty but doesn't set `local_disponivel=False`. With the new code, both flags must be False to skip. Update the test:

```python
def test_skip_quando_nacionais_vazios():
    state = _state()
    state.local_disponivel = False
    state.nacional_disponivel = False
    state.df_estab_nacional = pd.DataFrame()
    state.df_prof_nacional = pd.DataFrame()
    AuditoriaNacionalStage().execute(state)
    # assert no cross-check functions called (use mocks as before)
```

Or alternatively: set `local_disponivel=True, nacional_disponivel=True` with empty DataFrames to test `PeriodoInvariantError`. Adjust based on what the original test was checking.

- [ ] **Step 6: Run all auditoria tests**

```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_auditoria_local.py tests/pipeline/stages/test_auditoria_nacional.py -v --tb=short
```

Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add src/pipeline/stages/auditoria_local.py src/pipeline/stages/auditoria_nacional.py \
        tests/pipeline/stages/test_auditoria_local.py tests/pipeline/stages/test_auditoria_nacional.py
git commit -m "feat(pipeline): AuditoriaLocalStage e AuditoriaNacionalStage — guards OR/AND e PeriodoInvariantError"
```

---

### Task 8: MetricasStage + ExportacaoStage

**Files:**
- Modify: `src/pipeline/stages/metricas.py`
- Modify: `src/pipeline/stages/exportacao.py`
- Modify: `tests/pipeline/stages/test_metricas_stage.py`
- Modify: `tests/pipeline/stages/test_exportacao.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/pipeline/stages/test_metricas_stage.py`:

```python
def test_skip_quando_local_indisponivel():
    from pipeline.stages.metricas import MetricasStage
    from storage.database_loader import DatabaseLoader
    from storage.historico_reader import HistoricoReader

    state = PipelineState(
        competencia_ano=2024, competencia_mes=12,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=False, executar_hr=False,
        local_disponivel=False,
    )
    db = MagicMock(spec=DatabaseLoader)
    reader = MagicMock(spec=HistoricoReader)
    MetricasStage(db, reader).execute(state)
    db.gravar_metricas_avancadas.assert_not_called()
```

Add to `tests/pipeline/stages/test_exportacao.py` (after existing tests):

```python
@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.config")
def test_nao_escreve_csv(mock_config, mock_salvar, mock_criar, mock_loader_cls, tmp_path):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.DUCKDB_PATH = tmp_path / "test.duckdb"
    mock_config.LAST_RUN_PATH = tmp_path / "cache" / "last_run.json"
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    loader_instance = MagicMock()
    mock_loader_cls.return_value = loader_instance

    state = _state()
    state.output_path = tmp_path / "processed" / "Relatorio_2024-12.csv"
    ExportacaoStage().execute(state)

    assert not (tmp_path / "processed" / "Relatorio_2024-12.csv").exists()


@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.config")
def test_grava_pipeline_run(mock_config, mock_salvar, mock_criar, mock_loader_cls, tmp_path):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.DUCKDB_PATH = tmp_path / "test.duckdb"
    mock_config.LAST_RUN_PATH = tmp_path / "cache" / "last_run.json"
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    loader_instance = MagicMock()
    mock_loader_cls.return_value = loader_instance

    state = _state()
    state.output_path = tmp_path / "processed" / "report.csv"
    ExportacaoStage().execute(state)

    loader_instance.gravar_pipeline_run.assert_called_once()
    call_args = loader_instance.gravar_pipeline_run.call_args[0]
    assert call_args[0] == "2024-12"
    assert call_args[4] == "completo"


@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.config")
def test_status_sem_dados_locais_quando_local_indisponivel(mock_config, mock_loader_cls, tmp_path):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.DUCKDB_PATH = tmp_path / "test.duckdb"
    mock_config.LAST_RUN_PATH = tmp_path / "cache" / "last_run.json"
    loader_instance = MagicMock()
    mock_loader_cls.return_value = loader_instance

    state = _state()
    state.local_disponivel = False
    state.nacional_disponivel = True
    ExportacaoStage().execute(state)

    call_args = loader_instance.gravar_pipeline_run.call_args[0]
    assert call_args[4] == "sem_dados_locais"
```

- [ ] **Step 2: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_metricas_stage.py::test_skip_quando_local_indisponivel tests/pipeline/stages/test_exportacao.py::test_nao_escreve_csv -v
```

Expected: FAIL

- [ ] **Step 3: Update `src/pipeline/stages/metricas.py`**

Add guard at top of `execute`:

```python
    def execute(self, state: PipelineState) -> None:
        if not state.local_disponivel:
            logger.info("metricas=skipped motivo=sem_dados_locais competencia=%s", state.competencia_str)
            return
        # ... rest of existing code unchanged
```

- [ ] **Step 4: Replace `src/pipeline/stages/exportacao.py` entirely**

```python
"""ExportacaoStage — persistência DuckDB, JSON e pipeline_runs. Sem CSV/XLSX em disco."""
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

import config
from analysis.evolution_tracker import criar_snapshot, salvar_snapshot
from pipeline.state import PipelineState
from storage.database_loader import DatabaseLoader

logger = logging.getLogger(__name__)


def _gravar_last_run(state: PipelineState, last_run_path: Path) -> None:
    agora = datetime.now().isoformat(timespec="seconds")
    nacional_ok = state.executar_nacional and not state.df_prof_nacional.empty
    hr_ok = state.executar_hr
    dados = {
        "firebird": {"ts": agora, "ok": state.local_disponivel},
        "bigquery": {"ts": agora if nacional_ok else None, "ok": nacional_ok},
        "hr": {"ts": agora if hr_ok else None, "ok": hr_ok if state.executar_hr else None},
        "duckdb": {"ts": agora, "ok": True},
    }
    last_run_path.parent.mkdir(parents=True, exist_ok=True)
    last_run_path.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")


def _status_pipeline(state: PipelineState) -> str:
    if state.local_disponivel and state.nacional_disponivel:
        return "completo"
    if state.local_disponivel:
        return "parcial"
    if state.nacional_disponivel:
        return "sem_dados_locais"
    return "sem_dados"


class ExportacaoStage:
    nome = "exportacao"

    def execute(self, state: PipelineState) -> None:
        self._persistir_historico(state)

    def _persistir_historico(self, state: PipelineState) -> None:
        competencia = state.competencia_str
        loader = DatabaseLoader(config.DUCKDB_PATH)
        loader.inicializar_schema()

        if state.local_disponivel:
            snapshot = criar_snapshot(
                competencia,
                state.df_processado,
                state.df_ghost,
                state.df_missing,
                state.df_multi_unidades,
                state.df_acs_incorretos,
                state.df_ace_incorretos,
            )
            salvar_snapshot(snapshot, config.SNAPSHOTS_DIR)
            loader.gravar_metricas(snapshot)
            loader.gravar_auditoria(competencia, "GHOST", snapshot.total_ghost)
            loader.gravar_auditoria(competencia, "MISSING", snapshot.total_missing)
            loader.gravar_auditoria(competencia, "RQ005", snapshot.total_rq005)
            loader.gravar_auditoria(competencia, "RQ003B", len(state.df_multi_unidades))
            loader.gravar_auditoria(competencia, "RQ005_ACS", len(state.df_acs_incorretos))
            loader.gravar_auditoria(competencia, "RQ005_ACE", len(state.df_ace_incorretos))
            loader.gravar_auditoria(competencia, "RQ006", len(state.df_estab_fantasma))
            loader.gravar_auditoria(competencia, "RQ007", len(state.df_estab_ausente))
            loader.gravar_auditoria(competencia, "RQ008", len(state.df_prof_fantasma))
            loader.gravar_auditoria(competencia, "RQ009", len(state.df_prof_ausente))
            loader.gravar_auditoria(competencia, "RQ010", len(state.df_cbo_diverg))
            loader.gravar_auditoria(competencia, "RQ011", len(state.df_ch_diverg))

        _gravar_last_run(state, config.LAST_RUN_PATH)
        loader.gravar_pipeline_run(
            competencia,
            local_disponivel=state.local_disponivel,
            nacional_disponivel=state.nacional_disponivel,
            hr_disponivel=state.executar_hr,
            status=_status_pipeline(state),
        )
        logger.info("exportacao concluida competencia=%s", competencia)
```

- [ ] **Step 5: Run tests to verify PASS**

```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_metricas_stage.py tests/pipeline/stages/test_exportacao.py -v --tb=short
```

Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add src/pipeline/stages/metricas.py src/pipeline/stages/exportacao.py \
        tests/pipeline/stages/test_metricas_stage.py tests/pipeline/stages/test_exportacao.py
git commit -m "feat(pipeline): MetricasStage guard + ExportacaoStage sem CSV/XLSX, grava pipeline_runs"
```

---

### Task 9: `exportar_xlsx_periodo` + dashboard download button

**Files:**
- Modify: `src/export/report_generator.py`
- Modify: `scripts/pages/5_Metricas.py`
- Modify: `tests/export/test_report_generator.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/export/test_report_generator.py`:

```python
import io
from pathlib import Path

import duckdb
import pytest

from export.report_generator import exportar_xlsx_periodo


def _popular_duckdb_para_export(path: Path) -> None:
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute("""
            CREATE TABLE gold.profissionais_processados (
                competencia VARCHAR, cpf VARCHAR, cnes VARCHAR, cns VARCHAR,
                nome_profissional VARCHAR, sexo VARCHAR, cbo VARCHAR,
                tipo_vinculo VARCHAR, sus VARCHAR, ch_total INTEGER,
                ch_ambulatorial INTEGER, ch_outras INTEGER, ch_hospitalar INTEGER,
                fonte VARCHAR, alerta_status_ch VARCHAR, descricao_cbo VARCHAR,
                gravado_em TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE gold.glosas_profissional (
                competencia VARCHAR, regra VARCHAR, cpf VARCHAR, cns VARCHAR,
                nome_profissional VARCHAR, sexo VARCHAR, cnes_estabelecimento VARCHAR,
                motivo VARCHAR, criado_em_firebird TIMESTAMP,
                criado_em_pipeline TIMESTAMP, atualizado_em_pipeline TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE gold.metricas_avancadas (
                competencia VARCHAR PRIMARY KEY, taxa_anomalia_geral DOUBLE,
                p90_ch_total DOUBLE, proporcao_feminina_geral DOUBLE,
                n_reincidentes INTEGER, taxa_resolucao DOUBLE,
                velocidade_regularizacao_media DOUBLE, top_glosas_json VARCHAR,
                anomalias_por_cbo_json VARCHAR, proporcao_feminina_por_cnes_json VARCHAR,
                ranking_cnes_json VARCHAR, gravado_em TIMESTAMP
            )
        """)
        con.execute("""
            INSERT INTO gold.profissionais_processados VALUES
            ('2026-03','12345678901','2795001','123456789012345','Ana','F',
             '515105','30','S',40,20,10,10,'LOCAL','OK','ACS','2026-03-01')
        """)
        con.execute("""
            INSERT INTO gold.glosas_profissional VALUES
            ('2026-03','RQ008','12345678901','123456789012345','Ana','F',
             '2795001','motivo',NULL,'2026-03-01','2026-03-01')
        """)
        con.execute("""
            INSERT INTO gold.metricas_avancadas VALUES
            ('2026-03',0.1,40.0,0.6,2,0.5,3.0,'[]','[]','[]','[]','2026-03-01')
        """)


class TestExportarXlsxPeriodo:
    def test_retorna_bytes(self, tmp_path):
        _popular_duckdb_para_export(tmp_path / "test.duckdb")
        resultado = exportar_xlsx_periodo("2026-03", tmp_path / "test.duckdb")
        assert isinstance(resultado, bytes)
        assert len(resultado) > 0

    def test_bytes_e_xlsx_valido(self, tmp_path):
        from openpyxl import load_workbook
        _popular_duckdb_para_export(tmp_path / "test.duckdb")
        resultado = exportar_xlsx_periodo("2026-03", tmp_path / "test.duckdb")
        wb = load_workbook(io.BytesIO(resultado))
        assert "Profissionais" in wb.sheetnames

    def test_contem_aba_metricas(self, tmp_path):
        from openpyxl import load_workbook
        _popular_duckdb_para_export(tmp_path / "test.duckdb")
        resultado = exportar_xlsx_periodo("2026-03", tmp_path / "test.duckdb")
        wb = load_workbook(io.BytesIO(resultado))
        assert "Metricas" in wb.sheetnames

    def test_periodo_sem_dados_retorna_bytes(self, tmp_path):
        _popular_duckdb_para_export(tmp_path / "test.duckdb")
        resultado = exportar_xlsx_periodo("2025-01", tmp_path / "test.duckdb")
        assert isinstance(resultado, bytes)
```

- [ ] **Step 2: Run to verify FAIL**

```
./venv/Scripts/python.exe -m pytest tests/export/test_report_generator.py::TestExportarXlsxPeriodo -v
```

Expected: `ImportError: cannot import name 'exportar_xlsx_periodo'`

- [ ] **Step 3: Add `exportar_xlsx_periodo` to `src/export/report_generator.py`**

Add at the end of the file (after all existing code):

```python
import io as _io

import duckdb as _duckdb
from openpyxl import Workbook as _Workbook

from storage.historico_reader import REGRAS_AUDITORIA


def exportar_xlsx_periodo(competencia: str, duckdb_path: Path) -> bytes:
    """Gera XLSX em memória para uma competência a partir do DuckDB.

    Args:
        competencia: Competência no formato YYYY-MM.
        duckdb_path: Caminho para o arquivo DuckDB.

    Returns:
        Bytes do arquivo XLSX gerado.
    """
    wb = _Workbook()
    wb.remove(wb.active)

    with _duckdb.connect(str(duckdb_path), read_only=True) as con:
        _adicionar_aba_xlsx(wb, "Profissionais", _ler_df_safe(con, """
            SELECT cns, cpf, nome_profissional, sexo, cbo, cnes, tipo_vinculo,
                   sus, ch_total, ch_ambulatorial, ch_outras, ch_hospitalar,
                   fonte, alerta_status_ch, descricao_cbo
            FROM gold.profissionais_processados WHERE competencia = ?
        """, [competencia]))

        for regra in REGRAS_AUDITORIA:
            df_regra = _ler_df_safe(con, """
                SELECT * FROM gold.glosas_profissional
                WHERE competencia = ? AND regra = ?
            """, [competencia, regra])
            if not df_regra.empty:
                _adicionar_aba_xlsx(wb, regra[:31], df_regra)

        _adicionar_aba_xlsx(wb, "Metricas", _ler_df_safe(con, """
            SELECT taxa_anomalia_geral, p90_ch_total, proporcao_feminina_geral,
                   n_reincidentes, taxa_resolucao, velocidade_regularizacao_media
            FROM gold.metricas_avancadas WHERE competencia = ?
        """, [competencia]))

    buf = _io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _ler_df_safe(con, sql: str, params: list) -> pd.DataFrame:
    try:
        return con.execute(sql, params).df()
    except Exception:
        return pd.DataFrame()


def _adicionar_aba_xlsx(wb: _Workbook, nome: str, df: pd.DataFrame) -> None:
    ws = wb.create_sheet(title=nome)
    if df.empty:
        return
    ws.append(list(df.columns))
    for row in df.itertuples(index=False):
        ws.append(list(row))
```

- [ ] **Step 4: Run tests to verify PASS**

```
./venv/Scripts/python.exe -m pytest tests/export/test_report_generator.py::TestExportarXlsxPeriodo -v
```

Expected: all pass

- [ ] **Step 5: Add download button to `scripts/pages/5_Metricas.py`**

At the end of `scripts/pages/5_Metricas.py`, add:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from export.report_generator import exportar_xlsx_periodo

# ... (after existing KPI display code, near the end of the page)

st.divider()
if st.button("Gerar relatório XLSX"):
    dados = exportar_xlsx_periodo(competencia, config.DUCKDB_PATH)
    st.download_button(
        label="Baixar XLSX",
        data=dados,
        file_name=f"Relatorio_CNES_{competencia}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
```

- [ ] **Step 6: Run full suite**

```
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add src/export/report_generator.py tests/export/test_report_generator.py \
        scripts/pages/5_Metricas.py
git commit -m "feat(export): exportar_xlsx_periodo on-demand e botao download no dashboard"
```

---

### Task 10: Cleanup — update main.py + dashboard call sites + remove CSV_MAP

**Files:**
- Modify: `src/main.py`
- Modify: `scripts/pages/1_Tendencias.py`
- Modify: `scripts/pages/2_Por_Regra.py`
- Modify: `scripts/pages/4_Glosas.py`
- Modify: `src/storage/historico_reader.py` (remove `CSV_MAP` alias)

- [ ] **Step 1: Update `src/main.py`**

Change constructor calls and add `db_loader` to the snapshot stage:

Replace:
```python
    orchestrator = PipelineOrchestrator([
        IngestaoLocalStage(config.HISTORICO_DIR),
        ProcessamentoStage(),
        SnapshotLocalStage(config.HISTORICO_DIR),
        IngestaoNacionalStage(db_loader),
        ...
    ])
```

With:
```python
    orchestrator = PipelineOrchestrator([
        IngestaoLocalStage(config.HISTORICO_DIR, db_loader),
        ProcessamentoStage(),
        SnapshotLocalStage(config.HISTORICO_DIR, db_loader),
        IngestaoNacionalStage(db_loader),
        AuditoriaLocalStage(),
        AuditoriaNacionalStage(),
        MetricasStage(db_loader, historico_reader),
        ExportacaoStage(),
    ])
```

- [ ] **Step 2: Update `scripts/pages/2_Por_Regra.py`**

Find the line (around line 60):
```python
st.session_state[key] = reader.carregar_registros(regra, competencia)
```

Replace with:
```python
st.session_state[key] = reader.carregar_glosas_periodo(regra, competencia)
```

- [ ] **Step 3: Update `scripts/pages/1_Tendencias.py`**

Replace:
```python
from storage.historico_reader import CSV_MAP, HistoricoReader
...
_TODAS_REGRAS = list(CSV_MAP.keys())
```

With:
```python
from storage.historico_reader import REGRAS_AUDITORIA, HistoricoReader
...
_TODAS_REGRAS = list(REGRAS_AUDITORIA)
```

- [ ] **Step 4: Update `scripts/pages/4_Glosas.py`**

Apply the same change as Step 3:
```python
from storage.historico_reader import REGRAS_AUDITORIA, HistoricoReader
...
_TODAS_REGRAS = list(REGRAS_AUDITORIA)
```

- [ ] **Step 5: Remove `CSV_MAP` alias from `src/storage/historico_reader.py`**

Delete the `CSV_MAP` dict and its comment. Keep only `REGRAS_AUDITORIA`.

Also remove `carregar_registros` and `listar_competencias_para_regra` methods (they read from CSV files that no longer exist):

Delete:
```python
    def carregar_registros(self, regra: str, competencia: str) -> pd.DataFrame:
        ...
    def listar_competencias_para_regra(self, regra: str) -> list[str]:
        ...
```

- [ ] **Step 6: Run full test suite**

```
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Expected: all pass, no references to `CSV_MAP` or `carregar_registros` in failures

- [ ] **Step 7: Commit**

```bash
git add src/main.py src/storage/historico_reader.py \
        scripts/pages/1_Tendencias.py scripts/pages/2_Por_Regra.py scripts/pages/4_Glosas.py
git commit -m "refactor(pipeline): main.py construtores + dashboard migra para carregar_glosas_periodo; remove CSV_MAP"
```

---

### Final verification

- [ ] **Run complete test suite**

```
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Expected: all pass

- [ ] **Run linter**

```
./venv/Scripts/ruff.exe check src/ tests/
```

Expected: no errors
