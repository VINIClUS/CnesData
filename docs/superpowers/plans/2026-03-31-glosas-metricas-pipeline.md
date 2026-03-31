# Glosas, Métricas Avançadas e Otimização do Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Registrar glosas individuais por profissional/competência, adicionar SEXO ao schema canônico, otimizar consultas ao BigQuery via TTL+fingerprint, e computar métricas estatísticas avançadas.

**Architecture:** Extensões cirúrgicas — SEXO propagado do schema canônico, pipeline reordenado (Local→Processamento→Nacional→AuditoriaLocal→AuditoriaNacional→Métricas→Exportação), três novas tabelas DuckDB Gold, GlosasBuilder converte DataFrames de anomalias em registros individuais, MetricasStage computa e persiste métricas puras.

**Tech Stack:** Python 3.11+, DuckDB, pandas, pandera, openpyxl, hashlib (stdlib)

---

## File Map

### Novos
| Arquivo | Responsabilidade |
|---|---|
| `src/analysis/glosas_builder.py` | DataFrames de regras → schema gold.glosas_profissional |
| `src/analysis/metricas_avancadas.py` | Funções puras de cálculo de métricas |
| `src/pipeline/stages/auditoria_local.py` | RQ-003-B, RQ-005, Ghost, Missing |
| `src/pipeline/stages/auditoria_nacional.py` | RQ-006 a RQ-011 |
| `src/pipeline/stages/metricas.py` | Orquestra GlosasBuilder + MetricasAvancadas + DuckDB |
| `tests/analysis/test_glosas_builder.py` | Testes do GlosasBuilder |
| `tests/analysis/test_metricas_avancadas.py` | Testes das funções puras |
| `tests/pipeline/stages/test_auditoria_local.py` | Testes da nova stage local |
| `tests/pipeline/stages/test_auditoria_nacional.py` | Testes da nova stage nacional |
| `tests/pipeline/stages/test_metricas.py` | Testes do MetricasStage |

### Alterados
| Arquivo | Mudança |
|---|---|
| `src/ingestion/schemas.py` | SEXO em SCHEMA_PROFISSIONAL |
| `src/contracts/schemas.py` | SEXO em ProfissionalContract |
| `src/ingestion/cnes_nacional_adapter.py` | `df["SEXO"] = None` |
| `src/pipeline/state.py` | 3 novos campos |
| `src/config.py` | `NACIONAL_CACHE_TTL_DIAS` |
| `src/storage/database_loader.py` | 3 novas tabelas DDL + 4 novos métodos |
| `src/storage/historico_reader.py` | `carregar_glosas_historicas` |
| `src/pipeline/stages/ingestao_nacional.py` | TTL + fingerprint |
| `src/export/report_generator.py` | Aba Métricas Avançadas |
| `src/pipeline/stages/exportacao.py` | Passa `metricas_avancadas` ao relatório |
| `src/main.py` | Nova ordem de stages |
| `tests/contracts/test_schemas.py` | SEXO no contrato |
| `tests/pipeline/stages/test_ingestao_nacional.py` | TTL + fingerprint tests |
| `tests/storage/test_database_loader.py` | 3 novas tabelas + 4 métodos |
| `tests/storage/test_historico_reader.py` | `carregar_glosas_historicas` |

### Removidos
| Arquivo | Substituído por |
|---|---|
| `src/pipeline/stages/auditoria.py` | `auditoria_local.py` + `auditoria_nacional.py` |
| `tests/pipeline/stages/test_auditoria.py` | `test_auditoria_local.py` + `test_auditoria_nacional.py` |

---

## Task 1: SEXO no schema canônico + contrato + adapter nacional

**Files:**
- Modify: `src/ingestion/schemas.py`
- Modify: `src/contracts/schemas.py`
- Modify: `src/ingestion/cnes_nacional_adapter.py:132-144`
- Test: `tests/contracts/test_schemas.py`
- Test: `tests/ingestion/test_cnes_nacional_adapter.py`

- [ ] **Step 1: Escrever testes que falham**

Em `tests/contracts/test_schemas.py`, adicionar:
```python
def test_profissional_contract_aceita_sexo_nullable(contracts_df):
    import pandas as pd
    import pandera as pa
    from contracts.schemas import ProfissionalContract
    df = contracts_df["profissional"].copy()
    df["SEXO"] = None
    ProfissionalContract.validate(df, lazy=False)  # não deve levantar
```

Em `tests/ingestion/test_cnes_nacional_adapter.py`, adicionar ao final:
```python
def test_listar_profissionais_tem_coluna_sexo(adapter_com_cache, tmp_path):
    # adapter_com_cache já existente no arquivo de teste
    df = adapter_com_cache.listar_profissionais((2024, 12))
    assert "SEXO" in df.columns
    assert df["SEXO"].isna().all()
```

- [ ] **Step 2: Rodar e confirmar falha**
```
./venv/Scripts/python.exe -m pytest tests/contracts/test_schemas.py tests/ingestion/test_cnes_nacional_adapter.py -q --tb=short
```
Esperado: FAIL — `SEXO` não reconhecida.

- [ ] **Step 3: Adicionar SEXO ao SCHEMA_PROFISSIONAL**

Em `src/ingestion/schemas.py`, alterar `SCHEMA_PROFISSIONAL`:
```python
SCHEMA_PROFISSIONAL: Final[tuple[str, ...]] = (
    "CNS",
    "CPF",
    "NOME_PROFISSIONAL",
    "SEXO",
    "CBO",
    "CNES",
    "TIPO_VINCULO",
    "SUS",
    "CH_TOTAL",
    "CH_AMBULATORIAL",
    "CH_OUTRAS",
    "CH_HOSPITALAR",
    "FONTE",
)
```

- [ ] **Step 4: Adicionar SEXO ao ProfissionalContract**

Em `src/contracts/schemas.py`:
```python
class ProfissionalContract(pa.DataFrameModel):
    CNS: Series[str]
    CPF: Series[str] = pa.Field(nullable=True)
    NOME_PROFISSIONAL: Series[str]
    SEXO: Series[str] = pa.Field(nullable=True)
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
        strict = False
        coerce = False
```

- [ ] **Step 5: Adicionar SEXO=None ao adapter nacional**

Em `src/ingestion/cnes_nacional_adapter.py`, no método `_buscar_profissionais`, antes de `df["FONTE"] = _FONTE_NACIONAL`:
```python
    def _buscar_profissionais(self, ano: int, mes: int) -> pd.DataFrame:
        df = self._client.fetch_profissionais(self._id_municipio, ano, mes)
        df = df.rename(columns=_MAP_PROFISSIONAL)
        df["CPF"] = None
        df["SEXO"] = None
        df["SUS"] = df["SUS"].map({1: "S", 0: "N"}).fillna("N")
        df["CH_TOTAL"] = (
            df["CH_AMBULATORIAL"].fillna(0)
            + df["CH_OUTRAS"].fillna(0)
            + df["CH_HOSPITALAR"].fillna(0)
        ).astype(int)
        df["FONTE"] = _FONTE_NACIONAL
        logger.debug("listar_profissionais fonte=NACIONAL rows=%d", len(df))
        return df[list(SCHEMA_PROFISSIONAL)]
```

- [ ] **Step 6: Rodar testes e confirmar aprovação**
```
./venv/Scripts/python.exe -m pytest tests/contracts/test_schemas.py tests/ingestion/test_cnes_nacional_adapter.py tests/ingestion/test_cnes_local_adapter.py -q --tb=short
```
Esperado: PASS.

- [ ] **Step 7: Commit**
```bash
git add src/ingestion/schemas.py src/contracts/schemas.py src/ingestion/cnes_nacional_adapter.py tests/contracts/test_schemas.py tests/ingestion/test_cnes_nacional_adapter.py
git commit -m "feat(schema): adiciona SEXO ao SCHEMA_PROFISSIONAL e ProfissionalContract"
```

---

## Task 2: PipelineState — novos campos

**Files:**
- Modify: `src/pipeline/state.py`
- Test: `tests/pipeline/test_state.py`

- [ ] **Step 1: Escrever teste que falha**

Em `tests/pipeline/test_state.py`, adicionar:
```python
def test_state_tem_campos_novos():
    from pathlib import Path
    from pipeline.state import PipelineState
    state = PipelineState(
        competencia_ano=2026, competencia_mes=3,
        output_path=Path("x.csv"),
        executar_nacional=True, executar_hr=False,
    )
    assert state.nacional_validado is False
    assert state.fingerprint_local == ""
    assert state.metricas_avancadas == {}
```

- [ ] **Step 2: Rodar e confirmar falha**
```
./venv/Scripts/python.exe -m pytest tests/pipeline/test_state.py::test_state_tem_campos_novos -v
```
Esperado: FAIL — AttributeError.

- [ ] **Step 3: Adicionar campos ao PipelineState**

Em `src/pipeline/state.py`, após `df_ch_diverg`:
```python
    nacional_validado: bool = False
    fingerprint_local: str = ""
    metricas_avancadas: dict = field(default_factory=dict)
```

- [ ] **Step 4: Rodar e confirmar aprovação**
```
./venv/Scripts/python.exe -m pytest tests/pipeline/test_state.py -v
```
Esperado: PASS.

- [ ] **Step 5: Commit**
```bash
git add src/pipeline/state.py tests/pipeline/test_state.py
git commit -m "feat(state): adiciona nacional_validado, fingerprint_local, metricas_avancadas"
```

---

## Task 3: config.py — NACIONAL_CACHE_TTL_DIAS

**Files:**
- Modify: `src/config.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Escrever teste que falha**

Em `tests/test_config.py`, adicionar:
```python
def test_nacional_cache_ttl_dias_padrao():
    import config
    assert config.NACIONAL_CACHE_TTL_DIAS == 7
```

- [ ] **Step 2: Rodar e confirmar falha**
```
./venv/Scripts/python.exe -m pytest tests/test_config.py::test_nacional_cache_ttl_dias_padrao -v
```

- [ ] **Step 3: Adicionar constante em `src/config.py`**

Após o bloco `# ── Logs`:
```python
# ── Cache Nacional (BigQuery) ──────────────────────────────────────────────
NACIONAL_CACHE_TTL_DIAS: int = _exigir_inteiro("NACIONAL_CACHE_TTL_DIAS", 7)
```

- [ ] **Step 4: Rodar e confirmar aprovação**
```
./venv/Scripts/python.exe -m pytest tests/test_config.py -q --tb=short
```

- [ ] **Step 5: Commit**
```bash
git add src/config.py tests/test_config.py
git commit -m "feat(config): adiciona NACIONAL_CACHE_TTL_DIAS (padrão 7 dias)"
```

---

## Task 4: DatabaseLoader — 3 novas tabelas + 4 novos métodos

**Files:**
- Modify: `src/storage/database_loader.py`
- Test: `tests/storage/test_database_loader.py`

- [ ] **Step 1: Escrever testes que falham**

Adicionar ao final de `tests/storage/test_database_loader.py`:
```python
class TestNovasTabelas:
    def test_cria_glosas_profissional(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "glosas_profissional" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_cache_nacional(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "cache_nacional" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_metricas_avancadas(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "metricas_avancadas" in _tabelas_existentes(tmp_path / "test.duckdb")


class TestGravarGlosas:
    def test_insere_glosas(self, tmp_path):
        import pandas as pd
        from datetime import datetime
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        df = pd.DataFrame([{
            "competencia": "2026-03", "regra": "GHOST",
            "cpf": "12345678901", "cns": None,
            "nome_profissional": "JOAO", "sexo": "M",
            "cnes_estabelecimento": "1234567", "motivo": "AUSENTE_NO_RH",
            "criado_em_firebird": None,
            "criado_em_pipeline": datetime(2026, 3, 31),
            "atualizado_em_pipeline": datetime(2026, 3, 31),
        }])
        loader.gravar_glosas("2026-03", "GHOST", df)
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            resultado = con.execute(
                "SELECT * FROM gold.glosas_profissional WHERE competencia='2026-03'"
            ).df()
        assert len(resultado) == 1
        assert resultado.iloc[0]["cpf"] == "12345678901"

    def test_delete_antes_de_inserir(self, tmp_path):
        import pandas as pd
        from datetime import datetime
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        _row = lambda cpf: pd.DataFrame([{
            "competencia": "2026-03", "regra": "GHOST",
            "cpf": cpf, "cns": None, "nome_profissional": "X",
            "sexo": None, "cnes_estabelecimento": None, "motivo": None,
            "criado_em_firebird": None,
            "criado_em_pipeline": datetime(2026, 3, 31),
            "atualizado_em_pipeline": datetime(2026, 3, 31),
        }])
        loader.gravar_glosas("2026-03", "GHOST", _row("11111111111"))
        loader.gravar_glosas("2026-03", "GHOST", _row("22222222222"))
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            total = con.execute(
                "SELECT COUNT(*) FROM gold.glosas_profissional WHERE competencia='2026-03' AND regra='GHOST'"
            ).fetchone()[0]
        assert total == 1


class TestCacheNacional:
    def test_gravar_e_ler_cache(self, tmp_path):
        from datetime import datetime
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_cache_nacional("2026-03", "abc123")
        resultado = loader.ler_cache_nacional("2026-03")
        assert resultado is not None
        fingerprint, gravado_em = resultado
        assert fingerprint == "abc123"
        assert isinstance(gravado_em, datetime)

    def test_ler_cache_retorna_none_se_ausente(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert loader.ler_cache_nacional("2026-03") is None

    def test_gravar_cache_sobrescreve(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_cache_nacional("2026-03", "primeiro")
        loader.gravar_cache_nacional("2026-03", "segundo")
        fp, _ = loader.ler_cache_nacional("2026-03")
        assert fp == "segundo"


class TestMetricasAvancadas:
    def test_gravar_metricas(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_metricas_avancadas("2026-03", {
            "taxa_anomalia_geral": 0.12,
            "p90_ch_total": 40.0,
            "proporcao_feminina_geral": 0.65,
            "n_reincidentes": 3,
            "taxa_resolucao": 0.5,
            "velocidade_regularizacao_media": 2.0,
            "top_glosas_json": "[]",
            "anomalias_por_cbo_json": "[]",
            "proporcao_feminina_por_cnes_json": "[]",
            "ranking_cnes_json": "[]",
        })
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            df = con.execute(
                "SELECT taxa_anomalia_geral FROM gold.metricas_avancadas WHERE competencia='2026-03'"
            ).df()
        assert abs(df.iloc[0]["taxa_anomalia_geral"] - 0.12) < 1e-6
```

- [ ] **Step 2: Rodar e confirmar falha**
```
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py -q --tb=short
```

- [ ] **Step 3: Adicionar DDLs e métodos ao DatabaseLoader**

Em `src/storage/database_loader.py`, adicionar após `_DDL_AUDITORIA`:
```python
_DDL_GLOSAS = """
    CREATE TABLE IF NOT EXISTS gold.glosas_profissional (
        competencia             VARCHAR NOT NULL,
        regra                   VARCHAR NOT NULL,
        cpf                     VARCHAR,
        cns                     VARCHAR,
        nome_profissional       VARCHAR,
        sexo                    VARCHAR(1),
        cnes_estabelecimento    VARCHAR,
        motivo                  VARCHAR,
        criado_em_firebird      TIMESTAMP,
        criado_em_pipeline      TIMESTAMP NOT NULL,
        atualizado_em_pipeline  TIMESTAMP NOT NULL
    )
"""

_DDL_CACHE_NACIONAL = """
    CREATE TABLE IF NOT EXISTS gold.cache_nacional (
        competencia        VARCHAR PRIMARY KEY,
        fingerprint_local  VARCHAR NOT NULL,
        gravado_em         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
"""

_DDL_METRICAS_AVANCADAS = """
    CREATE TABLE IF NOT EXISTS gold.metricas_avancadas (
        competencia                      VARCHAR PRIMARY KEY,
        taxa_anomalia_geral              DOUBLE,
        p90_ch_total                     DOUBLE,
        proporcao_feminina_geral         DOUBLE,
        n_reincidentes                   INTEGER,
        taxa_resolucao                   DOUBLE,
        velocidade_regularizacao_media   DOUBLE,
        top_glosas_json                  VARCHAR,
        anomalias_por_cbo_json           VARCHAR,
        proporcao_feminina_por_cnes_json VARCHAR,
        ranking_cnes_json                VARCHAR,
        gravado_em                       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
"""
```

Em `inicializar_schema`, adicionar as três novas execuções:
```python
    def inicializar_schema(self) -> None:
        with self._conectar() as con:
            con.execute(_DDL_SCHEMA_GOLD)
            con.execute(_DDL_EVOLUCAO)
            con.execute(_DDL_AUDITORIA)
            con.execute(_DDL_GLOSAS)
            con.execute(_DDL_CACHE_NACIONAL)
            con.execute(_DDL_METRICAS_AVANCADAS)
        logger.info("schema_gold inicializado db=%s", self._caminho_db)
```

Adicionar os quatro novos métodos após `gravar_auditoria`:
```python
    def gravar_glosas(
        self, competencia: str, regra: str, df: pd.DataFrame
    ) -> None:
        """DELETE + INSERT de glosas para (competencia, regra).

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            regra: Código da regra (ex: 'GHOST').
            df: DataFrame com schema gold.glosas_profissional.
        """
        if df.empty:
            return
        with self._conectar() as con:
            con.execute(
                "DELETE FROM gold.glosas_profissional WHERE competencia=? AND regra=?",
                [competencia, regra],
            )
            con.register("_df_glosas", df)
            con.execute("INSERT INTO gold.glosas_profissional SELECT * FROM _df_glosas")
        logger.info("glosas gravadas competencia=%s regra=%s total=%d", competencia, regra, len(df))

    def gravar_cache_nacional(self, competencia: str, fingerprint: str) -> None:
        """UPSERT em gold.cache_nacional.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            fingerprint: SHA256 dos dados locais.
        """
        with self._conectar() as con:
            con.execute(
                "INSERT OR REPLACE INTO gold.cache_nacional (competencia, fingerprint_local, gravado_em) "
                "VALUES (?, ?, CURRENT_TIMESTAMP)",
                [competencia, fingerprint],
            )
        logger.info("cache_nacional gravado competencia=%s", competencia)

    def ler_cache_nacional(
        self, competencia: str
    ) -> tuple[str, datetime] | None:
        """Retorna (fingerprint, gravado_em) do cache nacional ou None se ausente.

        Args:
            competencia: Competência no formato 'YYYY-MM'.

        Returns:
            Tupla (fingerprint, gravado_em) ou None.
        """
        with self._conectar(read_only=True) as con:
            df = con.execute(
                "SELECT fingerprint_local, gravado_em FROM gold.cache_nacional WHERE competencia=?",
                [competencia],
            ).df()
        if df.empty:
            return None
        row = df.iloc[0]
        return str(row["fingerprint_local"]), row["gravado_em"].to_pydatetime()

    def gravar_metricas_avancadas(self, competencia: str, metricas: dict) -> None:
        """INSERT OR REPLACE em gold.metricas_avancadas.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            metricas: Dicionário com chaves correspondentes às colunas da tabela.
        """
        with self._conectar() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO gold.metricas_avancadas (
                    competencia, taxa_anomalia_geral, p90_ch_total,
                    proporcao_feminina_geral, n_reincidentes, taxa_resolucao,
                    velocidade_regularizacao_media, top_glosas_json,
                    anomalias_por_cbo_json, proporcao_feminina_por_cnes_json,
                    ranking_cnes_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    competencia,
                    metricas.get("taxa_anomalia_geral"),
                    metricas.get("p90_ch_total"),
                    metricas.get("proporcao_feminina_geral"),
                    metricas.get("n_reincidentes"),
                    metricas.get("taxa_resolucao"),
                    metricas.get("velocidade_regularizacao_media"),
                    metricas.get("top_glosas_json"),
                    metricas.get("anomalias_por_cbo_json"),
                    metricas.get("proporcao_feminina_por_cnes_json"),
                    metricas.get("ranking_cnes_json"),
                ],
            )
        logger.info("metricas_avancadas gravadas competencia=%s", competencia)
```

Adicionar import de `datetime` no topo do arquivo:
```python
from datetime import datetime
```

E import de `pandas` (necessário para `gravar_glosas`):
```python
import pandas as pd
```

- [ ] **Step 4: Rodar e confirmar aprovação**
```
./venv/Scripts/python.exe -m pytest tests/storage/test_database_loader.py -v --tb=short
```

- [ ] **Step 5: Commit**
```bash
git add src/storage/database_loader.py tests/storage/test_database_loader.py
git commit -m "feat(storage): 3 novas tabelas Gold e métodos gravar_glosas, cache_nacional, metricas_avancadas"
```

---

## Task 5: HistoricoReader — carregar_glosas_historicas

**Files:**
- Modify: `src/storage/historico_reader.py`
- Test: `tests/storage/test_historico_reader.py`

- [ ] **Step 1: Escrever teste que falha**

Adicionar ao final de `tests/storage/test_historico_reader.py`:
```python
class TestCarregarGlosasHistoricas:
    def _criar_db_com_glosas(self, tmp_path):
        from storage.database_loader import DatabaseLoader
        from datetime import datetime
        import pandas as pd
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        df = pd.DataFrame([
            {"competencia": "2026-01", "regra": "GHOST",
             "cpf": "11111111111", "cns": None, "nome_profissional": "ANA",
             "sexo": "F", "cnes_estabelecimento": "1234567", "motivo": "AUSENTE_NO_RH",
             "criado_em_firebird": None,
             "criado_em_pipeline": datetime(2026, 1, 10),
             "atualizado_em_pipeline": datetime(2026, 1, 10)},
            {"competencia": "2026-02", "regra": "GHOST",
             "cpf": "11111111111", "cns": None, "nome_profissional": "ANA",
             "sexo": "F", "cnes_estabelecimento": "1234567", "motivo": "AUSENTE_NO_RH",
             "criado_em_firebird": None,
             "criado_em_pipeline": datetime(2026, 2, 10),
             "atualizado_em_pipeline": datetime(2026, 2, 10)},
        ])
        loader.gravar_glosas("2026-01", "GHOST", df[df["competencia"] == "2026-01"])
        loader.gravar_glosas("2026-02", "GHOST", df[df["competencia"] == "2026-02"])
        return tmp_path / "test.duckdb"

    def test_retorna_todas_glosas(self, tmp_path):
        from storage.historico_reader import HistoricoReader
        db_path = self._criar_db_com_glosas(tmp_path)
        reader = HistoricoReader(db_path, tmp_path / "historico")
        df = reader.carregar_glosas_historicas()
        assert len(df) == 2
        assert set(df["competencia"]) == {"2026-01", "2026-02"}

    def test_filtra_por_competencia_inicio(self, tmp_path):
        from storage.historico_reader import HistoricoReader
        db_path = self._criar_db_com_glosas(tmp_path)
        reader = HistoricoReader(db_path, tmp_path / "historico")
        df = reader.carregar_glosas_historicas(competencia_inicio="2026-02")
        assert len(df) == 1
        assert df.iloc[0]["competencia"] == "2026-02"

    def test_retorna_dataframe_vazio_sem_dados(self, tmp_path):
        from storage.database_loader import DatabaseLoader
        from storage.historico_reader import HistoricoReader
        DatabaseLoader(tmp_path / "test.duckdb").inicializar_schema()
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        df = reader.carregar_glosas_historicas()
        assert df.empty
```

- [ ] **Step 2: Rodar e confirmar falha**
```
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py -q --tb=short
```

- [ ] **Step 3: Adicionar método ao HistoricoReader**

Em `src/storage/historico_reader.py`, adicionar após `contar_competencias`:
```python
    def carregar_glosas_historicas(
        self,
        competencia_inicio: str | None = None,
    ) -> pd.DataFrame:
        """Retorna todas as glosas de gold.glosas_profissional em ordem cronológica.

        Args:
            competencia_inicio: Competência mínima no formato YYYY-MM. None retorna todas.

        Returns:
            DataFrame com todas as colunas de gold.glosas_profissional.
        """
        condicao = "WHERE competencia >= ?" if competencia_inicio else ""
        params = [competencia_inicio] if competencia_inicio else []
        sql = f"SELECT * FROM gold.glosas_profissional {condicao} ORDER BY competencia, regra"
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            return con.execute(sql, params).df()
```

- [ ] **Step 4: Rodar e confirmar aprovação**
```
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py -v --tb=short
```

- [ ] **Step 5: Commit**
```bash
git add src/storage/historico_reader.py tests/storage/test_historico_reader.py
git commit -m "feat(storage): adiciona carregar_glosas_historicas ao HistoricoReader"
```

---

## Task 6: Split AuditoriaStage → AuditoriaLocalStage + AuditoriaNacionalStage

**Files:**
- Create: `src/pipeline/stages/auditoria_local.py`
- Create: `src/pipeline/stages/auditoria_nacional.py`
- Create: `tests/pipeline/stages/test_auditoria_local.py`
- Create: `tests/pipeline/stages/test_auditoria_nacional.py`
- Delete: `src/pipeline/stages/auditoria.py`
- Delete: `tests/pipeline/stages/test_auditoria.py`

- [ ] **Step 1: Criar `src/pipeline/stages/auditoria_local.py`**

```python
"""AuditoriaLocalStage — regras que não dependem do repositório nacional."""
import logging

import config
from analysis.rules_engine import (
    auditar_lotacao_ace_tace,
    auditar_lotacao_acs_tacs,
    detectar_folha_fantasma,
    detectar_multiplas_unidades,
    detectar_registro_ausente,
)
from ingestion.hr_client import carregar_folha
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)


class AuditoriaLocalStage:
    nome = "auditoria_local"

    def execute(self, state: PipelineState) -> None:
        self._regras_estruturais(state)
        self._regras_hr(state)

    def _regras_estruturais(self, state: PipelineState) -> None:
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
```

- [ ] **Step 2: Criar `src/pipeline/stages/auditoria_nacional.py`**

```python
"""AuditoriaNacionalStage — regras que cruzam dados locais com o repositório nacional."""
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


class AuditoriaNacionalStage:
    nome = "auditoria_nacional"

    def execute(self, state: PipelineState) -> None:
        if state.df_estab_nacional.empty and state.df_prof_nacional.empty:
            logger.warning(
                "nacional_cross_check=skipped motivo=dados_nacionais_vazios competencia=%s",
                state.competencia_str,
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
            adapter = CachingVerificadorCnes(
                CnesOficialWebAdapter(),
                config.CACHE_DIR / "cnes_verificados.json",
            )
            state.df_estab_fantasma = resolver_lag_rq006(state.df_estab_fantasma, adapter)

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

- [ ] **Step 3: Criar `tests/pipeline/stages/test_auditoria_local.py`**

```python
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.auditoria_local import AuditoriaLocalStage


def _state() -> PipelineState:
    state = PipelineState(
        competencia_ano=2026, competencia_mes=3,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True, executar_hr=True,
    )
    state.df_processado = pd.DataFrame({
        "CPF": ["12345678901"], "CNES": ["1234567"],
        "CBO": ["515105"], "CNS": ["123456789012345"],
    })
    state.df_estab_local = pd.DataFrame({"CNES": ["1234567"], "TIPO_UNIDADE": ["01"]})
    return state


@patch("pipeline.stages.auditoria_local.config")
@patch("pipeline.stages.auditoria_local.carregar_folha", return_value=pd.DataFrame({"CPF": [], "STATUS": []}))
@patch("pipeline.stages.auditoria_local.detectar_registro_ausente", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.detectar_folha_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.auditar_lotacao_ace_tace", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.auditar_lotacao_acs_tacs", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_local.detectar_multiplas_unidades", return_value=pd.DataFrame())
def test_regras_estruturais_sempre_executam(
    mock_multi, mock_acs, mock_ace, mock_ghost, mock_missing, mock_folha, mock_config
):
    mock_config.FOLHA_HR_PATH = MagicMock()
    mock_config.FOLHA_HR_PATH.exists.return_value = True
    state = _state()
    AuditoriaLocalStage().execute(state)
    mock_multi.assert_called_once()
    mock_acs.assert_called_once()
    mock_ace.assert_called_once()


@patch("pipeline.stages.auditoria_local.config")
def test_hr_skipped_quando_executar_hr_false(mock_config):
    mock_config.FOLHA_HR_PATH = None
    state = _state()
    state.executar_hr = False
    AuditoriaLocalStage().execute(state)
    assert state.df_ghost.empty
    assert state.df_missing.empty
```

- [ ] **Step 4: Criar `tests/pipeline/stages/test_auditoria_nacional.py`**

```python
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.auditoria_nacional import AuditoriaNacionalStage


def _state_com_nacional() -> PipelineState:
    state = PipelineState(
        competencia_ano=2026, competencia_mes=3,
        output_path=Path("data/processed/report.csv"),
        executar_nacional=True, executar_hr=False,
    )
    state.df_processado = pd.DataFrame({"CNS": ["123456789012345"], "CNES": ["1234567"], "CBO": ["515105"]})
    state.df_estab_local = pd.DataFrame({"CNES": ["1234567"], "TIPO_UNIDADE": ["01"]})
    state.df_prof_nacional = pd.DataFrame({"CNS": ["999999999999999"]})
    state.df_estab_nacional = pd.DataFrame({"CNES": ["9999999"]})
    return state


@patch("pipeline.stages.auditoria_nacional.config")
@patch("pipeline.stages.auditoria_nacional.CnesOficialWebAdapter")
@patch("pipeline.stages.auditoria_nacional.CachingVerificadorCnes")
@patch("pipeline.stages.auditoria_nacional.resolver_lag_rq006", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_nacional.detectar_divergencia_carga_horaria", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_nacional.detectar_divergencia_cbo", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_nacional.detectar_profissionais_ausentes_local", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_nacional.detectar_profissionais_fantasma", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_nacional.detectar_estabelecimentos_ausentes_local", return_value=pd.DataFrame())
@patch("pipeline.stages.auditoria_nacional.detectar_estabelecimentos_fantasma", return_value=pd.DataFrame())
def test_cruzamento_executa_com_dados_nacionais(
    mock_efant, mock_eaus, mock_pfant, mock_paus, mock_cbo, mock_ch,
    mock_resolver, mock_caching, mock_web, mock_config,
):
    mock_config.CACHE_DIR = Path("data/cache")
    state = _state_com_nacional()
    AuditoriaNacionalStage().execute(state)
    mock_efant.assert_called_once()
    mock_pfant.assert_called_once()


def test_skip_quando_dados_nacionais_vazios():
    state = _state_com_nacional()
    state.df_prof_nacional = pd.DataFrame()
    state.df_estab_nacional = pd.DataFrame()
    AuditoriaNacionalStage().execute(state)
    assert state.df_estab_fantasma.empty
    assert state.df_prof_fantasma.empty
```

- [ ] **Step 5: Rodar novos testes**
```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_auditoria_local.py tests/pipeline/stages/test_auditoria_nacional.py -v --tb=short
```
Esperado: PASS.

- [ ] **Step 6: Deletar arquivos antigos**
```bash
rm src/pipeline/stages/auditoria.py
rm tests/pipeline/stages/test_auditoria.py
```

- [ ] **Step 7: Rodar suite completa para confirmar que nada quebrou**
```
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

- [ ] **Step 8: Commit**
```bash
git add src/pipeline/stages/auditoria_local.py src/pipeline/stages/auditoria_nacional.py
git add tests/pipeline/stages/test_auditoria_local.py tests/pipeline/stages/test_auditoria_nacional.py
git rm src/pipeline/stages/auditoria.py tests/pipeline/stages/test_auditoria.py
git commit -m "refactor(pipeline): split AuditoriaStage em AuditoriaLocalStage + AuditoriaNacionalStage"
```

---

## Task 7: IngestaoNacionalStage — TTL + fingerprint

**Files:**
- Modify: `src/pipeline/stages/ingestao_nacional.py`
- Test: `tests/pipeline/stages/test_ingestao_nacional.py`

- [ ] **Step 1: Escrever testes que falham**

Adicionar ao final de `tests/pipeline/stages/test_ingestao_nacional.py`:
```python
class TestTTLFingerprint:
    def _state(self, tmp_path):
        from pathlib import Path
        import pandas as pd
        state = PipelineState(
            competencia_ano=2026, competencia_mes=3,
            output_path=tmp_path / "report.csv",
            executar_nacional=True, executar_hr=False,
        )
        state.df_processado = pd.DataFrame({
            "CPF": ["12345678901"], "CBO": ["515105"], "CNES": ["1234567"],
        })
        return state

    @patch("pipeline.stages.ingestao_nacional.DatabaseLoader")
    @patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
    def test_skip_quando_cache_valido(self, mock_adapter_cls, mock_loader_cls, tmp_path):
        from datetime import datetime, timedelta
        mock_loader = mock_loader_cls.return_value
        mock_loader.ler_cache_nacional.return_value = ("FINGERPRINT_IGUAL", datetime.now())
        state = self._state(tmp_path)
        # fingerprint calculado internamente deve ser "FINGERPRINT_IGUAL"
        with patch("pipeline.stages.ingestao_nacional.IngestaoNacionalStage._computar_fingerprint",
                   return_value="FINGERPRINT_IGUAL"):
            with patch("pipeline.stages.ingestao_nacional.config") as mock_cfg:
                mock_cfg.NACIONAL_CACHE_TTL_DIAS = 7
                mock_cfg.GCP_PROJECT_ID = "proj"
                mock_cfg.ID_MUNICIPIO_IBGE7 = "3543105"
                mock_cfg.CACHE_DIR = tmp_path
                mock_cfg.DUCKDB_PATH = tmp_path / "test.duckdb"
                IngestaoNacionalStage().execute(state)
        mock_adapter_cls.return_value.listar_profissionais.assert_not_called()

    @patch("pipeline.stages.ingestao_nacional.DatabaseLoader")
    @patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
    def test_busca_quando_fingerprint_mudou(self, mock_adapter_cls, mock_loader_cls, tmp_path):
        from datetime import datetime
        import pandas as pd
        mock_loader = mock_loader_cls.return_value
        mock_loader.ler_cache_nacional.return_value = ("FINGERPRINT_ANTIGO", datetime.now())
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.listar_profissionais.return_value = pd.DataFrame()
        mock_adapter.listar_estabelecimentos.return_value = pd.DataFrame()
        state = self._state(tmp_path)
        with patch("pipeline.stages.ingestao_nacional.IngestaoNacionalStage._computar_fingerprint",
                   return_value="FINGERPRINT_NOVO"):
            with patch("pipeline.stages.ingestao_nacional.config") as mock_cfg:
                mock_cfg.NACIONAL_CACHE_TTL_DIAS = 7
                mock_cfg.GCP_PROJECT_ID = "proj"
                mock_cfg.ID_MUNICIPIO_IBGE7 = "3543105"
                mock_cfg.CACHE_DIR = tmp_path
                mock_cfg.DUCKDB_PATH = tmp_path / "test.duckdb"
                IngestaoNacionalStage().execute(state)
        mock_adapter.listar_profissionais.assert_called_once()

    @patch("pipeline.stages.ingestao_nacional.DatabaseLoader")
    @patch("pipeline.stages.ingestao_nacional.CnesNacionalAdapter")
    def test_busca_quando_cache_expirado(self, mock_adapter_cls, mock_loader_cls, tmp_path):
        from datetime import datetime, timedelta
        import pandas as pd
        mock_loader = mock_loader_cls.return_value
        mock_loader.ler_cache_nacional.return_value = (
            "FINGERPRINT_IGUAL", datetime.now() - timedelta(days=10)
        )
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.listar_profissionais.return_value = pd.DataFrame()
        mock_adapter.listar_estabelecimentos.return_value = pd.DataFrame()
        state = self._state(tmp_path)
        with patch("pipeline.stages.ingestao_nacional.IngestaoNacionalStage._computar_fingerprint",
                   return_value="FINGERPRINT_IGUAL"):
            with patch("pipeline.stages.ingestao_nacional.config") as mock_cfg:
                mock_cfg.NACIONAL_CACHE_TTL_DIAS = 7
                mock_cfg.GCP_PROJECT_ID = "proj"
                mock_cfg.ID_MUNICIPIO_IBGE7 = "3543105"
                mock_cfg.CACHE_DIR = tmp_path
                mock_cfg.DUCKDB_PATH = tmp_path / "test.duckdb"
                IngestaoNacionalStage().execute(state)
        mock_adapter.listar_profissionais.assert_called_once()
```

- [ ] **Step 2: Rodar e confirmar falha**
```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_ingestao_nacional.py -q --tb=short
```

- [ ] **Step 3: Reescrever `src/pipeline/stages/ingestao_nacional.py`**

```python
"""IngestaoNacionalStage — BigQuery com TTL + fingerprint diff e soft-fail."""
import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import config
from ingestion.cnes_nacional_adapter import CnesNacionalAdapter
from pipeline.state import PipelineState
from storage.database_loader import DatabaseLoader

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
        fingerprint = self._computar_fingerprint(state.df_processado)
        state.fingerprint_local = fingerprint
        if not self._deve_buscar_nacional(state, fingerprint):
            logger.info("nacional_cache=hit competencia=%s", state.competencia_str)
            return
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
        state.nacional_validado = True
        loader = DatabaseLoader(config.DUCKDB_PATH)
        loader.inicializar_schema()
        loader.gravar_cache_nacional(state.competencia_str, fingerprint)
        logger.info(
            "ingestao_nacional profissionais=%d estabelecimentos=%d",
            len(state.df_prof_nacional),
            len(state.df_estab_nacional),
        )

    def _deve_buscar_nacional(self, state: PipelineState, fingerprint: str) -> bool:
        loader = DatabaseLoader(config.DUCKDB_PATH)
        loader.inicializar_schema()
        cache = loader.ler_cache_nacional(state.competencia_str)
        if cache is None:
            return True
        cached_fp, gravado_em = cache
        if cached_fp != fingerprint:
            return True
        return (datetime.now() - gravado_em).days >= config.NACIONAL_CACHE_TTL_DIAS

    @staticmethod
    def _computar_fingerprint(df) -> str:
        chaves = (
            df[["CPF", "CBO", "CNES"]].dropna()
            .sort_values(["CPF", "CBO", "CNES"])
            .values.tolist()
        )
        return hashlib.sha256(json.dumps(chaves).encode()).hexdigest()
```

- [ ] **Step 4: Rodar testes**
```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_ingestao_nacional.py -v --tb=short
```

- [ ] **Step 5: Commit**
```bash
git add src/pipeline/stages/ingestao_nacional.py tests/pipeline/stages/test_ingestao_nacional.py
git commit -m "feat(pipeline): IngestaoNacionalStage com TTL e fingerprint diff"
```

---

## Task 8: GlosasBuilder

**Files:**
- Create: `src/analysis/glosas_builder.py`
- Create: `tests/analysis/test_glosas_builder.py`

- [ ] **Step 1: Escrever testes que falham**

Criar `tests/analysis/test_glosas_builder.py`:
```python
"""Testes do GlosasBuilder."""
from datetime import datetime
from pathlib import Path

import pandas as pd

from pipeline.state import PipelineState


def _state_com_anomalias() -> PipelineState:
    state = PipelineState(
        competencia_ano=2026, competencia_mes=3,
        output_path=Path("x.csv"), executar_nacional=True, executar_hr=True,
    )
    state.df_multi_unidades = pd.DataFrame({
        "CPF": ["11111111111"], "CNS": ["111111111111111"],
        "NOME_PROFISSIONAL": ["ANA"], "SEXO": ["F"],
        "CNES": ["1234567"], "QTD_UNIDADES": [2],
    })
    state.df_acs_incorretos = pd.DataFrame()
    state.df_ace_incorretos = pd.DataFrame()
    state.df_ghost = pd.DataFrame({
        "CPF": ["22222222222"], "CNS": ["222222222222222"],
        "NOME_PROFISSIONAL": ["JOAO"], "SEXO": ["M"],
        "CNES": ["1234567"], "MOTIVO_GHOST": ["AUSENTE_NO_RH"],
    })
    state.df_missing = pd.DataFrame({
        "CPF": ["33333333333"], "NOME_PROFISSIONAL": ["MARIA"],
    })
    state.df_prof_fantasma = pd.DataFrame()
    state.df_prof_ausente = pd.DataFrame()
    state.df_cbo_diverg = pd.DataFrame()
    state.df_ch_diverg = pd.DataFrame()
    state.df_estab_fantasma = pd.DataFrame()
    state.df_estab_ausente = pd.DataFrame()
    return state


def test_construir_glosas_retorna_dataframe_com_colunas_corretas():
    from analysis.glosas_builder import construir_glosas
    state = _state_com_anomalias()
    df = construir_glosas("2026-03", state, datetime(2026, 3, 31))
    colunas_esperadas = {
        "competencia", "regra", "cpf", "cns", "nome_profissional",
        "sexo", "cnes_estabelecimento", "motivo",
        "criado_em_firebird", "criado_em_pipeline", "atualizado_em_pipeline",
    }
    assert colunas_esperadas.issubset(set(df.columns))


def test_construir_glosas_agrega_todas_regras():
    from analysis.glosas_builder import construir_glosas
    state = _state_com_anomalias()
    df = construir_glosas("2026-03", state, datetime(2026, 3, 31))
    assert "RQ003B" in df["regra"].values
    assert "GHOST" in df["regra"].values
    assert "MISSING" in df["regra"].values


def test_construir_glosas_competencia_preenchida():
    from analysis.glosas_builder import construir_glosas
    state = _state_com_anomalias()
    df = construir_glosas("2026-03", state, datetime(2026, 3, 31))
    assert (df["competencia"] == "2026-03").all()


def test_construir_glosas_motivo_ghost():
    from analysis.glosas_builder import construir_glosas
    state = _state_com_anomalias()
    df = construir_glosas("2026-03", state, datetime(2026, 3, 31))
    ghost = df[df["regra"] == "GHOST"]
    assert ghost.iloc[0]["motivo"] == "AUSENTE_NO_RH"


def test_construir_glosas_dataframes_vazios_ignorados():
    from analysis.glosas_builder import construir_glosas
    state = PipelineState(
        competencia_ano=2026, competencia_mes=3,
        output_path=Path("x.csv"), executar_nacional=True, executar_hr=False,
    )
    df = construir_glosas("2026-03", state, datetime(2026, 3, 31))
    assert df.empty


def test_construir_glosas_sexo_preservado():
    from analysis.glosas_builder import construir_glosas
    state = _state_com_anomalias()
    df = construir_glosas("2026-03", state, datetime(2026, 3, 31))
    multi = df[df["regra"] == "RQ003B"]
    assert multi.iloc[0]["sexo"] == "F"
```

- [ ] **Step 2: Rodar e confirmar falha**
```
./venv/Scripts/python.exe -m pytest tests/analysis/test_glosas_builder.py -v --tb=short
```

- [ ] **Step 3: Criar `src/analysis/glosas_builder.py`**

```python
"""GlosasBuilder — converte DataFrames de anomalias em registros gold.glosas_profissional."""
from datetime import datetime

import pandas as pd

from pipeline.state import PipelineState

_COLUNAS_GLOSA = [
    "competencia", "regra", "cpf", "cns", "nome_profissional",
    "sexo", "cnes_estabelecimento", "motivo",
    "criado_em_firebird", "criado_em_pipeline", "atualizado_em_pipeline",
]


def construir_glosas(
    competencia: str,
    state: PipelineState,
    criado_em_pipeline: datetime,
) -> pd.DataFrame:
    """Agrega todas as regras do state em DataFrame pronto para INSERT.

    Args:
        competencia: Competência no formato 'YYYY-MM'.
        state: Estado do pipeline com DataFrames de anomalias preenchidos.
        criado_em_pipeline: Timestamp da execução atual.

    Returns:
        DataFrame com schema gold.glosas_profissional.
    """
    blocos = [
        _extrair_rq003b(state.df_multi_unidades),
        _extrair_rq005(state.df_acs_incorretos, "RQ005_ACS"),
        _extrair_rq005(state.df_ace_incorretos, "RQ005_ACE"),
        _extrair_ghost(state.df_ghost),
        _extrair_missing(state.df_missing),
        _extrair_cns_cnes(state.df_prof_fantasma, "RQ008"),
        _extrair_cns_cnes(state.df_prof_ausente, "RQ009"),
        _extrair_rq010(state.df_cbo_diverg),
        _extrair_rq011(state.df_ch_diverg),
    ]
    nao_vazios = [b for b in blocos if not b.empty]
    if not nao_vazios:
        return pd.DataFrame(columns=_COLUNAS_GLOSA)
    df = pd.concat(nao_vazios, ignore_index=True)
    df["competencia"] = competencia
    df["criado_em_firebird"] = None
    df["criado_em_pipeline"] = criado_em_pipeline
    df["atualizado_em_pipeline"] = criado_em_pipeline
    return df[_COLUNAS_GLOSA]


def _base(regra: str) -> dict:
    return {"regra": regra, "cpf": None, "cns": None, "nome_profissional": None,
            "sexo": None, "cnes_estabelecimento": None, "motivo": None}


def _extrair_rq003b(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = df.apply(lambda r: {**_base("RQ003B"),
        "cpf": r.get("CPF"), "cns": r.get("CNS"),
        "nome_profissional": r.get("NOME_PROFISSIONAL"), "sexo": r.get("SEXO"),
        "cnes_estabelecimento": r.get("CNES")}, axis=1).tolist()
    return pd.DataFrame(rows)


def _extrair_rq005(df: pd.DataFrame, regra: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = df.apply(lambda r: {**_base(regra),
        "cpf": r.get("CPF"), "cns": r.get("CNS"),
        "nome_profissional": r.get("NOME_PROFISSIONAL"), "sexo": r.get("SEXO"),
        "cnes_estabelecimento": r.get("CNES")}, axis=1).tolist()
    return pd.DataFrame(rows)


def _extrair_ghost(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = df.apply(lambda r: {**_base("GHOST"),
        "cpf": r.get("CPF"), "cns": r.get("CNS"),
        "nome_profissional": r.get("NOME_PROFISSIONAL"), "sexo": r.get("SEXO"),
        "cnes_estabelecimento": r.get("CNES"), "motivo": r.get("MOTIVO_GHOST")}, axis=1).tolist()
    return pd.DataFrame(rows)


def _extrair_missing(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = df.apply(lambda r: {**_base("MISSING"),
        "cpf": r.get("CPF"), "nome_profissional": r.get("NOME_PROFISSIONAL")}, axis=1).tolist()
    return pd.DataFrame(rows)


def _extrair_cns_cnes(df: pd.DataFrame, regra: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = df.apply(lambda r: {**_base(regra),
        "cns": r.get("CNS"), "nome_profissional": r.get("NOME_PROFISSIONAL"),
        "cnes_estabelecimento": r.get("CNES")}, axis=1).tolist()
    return pd.DataFrame(rows)


def _extrair_rq010(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = df.apply(lambda r: {**_base("RQ010"),
        "cns": r.get("CNS"), "cnes_estabelecimento": r.get("CNES"),
        "motivo": f"CBO_LOCAL:{r.get('CBO_LOCAL')} CBO_NACIONAL:{r.get('CBO_NACIONAL')}"
    }, axis=1).tolist()
    return pd.DataFrame(rows)


def _extrair_rq011(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = df.apply(lambda r: {**_base("RQ011"),
        "cns": r.get("CNS"), "cnes_estabelecimento": r.get("CNES"),
        "motivo": f"DELTA_CH:{r.get('DELTA_CH')}"
    }, axis=1).tolist()
    return pd.DataFrame(rows)
```

- [ ] **Step 4: Rodar testes**
```
./venv/Scripts/python.exe -m pytest tests/analysis/test_glosas_builder.py -v --tb=short
```
Esperado: PASS.

- [ ] **Step 5: Commit**
```bash
git add src/analysis/glosas_builder.py tests/analysis/test_glosas_builder.py
git commit -m "feat(analysis): GlosasBuilder — DataFrames de regras para gold.glosas_profissional"
```

---

## Task 9: metricas_avancadas.py — funções puras

**Files:**
- Create: `src/analysis/metricas_avancadas.py`
- Create: `tests/analysis/test_metricas_avancadas.py`

- [ ] **Step 1: Escrever testes que falham**

Criar `tests/analysis/test_metricas_avancadas.py`:
```python
"""Testes das funções puras de métricas avançadas."""
import pandas as pd


def _df_vinculos():
    return pd.DataFrame({
        "CPF": ["11111111111", "22222222222", "33333333333", "44444444444"],
        "CNS": ["111", "222", "333", "444"],
        "SEXO": ["F", "M", "F", None],
        "CBO": ["515105", "515105", "322255", "322255"],
        "CNES": ["1111111", "1111111", "2222222", "2222222"],
        "CH_TOTAL": [40, 20, 40, 60],
    })


def _df_glosas_atual():
    return pd.DataFrame({
        "competencia": ["2026-03", "2026-03"],
        "regra": ["GHOST", "RQ005_ACS"],
        "cpf": ["11111111111", "33333333333"],
        "cns": ["111", "333"],
        "cnes_estabelecimento": ["1111111", "2222222"],
    })


def test_taxa_anomalia_geral():
    from analysis.metricas_avancadas import calcular_taxa_anomalia
    vinculos = _df_vinculos()
    glosas = _df_glosas_atual()
    taxa = calcular_taxa_anomalia(vinculos, glosas)
    assert abs(taxa - 0.5) < 1e-6  # 2 únicos de 4


def test_taxa_anomalia_zero_quando_sem_glosas():
    from analysis.metricas_avancadas import calcular_taxa_anomalia
    taxa = calcular_taxa_anomalia(_df_vinculos(), pd.DataFrame())
    assert taxa == 0.0


def test_p90_ch():
    from analysis.metricas_avancadas import calcular_p90_ch
    p90 = calcular_p90_ch(_df_vinculos())
    assert p90 == _df_vinculos()["CH_TOTAL"].quantile(0.90)


def test_proporcao_feminina_geral():
    from analysis.metricas_avancadas import calcular_proporcao_feminina
    prop = calcular_proporcao_feminina(_df_vinculos())
    # SEXO não-nulo: F, M, F → 2F de 3 = 0.666
    assert abs(prop - 2/3) < 1e-6


def test_proporcao_feminina_zero_sem_sexo():
    from analysis.metricas_avancadas import calcular_proporcao_feminina
    df = _df_vinculos().copy()
    df["SEXO"] = None
    assert calcular_proporcao_feminina(df) == 0.0


def test_top_glosas_retorna_lista():
    from analysis.metricas_avancadas import calcular_top_glosas
    resultado = calcular_top_glosas(_df_glosas_atual(), n=10)
    assert isinstance(resultado, list)
    assert len(resultado) > 0
    assert "total" in resultado[0]


def test_ranking_cnes_retorna_lista():
    from analysis.metricas_avancadas import calcular_ranking_cnes
    resultado = calcular_ranking_cnes(_df_vinculos(), _df_glosas_atual())
    assert isinstance(resultado, list)
    assert "indice_conformidade" in resultado[0]


def test_anomalias_por_cbo_retorna_lista():
    from analysis.metricas_avancadas import calcular_anomalias_por_cbo
    resultado = calcular_anomalias_por_cbo(_df_vinculos(), _df_glosas_atual(), {})
    assert isinstance(resultado, list)
    assert all("taxa" in item for item in resultado)


def test_proporcao_feminina_por_cnes():
    from analysis.metricas_avancadas import calcular_proporcao_feminina_por_cnes
    resultado = calcular_proporcao_feminina_por_cnes(_df_vinculos())
    assert isinstance(resultado, list)
    assert all("cnes" in item and "proporcao_f" in item for item in resultado)


def test_taxa_resolucao_zero_sem_historico():
    from analysis.metricas_avancadas import calcular_taxa_resolucao
    taxa = calcular_taxa_resolucao("2026-02", "2026-03", pd.DataFrame())
    assert taxa == 0.0


def test_taxa_resolucao_100_quando_todas_resolvidas():
    from analysis.metricas_avancadas import calcular_taxa_resolucao
    historico = pd.DataFrame({
        "competencia": ["2026-02", "2026-02"],
        "regra": ["GHOST", "GHOST"],
        "cpf": ["11111111111", "22222222222"],
        "cns": [None, None],
    })
    # Nenhuma dessas CPFs aparece em 2026-03 (sem dados de 2026-03 no histórico)
    taxa = calcular_taxa_resolucao("2026-02", "2026-03", historico)
    assert taxa == 1.0


def test_reincidencia_conta_profissionais_com_glosa_consecutiva():
    from analysis.metricas_avancadas import calcular_reincidencia
    historico = pd.DataFrame({
        "competencia": ["2026-02", "2026-03", "2026-02"],
        "regra": ["GHOST", "GHOST", "RQ005_ACS"],
        "cpf": ["11111111111", "11111111111", "22222222222"],
        "cns": [None, None, None],
    })
    n = calcular_reincidencia("2026-03", historico)
    assert n == 1  # apenas "11111111111" com GHOST reincide


def test_velocidade_regularizacao_zero_sem_historico():
    from analysis.metricas_avancadas import calcular_velocidade_regularizacao
    v = calcular_velocidade_regularizacao(pd.DataFrame(), "2026-03")
    assert v == 0.0
```

- [ ] **Step 2: Rodar e confirmar falha**
```
./venv/Scripts/python.exe -m pytest tests/analysis/test_metricas_avancadas.py -v --tb=short
```

- [ ] **Step 3: Criar `src/analysis/metricas_avancadas.py`**

```python
"""Funções puras de cálculo de métricas avançadas do pipeline CNES."""
import json

import pandas as pd


def calcular_taxa_anomalia(df_vinculos: pd.DataFrame, df_glosas: pd.DataFrame) -> float:
    """Proporção de profissionais únicos com ao menos uma glosa.

    Args:
        df_vinculos: DataFrame processado com coluna CPF.
        df_glosas: DataFrame de glosas com coluna cpf ou cns.

    Returns:
        Taxa entre 0.0 e 1.0.
    """
    if df_vinculos.empty or df_glosas.empty:
        return 0.0
    identificadores_anomalos = (
        df_glosas["cpf"].dropna().nunique() + df_glosas["cns"].dropna().nunique()
    )
    return min(identificadores_anomalos / len(df_vinculos), 1.0)


def calcular_p90_ch(df_vinculos: pd.DataFrame) -> float:
    """Percentil 90 da carga horária total.

    Args:
        df_vinculos: DataFrame com coluna CH_TOTAL.

    Returns:
        Valor do P90 ou 0.0 se vazio.
    """
    if df_vinculos.empty:
        return 0.0
    return float(df_vinculos["CH_TOTAL"].quantile(0.90))


def calcular_proporcao_feminina(df_vinculos: pd.DataFrame) -> float:
    """Proporção de registros com SEXO='F' entre os com SEXO não-nulo.

    Args:
        df_vinculos: DataFrame com coluna SEXO.

    Returns:
        Proporção entre 0.0 e 1.0.
    """
    if "SEXO" not in df_vinculos.columns:
        return 0.0
    com_sexo = df_vinculos[df_vinculos["SEXO"].notna() & (df_vinculos["SEXO"] != "None")]
    if com_sexo.empty:
        return 0.0
    return float((com_sexo["SEXO"] == "F").sum() / len(com_sexo))


def calcular_proporcao_feminina_por_cnes(df_vinculos: pd.DataFrame) -> list[dict]:
    """Proporção feminina agrupada por CNES.

    Args:
        df_vinculos: DataFrame com colunas SEXO e CNES.

    Returns:
        Lista de dicts [{cnes, proporcao_f, total}] ordenada por proporcao_f desc.
    """
    if df_vinculos.empty or "SEXO" not in df_vinculos.columns:
        return []
    com_sexo = df_vinculos[df_vinculos["SEXO"].notna() & (df_vinculos["SEXO"] != "None")]
    resultado = []
    for cnes, grupo in com_sexo.groupby("CNES"):
        prop_f = float((grupo["SEXO"] == "F").sum() / len(grupo))
        resultado.append({"cnes": cnes, "proporcao_f": round(prop_f, 4), "total": len(grupo)})
    return sorted(resultado, key=lambda x: x["proporcao_f"], reverse=True)


def calcular_top_glosas(df_glosas: pd.DataFrame, n: int = 10) -> list[dict]:
    """Top N profissionais por contagem de glosas.

    Args:
        df_glosas: DataFrame de glosas.
        n: Quantidade de profissionais no top.

    Returns:
        Lista de dicts [{identificador, nome, total}] ordenada por total desc.
    """
    if df_glosas.empty:
        return []
    df = df_glosas.copy()
    df["_id"] = df["cpf"].fillna(df["cns"]).fillna("DESCONHECIDO")
    agrupado = (
        df.groupby("_id")
        .agg(total=("regra", "count"), nome=("nome_profissional", "first"))
        .reset_index()
        .sort_values("total", ascending=False)
        .head(n)
    )
    return agrupado.rename(columns={"_id": "identificador"}).to_dict(orient="records")


def calcular_anomalias_por_cbo(
    df_vinculos: pd.DataFrame,
    df_glosas: pd.DataFrame,
    cbo_lookup: dict[str, str],
) -> list[dict]:
    """Contagem e taxa de anomalias por CBO.

    Args:
        df_vinculos: DataFrame com colunas CPF, CBO.
        df_glosas: DataFrame de glosas com coluna cpf.
        cbo_lookup: Dict CBO → descrição.

    Returns:
        Lista de dicts [{cbo, descricao, total_anomalias, taxa}] top 10 por total.
    """
    if df_vinculos.empty or df_glosas.empty:
        return []
    glosas_com_cbo = df_glosas.merge(
        df_vinculos[["CPF", "CBO"]].rename(columns={"CPF": "cpf"}),
        on="cpf", how="left",
    )
    totais_por_cbo = (
        glosas_com_cbo.groupby("CBO")["cpf"].count()
        .rename("total_anomalias")
        .reset_index()
    )
    vinculos_por_cbo = df_vinculos.groupby("CBO")["CPF"].count().rename("total_vinculos")
    totais_por_cbo = totais_por_cbo.merge(vinculos_por_cbo, on="CBO", how="left")
    totais_por_cbo["taxa"] = totais_por_cbo["total_anomalias"] / totais_por_cbo["total_vinculos"]
    totais_por_cbo["descricao"] = totais_por_cbo["CBO"].map(cbo_lookup).fillna("NAO CATALOGADO")
    return (
        totais_por_cbo.sort_values("total_anomalias", ascending=False)
        .head(10)
        .rename(columns={"CBO": "cbo"})
        [["cbo", "descricao", "total_anomalias", "taxa"]]
        .round({"taxa": 4})
        .to_dict(orient="records")
    )


def calcular_ranking_cnes(
    df_vinculos: pd.DataFrame,
    df_glosas: pd.DataFrame,
) -> list[dict]:
    """Ranking de CNES por volume de anomalias e índice de conformidade.

    Args:
        df_vinculos: DataFrame com colunas CNES, CPF.
        df_glosas: DataFrame de glosas com coluna cnes_estabelecimento.

    Returns:
        Lista de dicts [{cnes, total_anomalias, total_vinculos, indice_conformidade}].
    """
    if df_vinculos.empty:
        return []
    vinculos_por_cnes = df_vinculos.groupby("CNES")["CPF"].count().rename("total_vinculos")
    if df_glosas.empty:
        resultado = vinculos_por_cnes.reset_index().rename(columns={"CNES": "cnes"})
        resultado["total_anomalias"] = 0
        resultado["indice_conformidade"] = 1.0
        return resultado.to_dict(orient="records")
    anomalias_por_cnes = (
        df_glosas.groupby("cnes_estabelecimento")["regra"]
        .count()
        .rename("total_anomalias")
        .reset_index()
        .rename(columns={"cnes_estabelecimento": "CNES"})
    )
    merged = vinculos_por_cnes.reset_index().merge(anomalias_por_cnes, on="CNES", how="left")
    merged["total_anomalias"] = merged["total_anomalias"].fillna(0).astype(int)
    merged["indice_conformidade"] = (
        1 - (merged["total_anomalias"] / merged["total_vinculos"].replace(0, 1))
    ).clip(0, 1).round(4)
    return (
        merged.sort_values("total_anomalias", ascending=False)
        .rename(columns={"CNES": "cnes"})
        [["cnes", "total_anomalias", "total_vinculos", "indice_conformidade"]]
        .to_dict(orient="records")
    )


def calcular_taxa_resolucao(
    comp_anterior: str,
    comp_atual: str,
    df_glosas_historico: pd.DataFrame,
) -> float:
    """Proporção de glosas da competência anterior que não aparecem na atual.

    Args:
        comp_anterior: Competência anterior no formato YYYY-MM.
        comp_atual: Competência atual no formato YYYY-MM.
        df_glosas_historico: Histórico completo de glosas.

    Returns:
        Taxa de resolução entre 0.0 e 1.0.
    """
    if df_glosas_historico.empty:
        return 0.0
    df = df_glosas_historico.copy()
    df["_id"] = df["cpf"].fillna(df["cns"]).fillna("")
    anteriores = set(
        zip(df[df["competencia"] == comp_anterior]["_id"],
            df[df["competencia"] == comp_anterior]["regra"])
    )
    if not anteriores:
        return 0.0
    atuais = set(
        zip(df[df["competencia"] == comp_atual]["_id"],
            df[df["competencia"] == comp_atual]["regra"])
    )
    resolvidas = anteriores - atuais
    return round(len(resolvidas) / len(anteriores), 4)


def calcular_reincidencia(
    competencia_atual: str,
    df_glosas_historico: pd.DataFrame,
) -> int:
    """Conta profissionais com a mesma glosa em >= 2 competências consecutivas terminando na atual.

    Args:
        competencia_atual: Competência atual no formato YYYY-MM.
        df_glosas_historico: Histórico completo de glosas.

    Returns:
        Contagem de (identificador, regra) reincidentes.
    """
    if df_glosas_historico.empty:
        return 0
    df = df_glosas_historico.copy()
    df["_id"] = df["cpf"].fillna(df["cns"]).fillna("")
    competencias = sorted(df["competencia"].unique())
    if competencia_atual not in competencias or competencias.index(competencia_atual) == 0:
        return 0
    idx = competencias.index(competencia_atual)
    comp_anterior = competencias[idx - 1]
    atuais = set(zip(df[df["competencia"] == competencia_atual]["_id"],
                     df[df["competencia"] == competencia_atual]["regra"]))
    anteriores = set(zip(df[df["competencia"] == comp_anterior]["_id"],
                         df[df["competencia"] == comp_anterior]["regra"]))
    return len(atuais & anteriores)


def calcular_velocidade_regularizacao(
    df_glosas_historico: pd.DataFrame,
    competencia_atual: str,
) -> float:
    """Média de competências entre primeira aparição e resolução de glosas já regularizadas.

    Args:
        df_glosas_historico: Histórico completo de glosas.
        competencia_atual: Competência atual — glosas ainda presentes não contam.

    Returns:
        Média de competências para regularização ou 0.0 se não houver dados.
    """
    if df_glosas_historico.empty:
        return 0.0
    df = df_glosas_historico.copy()
    df["_id"] = df["cpf"].fillna(df["cns"]).fillna("")
    atuais = set(zip(
        df[df["competencia"] == competencia_atual]["_id"],
        df[df["competencia"] == competencia_atual]["regra"],
    ))
    passado = df[df["competencia"] < competencia_atual]
    duracoes = []
    for (identificador, regra), grupo in passado.groupby(["_id", "regra"]):
        if (identificador, regra) in atuais:
            continue
        comps_unicas = sorted(grupo["competencia"].unique())
        duracoes.append(len(comps_unicas))
    return round(sum(duracoes) / len(duracoes), 2) if duracoes else 0.0
```

- [ ] **Step 4: Rodar testes**
```
./venv/Scripts/python.exe -m pytest tests/analysis/test_metricas_avancadas.py -v --tb=short
```
Esperado: PASS.

- [ ] **Step 5: Commit**
```bash
git add src/analysis/metricas_avancadas.py tests/analysis/test_metricas_avancadas.py
git commit -m "feat(analysis): metricas_avancadas — funções puras de taxa, reincidência, resolução"
```

---

## Task 10: MetricasStage

**Files:**
- Create: `src/pipeline/stages/metricas.py`
- Create: `tests/pipeline/stages/test_metricas.py`

- [ ] **Step 1: Escrever testes que falham**

Criar `tests/pipeline/stages/test_metricas.py`:
```python
"""Testes do MetricasStage."""
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.state import PipelineState
from pipeline.stages.metricas import MetricasStage


def _state() -> PipelineState:
    state = PipelineState(
        competencia_ano=2026, competencia_mes=3,
        output_path=Path("x.csv"), executar_nacional=True, executar_hr=False,
    )
    state.df_processado = pd.DataFrame({
        "CPF": ["11111111111"], "CBO": ["515105"], "CNES": ["1234567"],
        "CNS": ["111111111111111"], "SEXO": ["F"], "CH_TOTAL": [40],
    })
    state.df_estab_local = pd.DataFrame({"CNES": ["1234567"], "NOME_FANTASIA": ["UBS"]})
    state.cbo_lookup = {}
    return state


@patch("pipeline.stages.metricas.HistoricoReader")
@patch("pipeline.stages.metricas.DatabaseLoader")
@patch("pipeline.stages.metricas.construir_glosas", return_value=pd.DataFrame())
@patch("pipeline.stages.metricas.config")
def test_metricas_stage_persiste_glosas(mock_cfg, mock_construir, mock_loader_cls, mock_reader_cls):
    mock_cfg.DUCKDB_PATH = Path("x.duckdb")
    mock_cfg.HISTORICO_DIR = Path("hist")
    mock_loader = mock_loader_cls.return_value
    mock_reader = mock_reader_cls.return_value
    mock_reader.carregar_glosas_historicas.return_value = pd.DataFrame()
    mock_reader.listar_competencias.return_value = []
    state = _state()
    MetricasStage().execute(state)
    mock_loader.gravar_metricas_avancadas.assert_called_once()


@patch("pipeline.stages.metricas.HistoricoReader")
@patch("pipeline.stages.metricas.DatabaseLoader")
@patch("pipeline.stages.metricas.construir_glosas", return_value=pd.DataFrame())
@patch("pipeline.stages.metricas.config")
def test_metricas_stage_preenche_state_metricas(mock_cfg, mock_construir, mock_loader_cls, mock_reader_cls):
    mock_cfg.DUCKDB_PATH = Path("x.duckdb")
    mock_cfg.HISTORICO_DIR = Path("hist")
    mock_reader = mock_reader_cls.return_value
    mock_reader.carregar_glosas_historicas.return_value = pd.DataFrame()
    mock_reader.listar_competencias.return_value = []
    state = _state()
    MetricasStage().execute(state)
    assert isinstance(state.metricas_avancadas, dict)
    assert "taxa_anomalia_geral" in state.metricas_avancadas
```

- [ ] **Step 2: Rodar e confirmar falha**
```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_metricas.py -v --tb=short
```

- [ ] **Step 3: Criar `src/pipeline/stages/metricas.py`**

```python
"""MetricasStage — computa glosas e métricas avançadas, persiste no DuckDB."""
import json
import logging
from datetime import datetime

import config
from analysis.glosas_builder import construir_glosas
from analysis.metricas_avancadas import (
    calcular_anomalias_por_cbo,
    calcular_p90_ch,
    calcular_proporcao_feminina,
    calcular_proporcao_feminina_por_cnes,
    calcular_ranking_cnes,
    calcular_reincidencia,
    calcular_taxa_anomalia,
    calcular_taxa_resolucao,
    calcular_velocidade_regularizacao,
    calcular_top_glosas,
)
from pipeline.state import PipelineState
from storage.database_loader import DatabaseLoader
from storage.historico_reader import HistoricoReader

logger = logging.getLogger(__name__)


class MetricasStage:
    nome = "metricas"

    def execute(self, state: PipelineState) -> None:
        agora = datetime.now()
        df_glosas = construir_glosas(state.competencia_str, state, agora)
        self._persistir_glosas(state.competencia_str, df_glosas)
        historico = self._carregar_historico(state.competencia_str)
        metricas = self._calcular(state, df_glosas, historico)
        state.metricas_avancadas = metricas
        loader = DatabaseLoader(config.DUCKDB_PATH)
        loader.gravar_metricas_avancadas(state.competencia_str, metricas)
        logger.info("metricas calculadas competencia=%s", state.competencia_str)

    def _persistir_glosas(self, competencia: str, df_glosas) -> None:
        loader = DatabaseLoader(config.DUCKDB_PATH)
        loader.inicializar_schema()
        for regra in df_glosas["regra"].unique() if not df_glosas.empty else []:
            loader.gravar_glosas(competencia, regra, df_glosas[df_glosas["regra"] == regra])

    def _carregar_historico(self, competencia: str):
        reader = HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)
        return reader.carregar_glosas_historicas()

    def _competencia_anterior(self, competencia: str) -> str | None:
        reader = HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)
        comps = reader.listar_competencias()
        idx = comps.index(competencia) if competencia in comps else -1
        return comps[idx - 1] if idx > 0 else None

    def _calcular(self, state: PipelineState, df_glosas, historico) -> dict:
        comp_anterior = self._competencia_anterior(state.competencia_str)
        return {
            "taxa_anomalia_geral": calcular_taxa_anomalia(state.df_processado, df_glosas),
            "p90_ch_total": calcular_p90_ch(state.df_processado),
            "proporcao_feminina_geral": calcular_proporcao_feminina(state.df_processado),
            "n_reincidentes": calcular_reincidencia(state.competencia_str, historico),
            "taxa_resolucao": (
                calcular_taxa_resolucao(comp_anterior, state.competencia_str, historico)
                if comp_anterior else 0.0
            ),
            "velocidade_regularizacao_media": calcular_velocidade_regularizacao(
                historico, state.competencia_str
            ),
            "top_glosas_json": json.dumps(calcular_top_glosas(df_glosas), ensure_ascii=False),
            "anomalias_por_cbo_json": json.dumps(
                calcular_anomalias_por_cbo(state.df_processado, df_glosas, state.cbo_lookup),
                ensure_ascii=False,
            ),
            "proporcao_feminina_por_cnes_json": json.dumps(
                calcular_proporcao_feminina_por_cnes(state.df_processado), ensure_ascii=False
            ),
            "ranking_cnes_json": json.dumps(
                calcular_ranking_cnes(state.df_processado, df_glosas), ensure_ascii=False
            ),
        }
```

- [ ] **Step 4: Rodar testes**
```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_metricas.py -v --tb=short
```

- [ ] **Step 5: Commit**
```bash
git add src/pipeline/stages/metricas.py tests/pipeline/stages/test_metricas.py
git commit -m "feat(pipeline): MetricasStage — glosas + métricas avançadas no DuckDB"
```

---

## Task 11: report_generator.py — aba Métricas Avançadas

**Files:**
- Modify: `src/export/report_generator.py`
- Test: `tests/export/test_report_generator.py`

- [ ] **Step 1: Escrever testes que falham**

Adicionar ao final de `tests/export/test_report_generator.py`:
```python
def test_gerar_relatorio_cria_aba_metricas_avancadas(tmp_path):
    import json
    from export.report_generator import gerar_relatorio
    caminho = tmp_path / "relatorio.xlsx"
    metricas = {
        "taxa_anomalia_geral": 0.1,
        "p90_ch_total": 40.0,
        "proporcao_feminina_geral": 0.6,
        "n_reincidentes": 2,
        "taxa_resolucao": 0.5,
        "velocidade_regularizacao_media": 2.0,
        "top_glosas_json": json.dumps([{"identificador": "111", "nome": "ANA", "total": 3}]),
        "anomalias_por_cbo_json": json.dumps([{"cbo": "515105", "descricao": "ACS", "total_anomalias": 2, "taxa": 0.1}]),
        "proporcao_feminina_por_cnes_json": json.dumps([]),
        "ranking_cnes_json": json.dumps([{"cnes": "1234567", "total_anomalias": 2, "total_vinculos": 10, "indice_conformidade": 0.8}]),
    }
    gerar_relatorio(caminho, {}, competencia="2026-03", metricas=metricas)
    from openpyxl import load_workbook
    wb = load_workbook(caminho)
    assert "Métricas Avancadas" in wb.sheetnames


def test_gerar_relatorio_sem_metricas_nao_cria_aba(tmp_path):
    from export.report_generator import gerar_relatorio
    caminho = tmp_path / "relatorio.xlsx"
    gerar_relatorio(caminho, {}, competencia="2026-03")
    from openpyxl import load_workbook
    wb = load_workbook(caminho)
    assert "Métricas Avancadas" not in wb.sheetnames
```

- [ ] **Step 2: Rodar e confirmar falha**
```
./venv/Scripts/python.exe -m pytest tests/export/test_report_generator.py -q --tb=short
```

- [ ] **Step 3: Modificar `src/export/report_generator.py`**

Alterar a assinatura de `gerar_relatorio`:
```python
def gerar_relatorio(
    caminho: Path,
    resultados: dict[str, pd.DataFrame],
    competencia: str = "",
    municipio: str = "Presidente Epitácio/SP",
    metricas: dict | None = None,
) -> None:
```

Alterar o corpo de `gerar_relatorio` para chamar a nova aba:
```python
    with pd.ExcelWriter(caminho, engine="openpyxl") as writer:
        _escrever_abas_dados(writer, resultados)
        _gerar_aba_resumo(writer.book, resultados, competencia, municipio)
        if metricas:
            _gerar_aba_metricas(writer.book, metricas)
        total_abas = len(writer.book.sheetnames)
```

Adicionar ao final do arquivo (antes de `_formatar_cabecalho`):
```python
def _gerar_aba_metricas(wb, metricas: dict) -> None:
    import json
    ws = wb.create_sheet("Métricas Avancadas")
    font_bold = Font(bold=True)
    indicadores = [
        ("Taxa de Anomalia Geral", f"{metricas.get('taxa_anomalia_geral', 0):.1%}"),
        ("P90 Carga Horária (h)", metricas.get("p90_ch_total", 0)),
        ("Proporção Feminina Geral", f"{metricas.get('proporcao_feminina_geral', 0):.1%}"),
        ("Profissionais Reincidentes", metricas.get("n_reincidentes", 0)),
        ("Taxa de Resolução vs Anterior", f"{metricas.get('taxa_resolucao', 0):.1%}"),
        ("Velocidade de Regularização (comp.)", metricas.get("velocidade_regularizacao_media", 0)),
    ]
    for linha, (rotulo, valor) in enumerate(indicadores, start=1):
        ws.cell(row=linha, column=1, value=rotulo).font = font_bold
        ws.cell(row=linha, column=2, value=valor)
    _escrever_bloco_json(ws, metricas.get("top_glosas_json", "[]"), 9,
                         ["identificador", "nome", "total"], "Top 10 Profissionais com Mais Glosas")
    _escrever_bloco_json(ws, metricas.get("anomalias_por_cbo_json", "[]"), 24,
                         ["cbo", "descricao", "total_anomalias", "taxa"], "Anomalias por CBO")
    _escrever_bloco_json(ws, metricas.get("ranking_cnes_json", "[]"), 39,
                         ["cnes", "total_anomalias", "total_vinculos", "indice_conformidade"],
                         "Ranking CNES por Conformidade")
    ws.column_dimensions["A"].width = 35
    ws.column_dimensions["B"].width = 20
    ws.column_dimensions["C"].width = 20
    ws.column_dimensions["D"].width = 20


def _escrever_bloco_json(ws, json_str: str, linha_ini: int, colunas: list[str], titulo: str) -> None:
    import json
    font_bold = Font(bold=True)
    fill = PatternFill(fill_type="solid", fgColor=_COR_CABECALHO)
    fonte_cab = Font(bold=True, color=_COR_FONTE_CABECALHO)
    ws.cell(row=linha_ini, column=1, value=titulo).font = font_bold
    for col_idx, col_nome in enumerate(colunas, start=1):
        cell = ws.cell(row=linha_ini + 1, column=col_idx, value=col_nome)
        cell.font = fonte_cab
        cell.fill = fill
    try:
        dados = json.loads(json_str or "[]")
    except (json.JSONDecodeError, TypeError):
        dados = []
    for offset, item in enumerate(dados, start=2):
        for col_idx, col_nome in enumerate(colunas, start=1):
            ws.cell(row=linha_ini + offset, column=col_idx, value=item.get(col_nome))
```

- [ ] **Step 4: Rodar testes**
```
./venv/Scripts/python.exe -m pytest tests/export/test_report_generator.py -v --tb=short
```

- [ ] **Step 5: Commit**
```bash
git add src/export/report_generator.py tests/export/test_report_generator.py
git commit -m "feat(export): aba Métricas Avancadas no relatório Excel"
```

---

## Task 12: ExportacaoStage — passa metricas ao relatório

**Files:**
- Modify: `src/pipeline/stages/exportacao.py`
- Test: `tests/pipeline/stages/test_exportacao.py`

- [ ] **Step 1: Escrever teste que falha**

Adicionar ao final de `tests/pipeline/stages/test_exportacao.py`:
```python
@patch("pipeline.stages.exportacao.gerar_relatorio")
@patch("pipeline.stages.exportacao.exportar_csv")
@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.config")
def test_exportacao_passa_metricas_ao_relatorio(
    mock_cfg, mock_salvar, mock_criar, mock_loader_cls, mock_csv, mock_relatorio, tmp_path
):
    from pathlib import Path
    from pipeline.state import PipelineState
    from pipeline.stages.exportacao import ExportacaoStage
    import pandas as pd
    mock_cfg.SNAPSHOTS_DIR = tmp_path
    mock_cfg.HISTORICO_DIR = tmp_path / "hist"
    mock_cfg.DUCKDB_PATH = tmp_path / "test.duckdb"
    mock_cfg.LAST_RUN_PATH = tmp_path / "last_run.json"
    mock_criar.return_value = MagicMock(data_competencia="2026-03")
    state = PipelineState(
        competencia_ano=2026, competencia_mes=3,
        output_path=tmp_path / "report.csv",
        executar_nacional=False, executar_hr=False,
    )
    state.metricas_avancadas = {"taxa_anomalia_geral": 0.1}
    ExportacaoStage().execute(state)
    _, kwargs = mock_relatorio.call_args
    assert kwargs.get("metricas") == {"taxa_anomalia_geral": 0.1}
```

- [ ] **Step 2: Rodar e confirmar falha**
```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_exportacao.py -q --tb=short
```

- [ ] **Step 3: Alterar `_gerar_relatorio` em `src/pipeline/stages/exportacao.py`**

```python
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
            metricas=state.metricas_avancadas or None,
        )
```

- [ ] **Step 4: Rodar testes**
```
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_exportacao.py -v --tb=short
```

- [ ] **Step 5: Commit**
```bash
git add src/pipeline/stages/exportacao.py tests/pipeline/stages/test_exportacao.py
git commit -m "feat(exportacao): repassa metricas_avancadas ao gerar_relatorio"
```

---

## Task 13: main.py — nova ordem de stages + integração final

**Files:**
- Modify: `src/main.py`
- Test: `tests/test_main.py`

- [ ] **Step 1: Escrever teste que falha**

Adicionar ao final de `tests/test_main.py`:
```python
def test_main_usa_nova_ordem_de_stages():
    import ast
    from pathlib import Path
    src = Path("src/main.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    # Verificar que AuditoriaStage não é importada
    imports = [
        node.names[0].name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    ]
    assert not any("auditoria" == n and "local" not in n and "nacional" not in n
                   for n in imports), "AuditoriaStage ainda importada"

def test_main_importa_metricas_stage():
    import ast
    from pathlib import Path
    src = Path("src/main.py").read_text(encoding="utf-8")
    assert "MetricasStage" in src
    assert "AuditoriaLocalStage" in src
    assert "AuditoriaNacionalStage" in src
```

- [ ] **Step 2: Rodar e confirmar falha**
```
./venv/Scripts/python.exe -m pytest tests/test_main.py -q --tb=short
```

- [ ] **Step 3: Atualizar `src/main.py`**

```python
"""Ponto de entrada do pipeline CnesData."""
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

import config
from cli import parse_args
from pipeline.orchestrator import PipelineOrchestrator
from pipeline.state import PipelineState
from pipeline.stages.auditoria_local import AuditoriaLocalStage
from pipeline.stages.auditoria_nacional import AuditoriaNacionalStage
from pipeline.stages.exportacao import ExportacaoStage
from pipeline.stages.ingestao_local import IngestaoLocalStage
from pipeline.stages.ingestao_nacional import IngestaoNacionalStage
from pipeline.stages.metricas import MetricasStage
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
    """Ponto de entrada principal.

    Returns:
        int: 0 = sucesso, 1 = erro.
    """
    args = parse_args()
    configurar_logging(verbose=args.verbose)
    state = _criar_estado(args)
    orchestrator = PipelineOrchestrator([
        IngestaoLocalStage(),
        ProcessamentoStage(),
        IngestaoNacionalStage(),
        AuditoriaLocalStage(),
        AuditoriaNacionalStage(),
        MetricasStage(),
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

- [ ] **Step 4: Rodar suite completa**
```
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```
Esperado: todos os testes passam.

- [ ] **Step 5: Lint**
```
./venv/Scripts/ruff.exe check src/ tests/
```
Corrigir qualquer erro antes de commitar.

- [ ] **Step 6: Commit final**
```bash
git add src/main.py tests/test_main.py
git commit -m "feat(main): nova ordem de stages — Local→Processamento→Nacional→AuditoriaLocal→AuditoriaNacional→Métricas→Exportação"
```
