# Performance Optimizations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduzir o tempo de execução do pipeline em > 50% via cache TTL do cascade_resolver, paralelização das fetches BigQuery, cache de resultados do BigQuery e remoção de dead code no transformer.

**Architecture:** Quatro mudanças independentes, ordenadas por impacto decrescente. Task 1 é trivial (dead code). Task 2 cria um novo módulo `src/analysis/verificacao_cache.py` (Decorator sobre `VerificadorCnes`). Task 3 paraleliza chamadas em `main.py` sem alterar os adapters. Task 4 adiciona cache pickle ao `CnesNacionalAdapter` via parâmetro `cache_dir`.

**Tech Stack:** `concurrent.futures.ThreadPoolExecutor` (stdlib), `json` (stdlib), `pickle` (stdlib), `pathlib.Path` (stdlib). Zero novas dependências externas.

---

## File Map

| Ação | Arquivo | Responsabilidade |
|---|---|---|
| Modificar | `src/processing/transformer.py:34-38` | Remover `TIPO_UNIDADE` e `COD_MUNICIPIO` de `_COLUNAS_TEXTO` |
| Criar | `src/analysis/verificacao_cache.py` | `CachingVerificadorCnes` — Decorator com cache JSON e TTL 24h |
| Criar | `tests/analysis/test_verificacao_cache.py` | 6 testes TDD do cache |
| Modificar | `src/config.py:89` | Adicionar `CACHE_DIR: Path` |
| Modificar | `src/main.py` | Usar `CachingVerificadorCnes` + `ThreadPoolExecutor` para BigQuery |
| Modificar | `tests/test_main.py` | Mocks para `CachingVerificadorCnes` e `ThreadPoolExecutor` |
| Modificar | `src/ingestion/cnes_nacional_adapter.py` | Adicionar `cache_dir` + `_ler_ou_cachear()` |
| Modificar | `tests/ingestion/test_cnes_nacional_adapter.py` | Testes de cache hit/miss/TTL |

---

## Task 1 — Remover dead code em `_COLUNAS_TEXTO` (transformer.py)

**Files:**
- Modify: `src/processing/transformer.py:34-38`

As colunas `"TIPO_UNIDADE"` e `"COD_MUNICIPIO"` estão em `_COLUNAS_TEXTO` mas **nunca existem** no DataFrame de entrada de `transformar()` — o guard `if coluna in resultado.columns` sempre retorna `False` para elas. São dead code com 2 iterações de loop desnecessárias.

- [ ] **Step 1: Confirmar que as colunas não existem no schema de entrada**

```bash
./venv/Scripts/python.exe -c "
import sys; sys.path.insert(0,'src')
from ingestion.schemas import SCHEMA_PROFISSIONAL
print(list(SCHEMA_PROFISSIONAL.keys()))
"
```

Saída esperada: lista sem `TIPO_UNIDADE` nem `COD_MUNICIPIO`.

- [ ] **Step 2: Rodar testes do transformer para confirmar baseline verde**

```bash
./venv/Scripts/python.exe -m pytest tests/processing/ -q --tb=short
```

Saída esperada: todos passando.

- [ ] **Step 3: Remover as colunas de `_COLUNAS_TEXTO`**

Em `src/processing/transformer.py`, linha 34, alterar:

```python
# Antes
_COLUNAS_TEXTO: Final[tuple[str, ...]] = (
    "CPF", "CNS", "NOME_PROFISSIONAL", "CBO", "CNES", "ESTABELECIMENTO",
    "NOME_SOCIAL", "SEXO", "TIPO_VINCULO", "SUS",
    "TIPO_UNIDADE", "COD_MUNICIPIO",
)

# Depois
_COLUNAS_TEXTO: Final[tuple[str, ...]] = (
    "CPF", "CNS", "NOME_PROFISSIONAL", "CBO", "CNES", "ESTABELECIMENTO",
    "NOME_SOCIAL", "SEXO", "TIPO_VINCULO", "SUS",
)
```

- [ ] **Step 4: Rodar testes**

```bash
./venv/Scripts/python.exe -m pytest tests/processing/ tests/test_main.py -q --tb=short
```

Saída esperada: todos passando (mesma contagem que Step 2).

- [ ] **Step 5: Commit**

```bash
git add src/processing/transformer.py
git commit -m "perf(transformer): remover TIPO_UNIDADE e COD_MUNICIPIO de _COLUNAS_TEXTO (dead code)"
```

---

## Task 2 — Cache TTL para cascade_resolver (MAIOR IMPACTO)

**Files:**
- Create: `src/analysis/verificacao_cache.py`
- Create: `tests/analysis/test_verificacao_cache.py`
- Modify: `src/config.py` (adicionar `CACHE_DIR`)
- Modify: `src/main.py` (usar `CachingVerificadorCnes`)
- Modify: `tests/test_main.py` (mock de `CachingVerificadorCnes`)

O cascade_resolver faz uma requisição HTTP por CNES e dorme 0.5s entre chamadas. Num run com 10 estabelecimentos fantasma: ~7–10s só de sleep. Numa segunda execução no mesmo dia, os mesmos CNES são re-verificados sem necessidade. Este task adiciona um cache JSON com TTL de 24h que evita HTTP em hits.

O padrão é Decorator: `CachingVerificadorCnes` implementa `VerificadorCnes` (Protocol) e envolve qualquer verificador real. O `main.py` instancia `CachingVerificadorCnes(CnesOficialWebAdapter(), config.CACHE_DIR / "cnes_verificados.json")`.

- [ ] **Step 1: Escrever testes RED**

Criar `tests/analysis/test_verificacao_cache.py`:

```python
"""Testes do CachingVerificadorCnes — cache TTL para verificações DATASUS."""

import json
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from analysis.verificacao_cache import CachingVerificadorCnes


def _verificador_mock(status: str = "CRITICO") -> MagicMock:
    m = MagicMock()
    m.verificar_estabelecimento.return_value = status
    return m


class TestCachingVerificadorCnes:

    def test_cache_miss_delega_ao_verificador_real(self, tmp_path):
        # Arrange
        verificador = _verificador_mock("CRITICO")
        cache = CachingVerificadorCnes(verificador, tmp_path / "cache.json")

        # Act
        resultado = cache.verificar_estabelecimento("1234567")

        # Assert
        assert resultado == "CRITICO"
        verificador.verificar_estabelecimento.assert_called_once_with("1234567")

    def test_cache_hit_nao_chama_verificador_real(self, tmp_path):
        # Arrange
        verificador = _verificador_mock("CRITICO")
        cache = CachingVerificadorCnes(verificador, tmp_path / "cache.json")
        cache.verificar_estabelecimento("1234567")  # popula cache

        # Act
        resultado = cache.verificar_estabelecimento("1234567")

        # Assert
        assert resultado == "CRITICO"
        assert verificador.verificar_estabelecimento.call_count == 1  # não chamou de novo

    def test_cache_expirado_chama_verificador_novamente(self, tmp_path):
        # Arrange — TTL de 0 segundos: qualquer entrada já está expirada
        verificador = _verificador_mock("CRITICO")
        cache = CachingVerificadorCnes(verificador, tmp_path / "cache.json", ttl_segundos=0)
        cache.verificar_estabelecimento("1234567")  # popula cache

        # Act — deve expirar imediatamente
        time.sleep(0.01)
        cache.verificar_estabelecimento("1234567")

        # Assert
        assert verificador.verificar_estabelecimento.call_count == 2

    def test_cache_persiste_entre_instancias(self, tmp_path):
        # Arrange — primeira instância popula o cache
        caminho = tmp_path / "cache.json"
        verificador1 = _verificador_mock("CRITICO")
        CachingVerificadorCnes(verificador1, caminho).verificar_estabelecimento("9999999")

        # Act — segunda instância lê do arquivo
        verificador2 = _verificador_mock("OUTRO")
        resultado = CachingVerificadorCnes(verificador2, caminho).verificar_estabelecimento("9999999")

        # Assert — leu do cache, não chamou verificador2
        assert resultado == "CRITICO"
        verificador2.verificar_estabelecimento.assert_not_called()

    def test_arquivo_corrompido_reinicia_cache(self, tmp_path):
        # Arrange
        caminho = tmp_path / "cache.json"
        caminho.write_text("{ JSON INVÁLIDO }", encoding="utf-8")
        verificador = _verificador_mock("CRITICO")

        # Act — deve ignorar o arquivo corrompido e funcionar normalmente
        cache = CachingVerificadorCnes(verificador, caminho)
        resultado = cache.verificar_estabelecimento("1234567")

        # Assert
        assert resultado == "CRITICO"

    def test_cria_diretorio_pai_se_inexistente(self, tmp_path):
        # Arrange
        caminho = tmp_path / "subdir" / "cache.json"
        verificador = _verificador_mock("CRITICO")

        # Act
        CachingVerificadorCnes(verificador, caminho).verificar_estabelecimento("1234567")

        # Assert
        assert caminho.exists()
```

- [ ] **Step 2: Confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/analysis/test_verificacao_cache.py -x --tb=short -q
```

Saída esperada: `ImportError: No module named 'analysis.verificacao_cache'`

- [ ] **Step 3: Criar `src/analysis/verificacao_cache.py`**

```python
"""CachingVerificadorCnes — decorator de cache TTL para verificações DATASUS."""

import json
import logging
import time
from pathlib import Path

from analysis.cascade_resolver import VerificadorCnes

logger = logging.getLogger(__name__)

_TTL_PADRAO: int = 86_400  # 24 horas em segundos


class CachingVerificadorCnes:
    """Decorator sobre VerificadorCnes com cache persistente JSON e TTL.

    Args:
        verificador: Implementação real do protocolo VerificadorCnes.
        caminho_cache: Arquivo JSON para persistência entre execuções.
        ttl_segundos: Tempo de vida de uma entrada (padrão: 86400 = 24h).
    """

    def __init__(
        self,
        verificador: VerificadorCnes,
        caminho_cache: Path,
        ttl_segundos: int = _TTL_PADRAO,
    ) -> None:
        self._verificador = verificador
        self._caminho_cache = caminho_cache
        self._ttl = ttl_segundos
        self._cache: dict[str, tuple[str, float]] = self._carregar()

    def verificar_estabelecimento(self, cnes: str) -> str:
        """Retorna status do cache se válido; senão delega ao verificador real.

        Args:
            cnes: Código CNES (7 dígitos).

        Returns:
            STATUS_CONFIRMADO | STATUS_LAG | STATUS_INDISPONIVEL
        """
        agora = time.time()
        if cnes in self._cache:
            status, gravado_em = self._cache[cnes]
            if agora - gravado_em < self._ttl:
                logger.info("cache_hit cnes=%s status=%s", cnes, status)
                return status

        status = self._verificador.verificar_estabelecimento(cnes)
        self._cache[cnes] = (status, agora)
        self._persistir()
        return status

    def _carregar(self) -> dict[str, tuple[str, float]]:
        if not self._caminho_cache.exists():
            return {}
        try:
            raw = json.loads(self._caminho_cache.read_text(encoding="utf-8"))
            return {k: (v[0], float(v[1])) for k, v in raw.items()}
        except (json.JSONDecodeError, KeyError, IndexError, TypeError):
            logger.warning("cache_corrompido path=%s reiniciando", self._caminho_cache)
            return {}

    def _persistir(self) -> None:
        self._caminho_cache.parent.mkdir(parents=True, exist_ok=True)
        self._caminho_cache.write_text(
            json.dumps(self._cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
```

- [ ] **Step 4: Confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/analysis/test_verificacao_cache.py -v --tb=short
```

Saída esperada: `6 passed`

- [ ] **Step 5: Adicionar `CACHE_DIR` ao `src/config.py`**

Em `src/config.py`, após a linha `DUCKDB_PATH`:

```python
CACHE_DIR: Path = RAIZ_PROJETO / os.getenv("CACHE_DIR", "data/cache")
```

- [ ] **Step 6: Atualizar `src/main.py` para usar `CachingVerificadorCnes`**

Adicionar import no topo de `src/main.py` (junto aos outros imports de `analysis/`):

```python
from analysis.verificacao_cache import CachingVerificadorCnes
```

Localizar o bloco (linhas ~245–247):

```python
        if executar_nacional and not df_estab_fantasma.empty:
            _adapter = CnesOficialWebAdapter()
            df_estab_fantasma = resolver_lag_rq006(df_estab_fantasma, _adapter)
```

Substituir por:

```python
        if executar_nacional and not df_estab_fantasma.empty:
            _adapter = CachingVerificadorCnes(
                CnesOficialWebAdapter(),
                config.CACHE_DIR / "cnes_verificados.json",
            )
            df_estab_fantasma = resolver_lag_rq006(df_estab_fantasma, _adapter)
```

- [ ] **Step 7: Adicionar mock de `CachingVerificadorCnes` em `tests/test_main.py`**

Em `tests/test_main.py`, na função `_aplicar_patches`, adicionar após `patch("main.CnesOficialWebAdapter")` (ou criar essa linha se não existir — procure por `CnesOficialWebAdapter` no arquivo):

```python
    stack.enter_context(patch("main.CachingVerificadorCnes"))
```

- [ ] **Step 8: Rodar suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Saída esperada: todos passando.

- [ ] **Step 9: Lint**

```bash
./venv/Scripts/ruff.exe check src/analysis/verificacao_cache.py src/config.py src/main.py --fix
./venv/Scripts/ruff.exe format src/analysis/verificacao_cache.py
```

- [ ] **Step 10: Commit**

```bash
git add src/analysis/verificacao_cache.py tests/analysis/test_verificacao_cache.py src/config.py src/main.py tests/test_main.py
git commit -m "perf(cascade): CachingVerificadorCnes — cache JSON TTL 24h para verificações DATASUS"
```

---

## Task 3 — Paralelizar fetches BigQuery no main.py

**Files:**
- Modify: `src/main.py`
- Modify: `tests/test_main.py`

As duas chamadas `repo_nacional.listar_profissionais(competencia)` e `repo_nacional.listar_estabelecimentos(competencia)` são independentes — cada uma faz uma query separada ao BigQuery. Executá-las em paralelo reduz o tempo de espera de `T_prof + T_estab` para `max(T_prof, T_estab)` (~40% de redução).

`basedosdados` / `google-cloud-bigquery` usa HTTP/REST internamente e é thread-safe para chamadas paralelas de leitura.

- [ ] **Step 1: Adicionar import no topo de `src/main.py`**

Junto aos outros imports da stdlib:

```python
from concurrent.futures import ThreadPoolExecutor
```

- [ ] **Step 2: Substituir as chamadas sequenciais por paralelas**

Localizar o bloco em `src/main.py` (linhas ~192–198):

```python
        if executar_nacional:
            repo_nacional = CnesNacionalAdapter(
                config.GCP_PROJECT_ID, config.ID_MUNICIPIO_IBGE7
            )
            competencia = (competencia_ano, competencia_mes)
            df_prof_nacional = repo_nacional.listar_profissionais(competencia)
            df_estab_nacional = repo_nacional.listar_estabelecimentos(competencia)
```

Substituir por:

```python
        if executar_nacional:
            repo_nacional = CnesNacionalAdapter(
                config.GCP_PROJECT_ID, config.ID_MUNICIPIO_IBGE7
            )
            competencia = (competencia_ano, competencia_mes)
            with ThreadPoolExecutor(max_workers=2) as pool:
                fut_prof = pool.submit(repo_nacional.listar_profissionais, competencia)
                fut_estab = pool.submit(repo_nacional.listar_estabelecimentos, competencia)
            df_prof_nacional = fut_prof.result()
            df_estab_nacional = fut_estab.result()
```

Nota: `.result()` é chamado FORA do bloco `with` — o `__exit__` do `ThreadPoolExecutor` aguarda ambas as futures antes de sair, então as futures já estão concluídas quando `.result()` é chamado.

- [ ] **Step 3: Verificar que os testes de main ainda passam**

```bash
./venv/Scripts/python.exe -m pytest tests/test_main.py -q --tb=short
```

Saída esperada: mesma contagem de testes passando que antes desta task.

Se um teste falhar com `MagicMock is not callable in thread`, adicionar `mock_adapter_nacional.listar_profissionais = MagicMock(return_value=df)` nos fixtures afetados. Os `MagicMock` são thread-safe para retorno de valor simples.

- [ ] **Step 4: Suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

- [ ] **Step 5: Lint**

```bash
./venv/Scripts/ruff.exe check src/main.py --fix
```

- [ ] **Step 6: Commit**

```bash
git add src/main.py
git commit -m "perf(main): paralelizar listar_profissionais e listar_estabelecimentos BigQuery com ThreadPoolExecutor"
```

---

## Task 4 — Cache pickle para resultados BigQuery no adapter

**Files:**
- Modify: `src/ingestion/cnes_nacional_adapter.py`
- Modify: `tests/ingestion/test_cnes_nacional_adapter.py`
- Modify: `src/main.py` (passar `cache_dir`)
- Modify: `tests/test_main.py` (atualizar mock do adapter)

Em desenvolvimento e em re-execuções no mesmo mês, as 2 queries BigQuery custam 15–60s cada por round-trip de rede. Um cache pickle por `(id_municipio, ano, mes, tipo)` com TTL de 1h elimina esse custo em todas as execuções subsequentes.

O adapter recebe `cache_dir: Path | None = None` no construtor. Quando `None` (padrão), o comportamento é idêntico ao atual — zero impacto em testes existentes.

- [ ] **Step 1: Escrever testes RED para o cache**

Adicionar ao FINAL de `tests/ingestion/test_cnes_nacional_adapter.py`:

```python
import pickle
from pathlib import Path


class TestCachePickle:

    def _adapter_com_cache(self, tmp_path: Path, mocker) -> CnesNacionalAdapter:
        adapter = CnesNacionalAdapter("proj", "1234567", cache_dir=tmp_path)
        mocker.patch.object(
            adapter._client,
            "fetch_estabelecimentos",
            return_value=_df_estabelecimentos_minimo(),
        )
        mocker.patch.object(
            adapter._client,
            "fetch_profissionais",
            return_value=_df_profissionais_minimo(),
        )
        return adapter

    def test_cache_miss_grava_arquivo_pickle(self, tmp_path, mocker):
        # Arrange
        adapter = self._adapter_com_cache(tmp_path, mocker)

        # Act
        adapter.listar_estabelecimentos((2024, 12))

        # Assert
        arquivos = list(tmp_path.glob("*.pkl"))
        assert len(arquivos) == 1

    def test_cache_hit_nao_chama_client(self, tmp_path, mocker):
        # Arrange
        adapter = self._adapter_com_cache(tmp_path, mocker)
        adapter.listar_estabelecimentos((2024, 12))  # popula cache

        # Act
        adapter.listar_estabelecimentos((2024, 12))

        # Assert — client chamado apenas 1 vez (no miss)
        assert adapter._client.fetch_estabelecimentos.call_count == 1

    def test_cache_expirado_chama_client_novamente(self, tmp_path, mocker):
        # Arrange — TTL de 0 segundos
        adapter = CnesNacionalAdapter("proj", "1234567", cache_dir=tmp_path, ttl_cache_segundos=0)
        mocker.patch.object(
            adapter._client, "fetch_estabelecimentos",
            return_value=_df_estabelecimentos_minimo(),
        )
        adapter.listar_estabelecimentos((2024, 12))

        # Act
        import time; time.sleep(0.01)
        adapter.listar_estabelecimentos((2024, 12))

        # Assert
        assert adapter._client.fetch_estabelecimentos.call_count == 2

    def test_cache_nao_usado_quando_cache_dir_none(self, mocker):
        # Arrange — adapter sem cache (padrão atual)
        adapter = CnesNacionalAdapter("proj", "1234567")
        mocker.patch.object(
            adapter._client, "fetch_estabelecimentos",
            return_value=_df_estabelecimentos_minimo(),
        )
        adapter.listar_estabelecimentos((2024, 12))
        adapter.listar_estabelecimentos((2024, 12))

        # Assert — sempre chama o client (sem cache)
        assert adapter._client.fetch_estabelecimentos.call_count == 2
```

Para os helpers de fixture (`_df_estabelecimentos_minimo`, `_df_profissionais_minimo`), use o padrão já presente no arquivo de testes (verifique as fixtures existentes antes de adicionar).

- [ ] **Step 2: Confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/ingestion/test_cnes_nacional_adapter.py::TestCachePickle -x --tb=short -q
```

Saída esperada: `TypeError` ou `ImportError` referente aos novos parâmetros.

- [ ] **Step 3: Modificar `src/ingestion/cnes_nacional_adapter.py`**

Adicionar imports no topo:

```python
import pickle
import time
from collections.abc import Callable
```

Alterar construtor e adicionar `_ler_ou_cachear`:

```python
_TTL_CACHE_PADRAO: int = 3_600  # 1 hora em segundos


class CnesNacionalAdapter:
    """Adapter entre o BigQuery (basedosdados) e o schema padronizado da ingestão."""

    def __init__(
        self,
        billing_project_id: str,
        id_municipio: str,
        cache_dir: Path | None = None,
        ttl_cache_segundos: int = _TTL_CACHE_PADRAO,
    ) -> None:
        self._client = CnesWebClient(billing_project_id)
        self._id_municipio = id_municipio
        self._cache_dir = cache_dir
        self._ttl = ttl_cache_segundos

    def _ler_ou_cachear(self, chave: str, buscar: Callable[[], pd.DataFrame]) -> pd.DataFrame:
        if self._cache_dir is None:
            return buscar()
        caminho = self._cache_dir / f"{chave}.pkl"
        if caminho.exists():
            idade = time.time() - caminho.stat().st_mtime
            if idade < self._ttl:
                logger.info("cache_hit chave=%s", chave)
                return pickle.loads(caminho.read_bytes())
        df = buscar()
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        caminho.write_bytes(pickle.dumps(df))
        logger.info("cache_gravado chave=%s rows=%d", chave, len(df))
        return df
```

Alterar `listar_estabelecimentos` para usar o cache:

```python
    def listar_estabelecimentos(self, competencia: tuple[int, int] | None = None) -> pd.DataFrame:
        """Retorna estabelecimentos nacionais com colunas padronizadas (FONTE=NACIONAL).

        Args:
            competencia: (ano, mes) obrigatório para o adapter nacional.

        Returns:
            DataFrame conforme SCHEMA_ESTABELECIMENTO.

        Raises:
            ValueError: Se competencia for None.
        """
        if competencia is None:
            raise ValueError("nacional_adapter competencia=obrigatoria")
        ano, mes = competencia
        chave = f"estab_{self._id_municipio}_{ano}_{mes:02d}"
        return self._ler_ou_cachear(chave, lambda: self._buscar_estabelecimentos(ano, mes))

    def _buscar_estabelecimentos(self, ano: int, mes: int) -> pd.DataFrame:
        df = self._client.fetch_estabelecimentos(self._id_municipio, ano, mes)
        df = df.rename(columns=_MAP_ESTABELECIMENTO)
        df["NOME_FANTASIA"] = None
        df["VINCULO_SUS"] = df["VINCULO_SUS"].map({1: "S", 0: "N"}).fillna("N")
        df["FONTE"] = _FONTE_NACIONAL
        logger.debug("listar_estabelecimentos fonte=NACIONAL rows=%d", len(df))
        return df[list(SCHEMA_ESTABELECIMENTO)]
```

Alterar `listar_profissionais` da mesma forma:

```python
    def listar_profissionais(self, competencia: tuple[int, int] | None = None) -> pd.DataFrame:
        """Retorna vínculos nacionais com colunas padronizadas (FONTE=NACIONAL).

        Args:
            competencia: (ano, mes) obrigatório para o adapter nacional.

        Returns:
            DataFrame conforme SCHEMA_PROFISSIONAL.

        Raises:
            ValueError: Se competencia for None.
        """
        if competencia is None:
            raise ValueError("nacional_adapter competencia=obrigatoria")
        ano, mes = competencia
        chave = f"prof_{self._id_municipio}_{ano}_{mes:02d}"
        return self._ler_ou_cachear(chave, lambda: self._buscar_profissionais(ano, mes))

    def _buscar_profissionais(self, ano: int, mes: int) -> pd.DataFrame:
        df = self._client.fetch_profissionais(self._id_municipio, ano, mes)
        df = df.rename(columns=_MAP_PROFISSIONAL)
        df["CPF"] = None
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

- [ ] **Step 4: Confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/ingestion/test_cnes_nacional_adapter.py -q --tb=short
```

Saída esperada: todos os testes passando (incluindo os 4 novos de cache).

- [ ] **Step 5: Atualizar `src/main.py` para passar `cache_dir`**

Localizar a construção do adapter em `src/main.py`:

```python
            repo_nacional = CnesNacionalAdapter(
                config.GCP_PROJECT_ID, config.ID_MUNICIPIO_IBGE7
            )
```

Substituir por:

```python
            repo_nacional = CnesNacionalAdapter(
                config.GCP_PROJECT_ID,
                config.ID_MUNICIPIO_IBGE7,
                cache_dir=config.CACHE_DIR,
            )
```

- [ ] **Step 6: Suite completa sem regressões**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Saída esperada: todos passando.

- [ ] **Step 7: Lint**

```bash
./venv/Scripts/ruff.exe check src/ingestion/cnes_nacional_adapter.py src/main.py tests/ingestion/test_cnes_nacional_adapter.py --fix
./venv/Scripts/ruff.exe format src/ingestion/cnes_nacional_adapter.py
```

- [ ] **Step 8: Commit**

```bash
git add src/ingestion/cnes_nacional_adapter.py tests/ingestion/test_cnes_nacional_adapter.py src/main.py
git commit -m "perf(nacional-adapter): cache pickle TTL 1h para resultados BigQuery"
```

---

## Self-Review — Spec Coverage

| Bottleneck do relatório | Coberto | Task |
|---|---|---|
| P1: TTL cache cascade_resolver | ✅ | Task 2 |
| P2: Parallel BigQuery fetches | ✅ | Task 3 |
| P3: BigQuery result cache | ✅ | Task 4 |
| P5: Dead columns transformer | ✅ | Task 1 |
| P4: Double normalization rules_engine | ⏳ Deferred | Impacto < 200ms em 10K rows; vale só quando dataset nacional crescer |
| P6: DuckDB connection consolidation | ⏳ Deferred | < 200ms; complexidade do context manager não justifica ganho |

### Notas

- **Task 3 (ThreadPoolExecutor)**: O `with ThreadPoolExecutor() as pool:` aguarda as futures no `__exit__`. Chamar `.result()` fora do bloco é seguro e mais legível do que chamar dentro.
- **Task 4 (pickle)**: O pickle é adequado para DataFrames pandas. O cache usa `st_mtime` do arquivo para TTL — simples e sem dependências extras. Se o arquivo corrompido precisar ser tratado, adicione `try/except` em `_ler_ou_cachear` em torno de `pickle.loads`.
- **`CACHE_DIR`** é adicionado em Task 2 e reutilizado em Task 4 — Task 4 depende de Task 2 estar concluída.

## Quick Reference

```bash
# Rodar apenas testes novos
./venv/Scripts/python.exe -m pytest tests/analysis/test_verificacao_cache.py tests/ingestion/test_cnes_nacional_adapter.py::TestCachePickle -v

# Suite completa
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q
```
