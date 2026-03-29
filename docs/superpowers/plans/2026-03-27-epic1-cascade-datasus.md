# Epic 1 — Double-Check Nacional via API Oficial DATASUS

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Após RQ-006 detectar estabelecimentos ausentes no BigQuery, um cascade check consulta a API DATASUS oficial para distinguir fantasmas reais de falsos positivos causados pelo lag de publicação (1-2 meses).

**Architecture:** `CnesOficialWebAdapter` faz chamadas HTTP à `apidadosabertos.saude.gov.br` com retry via `tenacity` em 5xx e fail-open em timeout. `cascade_resolver.py` contém `resolver_lag_rq006()` que aceita qualquer `VerificadorCnes` Protocol, anota `df_estab_fantasma` com `STATUS_VERIFICACAO` e filtra as linhas `RESOLVIDO: LAG_BASE_DOS_DADOS` antes do export. `main.py` chama esta função após `_cruzar_nacional()` completar.

**Tech Stack:** `requests` (já instalado), `tenacity` (instalar), `unittest.mock` para testes.

---

## File Map

| Arquivo | Ação | Responsabilidade |
|---|---|---|
| `src/ingestion/cnes_oficial_web_adapter.py` | Criar | HTTP adapter + tenacity retry + STATUS constants |
| `src/analysis/cascade_resolver.py` | Criar | `VerificadorCnes` Protocol + `resolver_lag_rq006()` |
| `tests/ingestion/test_cnes_oficial_web_adapter.py` | Criar | Testes unitários do adapter (mock `requests.Session`) |
| `tests/analysis/test_cascade_resolver.py` | Criar | Testes unitários do resolver (stub adapter) |
| `requirements.txt` | Modificar | Adicionar `tenacity` |
| `src/main.py` | Modificar | Importar + chamar `resolver_lag_rq006` após RQ-006 |

---

## Task 1: Dependência tenacity + esqueleto do adapter

**Files:**
- Modify: `requirements.txt`
- Create: `src/ingestion/cnes_oficial_web_adapter.py`

- [ ] **Step 1: Adicionar tenacity ao requirements.txt**

Inserir a linha abaixo após `SQLAlchemy==2.0.48` em `requirements.txt`:
```
tenacity==9.1.2
```

- [ ] **Step 2: Instalar tenacity**

```bash
./venv/Scripts/pip.exe install tenacity==9.1.2
```
Expected: `Successfully installed tenacity-9.1.2`

- [ ] **Step 3: Criar src/ingestion/cnes_oficial_web_adapter.py com esqueleto**

```python
"""Adapter para API de Dados Abertos do Ministério da Saúde — CNES oficial."""

import logging
import time

import requests
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://apidadosabertos.saude.gov.br/v1/cnes/estabelecimentos"

STATUS_CONFIRMADO = "CRITICO"
STATUS_LAG = "RESOLVIDO: LAG_BASE_DOS_DADOS"
STATUS_INDISPONIVEL = "API_INDISPONIVEL"


class _ServidorIndisponivel(Exception):
    pass


class CnesOficialWebAdapter:
    """Consulta estabelecimentos na API DATASUS oficial.

    Args:
        session: Sessão HTTP injetável. None = cria uma nova.
        auth_token: Bearer token opcional (necessário se API exigir autenticação).
    """

    def __init__(
        self,
        session: requests.Session | None = None,
        auth_token: str | None = None,
    ) -> None:
        self._session = session or requests.Session()
        if auth_token:
            self._session.headers["Authorization"] = f"Bearer {auth_token}"

    def verificar_estabelecimento(self, cnes: str) -> str:
        """Verifica se CNES existe na API DATASUS oficial.

        Args:
            cnes: Código CNES (7 dígitos).

        Returns:
            STATUS_CONFIRMADO | STATUS_LAG | STATUS_INDISPONIVEL
        """
        raise NotImplementedError

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(_ServidorIndisponivel),
        reraise=False,
    )
    def _chamar_com_retry(self, cnes: str) -> str:
        raise NotImplementedError
```

- [ ] **Step 4: Verificar que o módulo importa sem erro**

```bash
./venv/Scripts/python.exe -c "from ingestion.cnes_oficial_web_adapter import CnesOficialWebAdapter; print('ok')"
```
Expected: `ok`

- [ ] **Step 5: Commit**

```bash
git add requirements.txt src/ingestion/cnes_oficial_web_adapter.py
git commit -m "feat(ingestion): add tenacity + CnesOficialWebAdapter skeleton"
```

---

## Task 2: TDD — HTTP 200 confirma existência (STATUS_LAG)

**Files:**
- Create: `tests/ingestion/test_cnes_oficial_web_adapter.py`
- Modify: `src/ingestion/cnes_oficial_web_adapter.py`

- [ ] **Step 1: Criar arquivo de teste com helper e primeiro teste falhando**

```python
"""test_cnes_oficial_web_adapter.py — Testes unitários do adapter HTTP DATASUS."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ingestion.cnes_oficial_web_adapter import (
    CnesOficialWebAdapter,
    STATUS_CONFIRMADO,
    STATUS_INDISPONIVEL,
    STATUS_LAG,
)

_CNES = "0985333"


def _sessao_com_resposta(status_code: int) -> requests.Session:
    sessao = MagicMock(spec=requests.Session)
    resposta = MagicMock()
    resposta.status_code = status_code
    sessao.get.return_value = resposta
    sessao.headers = {}
    return sessao


def test_http_200_retorna_status_lag():
    adapter = CnesOficialWebAdapter(session=_sessao_com_resposta(200))
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_LAG
```

- [ ] **Step 2: Rodar para confirmar falha**

```bash
./venv/Scripts/python.exe -m pytest tests/ingestion/test_cnes_oficial_web_adapter.py::test_http_200_retorna_status_lag -v
```
Expected: `FAILED — NotImplementedError`

- [ ] **Step 3: Implementar _chamar_com_retry e verificar_estabelecimento para o caso 200**

Em `src/ingestion/cnes_oficial_web_adapter.py`, substituir os dois métodos:

```python
def verificar_estabelecimento(self, cnes: str) -> str:
    """Verifica se CNES existe na API DATASUS oficial.

    Args:
        cnes: Código CNES (7 dígitos).

    Returns:
        STATUS_CONFIRMADO | STATUS_LAG | STATUS_INDISPONIVEL
    """
    try:
        return self._chamar_com_retry(cnes)
    except (RetryError, requests.Timeout, requests.ConnectionError):
        logger.warning("api_oficial=indisponivel cnes=%s", cnes)
        return STATUS_INDISPONIVEL

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(_ServidorIndisponivel),
    reraise=False,
)
def _chamar_com_retry(self, cnes: str) -> str:
    resp = self._session.get(f"{_BASE_URL}/{cnes}", timeout=10)
    if resp.status_code in (500, 503):
        raise _ServidorIndisponivel(f"status={resp.status_code} cnes={cnes}")
    return STATUS_LAG if resp.status_code == 200 else STATUS_CONFIRMADO
```

- [ ] **Step 4: Rodar para confirmar que passa**

```bash
./venv/Scripts/python.exe -m pytest tests/ingestion/test_cnes_oficial_web_adapter.py::test_http_200_retorna_status_lag -v
```
Expected: `PASSED`

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/cnes_oficial_web_adapter.py tests/ingestion/test_cnes_oficial_web_adapter.py
git commit -m "test(ingestion): TDD HTTP 200 → STATUS_LAG no adapter DATASUS"
```

---

## Task 3: TDD — HTTP 404 e 204 confirmam fantasma (STATUS_CONFIRMADO)

**Files:**
- Modify: `tests/ingestion/test_cnes_oficial_web_adapter.py`

- [ ] **Step 1: Adicionar testes para 404 e 204**

```python
def test_http_404_retorna_status_confirmado():
    adapter = CnesOficialWebAdapter(session=_sessao_com_resposta(404))
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_CONFIRMADO


def test_http_204_retorna_status_confirmado():
    adapter = CnesOficialWebAdapter(session=_sessao_com_resposta(204))
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_CONFIRMADO
```

- [ ] **Step 2: Rodar para confirmar que passam (lógica já cobre estes casos)**

```bash
./venv/Scripts/python.exe -m pytest tests/ingestion/test_cnes_oficial_web_adapter.py::test_http_404_retorna_status_confirmado tests/ingestion/test_cnes_oficial_web_adapter.py::test_http_204_retorna_status_confirmado -v
```
Expected: ambos `PASSED` (a lógica `else STATUS_CONFIRMADO` já cobre 404 e 204)

- [ ] **Step 3: Commit**

```bash
git add tests/ingestion/test_cnes_oficial_web_adapter.py
git commit -m "test(ingestion): TDD HTTP 404/204 → STATUS_CONFIRMADO no adapter DATASUS"
```

---

## Task 4: TDD — Timeout e ConnectionError retornam STATUS_INDISPONIVEL (fail-open)

**Files:**
- Modify: `tests/ingestion/test_cnes_oficial_web_adapter.py`

- [ ] **Step 1: Adicionar testes de fail-open**

```python
def test_timeout_retorna_status_indisponivel():
    sessao = MagicMock(spec=requests.Session)
    sessao.get.side_effect = requests.Timeout()
    sessao.headers = {}
    adapter = CnesOficialWebAdapter(session=sessao)
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_INDISPONIVEL


def test_connection_error_retorna_status_indisponivel():
    sessao = MagicMock(spec=requests.Session)
    sessao.get.side_effect = requests.ConnectionError()
    sessao.headers = {}
    adapter = CnesOficialWebAdapter(session=sessao)
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_INDISPONIVEL
```

- [ ] **Step 2: Rodar para confirmar que passam**

```bash
./venv/Scripts/python.exe -m pytest tests/ingestion/test_cnes_oficial_web_adapter.py::test_timeout_retorna_status_indisponivel tests/ingestion/test_cnes_oficial_web_adapter.py::test_connection_error_retorna_status_indisponivel -v
```
Expected: ambos `PASSED`

- [ ] **Step 3: Commit**

```bash
git add tests/ingestion/test_cnes_oficial_web_adapter.py
git commit -m "test(ingestion): TDD fail-open (Timeout/ConnectionError → STATUS_INDISPONIVEL)"
```

---

## Task 5: TDD — HTTP 503 aciona retry (tenacity), esgotado → STATUS_INDISPONIVEL

**Files:**
- Modify: `tests/ingestion/test_cnes_oficial_web_adapter.py`

- [ ] **Step 1: Adicionar testes de retry com tenacity**

```python
def test_http_503_exaustao_retorna_status_indisponivel():
    """503 repetido 3x esgota o retry → STATUS_INDISPONIVEL."""
    adapter = CnesOficialWebAdapter(session=_sessao_com_resposta(503))
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_INDISPONIVEL


def test_http_503_seguido_200_retorna_status_lag():
    """503 na primeira tentativa, 200 na segunda → STATUS_LAG."""
    sessao = MagicMock(spec=requests.Session)
    resp_503 = MagicMock()
    resp_503.status_code = 503
    resp_200 = MagicMock()
    resp_200.status_code = 200
    sessao.get.side_effect = [resp_503, resp_200]
    sessao.headers = {}
    adapter = CnesOficialWebAdapter(session=sessao)
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_LAG
```

- [ ] **Step 2: Rodar com tenacity wait desabilitado para velocidade**

Os testes de retry do tenacity são lentos por padrão (backoff real). Para evitar isso em CI, usar `patch` no `wait`:

```python
def test_http_503_exaustao_retorna_status_indisponivel():
    with patch("ingestion.cnes_oficial_web_adapter.wait_exponential", return_value=lambda *a, **k: 0):
        adapter = CnesOficialWebAdapter(session=_sessao_com_resposta(503))
        assert adapter.verificar_estabelecimento(_CNES) == STATUS_INDISPONIVEL


def test_http_503_seguido_200_retorna_status_lag():
    sessao = MagicMock(spec=requests.Session)
    resp_503 = MagicMock(); resp_503.status_code = 503
    resp_200 = MagicMock(); resp_200.status_code = 200
    sessao.get.side_effect = [resp_503, resp_200]
    sessao.headers = {}
    with patch("ingestion.cnes_oficial_web_adapter.wait_exponential", return_value=lambda *a, **k: 0):
        adapter = CnesOficialWebAdapter(session=sessao)
        assert adapter.verificar_estabelecimento(_CNES) == STATUS_LAG
```

**Nota:** O patch de `wait_exponential` não funciona depois que o decorator `@retry` já foi aplicado no import do módulo. A abordagem correta para testar retry rápido é usar o parâmetro `wait=wait_none()` no decorator durante testes, ou substituir o método `_chamar_com_retry` na instância. Use a segunda abordagem:

```python
from tenacity import wait_none

def test_http_503_exaustao_retorna_status_indisponivel():
    adapter = CnesOficialWebAdapter(session=_sessao_com_resposta(503))
    adapter._chamar_com_retry = retry(
        stop=stop_after_attempt(3),
        wait=wait_none(),
        retry=retry_if_exception_type(_ServidorIndisponivel),
        reraise=False,
    )(adapter._chamar_com_retry.__wrapped__)
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_INDISPONIVEL


def test_http_503_seguido_200_retorna_status_lag():
    sessao = MagicMock(spec=requests.Session)
    resp_503 = MagicMock(); resp_503.status_code = 503
    resp_200 = MagicMock(); resp_200.status_code = 200
    sessao.get.side_effect = [resp_503, resp_200]
    sessao.headers = {}
    adapter = CnesOficialWebAdapter(session=sessao)
    adapter._chamar_com_retry = retry(
        stop=stop_after_attempt(3),
        wait=wait_none(),
        retry=retry_if_exception_type(_ServidorIndisponivel),
        reraise=False,
    )(adapter._chamar_com_retry.__wrapped__)
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_LAG
```

Para que `__wrapped__` funcione, é necessário que o método seja um método de instância wrappado por tenacity. Adicionar `import` no topo do test file:

```python
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_none
from ingestion.cnes_oficial_web_adapter import _ServidorIndisponivel
```

- [ ] **Step 3: Rodar todos os testes do adapter**

```bash
./venv/Scripts/python.exe -m pytest tests/ingestion/test_cnes_oficial_web_adapter.py -v
```
Expected: todos `PASSED`

- [ ] **Step 4: Commit**

```bash
git add tests/ingestion/test_cnes_oficial_web_adapter.py
git commit -m "test(ingestion): TDD retry tenacity 503 + exaustão → STATUS_INDISPONIVEL"
```

---

## Task 6: Criar cascade_resolver.py com Protocol e resolver_lag_rq006

**Files:**
- Create: `src/analysis/cascade_resolver.py`

- [ ] **Step 1: Criar src/analysis/cascade_resolver.py**

```python
"""cascade_resolver.py — Resolução de falsos positivos RQ-006 via API DATASUS."""

import logging
import time
from typing import Protocol

import pandas as pd

from ingestion.cnes_oficial_web_adapter import STATUS_LAG

logger = logging.getLogger(__name__)


class VerificadorCnes(Protocol):
    """Contrato para qualquer adapter que verifique estabelecimentos na API oficial."""

    def verificar_estabelecimento(self, cnes: str) -> str: ...


def resolver_lag_rq006(
    df: pd.DataFrame,
    verificador: VerificadorCnes,
    sleep_entre_chamadas: float = 0.5,
) -> pd.DataFrame:
    """Filtra falsos positivos RQ-006 via API DATASUS oficial.

    Itera sobre CNES do DataFrame, consulta a API para cada um e anota
    STATUS_VERIFICACAO. Linhas com STATUS_LAG são removidas do resultado.

    Args:
        df: Estabelecimentos fantasma detectados pela RQ-006.
        verificador: Adapter HTTP compatível com VerificadorCnes Protocol.
        sleep_entre_chamadas: Delay em segundos entre requisições (rate limiting).

    Returns:
        DataFrame anotado com coluna STATUS_VERIFICACAO, sem linhas LAG resolvidas.
    """
    if df.empty:
        return df.copy()

    statuses = []
    for cnes in df["CNES"]:
        statuses.append(verificador.verificar_estabelecimento(cnes))
        time.sleep(sleep_entre_chamadas)

    resultado = df.copy()
    resultado["STATUS_VERIFICACAO"] = statuses

    n_lag = (resultado["STATUS_VERIFICACAO"] == STATUS_LAG).sum()
    n_indisponivel = (resultado["STATUS_VERIFICACAO"] != STATUS_LAG).sum()

    logger.info(
        "cascade_rq006 total=%d lag_removidos=%d remanescentes=%d",
        len(df), n_lag, n_indisponivel,
    )

    return resultado[resultado["STATUS_VERIFICACAO"] != STATUS_LAG].copy()
```

- [ ] **Step 2: Verificar que importa sem erro**

```bash
./venv/Scripts/python.exe -c "from analysis.cascade_resolver import resolver_lag_rq006; print('ok')"
```
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/analysis/cascade_resolver.py
git commit -m "feat(analysis): add cascade_resolver with VerificadorCnes Protocol"
```

---

## Task 7: TDD — resolver_lag_rq006 comportamento completo

**Files:**
- Create: `tests/analysis/test_cascade_resolver.py`

- [ ] **Step 1: Criar test_cascade_resolver.py com stub adapter e todos os cenários**

```python
"""test_cascade_resolver.py — Testes unitários do resolver de lag RQ-006."""

from unittest.mock import patch

import pandas as pd
import pytest

from analysis.cascade_resolver import resolver_lag_rq006
from ingestion.cnes_oficial_web_adapter import (
    STATUS_CONFIRMADO,
    STATUS_INDISPONIVEL,
    STATUS_LAG,
)


def _df_fantasma(cnes_list: list[str]) -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": cnes_list,
        "NOME_FANTASIA": [f"UBS {c}" for c in cnes_list],
        "TIPO_UNIDADE": ["01"] * len(cnes_list),
        "FONTE": ["LOCAL"] * len(cnes_list),
    })


class _StubVerificador:
    def __init__(self, mapa: dict[str, str]) -> None:
        self._mapa = mapa

    def verificar_estabelecimento(self, cnes: str) -> str:
        return self._mapa.get(cnes, STATUS_CONFIRMADO)


def test_df_vazio_retorna_df_vazio():
    df = pd.DataFrame(columns=["CNES", "NOME_FANTASIA", "TIPO_UNIDADE", "FONTE"])
    resultado = resolver_lag_rq006(df, _StubVerificador({}), sleep_entre_chamadas=0)
    assert resultado.empty
    assert list(resultado.columns) == list(df.columns)


def test_todos_confirmados_retornam_com_status_critico():
    df = _df_fantasma(["0001111", "0002222"])
    stub = _StubVerificador({"0001111": STATUS_CONFIRMADO, "0002222": STATUS_CONFIRMADO})
    resultado = resolver_lag_rq006(df, stub, sleep_entre_chamadas=0)
    assert len(resultado) == 2
    assert (resultado["STATUS_VERIFICACAO"] == STATUS_CONFIRMADO).all()


def test_todos_lag_sao_filtrados():
    df = _df_fantasma(["0001111", "0002222"])
    stub = _StubVerificador({"0001111": STATUS_LAG, "0002222": STATUS_LAG})
    resultado = resolver_lag_rq006(df, stub, sleep_entre_chamadas=0)
    assert resultado.empty


def test_misto_retorna_apenas_nao_lag():
    df = _df_fantasma(["0001111", "0002222", "0003333"])
    stub = _StubVerificador({
        "0001111": STATUS_LAG,
        "0002222": STATUS_CONFIRMADO,
        "0003333": STATUS_INDISPONIVEL,
    })
    resultado = resolver_lag_rq006(df, stub, sleep_entre_chamadas=0)
    assert len(resultado) == 2
    assert "0001111" not in resultado["CNES"].values
    assert "0002222" in resultado["CNES"].values
    assert "0003333" in resultado["CNES"].values


def test_api_indisponivel_permanece_no_resultado():
    df = _df_fantasma(["0001111"])
    stub = _StubVerificador({"0001111": STATUS_INDISPONIVEL})
    resultado = resolver_lag_rq006(df, stub, sleep_entre_chamadas=0)
    assert len(resultado) == 1
    assert resultado.iloc[0]["STATUS_VERIFICACAO"] == STATUS_INDISPONIVEL


def test_sleep_chamado_uma_vez_por_cnes():
    df = _df_fantasma(["0001111", "0002222"])
    stub = _StubVerificador({})
    with patch("analysis.cascade_resolver.time.sleep") as mock_sleep:
        resolver_lag_rq006(df, stub, sleep_entre_chamadas=0.5)
    assert mock_sleep.call_count == 2
    mock_sleep.assert_called_with(0.5)


def test_colunas_originais_preservadas():
    df = _df_fantasma(["0001111"])
    stub = _StubVerificador({"0001111": STATUS_CONFIRMADO})
    resultado = resolver_lag_rq006(df, stub, sleep_entre_chamadas=0)
    for col in df.columns:
        assert col in resultado.columns
    assert "STATUS_VERIFICACAO" in resultado.columns
```

- [ ] **Step 2: Rodar para confirmar que falham**

```bash
./venv/Scripts/python.exe -m pytest tests/analysis/test_cascade_resolver.py -v
```
Expected: todos `FAILED` (módulo existe mas lógica incompleta em cenários edge)

Na verdade, a maioria deve passar. Corrigir qualquer falha antes do próximo passo.

- [ ] **Step 3: Rodar suite completa de testes para garantir não-regressão**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```
Expected: todos passando

- [ ] **Step 4: Commit**

```bash
git add tests/analysis/test_cascade_resolver.py
git commit -m "test(analysis): TDD resolver_lag_rq006 — 7 cenários completos"
```

---

## Task 8: Integrar cascade_resolver no main.py

**Files:**
- Modify: `src/main.py`
- Modify: `tests/test_main.py`

- [ ] **Step 1: Ler tests/test_main.py para identificar onde mockar o novo adapter**

```bash
./venv/Scripts/python.exe -m pytest tests/test_main.py -v --collect-only
```

Identificar os testes que chamam `_cruzar_nacional` ou que testam o pipeline com `executar_nacional=True`.

- [ ] **Step 2: Adicionar imports em src/main.py**

No bloco de imports existente, adicionar:

```python
from analysis.cascade_resolver import resolver_lag_rq006
from ingestion.cnes_oficial_web_adapter import CnesOficialWebAdapter
```

- [ ] **Step 3: Integrar chamada cascade após _cruzar_nacional no main()**

No `main()`, localizar o bloco:

```python
        df_estab_fantasma = nac["estab_fantasma"]
```

Substituir por:

```python
        df_estab_fantasma = nac["estab_fantasma"]
        if executar_nacional and not df_estab_fantasma.empty:
            _adapter = CnesOficialWebAdapter()
            df_estab_fantasma = resolver_lag_rq006(df_estab_fantasma, _adapter)
```

- [ ] **Step 4: Rodar testes existentes do main.py**

```bash
./venv/Scripts/python.exe -m pytest tests/test_main.py -v --tb=short
```

Se algum teste falhar porque `CnesOficialWebAdapter` faz chamadas HTTP reais, adicionar mock no fixture correspondente. O padrão já usado no projeto é `unittest.mock.patch`. Adicionar nos testes que cobrem fluxo nacional:

```python
with patch(
    "main.CnesOficialWebAdapter",
    return_value=MagicMock(verificar_estabelecimento=lambda cnes: "CRITICO"),
):
```

- [ ] **Step 5: Rodar suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```
Expected: todos passando

- [ ] **Step 6: Lint**

```bash
./venv/Scripts/ruff.exe check src/ tests/
```
Expected: sem erros

- [ ] **Step 7: Commit**

```bash
git add src/main.py tests/test_main.py
git commit -m "feat(main): integrate cascade_resolver after RQ-006 (Epic 1)"
```

---

## Self-Review

### Spec coverage

| Requisito do PRD | Task que implementa |
|---|---|
| Bulk check via BigQuery primeiro | Não alterado — `_cruzar_nacional()` executa primeiro (Task 8) |
| Adapter recebe apenas CNES locais não encontrados no BigQuery | Task 8 — `resolver_lag_rq006(df_estab_fantasma, ...)` |
| HTTP 200 → `RESOLVIDO: LAG_BASE_DOS_DADOS` | Task 2 + Task 7 |
| HTTP 404/204 → anomalia confirmada como Crítica | Task 3 + Task 7 |
| Fail-open: API fora do ar → assume resultado BigQuery + WARNING | Task 4 + Task 7 |
| `tenacity` Exponential Backoff em HTTP 500/503 | Task 1 + Task 5 |
| `time.sleep(0.5)` entre chamadas (rate limiting) | Task 6 + Task 7 (`test_sleep_chamado`) |
| Autenticação Bearer Token opcional | Task 1 (`auth_token` param no `__init__`) |

### Gaps identificados

Nenhum gap. Todos os requisitos do PRD têm task correspondente.

### Verificação de tipos/assinaturas

- `resolver_lag_rq006(df, verificador, sleep_entre_chamadas=0.5)` — consistente em Tasks 6 e 7
- `STATUS_CONFIRMADO`, `STATUS_LAG`, `STATUS_INDISPONIVEL` — definidos em Task 1, importados em Tasks 6 e 7
- `_StubVerificador` satisfaz `VerificadorCnes` Protocol via duck typing — verificado pela assinatura `verificar_estabelecimento(self, cnes: str) -> str`
