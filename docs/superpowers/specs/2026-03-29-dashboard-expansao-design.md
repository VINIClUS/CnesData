# Dashboard Expansão — Design Spec

**Data:** 2026-03-29
**Escopo:** `scripts/dashboard_status.py`, `scripts/dashboard.py`, `scripts/pages/1_Tendencias.py`, `tests/scripts/test_dashboard_status.py`, `CLAUDE.md`

---

## Objetivo

Três melhorias independentes no dashboard:

1. **5º card — API DATASUS**: health check com cache + verificação de auth token.
2. **Detalhes expandidos nos cards + expander de diretórios**: mais contexto em cada card de status e exibição dos paths de saída.
3. **Escala logarítmica em Tendências**: toggle sidebar para eixo Y log/linear.

---

## 1. Novo env var

| Variável | Obrigatório | Descrição |
|---|---|---|
| `DATASUS_AUTH_TOKEN` | não | Bearer token para `apidadosabertos.saude.gov.br` |

Adicionar ao quadro de variáveis em `CLAUDE.md`.

---

## 2. Módulo `scripts/dashboard_status.py`

### 2.1 `CardInfo` — novo campo `detalhe`

```python
@dataclass
class CardInfo:
    titulo: str
    fonte: str
    range_str: str | None = None
    detalhe: str | None = None
```

`_render_card` renderiza `detalhe` como linha extra (font 9px, cor #777) abaixo de `range_str`.

### 2.2 Funções novas: `_executar_health_check_datasus` + `_verificar_datasus`

```python
@st.cache_data(ttl=3_600)
def _verificar_datasus() -> DepStatus:
    token = os.getenv("DATASUS_AUTH_TOKEN")
    if not token:
        return DepStatus(ok=None)
    return _executar_health_check_datasus(
        "https://apidadosabertos.saude.gov.br/v1/cnes/estabelecimentos",
        token,
    )


def _executar_health_check_datasus(url: str, token: str) -> DepStatus:
    try:
        import requests
        headers = {"Authorization": f"Bearer {token}"}
        r = requests.get(url, headers=headers, params={"limit": 1}, timeout=5)
        if r.status_code < 300:
            return DepStatus(ok=True)
        if r.status_code == 401:
            return DepStatus(ok=False, erro="token inválido")
        return DepStatus(ok=False, erro=f"HTTP {r.status_code}")
    except Exception:
        return DepStatus(ok=False, erro="inacessível")
```

**Import tardio de `requests`** dentro de `try` para não quebrar em ambientes sem o pacote instalado. Na prática `requests` já está no projeto, mas o padrão é consistente com `basedosdados`.

**Sem token configurado** → `ok=None` (não configurado — mesmo comportamento dos outros cards opcionais). O health check não é executado, cache não é populado.

### 2.3 `renderizar_container_status` — 5º card + detalhes

Assinatura não muda. Internamente:

- Chama `_verificar_datasus()` para obter status do card DATASUS.
- Passa `detalhe` para cada `CardInfo`:
  - Firebird: `Path(os.getenv("DB_PATH", "")).name`
  - BigQuery: `os.getenv("GCP_PROJECT_ID", "—")`
  - DuckDB: `duckdb_path.name` (já recebido via contexto de chamada — ver §3)
  - HR/XLSX: `Path(os.getenv("FOLHA_HR_PATH", "")).name or "não configurado"`
  - DATASUS: `"token: configurado"` se `DATASUS_AUTH_TOKEN` presente, senão `"sem token"`
- Muda `st.columns(4)` → `st.columns(5)` e adiciona 5º card.

**`renderizar_container_status` precisa de `duckdb_path`** para o detalhe do card DuckDB. A assinatura ganha um 4º parâmetro:

```python
def renderizar_container_status(
    status: dict[str, DepStatus],
    competencias: list[str],
    cobertura: tuple[int, int],
    duckdb_path: Path,
) -> None:
```

Callers em `dashboard.py` passam `config.DUCKDB_PATH`.

### 2.4 Nova função `renderizar_container_diretorios`

```python
def renderizar_container_diretorios(
    output_path: Path,
    historico_dir: Path,
    duckdb_path: Path,
) -> None:
    """Renderiza expander com os diretórios de saída configurados.

    Args:
        output_path: Caminho do CSV principal de saída.
        historico_dir: Diretório de histórico de CSVs.
        duckdb_path: Caminho do arquivo DuckDB.
    """
    with st.expander("📁 Diretórios de Saída", expanded=False):
        st.markdown(f"**DuckDB:** `{duckdb_path}`")
        st.markdown(f"**Histórico:** `{historico_dir}`")
        st.markdown(f"**CSV saída:** `{output_path}`")
```

---

## 3. `scripts/dashboard.py`

Duas chamadas a `renderizar_container_status` ganham 4º argumento `config.DUCKDB_PATH`:

```python
renderizar_container_status(status, [], (0, 0), config.DUCKDB_PATH)
renderizar_container_status(status, competencias, cobertura, config.DUCKDB_PATH)
```

Logo abaixo do status container (antes do `selectbox` de competência):

```python
from dashboard_status import renderizar_container_diretorios
renderizar_container_diretorios(config.OUTPUT_PATH, config.HISTORICO_DIR, config.DUCKDB_PATH)
```

---

## 4. `scripts/pages/1_Tendencias.py`

Adicionar na sidebar, após o multiselect de regras:

```python
escala_log = st.sidebar.checkbox("Escala logarítmica", value=False)
```

Após `fig.update_xaxes(...)`:

```python
if escala_log:
    fig.update_yaxes(type="log")
```

Após `st.plotly_chart(...)`:

```python
if escala_log:
    st.caption("Valores zero não são exibidos em escala logarítmica.")
```

---

## 5. Testes

### `tests/scripts/test_dashboard_status.py` — adicionar

```python
class TestExecutarHealthCheckDatasus:

    def test_retorna_ok_true_quando_resposta_2xx(self, monkeypatch):
        mock_requests = MagicMock()
        mock_requests.get.return_value.status_code = 200
        monkeypatch.setitem(sys.modules, "requests", mock_requests)
        from importlib import reload
        import dashboard_status; reload(dashboard_status)
        from dashboard_status import _executar_health_check_datasus
        assert _executar_health_check_datasus("https://url", "token-abc").ok is True

    def test_retorna_false_com_erro_token_invalido_quando_401(self, monkeypatch):
        mock_requests = MagicMock()
        mock_requests.get.return_value.status_code = 401
        monkeypatch.setitem(sys.modules, "requests", mock_requests)
        from importlib import reload
        import dashboard_status; reload(dashboard_status)
        from dashboard_status import _executar_health_check_datasus
        result = _executar_health_check_datasus("https://url", "token-abc")
        assert result.ok is False
        assert "token" in result.erro

    def test_retorna_false_inacessivel_quando_excecao(self, monkeypatch):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = Exception("timeout")
        monkeypatch.setitem(sys.modules, "requests", mock_requests)
        from importlib import reload
        import dashboard_status; reload(dashboard_status)
        from dashboard_status import _executar_health_check_datasus
        result = _executar_health_check_datasus("https://url", "token-abc")
        assert result.ok is False
        assert "inacessível" in result.erro

    def test_retorna_false_com_http_status_quando_outro_erro(self, monkeypatch):
        mock_requests = MagicMock()
        mock_requests.get.return_value.status_code = 503
        monkeypatch.setitem(sys.modules, "requests", mock_requests)
        from importlib import reload
        import dashboard_status; reload(dashboard_status)
        from dashboard_status import _executar_health_check_datasus
        result = _executar_health_check_datasus("https://url", "token-abc")
        assert result.ok is False
        assert "503" in result.erro
```

---

## 6. Arquivos afetados

| Ação | Arquivo |
|---|---|
| Modificar | `scripts/dashboard_status.py` |
| Modificar | `scripts/dashboard.py` |
| Modificar | `scripts/pages/1_Tendencias.py` |
| Modificar | `tests/scripts/test_dashboard_status.py` |
| Modificar | `CLAUDE.md` |
