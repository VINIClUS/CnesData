# Competência Precision — Design Spec

**Data:** 2026-03-29
**Escopo:** `src/storage/`, `scripts/dashboard_status.py`, `scripts/dashboard.py`, `scripts/pages/`

---

## Objetivo

Aumentar a precisão dos dados exibidos no dashboard em duas dimensões:

1. **Validação temporal das competências CNES Local** — o Firebird é um banco ao vivo sem histórico. Competências anteriores só são confiáveis se os dados foram gravados dentro da janela de coleta daquela competência. Filtrar `listar_competencias()` pela coluna `gravado_em` já presente no DuckDB.

2. **Range BigQuery via query direta** — em vez de depender do `last_run.json` (atualizado apenas quando o pipeline roda), o dashboard consulta o BigQuery diretamente uma vez por dia com cache, com cadeia de fallback para `last_run.json`.

---

## 1. Janela de Validade de Competência

### Definição

Para a competência `YYYY-MM`, os dados são considerados válidos se foram gravados entre:

- **Início (inclusivo):** 5º dia útil do mês `YYYY-MM`
- **Fim (exclusivo):** 5º dia útil do mês `YYYY-(MM+1)`

**Dia útil:** segunda a sexta, excluindo feriados nacionais brasileiros (lib `holidays`, country `BR`).

### Exemplos

| Competência | Início válido | Fim exclusivo |
|---|---|---|
| `2026-01` | 07/01/2026 (5º dia útil de jan) | 06/02/2026 (5º dia útil de fev) |
| `2026-02` | 06/02/2026 | 06/03/2026 |
| `2026-03` | 06/03/2026 | 08/04/2026 |

---

## 2. Módulo `src/storage/competencia_utils.py`

Módulo novo. Centraliza cálculo de dias úteis e janela de competência. Sem dependência de Streamlit — testável de forma isolada.

```python
from datetime import date, timedelta
from functools import lru_cache
import holidays

_BR = holidays.country_holidays("BR")


@lru_cache(maxsize=128)
def quinto_dia_util(ano: int, mes: int) -> date:
    """Retorna o 5º dia útil (seg–sex, sem feriados nacionais BR) do mês.

    Args:
        ano: Ano calendário (ex: 2026).
        mes: Mês calendário 1–12.

    Returns:
        Data do 5º dia útil do mês.
    """
    dia = date(ano, mes, 1)
    count = 0
    while count < 5:
        if dia.weekday() < 5 and dia not in _BR:
            count += 1
        if count < 5:
            dia += timedelta(days=1)
    return dia


def janela_valida(competencia: str) -> tuple[date, date]:
    """Retorna (início_inclusivo, fim_exclusivo) da janela de coleta.

    Args:
        competencia: Competência no formato YYYY-MM.

    Returns:
        Tupla (inicio, fim) onde inicio é inclusivo e fim é exclusivo.
    """
    ano, mes = int(competencia[:4]), int(competencia[5:7])
    inicio = quinto_dia_util(ano, mes)
    ano_prox = ano if mes < 12 else ano + 1
    mes_prox = mes + 1 if mes < 12 else 1
    fim = quinto_dia_util(ano_prox, mes_prox)
    return inicio, fim
```

**`@lru_cache(maxsize=128)`** no `quinto_dia_util`: o cálculo é determinístico para qualquer mês passado. O dashboard chama `janela_valida` para cada competência no DuckDB por sessão — o cache evita recalcular os mesmos meses.

---

## 3. `src/storage/historico_reader.py` — Métodos novos

Dois métodos adicionados sem alterar o `listar_competencias()` existente (backward-compatible).

### `listar_competencias_validas() -> list[str]`

Retorna competências cujo `gravado_em` cai dentro da janela de validade da competência correspondente. Ordenação ascendente (YYYY-MM).

Lógica aplicada em Python após a query (DuckDB retorna todas as competências com seus `gravado_em` mínimo e máximo; Python filtra pela janela):

```python
def listar_competencias_validas(self) -> list[str]:
    """Competências com gravado_em dentro da janela de coleta CNES Local.

    Returns:
        Lista de competências YYYY-MM em ordem ascendente.
    """
    rows = self._con.execute(
        "SELECT data_competencia, MIN(gravado_em) AS primeiro_gravado "
        "FROM gold.evolucao_metricas_mensais "
        "GROUP BY data_competencia ORDER BY data_competencia"
    ).fetchall()
    validas = []
    for comp, primeiro_gravado in rows:
        inicio, fim = janela_valida(comp)
        if inicio <= primeiro_gravado.date() < fim:
            validas.append(comp)
    return validas
```

### `contar_competencias() -> tuple[int, int]`

Retorna `(válidas, total)` para o card DuckDB no status container.

```python
def contar_competencias(self) -> tuple[int, int]:
    """Retorna (válidas, total) de competências no DuckDB.

    Returns:
        Tupla (n_validas, n_total).
    """
    total = len(self.listar_competencias())
    validas = len(self.listar_competencias_validas())
    return validas, total
```

### Uso nas páginas do dashboard

As três páginas trocam `listar_competencias()` por `listar_competencias_validas()`:
- `scripts/dashboard.py`
- `scripts/pages/1_Tendencias.py`
- `scripts/pages/2_Por_Regra.py`

---

## 4. `scripts/dashboard_status.py` — Range BigQuery + fallback

### Nova função `_consultar_range_bigquery`

```python
@st.cache_data(ttl=86_400)
def _consultar_range_bigquery(project_id: str) -> tuple[str, str] | None:
    """Query BigQuery para obter range de competências. Cache de 1 dia.

    Args:
        project_id: GCP project ID.

    Returns:
        (min_competencia, max_competencia) ou None se falhar.
    """
    try:
        from google.cloud import bigquery  # import tardio — BQ pode não estar instalado
        client = bigquery.Client(project=project_id)
        query = (
            "SELECT MIN(competencia) AS min_comp, MAX(competencia) AS max_comp "
            f"FROM `{project_id}.{_BQ_TABLE}`"
        )
        row = list(client.query(query).result())[0]
        return str(row.min_comp), str(row.max_comp)
    except Exception:
        return None
```

`_BQ_TABLE` é uma constante de módulo com o nome completo da tabela nacional (verificar `data_dictionary.md` na implementação).

**Import tardio de `google.cloud.bigquery`** sob `try/except ImportError` para não quebrar o dashboard quando o pacote não está instalado ou BigQuery não está configurado.

### Cadeia de fallback para o range CNES Nacional

```
1. _consultar_range_bigquery(project_id)   → cache diário
2. Se None → last_run.json["bigquery"]["competencia_range"]
3. Se nenhum → "—"
```

A lógica de fallback fica dentro de `renderizar_container_status()`. O campo `competencia_range` no `last_run.json` **não existe ainda** — `_gravar_last_run` não o grava. Por isso o fallback 2 é implementado lendo `last_run.json` diretamente via `_ler_last_run()` e verificando a chave `competencia_range`. Se ausente, pula para "—". Nenhuma alteração no pipeline é necessária para essa feature — o fallback 2 simplesmente fica inativo até que o campo seja adicionado futuramente.

### Card DuckDB — cobertura de competências

`renderizar_container_status()` recebe adicionalmente `(validas, total)` de `reader.contar_competencias()` e constrói:

```python
CardInfo("Histórico", "DuckDB", f"{validas} válidas / {total} disponíveis")
```

---

## 5. Impacto em `DepStatus` e `CardInfo`

`DepStatus` **não muda** — permanece com `ok`, `ts`, `erro`. O range do BigQuery é passado diretamente para `CardInfo.range_str` em `renderizar_container_status()`, sem poluir o modelo de status operacional.

---

## 6. Dependências

| Pacote | Versão | Uso |
|---|---|---|
| `holidays` | `>=0.46` | Feriados nacionais BR para cálculo de dias úteis |

Adicionar ao `requirements.txt`. `google-cloud-bigquery` já está no projeto (usado por `CnesNacionalAdapter`).

---

## 7. Testes

### `tests/storage/test_competencia_utils.py` (novo)

```python
class TestQuintoDiaUtil:
    def test_janeiro_2026(self):
        # 01/01 feriado, dias úteis: 02,05,06,07,08 → 5º = 08/01
        assert quinto_dia_util(2026, 1) == date(2026, 1, 8)

    def test_mes_sem_feriados(self):
        # Junho 2026: dias úteis 01,02,03,04,05 → 5º = 05/06
        assert quinto_dia_util(2026, 6) == date(2026, 6, 5)

    def test_lru_cache_reutiliza(self):
        r1 = quinto_dia_util(2026, 1)
        r2 = quinto_dia_util(2026, 1)
        assert r1 is r2  # mesmo objeto — veio do cache

class TestJanelaValida:
    def test_inicio_e_fim_competencia(self):
        inicio, fim = janela_valida("2026-01")
        assert inicio == quinto_dia_util(2026, 1)
        assert fim == quinto_dia_util(2026, 2)

    def test_virada_de_ano(self):
        inicio, fim = janela_valida("2026-12")
        assert fim == quinto_dia_util(2027, 1)
```

### `tests/storage/test_historico_reader.py` (modificar)

Adicionar testes para `listar_competencias_validas()` e `contar_competencias()` com DuckDB em memória e `gravado_em` controlados:

- Competência com `gravado_em` dentro da janela → incluída
- Competência com `gravado_em` fora da janela (captura tardia) → excluída
- `contar_competencias()` retorna tuple correto

### `tests/scripts/test_dashboard_status.py` (modificar)

- `_consultar_range_bigquery` retorna `None` quando `google.cloud.bigquery` não está disponível
- Fallback para `last_run.json` quando BQ query retorna `None`
- Fallback para `"—"` quando nem BQ nem `last_run.json` têm range

---

## 8. Arquivos Afetados

| Ação | Arquivo |
|---|---|
| Criar | `src/storage/competencia_utils.py` |
| Modificar | `src/storage/historico_reader.py` |
| Modificar | `scripts/dashboard_status.py` |
| Modificar | `scripts/dashboard.py` |
| Modificar | `scripts/pages/1_Tendencias.py` |
| Modificar | `scripts/pages/2_Por_Regra.py` |
| Modificar | `requirements.txt` |
| Criar | `tests/storage/test_competencia_utils.py` |
| Modificar | `tests/storage/test_historico_reader.py` |
| Modificar | `tests/scripts/test_dashboard_status.py` |
