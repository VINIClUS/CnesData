---
name: testing-patterns
description: Testing patterns, TDD workflow, factory functions, and mock strategies for deterministic, maintainable test suites.
risk: unknown
source: community
date_added: '2026-02-27'
---

## Testing Philosophy

**Test-Driven Development (TDD):**
- Write failing test FIRST
- Implement minimal code to pass
- Refactor after green
- Never write production code without a failing test

**Behavior-Driven Testing:**
- Test behavior, not implementation
- Focus on public APIs and business requirements
- Avoid testing implementation details
- Use descriptive test names that describe behavior

**Factory Pattern:**
- Create `get_mock_x(overrides=None)` functions (Python) or `getMockX(overrides?)` (TypeScript)
- Provide sensible defaults
- Allow overriding specific properties
- Keep tests DRY and maintainable

## Factory Pattern (Python / pytest)

```python
from typing import Any

def get_mock_professional(overrides: dict[str, Any] | None = None) -> dict:
    defaults = {
        "cpf": "123.456.789-09",
        "nome": "JOAO DA SILVA",
        "cbo": "225125",
        "carga_horaria": 40,
        "ativo": True,
    }
    return {**defaults, **(overrides or {})}

# Usage in tests
def test_detecta_profissional_inativo():
    prof = get_mock_professional({"ativo": False})
    result = detectar_inativos([prof])
    assert len(result) == 1
```

## Mocking Patterns (Python)

```python
from unittest.mock import patch, MagicMock
import pandas as pd

# Mock fdb cursor (Firebird) to inject controlled DataFrames
@patch("src.ingestion.cnes_client.fdb.connect")
def test_extrai_profissionais(mock_connect):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("12345678909", "JOAO SILVA", "225125")]
    mock_cursor.description = [("CPF",), ("NOME",), ("CBO",)]
    mock_connect.return_value.__enter__.return_value.cursor.return_value = mock_cursor

    result = extrair_profissionais("fake.gdb")
    assert not result.empty
    assert "CPF" in result.columns

# Mock pandas read_sql
@patch("src.ingestion.cnes_client.pd.read_sql")
def test_query_retorna_dataframe_vazio(mock_read_sql):
    mock_read_sql.return_value = pd.DataFrame()
    result = buscar_vinculos("fake.gdb")
    assert result.empty
```

## Test Structure

```python
import pytest

class TestNomeDoComponente:
    def test_comportamento_esperado_com_entrada_valida(self):
        ...

    def test_retorna_vazio_quando_fonte_vazia(self):
        ...

    def test_levanta_erro_com_cpf_invalido(self):
        with pytest.raises(ValueError, match="CPF inválido"):
            ...

    def test_ignora_registros_com_campo_nulo(self):
        ...
```

## Anti-Patterns to Avoid

```python
# Bad - testing the mock, not the behavior
assert mock_cursor.fetchall.called

# Good - testing actual output
assert "CPF" in result.columns
assert len(result) == 3

# Bad - duplicated, inconsistent test data
def test_a():
    prof = {"cpf": "123", "nome": "JOAO"}  # missing fields

def test_b():
    prof = {"cpf": "456", "nome": "MARIA", "cbo": "225125"}  # different shape

# Good - factory with consistent defaults
def test_a():
    prof = get_mock_professional({"cpf": "00000000191"})
```

## Best Practices

1. **Always use factory functions** for mock data
2. **Test behavior, not implementation** — assert on output, not on internal calls
3. **Use descriptive test names** — `test_rejeita_cpf_com_digito_invalido` not `test_validate`
4. **Organize with class/describe blocks** — group by component and scenario
5. **Isolate mocks** — patch at the boundary (DB connection, HTTP client), never deep inside
6. **One behavior per test** — keep assertions focused
7. **Never use live DB connections in tests** — always mock fdb/Firebird at boundary

## Running Tests (this project)

```bash
# Unit tests only (fast)
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short

# Single module
./venv/Scripts/python.exe -m pytest tests/analysis/test_rules_engine.py -v

# With coverage
./venv/Scripts/python.exe -m pytest tests/ --cov=src --cov-report=term-missing
```
