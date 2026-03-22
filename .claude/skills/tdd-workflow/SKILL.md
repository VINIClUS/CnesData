---
name: tdd-workflow
description: >
  Enforces strict Test-Driven Development with pytest. Auto-activates when
  implementing new features, adding functionality, or when user says "implement",
  "add feature", "build", "TDD", or "test first". Does NOT activate for
  bug fixes, docs, or config changes.
---
<workflow>

## Strict Test-Driven Development (TDD) — Red / Green / Refactor

<important>
Claude's natural tendency is to write implementation first and then backfill tests.
This workflow INVERTS that order. Implementation code MUST NOT exist before a failing
test demands it. If you find yourself writing production code without a red test,
STOP — you are violating the protocol. Go back to Phase 1.
</important>

### Toolchain

- **Test runner:** `pytest` (with `-x --tb=short -q` for fast iteration; full verbose for final run)
- **Mocking:** `pytest-mock` (`mocker` fixture) + `unittest.mock` when needed
- **Coverage:** `pytest-cov` (enforce `--cov --cov-fail-under=80`)
- **Assertions:** native `assert` with `pytest.raises`, `pytest.approx`, `pytest.warns`
- **Parametrize:** `@pytest.mark.parametrize` for input/output matrix testing
- **Fixtures:** `conftest.py` per test directory — shared setup belongs there, not duplicated across files
- **Async:** `pytest-asyncio` with `@pytest.mark.asyncio` when testing async code

### Phase 0 · ANALYZE (before any code or test)

1. Read the requirement or feature description in full.
2. Identify the public interface: function signatures, class methods, API endpoints, or CLI commands that will be created or modified.
3. List every behavior to test — decompose into:
   - **Happy paths** — expected inputs produce expected outputs.
   - **Edge cases** — boundary values, empty inputs, maximum sizes, type coercions.
   - **Error cases** — invalid input, missing dependencies, permission failures, timeouts.
   - **Integration points** — external services, databases, file I/O that must be mocked.
4. Write this list as a checklist in a comment at the top of the test file (or in a plan if the task is complex). Confirm the plan with the user before proceeding.

### Phase 1 · RED — Write Failing Tests

<important>
Do NOT write any implementation code during this phase. Not even stubs, not even
empty functions. Import from the module that DOES NOT YET EXIST. Let the import
error itself be the first "red."
</important>

**Rules:**
- One test file per module/feature: `tests/test_<module>.py`.
- Name tests by behavior: `test_rejects_expired_token`, `test_returns_empty_list_when_no_results`, NOT `test_function_1`.
- Follow Arrange / Act / Assert structure in every test — separated by blank lines with `# Arrange`, `# Act`, `# Assert` comments.
- Use `@pytest.mark.parametrize` for any behavior with ≥3 input/output variations instead of duplicating test functions.
- Use fixtures in `conftest.py` for shared state (database sessions, HTTP clients, temporary directories).
- Mock external dependencies at the boundary — use `mocker.patch("module.dependency")`, never mock internals of the unit under test.
- Each test must be independent: no shared mutable state, no reliance on execution order.
- Include at least one test per identified behavior from Phase 0's checklist.

**Fixture conventions:**
```python
# conftest.py — shared fixtures live here, not in individual test files

import pytest
from unittest.mock import AsyncMock

@pytest.fixture
def sample_user():
    """A valid user dict for happy-path tests."""
    return {"id": "u-123", "email": "ada@example.com", "role": "admin"}

@pytest.fixture
def mock_db(mocker):
    """Mocked database session — patches at the service boundary."""
    db = mocker.patch("app.services.user_service.get_db")
    db.return_value.__enter__ = mocker.Mock(return_value=db)
    db.return_value.__exit__ = mocker.Mock(return_value=False)
    return db

@pytest.fixture
def mock_http_client(mocker):
    """Mocked external HTTP client — prevents real network calls."""
    client = mocker.patch("app.clients.payment_client.httpx.AsyncClient")
    client.return_value.__aenter__ = AsyncMock(return_value=client)
    client.return_value.__aexit__ = AsyncMock(return_value=False)
    return client
```

**Test structure example:**
```python
import pytest
from app.services.auth_service import authenticate

class TestAuthenticate:
    """Tests for the authenticate() function."""

    def test_returns_token_for_valid_credentials(self, mock_db, sample_user):
        # Arrange
        mock_db.get_user_by_email.return_value = sample_user

        # Act
        result = authenticate(email="ada@example.com", password="correct")

        # Assert
        assert result.token is not None
        assert result.user_id == "u-123"

    def test_raises_auth_error_for_wrong_password(self, mock_db, sample_user):
        # Arrange
        mock_db.get_user_by_email.return_value = sample_user

        # Act & Assert
        with pytest.raises(AuthenticationError, match="invalid credentials"):
            authenticate(email="ada@example.com", password="wrong")

    @pytest.mark.parametrize("bad_email", [
        "",
        "not-an-email",
        None,
        "a" * 256 + "@example.com",
    ])
    def test_raises_validation_error_for_invalid_email(self, bad_email):
        # Act & Assert
        with pytest.raises(ValidationError):
            authenticate(email=bad_email, password="any")

    def test_raises_timeout_when_db_unreachable(self, mock_db):
        # Arrange
        mock_db.get_user_by_email.side_effect = ConnectionTimeoutError()

        # Act & Assert
        with pytest.raises(ServiceUnavailableError):
            authenticate(email="ada@example.com", password="any")
```

**After writing tests, run them:**
```bash
pytest tests/test_<module>.py -x --tb=short -q
```

**Expected:** ALL tests FAIL (ImportError or AssertionError). If any test passes, it is either testing the wrong thing or duplicating an existing behavior — investigate and fix before proceeding.

**Commit the failing tests now:**
```bash
git add tests/
git commit -m "test(<scope>): red — failing tests for <feature>"
```

<important>
Committing failing tests BEFORE implementation creates a safety net. If Claude
modifies tests during Phase 2 to make them pass (instead of fixing the
implementation), the git diff will expose exactly what changed. This is the single
most important TDD safeguard when working with AI agents.
</important>

### Phase 2 · GREEN — Minimal Implementation

<important>
Write the MINIMUM code required to make every red test turn green.
Do NOT add features, optimizations, or "nice-to-haves" that are not demanded by
an existing test. Do NOT modify or delete any test from Phase 1.
If a test is wrong, go BACK to Phase 1 and fix the test first, then return here.
</important>

**Rules:**
- Create the module file(s) that the tests import.
- Implement the simplest possible logic that satisfies each test — ugly is fine, duplication is fine, hardcoded values are fine IF a test demands exactly that output.
- Run tests after every meaningful change (function, branch, error handler):
  ```bash
  pytest tests/test_<module>.py -x --tb=short -q
  ```
- Keep going until the full targeted test file reports **all green**:
  ```bash
  pytest tests/test_<module>.py -v
  ```
- Then run the full project suite to catch regressions:
  ```bash
  pytest --tb=short -q
  ```
- If any pre-existing test breaks, fix the regression NOW — do not proceed with a broken suite.

**Once all tests pass, commit:**
```bash
git add .
git commit -m "feat(<scope>): green — implementation for <feature>"
```

### Phase 3 · REFACTOR — Improve Without Changing Behavior

<important>
Refactoring means restructuring code WITHOUT changing its external behavior.
The test suite is your proof: if tests stay green, behavior is preserved.
If you need to change behavior, go back to Phase 1 and write a new test first.
</important>

**Refactoring targets (apply in this order):**
1. **Eliminate duplication** — extract shared logic into helper functions or base classes.
2. **Improve naming** — rename variables, functions, classes to reveal intent.
3. **Simplify conditionals** — use early returns, guard clauses, polymorphism.
4. **Extract modules** — split files that exceed 500 lines or mix concerns.
5. **Improve error handling** — replace generic exceptions with domain-specific ones.
6. **Optimize mocks** — consolidate repeated mock setup into fixtures in `conftest.py`.

**Run tests after EVERY individual refactoring step:**
```bash
pytest tests/test_<module>.py -x --tb=short -q
```

**If any test turns red during refactoring, UNDO the last change immediately.** Do not debug forward — revert and try a different approach.

**Once refactoring is complete and all tests are green:**
```bash
pytest --tb=short -q                          # full suite
pytest --cov=app --cov-fail-under=80 -q       # coverage gate
```

**Commit:**
```bash
git add .
git commit -m "refactor(<scope>): clean up <feature> implementation"
```

### Phase 4 · VERIFY — Final Quality Gate

Run this full sequence. If any step fails, fix it before reporting the task complete:

```bash
# 1. Lint
ruff check . --fix && ruff format .

# 2. Type check (if applicable)
ty check                  # or: mypy . --strict

# 3. Full test suite with coverage
pytest --cov=app --cov-report=term-missing --cov-fail-under=80 -v

# 4. Security scan (if applicable)
pip-audit
```

Report the results to the user. Include: number of tests passed, coverage percentage, any warnings or notes.

### Mocking Strategy — Decision Guide

Use this decision tree to choose the right mocking approach:

**Does the dependency make network calls, hit a database, or touch the filesystem?**
→ YES: Mock it at the service/client boundary using `mocker.patch`.
→ NO: Use the real implementation (no mock needed).

**Is the dependency another module in THIS project?**
→ YES: Prefer NOT to mock it — test the integration. Only mock if it has expensive side effects or makes the test non-deterministic.
→ NO (third-party): Mock it. You don't control its behavior.

**Is the test checking interaction (was X called with Y?) or state (does the result equal Z)?**
→ Interaction: Use `mock.assert_called_once_with(...)` or `mocker.spy`.
→ State: Use plain `assert` on the return value — no mock needed for the assertion.

**Mock placement rules:**
- Patch where the dependency is USED, not where it is DEFINED:
  `mocker.patch("app.services.order_service.payment_client")` ← correct
  `mocker.patch("app.clients.payment_client")` ← usually wrong
- Use `mocker.patch.object` when you need to mock a method on a specific instance.
- Use `mocker.MagicMock(spec=RealClass)` to catch attribute errors early — the mock will only allow attributes that exist on the real class.
- For async mocks: `mocker.AsyncMock(return_value=...)`.

**Common patterns:**
```python
# Mock a return value
mock_service.get_user.return_value = sample_user

# Mock an exception
mock_service.get_user.side_effect = NotFoundException("user not found")

# Mock a sequence of returns
mock_service.get_next.side_effect = [item1, item2, StopIteration()]

# Spy — call the real function but track calls
spy = mocker.spy(user_service, "validate_email")
result = user_service.register(email="test@example.com")
spy.assert_called_once_with("test@example.com")

# Context manager mock
mock_file = mocker.patch("builtins.open", mocker.mock_open(read_data="content"))

# Freeze time for deterministic date/time tests
mocker.patch("app.services.billing.datetime", wraps=datetime)
mock_now = mocker.patch("app.services.billing.datetime.now")
mock_now.return_value = datetime(2025, 1, 15, 12, 0, 0)
```

### Test Organization Convention

```
project/
├── app/
│   ├── services/
│   │   ├── auth_service.py
│   │   └── order_service.py
│   ├── clients/
│   │   └── payment_client.py
│   └── models/
│       └── user.py
├── tests/
│   ├── conftest.py               # Project-wide fixtures (db, http client, factories)
│   ├── services/
│   │   ├── conftest.py           # Service-layer fixtures (mock repos, sample data)
│   │   ├── test_auth_service.py
│   │   └── test_order_service.py
│   ├── clients/
│   │   ├── conftest.py           # Client-layer fixtures (mock HTTP responses)
│   │   └── test_payment_client.py
│   └── models/
│       └── test_user.py
├── pyproject.toml                # pytest config section
└── CLAUDE.md
```

**`pyproject.toml` pytest config:**
```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = "-x --tb=short -q --strict-markers"
markers = [
    "slow: marks tests that take >1s (deselect with '-m \"not slow\"')",
    "integration: marks tests that require external services",
]
```

### Anti-Patterns — Claude MUST Avoid These

| Anti-Pattern | Why It's Wrong | What To Do Instead |
|---|---|---|
| Writing implementation before tests | Defeats TDD — tests become validation, not design drivers | Always Phase 1 first |
| Modifying tests to make them pass | Masks implementation bugs | Fix the implementation; if the test is wrong, commit the fix to the test separately with a clear message |
| Testing private/internal methods directly | Couples tests to implementation, breaks on refactor | Test through the public API |
| Mocking the unit under test | Proves nothing — you're testing your own mocks | Mock only external dependencies |
| One giant test function with many asserts | Hard to diagnose failures, obscures which behavior broke | One behavior per test function |
| Using `time.sleep()` in tests | Makes tests slow and flaky | Mock time or use `freezegun`/`mocker.patch` on datetime |
| Shared mutable state between tests | Tests pass in isolation but fail in sequence (or vice versa) | Use fresh fixtures per test; avoid module-level variables |
| Catching broad exceptions in tests | `except Exception` hides the real error type | Use `pytest.raises(SpecificError)` |
| Skipping tests with `@pytest.mark.skip` without reason | Invisible tech debt | Either fix the test or delete it — never skip silently |

### TDD Cycle Summary

```
  ┌─────────────────────────────────────────────────┐
  │  Phase 0: ANALYZE                               │
  │  Read requirement → list behaviors → plan tests  │
  └──────────────────────┬──────────────────────────┘
                         ▼
  ┌─────────────────────────────────────────────────┐
  │  Phase 1: RED 🔴                                │
  │  Write failing tests → run → ALL FAIL           │
  │  → commit tests                                 │
  └──────────────────────┬──────────────────────────┘
                         ▼
  ┌─────────────────────────────────────────────────┐
  │  Phase 2: GREEN 🟢                              │
  │  Minimal implementation → run → ALL PASS        │
  │  → commit implementation                        │
  └──────────────────────┬──────────────────────────┘
                         ▼
  ┌─────────────────────────────────────────────────┐
  │  Phase 3: REFACTOR 🔵                           │
  │  Clean up code → run after each change → GREEN  │
  │  → commit refactoring                           │
  └──────────────────────┬──────────────────────────┘
                         ▼
  ┌─────────────────────────────────────────────────┐
  │  Phase 4: VERIFY ✅                             │
  │  Lint → type check → full suite → coverage      │
  │  → report results                               │
  └─────────────────────────────────────────────────┘
```

For each new behavior or bugfix after initial implementation, repeat from Phase 1.
A bugfix ALWAYS starts with a failing test that reproduces the bug.

</workflow>