"""SQL injection payloads passam por parameterized query sem execucao."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st
from sqlalchemy import create_engine, text

_INJECTION_TOKENS = ["'", ";", "--", "/*", "*/", "' OR '1'='1", "); DROP TABLE t;--"]


def _injection_payload() -> st.SearchStrategy[str]:
    prefix = st.text(min_size=0, max_size=30)
    token = st.sampled_from(_INJECTION_TOKENS)
    suffix = st.text(min_size=0, max_size=30)
    return st.builds(lambda a, b, c: a + b + c, prefix, token, suffix)


@pytest.mark.negative
@given(payload=_injection_payload())
def test_sql_injection_nao_executa(payload):
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (v TEXT)"))
        conn.execute(text("INSERT INTO t VALUES (:v)"), {"v": payload})
        row = conn.execute(
            text("SELECT v FROM t WHERE v = :v"), {"v": payload}
        ).first()
        assert row is not None
        assert row[0] == payload
