"""SQLAlchemy Session enter/exit 1000 times must not leak."""
from __future__ import annotations

import pytest

pytest.importorskip("pytest_memray")

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session


@pytest.mark.limit_memory("20 MB")
def test_session_scope_no_leak():
    engine = create_engine("sqlite:///:memory:")
    for _ in range(1000):
        with Session(engine) as s:
            s.execute(text("SELECT 1"))
