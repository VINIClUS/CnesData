"""ContextVar tenant isolation under concurrent access."""
from __future__ import annotations

import contextvars
import threading
import time

import pytest
from hypothesis import given
from hypothesis import strategies as st


@pytest.mark.race
@given(tenants=st.lists(
    st.text(alphabet="0123456789", min_size=6, max_size=6),
    min_size=2, max_size=5, unique=True,
))
def test_context_var_isolada_por_thread(tenants, executor):
    """Cada thread deve ver seu proprio tenant — nunca vazamento."""
    from cnes_domain.tenant import get_tenant_id, set_tenant_id

    results: dict[int, tuple[str, str | None]] = {}
    lock = threading.Lock()

    def worker(t: str):
        ctx = contextvars.copy_context()

        def inner():
            set_tenant_id(t)
            time.sleep(0.005)
            observed = get_tenant_id()
            with lock:
                results[threading.get_ident()] = (t, observed)

        ctx.run(inner)

    futures = [executor.submit(worker, t) for t in tenants]
    for f in futures:
        f.result()

    for assigned, observed in results.values():
        assert assigned == observed, f"tenant_leak assigned={assigned} observed={observed}"
