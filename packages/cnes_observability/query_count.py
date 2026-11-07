"""Contagem de queries isolada por request e compartilhada com seu threadpool."""

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar

_query_count: ContextVar[list[int] | None] = ContextVar("query_count", default=None)


@contextmanager
def count_queries() -> Generator[list[int]]:
    counter = [0]
    token = _query_count.set(counter)
    try:
        yield counter
    finally:
        _query_count.reset(token)


def increment_query_count() -> None:
    counter = _query_count.get()
    if counter is not None:
        counter[0] += 1
