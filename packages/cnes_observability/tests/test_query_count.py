"""Isolamento e propagação do contador de queries."""

from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context

import pytest

from cnes_observability.query_count import count_queries, increment_query_count


def test_conta_queries_no_threadpool_do_request():
    with count_queries() as counter:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(copy_context().run, increment_query_count).result()
        assert counter == [1]


def test_request_novo_nao_herda_contagem_anterior():
    with count_queries() as first:
        increment_query_count()
    increment_query_count()
    with count_queries() as second:
        assert second == [0]
    assert first == [1]


def test_restaura_contexto_apos_excecao():
    with count_queries() as outer:
        with pytest.raises(ValueError, match="falha"):
            with count_queries():
                raise ValueError("falha")
        increment_query_count()
        assert outer == [1]
