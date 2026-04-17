"""Regressão: CircuitBreaker deve ser seguro sob acesso concorrente."""
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from cnes_domain.pipeline.circuit_breaker import CircuitBreaker


def _fn_erro() -> None:
    raise ConnectionError("sim")


class TestCircuitBreakerConcurrency:
    @pytest.mark.concurrency
    def test_mil_threads_falhando_contagem_consistente(self) -> None:
        cb = CircuitBreaker(failure_threshold=10_000, base_delay=0.0)

        def worker() -> None:
            try:
                cb.call(_fn_erro)
            except Exception:
                pass

        with ThreadPoolExecutor(max_workers=100) as pool:
            futures = [pool.submit(worker) for _ in range(10_000)]
            for f in futures:
                f.result()

        assert cb._falhas_consecutivas == 10_000, (
            f"contagem perdeu writes concorrentes: {cb._falhas_consecutivas}"
        )

    @pytest.mark.concurrency
    def test_race_aberto_em_nao_corrompe_estado(self) -> None:
        cb = CircuitBreaker(failure_threshold=3, base_delay=0.0)

        barrier = threading.Barrier(50)

        def worker() -> None:
            barrier.wait()
            try:
                cb.call(_fn_erro)
            except Exception:
                pass

        threads = [threading.Thread(target=worker) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert cb.is_open is True
        assert cb._aberto_em is not None
        assert cb._falhas_consecutivas >= cb._threshold
