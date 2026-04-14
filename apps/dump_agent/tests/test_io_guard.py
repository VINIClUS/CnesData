"""Testes para circuit breakers de I/O."""

import pytest

from dump_agent.io_guard import (
    InsufficientDiskError,
    SpoolGuard,
    SpoolLimitExceeded,
    pre_flight_check,
)


class TestPreFlightCheck:
    def test_aceita_disco_com_espaco_suficiente(self, tmp_path):
        pre_flight_check(tmp_path, min_free_mb=1)

    def test_rejeita_disco_sem_espaco(self, tmp_path):
        with pytest.raises(InsufficientDiskError, match="free_mb"):
            pre_flight_check(tmp_path, min_free_mb=999_999_999)


class TestSpoolGuard:
    def test_aceita_escrita_dentro_do_limite(self):
        guard = SpoolGuard(max_bytes=1000)
        guard.track(500)
        guard.track(400)

    def test_rejeita_escrita_acima_do_limite(self):
        guard = SpoolGuard(max_bytes=1000)
        guard.track(600)
        with pytest.raises(SpoolLimitExceeded, match="1000"):
            guard.track(500)

    def test_acumula_bytes_corretamente(self):
        guard = SpoolGuard(max_bytes=100)
        guard.track(30)
        guard.track(30)
        guard.track(30)
        assert guard.total_bytes == 90

    def test_limite_exato_nao_dispara(self):
        guard = SpoolGuard(max_bytes=100)
        guard.track(100)

    def test_reset_zera_contagem(self):
        guard = SpoolGuard(max_bytes=100)
        guard.track(90)
        guard.reset()
        assert guard.total_bytes == 0
        guard.track(90)
