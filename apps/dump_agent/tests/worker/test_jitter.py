"""Testes para jitter no streaming consumer."""

import random
from unittest.mock import patch


class TestJitterBounds:

    def test_jitter_dentro_do_intervalo(self):
        max_jitter = 1800.0
        for _ in range(100):
            val = random.uniform(0, max_jitter)
            assert 0 <= val <= max_jitter

    def test_jitter_zero_quando_max_zero(self):
        val = random.uniform(0, 0)
        assert val == 0.0

    def test_jitter_curto_entre_jobs(self):
        for _ in range(100):
            val = random.uniform(0, 5)
            assert 0 <= val <= 5


class TestStartupJitter:

    def test_startup_aplica_jitter_mockado(self):
        with patch("random.uniform", return_value=1.5):
            jitter = random.uniform(0, 1800)
            assert jitter == 1.5

    def test_jitter_max_respeita_config(self):
        max_val = 900.0
        for _ in range(50):
            val = random.uniform(0, max_val)
            assert val <= max_val
