from datetime import date
from unittest.mock import patch

from pipeline_runner import competencia_atual


class TestCompetenciaAtual:
    def test_retorna_ano_e_mes_de_hoje(self):
        hoje = date(2026, 3, 31)
        with patch("pipeline_runner.date") as mock_date:
            mock_date.today.return_value = hoje
            ano, mes = competencia_atual()
        assert ano == 2026
        assert mes == 3
