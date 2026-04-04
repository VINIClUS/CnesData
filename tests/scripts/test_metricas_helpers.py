"""Testes das funções auxiliares de métricas avançadas."""
import pytest

from metricas_helpers import _parsear_metricas


def _raw() -> dict:
    return {
        "competencia":                   "2026-03",
        "taxa_anomalia_geral":            0.15,
        "p90_ch_total":                   40.0,
        "proporcao_feminina_geral":        0.62,
        "n_reincidentes":                 3,
        "taxa_resolucao":                 0.80,
        "velocidade_regularizacao_media":  12.5,
        "top_glosas_json":                '[{"regra": "RQ008", "n": 5}]',
        "anomalias_por_cbo_json":         '{"225125": 3, "225170": 2}',
        "proporcao_feminina_por_cnes_json": '{"2795001": 0.7}',
        "ranking_cnes_json":              '[{"cnes": "2795001", "n": 8}]',
        "gravado_em":                     None,
    }


class TestParsearMetricas:

    def test_campos_numericos_preservados(self):
        result = _parsear_metricas(_raw())
        assert result["taxa_anomalia_geral"] == pytest.approx(0.15)
        assert result["n_reincidentes"] == 3

    def test_top_glosas_e_lista_parseada(self):
        result = _parsear_metricas(_raw())
        assert isinstance(result["top_glosas"], list)
        assert result["top_glosas"][0]["regra"] == "RQ008"

    def test_anomalias_por_cbo_e_dict_parseado(self):
        result = _parsear_metricas(_raw())
        assert isinstance(result["anomalias_por_cbo"], dict)
        assert result["anomalias_por_cbo"]["225125"] == 3

    def test_ranking_cnes_e_lista_parseada(self):
        result = _parsear_metricas(_raw())
        assert isinstance(result["ranking_cnes"], list)
        assert result["ranking_cnes"][0]["cnes"] == "2795001"

    def test_proporcao_feminina_por_cnes_e_dict_parseado(self):
        result = _parsear_metricas(_raw())
        assert isinstance(result["proporcao_feminina_por_cnes"], dict)
        assert result["proporcao_feminina_por_cnes"]["2795001"] == pytest.approx(0.7)

    def test_json_nulo_retorna_estruturas_vazias(self):
        raw = _raw()
        raw["top_glosas_json"] = None
        raw["anomalias_por_cbo_json"] = None
        raw["ranking_cnes_json"] = None
        raw["proporcao_feminina_por_cnes_json"] = None
        result = _parsear_metricas(raw)
        assert result["top_glosas"] == []
        assert result["anomalias_por_cbo"] == {}
        assert result["ranking_cnes"] == []
        assert result["proporcao_feminina_por_cnes"] == {}
