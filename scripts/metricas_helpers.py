"""Funções puras de parsing de métricas avançadas para a página Métricas."""
import json


def _parsear_metricas(raw: dict) -> dict:
    """Converte campos JSON em estruturas tipadas.

    Args:
        raw: Dict com colunas de gold.metricas_avancadas.

    Returns:
        Dict com JSON fields parseados para list/dict.
    """
    def _json_list(val) -> list:
        return json.loads(val) if val else []

    def _json_dict(val) -> dict:
        return json.loads(val) if val else {}

    return {
        **raw,
        "top_glosas":                  _json_list(raw.get("top_glosas_json")),
        "anomalias_por_cbo":           _json_dict(raw.get("anomalias_por_cbo_json")),
        "proporcao_feminina_por_cnes":  _json_dict(raw.get("proporcao_feminina_por_cnes_json")),
        "ranking_cnes":                _json_list(raw.get("ranking_cnes_json")),
    }
