"""test_cascade_resolver.py — Testes unitários do resolver de lag RQ-006."""

from unittest.mock import patch

import pandas as pd

from analysis.cascade_resolver import resolver_lag_rq006
from ingestion.cnes_oficial_web_adapter import (
    STATUS_CONFIRMADO,
    STATUS_INDISPONIVEL,
    STATUS_LAG,
)


def _df_fantasma(cnes_list: list[str]) -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": cnes_list,
        "NOME_FANTASIA": [f"UBS {c}" for c in cnes_list],
        "TIPO_UNIDADE": ["01"] * len(cnes_list),
        "FONTE": ["LOCAL"] * len(cnes_list),
    })


class _StubVerificador:
    def __init__(self, mapa: dict[str, str]) -> None:
        self._mapa = mapa

    def verificar_estabelecimento(self, cnes: str) -> str:
        return self._mapa.get(cnes, STATUS_CONFIRMADO)


def test_df_vazio_retorna_df_vazio():
    df = pd.DataFrame(columns=["CNES", "NOME_FANTASIA", "TIPO_UNIDADE", "FONTE"])
    resultado = resolver_lag_rq006(df, _StubVerificador({}), sleep_entre_chamadas=0)
    assert resultado.empty
    assert list(resultado.columns) == list(df.columns)


def test_todos_confirmados_retornam_com_status_critico():
    df = _df_fantasma(["0001111", "0002222"])
    stub = _StubVerificador({"0001111": STATUS_CONFIRMADO, "0002222": STATUS_CONFIRMADO})
    resultado = resolver_lag_rq006(df, stub, sleep_entre_chamadas=0)
    assert len(resultado) == 2
    assert (resultado["STATUS_VERIFICACAO"] == STATUS_CONFIRMADO).all()


def test_todos_lag_sao_filtrados():
    df = _df_fantasma(["0001111", "0002222"])
    stub = _StubVerificador({"0001111": STATUS_LAG, "0002222": STATUS_LAG})
    resultado = resolver_lag_rq006(df, stub, sleep_entre_chamadas=0)
    assert resultado.empty


def test_misto_retorna_apenas_nao_lag():
    df = _df_fantasma(["0001111", "0002222", "0003333"])
    stub = _StubVerificador({
        "0001111": STATUS_LAG,
        "0002222": STATUS_CONFIRMADO,
        "0003333": STATUS_INDISPONIVEL,
    })
    resultado = resolver_lag_rq006(df, stub, sleep_entre_chamadas=0)
    assert len(resultado) == 2
    assert "0001111" not in resultado["CNES"].values
    assert "0002222" in resultado["CNES"].values
    assert "0003333" in resultado["CNES"].values


def test_api_indisponivel_permanece_no_resultado():
    df = _df_fantasma(["0001111"])
    stub = _StubVerificador({"0001111": STATUS_INDISPONIVEL})
    resultado = resolver_lag_rq006(df, stub, sleep_entre_chamadas=0)
    assert len(resultado) == 1
    assert resultado.iloc[0]["STATUS_VERIFICACAO"] == STATUS_INDISPONIVEL


def test_sleep_chamado_uma_vez_por_cnes():
    df = _df_fantasma(["0001111", "0002222"])
    stub = _StubVerificador({})
    with patch("analysis.cascade_resolver.time.sleep") as mock_sleep:
        resolver_lag_rq006(df, stub, sleep_entre_chamadas=0.5)
    assert mock_sleep.call_count == 2
    mock_sleep.assert_called_with(0.5)


def test_colunas_originais_preservadas():
    df = _df_fantasma(["0001111"])
    stub = _StubVerificador({"0001111": STATUS_CONFIRMADO})
    resultado = resolver_lag_rq006(df, stub, sleep_entre_chamadas=0)
    for col in df.columns:
        assert col in resultado.columns
    assert "STATUS_VERIFICACAO" in resultado.columns
