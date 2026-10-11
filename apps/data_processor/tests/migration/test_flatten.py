"""Achatamento canonico: chaves, listas, vazios, datas e rejeicoes."""
from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from data_processor.migration.flatten import (
    EMPTY_DICT,
    EMPTY_LIST,
    canonical_rows,
    flatten_payload,
)


def test_achata_objetos_com_chaves_pontuadas() -> None:
    value = {"kpis": {"linhas": 3, "ok": True}, "nota": None, "lista": []}

    flat = flatten_payload("serving", value, {})

    assert flat == {
        "serving::kpis.linhas": 3, "serving::kpis.ok": True, "serving::nota": None,
        "serving::lista": EMPTY_LIST,
    }


def test_achata_linhas_chaveadas_sem_depender_da_ordem() -> None:
    rows = [{"cnes": "B", "n": 2}, {"cnes": "A", "n": 1}]

    forward = flatten_payload("gold", rows, {"": ("cnes",)})
    backward = flatten_payload("gold", rows[::-1], {"": ("cnes",)})

    assert forward == backward == {
        "gold::[A].cnes": "A", "gold::[A].n": 1, "gold::[B].cnes": "B", "gold::[B].n": 2,
    }


def test_achata_chave_composta_com_caminho_pontuado_e_nulo() -> None:
    rows = [{"natural_key": {"identity": "I"}, "field": None, "v": 1}]

    flat = flatten_payload("div", rows, {"": ("natural_key.identity", "field")})

    assert flat["div::[I|].natural_key.identity"] == "I"
    assert flat["div::[I|].v"] == 1


def test_achata_listas_aninhadas_chaveadas_e_posicionais() -> None:
    value = {"estabelecimentos": [{"cnes": "X", "n": 1}], "missing_sources": ["a", "b"]}

    flat = flatten_payload("d", value, {"estabelecimentos": ("cnes",)})

    assert flat == {
        "d::estabelecimentos[X].cnes": "X", "d::estabelecimentos[X].n": 1,
        "d::missing_sources[0]": "a", "d::missing_sources[1]": "b",
    }


def test_achata_datas_como_iso() -> None:
    value = {"dia": date(2026, 1, 3), "instante": datetime(2026, 1, 3, 12, tzinfo=UTC)}

    flat = flatten_payload("d", value, {})

    assert flat == {"d::dia": "2026-01-03", "d::instante": "2026-01-03T12:00:00+00:00"}


@pytest.mark.parametrize("value", [{"x": 1.5}, [{"k": "a", "x": 0.0}]])
def test_rejeita_float(value: Any) -> None:
    with pytest.raises(ValueError, match="float_not_allowed"):
        flatten_payload("d", value, {"": ("k",)})


def test_rejeita_metricas_com_o_mesmo_nome() -> None:
    with pytest.raises(ValueError, match=r"duplicate_metric metric=d::a\.b"):
        flatten_payload("d", {"a.b": 1, "a": {"b": 2}}, {})


def test_rejeita_valor_de_tipo_nao_suportado() -> None:
    with pytest.raises(ValueError, match="unsupported_value"):
        flatten_payload("d", {"x": b"bytes"}, {})


def test_rejeita_chave_duplicada() -> None:
    rows = [{"k": "a", "v": 1}, {"k": "a", "v": 2}]

    with pytest.raises(ValueError, match=r"duplicate_key key=a"):
        canonical_rows(rows, ("k",))
    with pytest.raises(ValueError, match="duplicate_key"):
        flatten_payload("d", rows, {"": ("k",)})


def test_rejeita_linha_sem_a_coluna_da_chave() -> None:
    with pytest.raises(ValueError, match="key_missing column=k"):
        canonical_rows([{"outra": 1}], ("k",))


def test_linhas_canonicas_independem_da_ordem() -> None:
    rows = [{"k": "b", "v": 2}, {"k": "a", "v": 1}, {"k": "c", "v": 3}]

    forward = canonical_rows(rows, ("k",))
    backward = canonical_rows(rows[::-1], ("k",))

    assert list(forward) == list(backward) == ["a", "b", "c"]
    assert forward == backward


def test_rejeita_chave_de_linha_com_colchete_de_fechamento() -> None:
    with pytest.raises(ValueError, match="key_bracket_invalid"):
        canonical_rows([{"k": "a]b"}], ("k",))


def test_container_vazio_vira_folha_tipada_no_caminho_do_container() -> None:
    value = {"a": [], "b": {}, "c": {"d": []}, "e": [{"k": "x"}]}

    flat = flatten_payload("d", value, {"e": ("k",)})

    assert flat == {
        "d::a": EMPTY_LIST, "d::b": EMPTY_DICT, "d::c.d": EMPTY_LIST,
        "d::e[x].k": "x",
    }


def test_lista_raiz_e_lista_chaveada_vazias_viram_folha() -> None:
    assert flatten_payload("d", [], {"": ("k",)}) == {"d::": EMPTY_LIST}
    assert flatten_payload("d", {}, {}) == {"d::": EMPTY_DICT}
    assert flatten_payload("d", {"rows": []}, {"rows": ("k",)}) == {"d::rows": EMPTY_LIST}


def test_folhas_vazias_sao_distintas_entre_si_de_texto_e_de_nulo() -> None:
    leaves: list[Any] = [EMPTY_LIST, EMPTY_DICT, None, "[]", "{}", "", 0, False]

    assert len({(type(item), repr(item)) for item in leaves}) == len(leaves)
    assert EMPTY_LIST != EMPTY_DICT
    assert EMPTY_LIST != "[]"
    assert EMPTY_DICT != "{}"
