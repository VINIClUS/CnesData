"""TDD de reconstruct_from_deltas: replay CDC (I/U/D) sobre snapshot FULL."""

from __future__ import annotations

import polars as pl

from data_processor.pipeline.delta_reconstruction import reconstruct_from_deltas

_NATURAL_KEY = ("CPF", "CNS", "CNES", "CBO")


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def test_reconstroi_full_mais_delta_upsert_e_delete() -> None:
    base = _frame(
        [
            {"CPF": "11111111111", "CNS": "111111111111111", "CNES": "0000001",
             "CBO": "000001", "NOME_PROFISSIONAL": "Ana"},
            {"CPF": "22222222222", "CNS": "222222222222222", "CNES": "0000001",
             "CBO": "000001", "NOME_PROFISSIONAL": "Bruno"},
            {"CPF": "33333333333", "CNS": "333333333333333", "CNES": "0000001",
             "CBO": "000001", "NOME_PROFISSIONAL": "Carla"},
        ]
    )
    delta = _frame(
        [
            {"CPF": "22222222222", "CNS": "222222222222222", "CNES": "0000001",
             "CBO": "000001", "NOME_PROFISSIONAL": "Bruno Atualizado", "_op": "U"},
            {"CPF": "33333333333", "CNS": "333333333333333", "CNES": "0000001",
             "CBO": "000001", "NOME_PROFISSIONAL": None, "_op": "D"},
        ]
    )

    result = reconstruct_from_deltas(base, [delta], _NATURAL_KEY)

    assert result.height == 2
    rows = {row["CPF"]: row["NOME_PROFISSIONAL"] for row in result.to_dicts()}
    assert rows == {"11111111111": "Ana", "22222222222": "Bruno Atualizado"}


def test_aplica_upsert_e_delete_em_chave_com_cns_nulo() -> None:
    base = _frame(
        [
            {"CPF": "10000000001", "CNS": None, "CNES": "0000001", "CBO": "000001",
             "NOME_PROFISSIONAL": "Ana"},
            {"CPF": "10000000002", "CNS": None, "CNES": "0000002", "CBO": "000002",
             "NOME_PROFISSIONAL": "Bea"},
        ]
    )
    delta = _frame(
        [
            {"CPF": "10000000001", "CNS": None, "CNES": "0000001", "CBO": "000001",
             "NOME_PROFISSIONAL": "Ana Atualizada", "_op": "U"},
            {"CPF": "10000000002", "CNS": None, "CNES": "0000002", "CBO": "000002",
             "NOME_PROFISSIONAL": None, "_op": "D"},
        ]
    )

    result = reconstruct_from_deltas(base, [delta], _NATURAL_KEY)

    assert result.height == 1
    row = result.to_dicts()[0]
    assert row["CPF"] == "10000000001"
    assert row["CNS"] is None
    assert row["NOME_PROFISSIONAL"] == "Ana Atualizada"


def test_delete_vence_upsert_na_mesma_chave_do_mesmo_delta() -> None:
    base = _frame(
        [
            {"CPF": "44444444444", "CNS": "444444444444444", "CNES": "0000001",
             "CBO": "000001", "NOME_PROFISSIONAL": "Diana"},
        ]
    )
    delta = _frame(
        [
            {"CPF": "44444444444", "CNS": "444444444444444", "CNES": "0000001",
             "CBO": "000001", "NOME_PROFISSIONAL": "Diana Atualizada", "_op": "U"},
            {"CPF": "44444444444", "CNS": "444444444444444", "CNES": "0000001",
             "CBO": "000001", "NOME_PROFISSIONAL": None, "_op": "D"},
        ]
    )

    result = reconstruct_from_deltas(base, [delta], _NATURAL_KEY)

    assert result.height == 0


def test_reconstrucao_nao_muta_frames_de_entrada() -> None:
    base = _frame(
        [
            {"CPF": "55555555555", "CNS": "555555555555555", "CNES": "0000001",
             "CBO": "000001", "NOME_PROFISSIONAL": "Elis"},
        ]
    )
    delta = _frame(
        [
            {"CPF": "55555555555", "CNS": "555555555555555", "CNES": "0000001",
             "CBO": "000001", "NOME_PROFISSIONAL": "Elis Atualizada", "_op": "U"},
        ]
    )
    base_before = base.clone()
    delta_before = delta.clone()

    reconstruct_from_deltas(base, [delta], _NATURAL_KEY)

    assert base.equals(base_before)
    assert delta.equals(delta_before)


def test_lista_de_deltas_vazia_devolve_base_equivalente() -> None:
    base = _frame(
        [
            {"CPF": "66666666666", "CNS": "666666666666666", "CNES": "0000001",
             "CBO": "000001", "NOME_PROFISSIONAL": "Fabio"},
        ]
    )

    result = reconstruct_from_deltas(base, [], _NATURAL_KEY)

    assert result.equals(base)


def test_delta_sem_colunas_opcionais_e_reconstruido() -> None:
    base = _frame(
        [
            {"CPF": "77777777777", "CNS": "777777777777777", "CNES": "0000001",
             "CBO": "000001", "NOME_PROFISSIONAL": "Gil", "NOME_SOCIAL": "Gigi"},
        ]
    )
    delta = _frame(
        [
            {"CPF": "88888888888", "CNS": "888888888888888", "CNES": "0000002",
             "CBO": "000002", "NOME_PROFISSIONAL": "Helo", "_op": "I"},
        ]
    )

    result = reconstruct_from_deltas(base, [delta], _NATURAL_KEY)

    assert result.height == 2
    new_row = result.filter(pl.col("CPF") == "88888888888").to_dicts()[0]
    assert new_row["NOME_SOCIAL"] is None
