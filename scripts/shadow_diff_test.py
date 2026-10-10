"""Teste do shadow diff."""
from pathlib import Path

import polars as pl
import pytest

from scripts.shadow_diff import Difference, compare_parquets, diff_frames, main, normalize_df


def test_normalize_df_ordena_canonicamente() -> None:
    df = pl.DataFrame({"b": [2, 1], "a": ["y", "x"]})
    norm = normalize_df(df)
    assert norm.columns == sorted(df.columns)
    # sorted por todas colunas ascending
    assert norm.row(0) == ("x", 1)


def test_compare_parquets_identicos(tmp_path: Path) -> None:
    df = pl.DataFrame({"cnes": ["0001", "0002"], "nome": ["A", "B"]})
    p1 = tmp_path / "a.parquet"
    p2 = tmp_path / "b.parquet"
    df.write_parquet(p1)
    df.write_parquet(p2)
    result = compare_parquets(p1, p2)
    assert result.identical is True
    assert result.diff_rows == 0


def test_compare_parquets_difere(tmp_path: Path) -> None:
    a = pl.DataFrame({"cnes": ["0001"], "nome": ["A"]})
    b = pl.DataFrame({"cnes": ["0001"], "nome": ["B"]})
    pa = tmp_path / "a.parquet"
    pb = tmp_path / "b.parquet"
    a.write_parquet(pa)
    b.write_parquet(pb)
    result = compare_parquets(pa, pb)
    assert result.identical is False
    assert result.diff_rows == 1


def _write_pair(tmp_path: Path, a: pl.DataFrame, b: pl.DataFrame) -> tuple[Path, Path]:
    pa, pb = tmp_path / "a.parquet", tmp_path / "b.parquet"
    a.write_parquet(pa)
    b.write_parquet(pb)
    return pa, pb


def test_compare_parquets_nulo_contra_valor_e_diferenca(tmp_path: Path) -> None:
    a = pl.DataFrame({"cnes": ["0001", "0002"], "nome": ["A", None]})
    b = pl.DataFrame({"cnes": ["0001", "0002"], "nome": ["A", "B"]})

    result = compare_parquets(*_write_pair(tmp_path, a, b))

    assert result.identical is False
    assert result.diff_rows == 1


def test_compare_parquets_valor_contra_nulo_em_lista_e_diferenca(tmp_path: Path) -> None:
    a = pl.DataFrame({"cnes": ["0001"], "ids": [["x"]]})
    schema = {"cnes": pl.String, "ids": pl.List(pl.String)}
    b = pl.DataFrame({"cnes": ["0001"], "ids": [None]}, schema=schema)

    result = compare_parquets(*_write_pair(tmp_path, a, b))

    assert (result.identical, result.diff_rows) == (False, 1)


def test_diff_frames_aponta_chave_coluna_e_valores_sem_depender_da_ordem() -> None:
    a = pl.DataFrame({"cnes": ["0001", "0002"], "nome": ["A", "B"], "ch": [1, 2]})
    b = pl.DataFrame({"cnes": ["0002", "0001"], "nome": ["X", "A"], "ch": [2, 1]})

    assert diff_frames(a, b, ["cnes"]) == (Difference("cell", ("0002",), "nome", "B", "X"),)


def test_diff_frames_trata_nulo_contra_valor_como_diferenca_por_chave() -> None:
    a = pl.DataFrame({"cnes": ["0001", "0002"], "nome": [None, "B"]})
    b = pl.DataFrame({"cnes": ["0001", "0002"], "nome": ["A", None]})

    assert diff_frames(a, b, ["cnes"]) == (
        Difference("cell", ("0001",), "nome", None, "A"),
        Difference("cell", ("0002",), "nome", "B", None),
    )


def test_diff_frames_aponta_linha_ausente_por_lado() -> None:
    a = pl.DataFrame({"cnes": ["0001", "0003"], "nome": ["A", "C"]})
    b = pl.DataFrame({"cnes": ["0001", "0004"], "nome": ["A", "D"]})

    assert diff_frames(a, b, ["cnes"]) == (
        Difference("left_only", ("0003",), None, None, None),
        Difference("right_only", ("0004",), None, None, None),
    )


def test_diff_frames_aponta_chave_duplicada_com_a_contagem_por_lado() -> None:
    a = pl.DataFrame({"cnes": ["0001", "0001"], "nome": ["A", "A"]})
    b = pl.DataFrame({"cnes": ["0001"], "nome": ["A"]})

    assert diff_frames(a, b, ["cnes"]) == (Difference("duplicate_key", ("0001",), None, 2, 1),)


def test_diff_frames_com_chave_composta_identifica_a_linha() -> None:
    a = pl.DataFrame({"k1": ["a", "a"], "k2": [1, 2], "v": ["x", "y"]})
    b = pl.DataFrame({"k1": ["a", "a"], "k2": [1, 2], "v": ["x", "z"]})

    assert diff_frames(a, b, ["k1", "k2"]) == (Difference("cell", ("a", "2"), "v", "y", "z"),)


def test_diff_frames_sem_chave_compara_por_posicao_apos_normalizar() -> None:
    a = pl.DataFrame({"cnes": ["0001", "0002"], "nome": ["A", None]})
    b = pl.DataFrame({"cnes": ["0001", "0002"], "nome": ["A", "B"]})

    assert diff_frames(normalize_df(a), normalize_df(b)) == (
        Difference("cell", ("1",), "nome", None, "B"),
    )


def test_diff_frames_sem_chave_devolve_listas_como_listas() -> None:
    a = pl.DataFrame({"ids": [["x"]]})
    b = pl.DataFrame({"ids": [["y"]]})

    assert diff_frames(a, b) == (Difference("cell", ("0",), "ids", ["x"], ["y"]),)


def test_diff_frames_sem_chave_aponta_linhas_excedentes() -> None:
    a = pl.DataFrame({"cnes": ["0001", "0002"]})
    b = pl.DataFrame({"cnes": ["0001"]})

    assert diff_frames(a, b) == (Difference("left_only", ("1",), None, None, None),)


@pytest.mark.parametrize(("right", "key", "code"), [
    (pl.DataFrame({"cnes": ["0001"], "outra": [1]}), ["cnes"], "column_mismatch"),
    (pl.DataFrame({"cnes": ["0001"], "nome": ["A"]}), ["inexistente"], "key_missing"),
])
def test_diff_frames_rejeita_colunas_ou_chave_invalidas(
    right: pl.DataFrame, key: list[str], code: str
) -> None:
    left = pl.DataFrame({"cnes": ["0001"], "nome": ["A"]})

    with pytest.raises(ValueError, match=code):
        diff_frames(left, right, key)


def _run(tmp_path: Path, a: pl.DataFrame, b: pl.DataFrame, *extra: str) -> int:
    pa, pb = _write_pair(tmp_path, a, b)
    return main(["--python", str(pa), "--go", str(pb), *extra])


def test_main_preserva_os_codigos_de_saida_zero_e_um(tmp_path: Path) -> None:
    same = pl.DataFrame({"cnes": ["0001"], "nome": ["A"]})
    other = pl.DataFrame({"cnes": ["0001"], "nome": ["B"]})

    assert _run(tmp_path, same, same) == 0
    assert _run(tmp_path, same, other) == 1


def test_main_com_chave_repetivel_detecta_linha_ausente_e_duplicada(tmp_path: Path) -> None:
    a = pl.DataFrame({"k1": ["a"], "k2": [1], "v": ["x"]})
    missing = pl.DataFrame({"k1": ["a"], "k2": [2], "v": ["x"]})
    duplicated = pl.DataFrame({"k1": ["a", "a"], "k2": [1, 1], "v": ["x", "x"]})
    keys = ("--key", "k1", "--key", "k2")

    assert _run(tmp_path, a, a, *keys) == 0
    assert _run(tmp_path, a, missing, *keys) == 1
    assert _run(tmp_path, a, duplicated, *keys) == 1


def test_main_retorna_um_quando_as_colunas_diferem(tmp_path: Path) -> None:
    a = pl.DataFrame({"cnes": ["0001"]})
    b = pl.DataFrame({"cnes": ["0001"], "nome": ["A"]})

    assert _run(tmp_path, a, b, "--key", "cnes") == 1
