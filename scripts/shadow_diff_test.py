"""Teste do shadow diff."""
import logging
import re
from hashlib import sha256
from pathlib import Path
from typing import Any

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

    assert diff_frames(a, b, ["k1", "k2"]) == (Difference("cell", ("a", 2), "v", "y", "z"),)


def test_diff_frames_com_chave_nao_funde_nulo_com_o_texto_none(tmp_path: Path) -> None:
    a = pl.DataFrame({"k": [None, "None"], "v": ["x", "y"]})
    b = pl.DataFrame({"k": [None, "None"], "v": ["X", "Y"]})

    assert diff_frames(a, b, ["k"]) == (
        Difference("cell", (None,), "v", "x", "X"),
        Difference("cell", ("None",), "v", "y", "Y"),
    )
    assert compare_parquets(*_write_pair(tmp_path, a, b), key=["k"]).diff_rows == 2


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


@pytest.mark.parametrize(("left", "right"), [
    (pl.Series("v", [1]), pl.Series("v", [True])),
    (pl.Series("v", [1]), pl.Series("v", [1.0])),
    (pl.Series("v", [True]), pl.Series("v", [1.0])),
])
def test_diff_frames_com_chave_distingue_inteiro_booleano_e_ponto_flutuante(
    left: pl.Series, right: pl.Series
) -> None:
    a = pl.DataFrame({"k": ["a"]}).with_columns(left)
    b = pl.DataFrame({"k": ["a"]}).with_columns(right)

    found = diff_frames(a, b, ["k"])

    assert [(item.kind, item.key, item.column) for item in found] == [("cell", ("a",), "v")]
    assert [type(found[0].left), type(found[0].right)] == [type(left[0]), type(right[0])]


@pytest.mark.parametrize(("left", "right", "different"), [
    ([[1, 2]], [[1, 2]], False),
    ([[1, 2]], [[1, 3]], True),
    ([[1]], [[1, 2]], True),
    ([[1]], [[True]], True),
    ([{"a": 1}], [{"a": 1}], False),
    ([{"a": 1}], [{"a": True}], True),
    ([{"a": 1}], [{"b": 1}], True),
])
def test_diff_frames_com_chave_compara_listas_e_structs_com_tipos_estritos(
    left: list[Any], right: list[Any], different: bool
) -> None:
    a = pl.DataFrame({"k": ["a"], "v": left})
    b = pl.DataFrame({"k": ["a"], "v": right})

    assert bool(diff_frames(a, b, ["k"])) is different


def test_diff_frames_com_chave_trata_nan_como_igual_a_nan_igual_ao_modo_posicional() -> None:
    nan = float("nan")
    a = pl.DataFrame({"k": ["a", "b"], "v": [nan, 1.0]})
    b = pl.DataFrame({"k": ["a", "b"], "v": [nan, nan]})

    assert diff_frames(a.head(1), b.head(1)) == ()
    assert diff_frames(a.head(1), b.head(1), ["k"]) == ()
    assert [item.key for item in diff_frames(a, b, ["k"])] == [("b",)]


def test_compare_parquets_com_chave_resume_em_linhas_e_sem_celulas(tmp_path: Path) -> None:
    a = pl.DataFrame({"k": ["a", "b", "c"], "v": [1, 2, 3], "w": ["x", "y", "z"]})
    b = pl.DataFrame({"k": ["a", "b", "d"], "v": [9, 8, 3], "w": ["x", "q", "z"]})

    keyed = compare_parquets(*_write_pair(tmp_path, a, b), key=["k"])
    positional = compare_parquets(*_write_pair(tmp_path, a, b))

    assert (keyed.identical, keyed.diff_rows, keyed.summary) == (False, 4, "4 row diffs")
    assert len(keyed.differences) == 5
    assert positional.summary.endswith("cell diffs")


_CPF_A, _CPF_B = "12345678901", "99999999999"


def _pii_frames() -> tuple[pl.DataFrame, pl.DataFrame]:
    left = pl.DataFrame({
        "cpf": [_CPF_A, _CPF_B], "nome": ["Maria Silva", "Joao Souza"], "obs": ["x", "y"],
    })
    right = pl.DataFrame({
        "cpf": [_CPF_A, "88888888888"], "nome": ["Maria Souza", "Joao Souza"], "obs": ["z", "y"],
    })
    return left, right


def _key_tokens(text: str) -> list[tuple[str, str]]:
    return re.findall(r"difference kind=cell column=(\w+) key_token=([0-9a-f]{12})", text)


def test_main_loga_so_tipo_coluna_e_token_da_chave_por_padrao(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    assert _run(tmp_path, *_pii_frames(), "--key", "cpf") == 1

    tokens = _key_tokens(caplog.text)
    assert [column for column, _ in tokens] == ["nome", "obs"]
    assert len({token for _, token in tokens}) == 1
    assert sha256(_CPF_A.encode()).hexdigest()[:12] not in caplog.text
    for secret in (_CPF_A, _CPF_B, "88888888888", "Maria", "Silva", "Souza", "Joao"):
        assert secret not in caplog.text


def test_main_gera_tokens_de_chave_diferentes_a_cada_execucao(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    first, second = tmp_path / "a", tmp_path / "b"
    first.mkdir()
    second.mkdir()

    assert _run(first, *_pii_frames(), "--key", "cpf") == 1
    earlier = {token for _, token in _key_tokens(caplog.text)}
    caplog.clear()
    assert _run(second, *_pii_frames(), "--key", "cpf") == 1
    later = {token for _, token in _key_tokens(caplog.text)}

    assert len(earlier) == len(later) == 1
    assert earlier != later


def test_main_loga_chaves_e_valores_somente_com_show_values(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    assert _run(tmp_path, *_pii_frames(), "--key", "cpf", "--show-values") == 1

    assert f"key=('{_CPF_A}',) column=nome left='Maria Silva' right='Maria Souza'" in caplog.text
    assert "key_token" not in caplog.text


def test_main_sem_chave_tambem_nao_loga_valores_por_padrao(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    assert _run(tmp_path, *_pii_frames()) == 1

    assert "difference kind=cell column=" in caplog.text
    assert "Maria" not in caplog.text
    assert _CPF_A not in caplog.text
