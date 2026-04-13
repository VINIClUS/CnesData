"""Testes do HR encoding cleanup generator."""
from pathlib import Path

import pytest
from cnes_infra.ingestion.hr_client import _detectar_encoding, _linhas_limpas, carregar_folha

_COLUNAS_MINIMAS = ["CPF", "NOME", "DATA_ADMISSAO", "DATA_DEMISSAO", "STATUS"]


def _escrever_csv(path: Path, linhas: list[str], encoding: str = "utf-8") -> None:
    path.write_bytes("\n".join(linhas).encode(encoding))


def test_detectar_encoding_utf8(tmp_path):
    f = tmp_path / "folha.csv"
    _escrever_csv(f, ["CPF,NOME", "12345678901,José"], encoding="utf-8")
    assert _detectar_encoding(f) in ("utf-8-sig", "utf-8")


def test_detectar_encoding_cp1252(tmp_path):
    f = tmp_path / "folha.csv"
    f.write_bytes("CPF,NOME\n12345678901,João".encode("cp1252"))
    assert _detectar_encoding(f) == "cp1252"


def test_linhas_limpas_remove_null_bytes(tmp_path):
    f = tmp_path / "folha.csv"
    conteudo = "CPF,NOME\n123\x0045678901,TESTE\n"
    f.write_bytes(conteudo.encode("utf-8"))
    linhas = list(_linhas_limpas(f))
    assert all("\x00" not in linha for linha in linhas)


def test_linhas_limpas_utf8sig(tmp_path):
    f = tmp_path / "folha.csv"
    f.write_bytes("CPF,NOME\n12345678901,Maria".encode("utf-8-sig"))
    linhas = list(_linhas_limpas(f))
    assert "CPF" in linhas[0]


def _gerar_csv_completo(tmp_path: Path, encoding: str = "utf-8") -> Path:
    f = tmp_path / "folha.csv"
    cabecalho = ",".join(_COLUNAS_MINIMAS)
    linha = "12345678901,NOME TESTE,2024-01-01,,ATIVO"
    _escrever_csv(f, [cabecalho, linha], encoding=encoding)
    return f


def test_carregar_folha_utf8(tmp_path):
    f = _gerar_csv_completo(tmp_path, encoding="utf-8")
    df = carregar_folha(f)
    assert not df.is_empty()
    assert "CPF" in df.columns


def test_carregar_folha_cp1252(tmp_path):
    f = tmp_path / "folha.csv"
    cabecalho = ",".join(_COLUNAS_MINIMAS)
    linha = "12345678901,João da Silva,2024-01-01,,ATIVO"
    f.write_bytes(f"{cabecalho}\n{linha}".encode("cp1252"))
    df = carregar_folha(f)
    assert not df.is_empty()
    assert "CPF" in df.columns


def test_carregar_folha_null_bytes_ignorados(tmp_path):
    f = tmp_path / "folha.csv"
    cabecalho = ",".join(_COLUNAS_MINIMAS)
    linha = "123\x0045678901,NOME,2024-01-01,,ATIVO"
    f.write_bytes(f"{cabecalho}\n{linha}".encode("utf-8"))
    df = carregar_folha(f)
    assert not df.is_empty()


def test_carregar_folha_extensao_invalida(tmp_path):
    from cnes_infra.ingestion.hr_client import HrSchemaError

    f = tmp_path / "folha.txt"
    f.write_text("CPF,NOME")
    with pytest.raises(HrSchemaError):
        carregar_folha(f)


def test_carregar_folha_colunas_faltando(tmp_path):
    from cnes_infra.ingestion.hr_client import HrSchemaError

    f = tmp_path / "folha.csv"
    _escrever_csv(f, ["CPF,NOME", "12345678901,TESTE"])
    with pytest.raises(HrSchemaError):
        carregar_folha(f)
