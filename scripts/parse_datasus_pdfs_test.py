"""Teste do parse_datasus_pdfs."""
from scripts.parse_datasus_pdfs import extract_schema_tables


def test_extract_schema_tables_retorna_linhas_tabela() -> None:
    fake_page_text = """
Tabela de Procedimentos
Codigo  Descricao
0101010010  Consulta medica em atencao basica
0301060010  Cirurgia de catarata
"""
    tables = extract_schema_tables(fake_page_text)
    assert len(tables) >= 1
    assert any("0101010010" in row for row in tables[0])
