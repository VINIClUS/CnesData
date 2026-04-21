"""Teste do introspect_sia_dbf."""
from pathlib import Path

from scripts.introspect_sia_dbf import DBFInfo, format_dbf_markdown


def test_format_dbf_markdown_lista_colunas() -> None:
    info = DBFInfo(
        path=Path("S_APA.DBF"),
        encoding="cp1252",
        record_count=100,
        fields=[
            ("CNES", "C", 7, 0),
            ("CBO", "C", 6, 0),
            ("QT", "N", 11, 0),
            ("VL", "N", 10, 2),
        ],
    )
    md = format_dbf_markdown(info)
    assert "### `S_APA.DBF`" in md
    assert "100 records" in md
    assert "| CNES | C(7) |" in md
    assert "| VL | N(10,2) |" in md
