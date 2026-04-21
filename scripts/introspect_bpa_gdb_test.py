"""Teste do introspect_bpa_gdb - parse + format."""
from scripts.introspect_bpa_gdb import ColumnInfo, TableInfo, format_table_markdown


def test_format_table_markdown_emite_seccao() -> None:
    t = TableInfo(
        name="TB_PRODUCAO",
        columns=[
            ColumnInfo(name="ID", type_name="INTEGER", nullable=False, length=None),
            ColumnInfo(name="COD_CNES", type_name="CHAR", nullable=False, length=7),
            ColumnInfo(name="VL_APROV", type_name="NUMERIC", nullable=True, length=None),
        ],
        row_count_estimate=12345,
    )
    md = format_table_markdown(t)
    assert "### `TB_PRODUCAO`" in md
    assert "12345 rows" in md
    assert "| ID | INTEGER |" in md
    assert "| COD_CNES | CHAR(7) |" in md
    assert "| VL_APROV | NUMERIC |" in md
    assert "nullable" in md.lower()
