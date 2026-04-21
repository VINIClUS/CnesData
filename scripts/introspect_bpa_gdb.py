"""Introspeccao BPAMAG.GDB via fdb (Firebird 1.5/2.5 compat).

Le RDB$RELATIONS + RDB$RELATION_FIELDS + RDB$FIELDS para extrair schema
de tabelas nao-sistema. Emite markdown em docs/data-dictionary-bpa.md.

Uso:
    python scripts/introspect_bpa_gdb.py \\
        --gdb E:/BPA/BPAMAG.GDB \\
        --dll E:/BPA/fbclient.dll \\
        --output docs/data-dictionary-bpa.md
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    name: str
    type_name: str
    nullable: bool
    length: int | None


@dataclass
class TableInfo:
    name: str
    columns: list[ColumnInfo]
    row_count_estimate: int


_FB_TYPE_MAP = {
    7: "SMALLINT", 8: "INTEGER", 10: "FLOAT", 12: "DATE", 13: "TIME",
    14: "CHAR", 16: "BIGINT", 27: "DOUBLE PRECISION", 35: "TIMESTAMP",
    37: "VARCHAR", 261: "BLOB",
}


def _connect(gdb: Path, dll: Path):
    import fdb
    fdb.load_api(str(dll))
    return fdb.connect(
        dsn=str(gdb), user="SYSDBA", password="masterkey",
        charset="WIN1252",
    )


def list_tables(conn) -> list[str]:
    cur = conn.cursor()
    cur.execute("""
        SELECT TRIM(RDB$RELATION_NAME)
        FROM RDB$RELATIONS
        WHERE RDB$SYSTEM_FLAG = 0
        ORDER BY RDB$RELATION_NAME
    """)
    return [r[0] for r in cur.fetchall()]


def describe_table(conn, table_name: str) -> TableInfo:
    cur = conn.cursor()
    cur.execute("""
        SELECT
            TRIM(RF.RDB$FIELD_NAME),
            F.RDB$FIELD_TYPE,
            F.RDB$FIELD_LENGTH,
            RF.RDB$NULL_FLAG
        FROM RDB$RELATION_FIELDS RF
        JOIN RDB$FIELDS F ON F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE
        WHERE RF.RDB$RELATION_NAME = ?
        ORDER BY RF.RDB$FIELD_POSITION
    """, (table_name,))
    columns = []
    for name, ftype, length, null_flag in cur.fetchall():
        columns.append(ColumnInfo(
            name=name,
            type_name=_FB_TYPE_MAP.get(ftype, f"UNKNOWN({ftype})"),
            nullable=(null_flag != 1),
            length=length if length and length > 0 else None,
        ))

    try:
        cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        (count,) = cur.fetchone()
    except Exception:
        count = -1
    return TableInfo(name=table_name, columns=columns, row_count_estimate=count)


def format_table_markdown(t: TableInfo) -> str:
    lines = [
        f"### `{t.name}`",
        "",
        f"- Rows (estimate): {t.row_count_estimate} rows",
        f"- Columns: {len(t.columns)}",
        "",
        "| Coluna | Tipo | Nullable |",
        "|---|---|---|",
    ]
    for c in t.columns:
        type_str = f"{c.type_name}({c.length})" if c.length else c.type_name
        null_str = "nullable" if c.nullable else "NOT NULL"
        lines.append(f"| {c.name} | {type_str} | {null_str} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gdb", type=Path, required=True)
    parser.add_argument("--dll", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    conn = _connect(args.gdb, args.dll)
    try:
        tables = list_tables(conn)
        logger.info("bpa_tables_found count=%d", len(tables))

        out_lines = [
            "# Dicionario de Dados - BPA-Mag (BPAMAG.GDB)",
            "",
            "> Introspeccao automatica via `scripts/introspect_bpa_gdb.py`.",
            f"> Source: `{args.gdb}` | Firebird 1.5/2.5 | Charset: WIN1252",
            f"> Tabelas nao-sistema: {len(tables)}",
            "",
            "## Tabelas",
            "",
        ]
        for t_name in tables:
            info = describe_table(conn, t_name)
            out_lines.append(format_table_markdown(info))

        args.output.write_text("\n".join(out_lines), encoding="utf-8")
        logger.info("bpa_dict_written path=%s tables=%d", args.output, len(tables))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
