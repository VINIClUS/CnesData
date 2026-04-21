"""Introspeccao de DBF files em E:/siasus/ via dbfread.

Le cada .DBF, extrai field_names + types + record_count, emite markdown em
docs/data-dictionary-sia.md.

Uso:
    python scripts/introspect_sia_dbf.py \\
        --dir E:/siasus/ \\
        --output docs/data-dictionary-sia.md
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class DBFInfo:
    path: Path
    encoding: str
    record_count: int
    fields: list[tuple[str, str, int, int]]  # (name, type, length, decimal)


def read_dbf(dbf_path: Path) -> DBFInfo:
    from dbfread import DBF
    d = DBF(str(dbf_path), encoding="cp1252", ignore_missing_memofile=True)
    fields = [
        (f.name, f.type, f.length, f.decimal_count or 0)
        for f in d.fields
    ]
    return DBFInfo(
        path=dbf_path,
        encoding="cp1252",
        record_count=len(d),
        fields=fields,
    )


def format_dbf_markdown(info: DBFInfo) -> str:
    size_bytes = info.path.stat().st_size if info.path.exists() else 0
    lines = [
        f"### `{info.path.name}`",
        "",
        f"- {info.record_count} records",
        f"- Size: {size_bytes:,} bytes",
        f"- Encoding: {info.encoding}",
        f"- Fields: {len(info.fields)}",
        "",
        "| Field | Type | Length | Decimal |",
        "|---|---|---|---|",
    ]
    for name, ftype, length, decimal in info.fields:
        type_str = f"{ftype}({length})" if decimal == 0 else f"{ftype}({length},{decimal})"
        lines.append(f"| {name} | {type_str} | {length} | {decimal} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    dbfs = sorted(
        list(args.dir.glob("*.DBF")) + list(args.dir.glob("*.dbf")),
        key=lambda p: p.name,
    )
    logger.info("sia_dbfs_found count=%d", len(dbfs))

    out_lines = [
        "# Dicionario de Dados - SIA (SIASUS DBFs)",
        "",
        "> Introspeccao automatica via `scripts/introspect_sia_dbf.py`.",
        f"> Source: `{args.dir}`",
        f"> DBFs: {len(dbfs)}",
        "",
        "## Summary",
        "",
        "| Arquivo | Records | Size (bytes) |",
        "|---|---|---|",
    ]

    infos = []
    for dbf in dbfs:
        try:
            info = read_dbf(dbf)
            infos.append(info)
        except Exception as e:
            logger.warning("dbf_skip path=%s err=%s", dbf, e)
            continue

    infos.sort(key=lambda i: -i.record_count)
    out_lines.extend(
        f"| {info.path.name} | {info.record_count} | {info.path.stat().st_size:,} |"
        for info in infos
    )

    out_lines.append("")
    out_lines.append("## Detalhes por DBF")
    out_lines.append("")
    out_lines.extend(format_dbf_markdown(info) for info in infos)

    args.output.write_text("\n".join(out_lines), encoding="utf-8")
    logger.info("sia_dict_written path=%s dbfs=%d", args.output, len(infos))
    return 0


if __name__ == "__main__":
    sys.exit(main())
