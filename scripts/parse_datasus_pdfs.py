"""Extrai schemas/tabelas de PDFs Datasus (MANUAL_OPERACIONAL_SIA.pdf etc.).

Uso:
    python scripts/parse_datasus_pdfs.py \\
        --pdf E:/siasus/MANUAL_OPERACIONAL_SIA.pdf \\
        --output docs/_tmp_sia_pdf_extract.md
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_schema_tables(text: str) -> list[list[str]]:
    """Heuristica simples: agrupa linhas que comecam com codigo numerico."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    table: list[str] = []
    tables: list[list[str]] = []
    for ln in lines:
        if re.match(r"^\d{6,12}\s", ln):
            table.append(ln)
        else:
            if len(table) >= 2:
                tables.append(table)
            table = []
    if len(table) >= 2:
        tables.append(table)
    return tables


def main() -> int:
    import pdfplumber
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdf", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    with pdfplumber.open(str(args.pdf)) as pdf:
        parts = [
            f"# Extract from `{args.pdf.name}`",
            "",
            f"> Pages: {len(pdf.pages)}",
            "",
        ]
        for i, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ""
            tables = extract_schema_tables(text)
            if tables:
                parts.append(f"## Page {i}")
                parts.append("")
                for t in tables:
                    parts.append("```")
                    parts.extend(t[:30])
                    if len(t) > 30:
                        parts.append(f"... ({len(t) - 30} more rows)")
                    parts.append("```")
                    parts.append("")

        args.output.write_text("\n".join(parts), encoding="utf-8")
        logger.info("pdf_extract_written path=%s pages=%d", args.output, len(pdf.pages))
    return 0


if __name__ == "__main__":
    sys.exit(main())
