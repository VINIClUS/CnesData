"""Atualiza docs raiz pós-remoção do dump_agent Python.

Targets:
- CLAUDE.md raiz (remove row apps/dump_agent/ do monorepo map)
- docs/architecture.md (troca Python daemon → Go daemon)
- docs/roadmap.md (marca migration Go como DONE)
- README.md (atualiza references)
"""
from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def update_claude_md(path: Path) -> bool:
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8")
    pattern = r"\|\s*`apps/dump_agent/`\s*\|[^\n]*\n"
    new_text = re.sub(pattern, "", text)
    if new_text != text:
        path.write_text(new_text, encoding="utf-8")
        return True
    return False


def update_architecture_md(path: Path) -> bool:
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8")
    new_text = text.replace("dump_agent (Python)", "dumpagent (Go)")
    new_text = new_text.replace("`dump_agent`", "`dumpagent_go`")
    if new_text != text:
        path.write_text(new_text, encoding="utf-8")
        return True
    return False


def update_roadmap(path: Path) -> bool:
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8")
    new_text = re.sub(
        r"- \[ \] (Migração Go|migration Go|Go migration|dumpagent Go)",
        r"- [x] \1",
        text,
    )
    if new_text != text:
        path.write_text(new_text, encoding="utf-8")
        return True
    return False


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    root = Path(".")
    changed = []
    if update_claude_md(root / "CLAUDE.md"):
        changed.append("CLAUDE.md")
    if update_architecture_md(root / "docs" / "architecture.md"):
        changed.append("docs/architecture.md")
    if update_roadmap(root / "docs" / "roadmap.md"):
        changed.append("docs/roadmap.md")

    logger.info("docs_updated files=%s", changed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
