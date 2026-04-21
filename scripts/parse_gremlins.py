"""Formata report JSON do gremlins em markdown para GITHUB_STEP_SUMMARY."""
from __future__ import annotations

import json
import sys
from pathlib import Path


def summarize(report: dict) -> str:
    total = report["mutations"]
    killed = report["killed"]
    pct = (killed / total * 100) if total else 0.0
    out = [
        "## Gremlins Mutation Score\n",
        f"**Mutation Score: {killed}/{total} ({pct:.2f}%)**\n",
        f"- Killed: {killed}",
        f"- Lived: {report['lived']}",
        f"- Timed out: {report['timed_out']}",
        "",
        "### Per-package",
        "",
        "| Package | Killed / Total | Score |",
        "|---|---|---|",
    ]
    for pkg, stats in report.get("by_package", {}).items():
        p_total = stats["total"]
        p_killed = stats["killed"]
        p_pct = (p_killed / p_total * 100) if p_total else 0.0
        out.append(f"| {pkg} | {p_killed} / {p_total} | {p_pct:.2f}% |")
    return "\n".join(out) + "\n"


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: parse_gremlins.py <report.json>", file=sys.stderr)
        return 1
    report = json.loads(Path(sys.argv[1]).read_text())
    print(summarize(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
