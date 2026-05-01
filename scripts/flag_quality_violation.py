"""Emit quality violation label + STEP_SUMMARY; exit 1 to fail CI job."""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys

logger = logging.getLogger(__name__)

LABEL_MAP: dict[str, str] = {
    "n+1": "needs-quality-review",
    "race": "needs-quality-review",
    "memleak": "needs-quality-review",
    "memleak-soft": "needs-quality-review",
    "chaos": "needs-chaos-review",
    "negative": "needs-security-review",
}


def _apply_label(pr_number: str, label: str) -> None:
    cmd = ["gh", "pr", "edit", pr_number, "--add-label", label]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        logger.warning("flag_violation: label_apply_failed label=%s err=%s", label, exc)


def _write_summary(kind: str, details: str) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    lines = [
        f"## Quality violation — {kind}",
        "",
        f"- Details: `{details}`",
        "",
    ]
    with open(summary_path, "a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def flag(kind: str, details: str) -> None:
    """Apply label + summary; exit 1 unless kind ends with '-soft'."""
    logger.error("quality_violation kind=%s details=%s", kind, details)

    label = LABEL_MAP.get(kind)
    pr_number = os.environ.get("PR_NUMBER")
    token = os.environ.get("GITHUB_TOKEN")
    if label and pr_number and token:
        _apply_label(pr_number, label)

    _write_summary(kind, details)

    if kind.endswith("-soft"):
        sys.exit(0)
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", required=True, choices=list(LABEL_MAP.keys()))
    parser.add_argument("--details", required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    flag(args.kind, args.details)


if __name__ == "__main__":
    main()
