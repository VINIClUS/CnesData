"""Preâmbulo da promoção: SHA contido em develop, checks obrigatórios verdes e freeze ausente."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

from release.build_candidate import VERIFY_JOBS

logger = logging.getLogger(__name__)
FAILED = frozenset({"failure", "cancelled", "timed_out", "action_required", "stale",
                    "startup_failure"})
_SHA = re.compile(r"[0-9a-f]{40}")
_FREEZE = re.compile(r"arn:aws:iam::\d{12}:policy/cnesdata-budget-freeze")


class PreambleError(RuntimeError):
    pass


def check_preamble(evidence: dict[str, Any]) -> None:
    """Raises:
    PreambleError: SHA fora de develop, check obrigatório não verde ou freeze presente.
    """
    sha = evidence.get("source_sha")
    if not isinstance(sha, str) or _SHA.fullmatch(sha) is None:
        raise PreambleError("source_sha=invalid")
    _require_develop(evidence["compare"], sha)
    _require_checks(evidence["check_runs"], sha)
    _require_unfrozen(evidence["attached_policies"], evidence["freeze_policy_arn"])


def main(argv: list[str] | None = None) -> int:
    """Returns:
    0 quando o preâmbulo passa; 1 para qualquer recusa ou evidência ilegível.
    """
    parser = argparse.ArgumentParser(description="Preâmbulo da promoção de produção.")
    parser.add_argument("--evidence", type=Path, required=True)
    arguments = parser.parse_args(argv)
    try:
        check_preamble(json.loads(arguments.evidence.read_text(encoding="utf-8")))
    except (OSError, ValueError, KeyError, TypeError, PreambleError) as error:
        logger.error("promotion_preamble=refused reason=%s", error)
        return 1
    logger.info("promotion_preamble=accepted")
    return 0


def _require_develop(compare: dict[str, Any], sha: str) -> None:
    if compare.get("status") not in {"identical", "ahead"} or (
        compare.get("merge_base_commit", {}).get("sha") != sha
    ):
        raise PreambleError("source=outside_develop")


def _require_checks(listing: dict[str, Any], sha: str) -> None:
    runs = listing.get("check_runs", [])
    if listing.get("total_count") != len(runs) or len(runs) > 100:
        raise PreambleError("check=pagination")
    latest: dict[str, dict[str, Any]] = {}
    for run in sorted(runs, key=lambda item: item["id"]):
        latest[run["name"]] = run
    missing = VERIFY_JOBS - latest.keys()
    if missing:
        raise PreambleError(f"check=missing names={sorted(missing)}")
    for name, run in latest.items():
        if run.get("head_sha") != sha or run.get("conclusion") in FAILED:
            raise PreambleError(f"check=not_green name={name}")
        if name in VERIFY_JOBS and (run.get("status"), run.get("conclusion")) != (
            "completed",
            "success",
        ):
            raise PreambleError(f"check=not_green name={name}")


def _require_unfrozen(listing: dict[str, Any], freeze: str) -> None:
    if not isinstance(freeze, str) or _FREEZE.fullmatch(freeze) is None:
        raise PreambleError("freeze=policy_unknown")
    if listing.get("IsTruncated") is not False:
        raise PreambleError("freeze=listing_truncated")
    if any(item.get("PolicyArn") == freeze for item in listing.get("AttachedPolicies", [])):
        raise PreambleError("freeze=present")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.exit(main())
