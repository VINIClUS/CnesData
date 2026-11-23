"""CLI da Phase A: promove a imagem ao ECR e monta os inputs a partir do recibo local."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3
import httpx
from botocore.exceptions import BotoCoreError, ClientError

from release.build_candidate import GitHub
from release.verify_candidate import canonical, require
from scripts.promotion.ecr_registry import connect
from scripts.promotion.phase_a import prepare_inputs
from scripts.promotion.phase_a_private import (
    WRITER_ROLE,
    stage_private,
    upload_private,
    validate_target,
)
from scripts.promotion.promote_ecr import promote_candidate

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

type Node = dict[str, Any]
REPOSITORY = "VINIClUS/CnesData"
REGION = "us-east-2"
FAILURES = (ValueError, KeyError, TypeError, OSError, httpx.HTTPError, BotoCoreError, ClientError)
logger = logging.getLogger(__name__)


def _load(path: str) -> Node:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _promote(arguments: argparse.Namespace, api: Any, registry: Any, now: datetime) -> None:
    governed = {
        "account_id": arguments.account_id,
        "region": arguments.region,
        "now": now,
        "candidate_creator": _load(arguments.creator),
    }
    receipt = promote_candidate(_load(arguments.reference), api, registry, governed)
    Path(arguments.output).write_bytes(canonical(receipt))


def _phase_inputs(arguments: argparse.Namespace) -> None:
    governed = {
        "account_id": arguments.account_id,
        "region": arguments.region,
        "source_sha": arguments.source_sha,
        "canary_trusted_principal_arn": arguments.canary_trusted_principal_arn,
    }
    inputs = prepare_inputs(_load(arguments.promotion), _load(arguments.baseline), governed)
    Path(arguments.output).write_bytes(canonical(inputs))


def _identity(arguments: argparse.Namespace) -> Node:
    return {
        "source_sha": arguments.source_sha,
        "run_id": int(arguments.run_id),
        "run_attempt": int(arguments.run_attempt),
    }


def _stage(arguments: argparse.Namespace) -> None:
    stage_private(Path(arguments.directory), _identity(arguments))


def _upload(arguments: argparse.Namespace, seams: Seams) -> None:
    target = {
        "bucket": arguments.bucket,
        "prefix": arguments.prefix,
        "kms_key_arn": arguments.kms_key_arn,
        "identity": _identity(arguments),
    }
    validate_target(target)
    session = _authenticated_session(arguments, seams)
    arn = session.client("sts").get_caller_identity()["Arn"]
    prefix = f"arn:aws:sts::{arguments.account_id}:assumed-role/{WRITER_ROLE}/"
    require(arn.startswith(prefix), "sts=writer_role")
    upload_private(Path(arguments.directory), target, session.client("s3"))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phase A: promoção ECR e inputs locais.")
    commands = parser.add_subparsers(dest="command", required=True)
    promote = commands.add_parser("promote-ecr", help="Promove a imagem e grava promotion.json.")
    for name in ("reference", "creator", "account-id", "region", "output"):
        promote.add_argument(f"--{name}", required=True)
    inputs = commands.add_parser("phase-inputs", help="Monta phase-inputs.json do recibo.")
    for name in (
        "promotion",
        "baseline",
        "account-id",
        "region",
        "source-sha",
        "canary-trusted-principal-arn",
        "output",
    ):
        inputs.add_argument(f"--{name}", required=True)
    stage = commands.add_parser("stage-private", help="Grava candidate.json e phase-a-public.json.")
    upload = commands.add_parser("upload-private", help="Grava os três objetos privados no S3.")
    for sub, extra in ((stage, ()), (upload, ("account-id", "bucket", "prefix", "kms-key-arn"))):
        for name in ("directory", "source-sha", "run-id", "run-attempt", *extra):
            sub.add_argument(f"--{name}", required=True)
    return parser


def _now() -> datetime:
    return datetime.now(UTC)


def _new_session() -> Any:
    return boto3.Session(region_name=REGION)


@dataclass(frozen=True)
class Seams:
    """Fronteiras externas substituíveis nos testes."""

    api_factory: Callable[[str], Any] = GitHub
    session_factory: Callable[[], Any] = _new_session
    registry_factory: Callable[[Any], Any] = connect
    clock: Callable[[], datetime] = _now


def _authenticated_session(arguments: argparse.Namespace, seams: Seams) -> Any:
    session = seams.session_factory()
    account = session.client("sts").get_caller_identity()["Account"]
    require(account == arguments.account_id, "sts=account_mismatch")
    return session


def _dispatch(arguments: argparse.Namespace, seams: Seams) -> None:
    if arguments.command == "promote-ecr":
        session = _authenticated_session(arguments, seams)
        api = seams.api_factory(REPOSITORY)
        _promote(arguments, api, seams.registry_factory(session), seams.clock())
    elif arguments.command == "phase-inputs":
        _phase_inputs(arguments)
    elif arguments.command == "stage-private":
        _stage(arguments)
    else:
        _upload(arguments, seams)


def main(
    argv: Sequence[str] | None = None,
    *,
    seams: Seams | None = None,
) -> int:
    """Returns: 0 com o arquivo gravado; 2 em qualquer recusa, sem arquivo de saída."""
    arguments = _parser().parse_args(argv)
    seams = seams or Seams()
    try:
        _dispatch(arguments, seams)
    except FAILURES as error:
        reason = str(error) if isinstance(error, ValueError) else type(error).__name__
        logger.error("%s=failed reason=%s", arguments.command.replace("-", "_"), reason)
        return 2
    output = getattr(arguments, "output", None) or arguments.directory
    logger.info("%s=written output=%s", arguments.command.replace("-", "_"), output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
