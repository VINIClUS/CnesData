"""CLI do worker de billing: dreno do inbox e recovery de webhooks Stripe."""

import argparse
import logging
import os
from collections.abc import Sequence
from typing import Any

from billing_worker.worker import BillingWorker, build_worker
from cnes_domain.billing.errors import BillingError
from cnes_domain.billing.inbox import STRIPE_EVENT_PAGE_LIMIT, RecoveryResult
from cnes_infra.billing.secrets_manager import SecretProviderError
from cnes_infra.billing.settings import BillingConfigurationError

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_RETRY = 1
EXIT_INVALID = 2


def _limit(raw: str) -> int:
    value = int(raw)
    if not 1 <= value <= STRIPE_EVENT_PAGE_LIMIT:
        raise argparse.ArgumentTypeError(f"limit_out_of_range value={value}")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="billing-worker")
    commands = parser.add_subparsers(dest="command", required=True)
    inbox = commands.add_parser("inbox", help="drena eventos vencidos do inbox")
    inbox.add_argument("--limit", type=_limit, default=STRIPE_EVENT_PAGE_LIMIT)
    commands.add_parser("recover", help="processa uma página do cursor de eventos Stripe")
    return parser


def _session(region: str) -> Any:
    from boto3.session import Session

    return Session(region_name=region)


def _execute(worker: BillingWorker, args: argparse.Namespace) -> RecoveryResult:
    if args.command == "inbox":
        return worker.run_inbox(args.limit)
    return worker.run_recover()


def _log_done(command: str, result: RecoveryResult) -> None:
    logger.info(
        "billing_worker_cycle_done command=%s scanned=%d imported=%d reprocessed=%d "
        "failed=%d next_cursor=%s",
        command, result.scanned, result.imported, result.reprocessed, result.failed,
        result.next_cursor,
    )


def _build() -> tuple[BillingWorker | None, int]:
    try:
        return build_worker(os.environ, _session), EXIT_OK
    except BillingConfigurationError as error:
        logger.error("billing_worker_config_invalid code=%s", error.code)
        return None, EXIT_INVALID
    except SecretProviderError as error:
        logger.error(
            "billing_worker_secret_failed code=%s retryable=%s", error.code, error.retryable,
        )
        return None, EXIT_RETRY if error.retryable else EXIT_INVALID


def _parse(argv: Sequence[str] | None) -> argparse.Namespace | int:
    try:
        return _parser().parse_args(argv)
    except SystemExit as exit_:
        return exit_.code if isinstance(exit_.code, int) else EXIT_INVALID


def main(argv: Sequence[str] | None = None) -> int:
    """Args: argv: Argumentos do CLI (inbox [--limit N] | recover).
    Returns: 0 ciclo concluído; 1 falha retryable/registro durável; 2 entrada inválida.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parse(argv)
    if isinstance(args, int):
        return args
    worker, code = _build()
    if worker is None:
        if code == EXIT_OK:
            logger.info("billing_worker_skipped mode=disabled")
        return code
    try:
        result = _execute(worker, args)
    except BillingError as error:
        code_name = getattr(error, "code", type(error).__name__)
        logger.error("billing_worker_cycle_failed command=%s code=%s", args.command, code_name)
        return EXIT_RETRY
    _log_done(args.command, result)
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
