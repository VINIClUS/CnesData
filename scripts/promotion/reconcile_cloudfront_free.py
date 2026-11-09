"""Reconciliação manual da assinatura FREE exata, sem fallback ou substituição."""

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

FREE_QUOTA = 3
TOTAL_QUOTA = 100
CLIENT_CONFIG: dict[str, Any] = {
    "ignore_configured_endpoint_urls": True,
    "retries": {"total_max_attempts": 1},
    "connect_timeout": 10,
    "read_timeout": 30,
}
NO_RETRY = Config(**CLIENT_CONFIG)


class FreeRefused(ValueError):
    """Operação recusada com código público fixo."""


@dataclass(repr=False)
class Clients:
    ppm: Any
    cloudfront: Any
    freetier: Any


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise FreeRefused(f"reason={reason}")


def _binding(binding: dict[str, str]) -> tuple[str, str]:
    _require(set(binding) == {"distribution_arn", "web_acl_arn"}, "binding_fields")
    dist = re.fullmatch(
        r"arn:aws:cloudfront::([0-9]{12}):distribution/([A-Z0-9]+)", binding["distribution_arn"]
    )
    _require(dist is not None, "distribution")
    assert dist is not None
    account, identifier = dist.groups()
    _require(
        re.fullmatch(
            rf"arn:aws:wafv2:us-east-1:{account}:global/webacl/[A-Za-z0-9_-]+/[a-z0-9-]+",
            binding["web_acl_arn"],
        )
        is not None,
        "waf",
    )
    return account, identifier


def _subscriptions(ppm: Any) -> list[dict[str, Any]]:
    result, seen, request = [], set(), {}
    while True:
        page = ppm.list_subscriptions(**request)
        result.extend(page["subscriptionSummaries"])
        _require(len(result) <= TOTAL_QUOTA, "inventory_limit")
        token = page.get("nextToken")
        if token is None:
            break
        _require(isinstance(token, str) and bool(token) and token not in seen, "pagination")
        seen.add(token)
        request = {"nextToken": token}
    _require(len({row["arn"] for row in result}) == len(result), "duplicate_subscription")
    return result


def _distribution(cf: Any, binding: dict[str, str], identifier: str) -> None:
    result = cf.get_distribution(Id=identifier)["Distribution"]
    _require(result["ARN"] == binding["distribution_arn"], "distribution_identity")
    _require(result["Status"] == "Deployed", "distribution_pending")
    config = result["DistributionConfig"]
    _require(config["WebACLId"] == binding["web_acl_arn"], "waf_binding")
    _require(config["ViewerCertificate"].get("SSLSupportMethod") != "vip", "paid_certificate")
    origins = config["Origins"]["Items"]
    _require(not any(x.get("OriginShield", {}).get("Enabled") for x in origins), "origin_shield")
    behaviors = [config["DefaultCacheBehavior"], *config.get("CacheBehaviors", {}).get("Items", [])]
    _require(not any(x.get("RealtimeLogConfigArn") for x in behaviors), "realtime_logs")
    linked = cf.list_distributions_by_web_acl_id(WebACLId=binding["web_acl_arn"])[
        "DistributionList"
    ]
    _require(linked.get("IsTruncated") is False, "waf_inventory_partial")
    _require([x["ARN"] for x in linked.get("Items", [])] == [result["ARN"]], "waf_shared")
    _monitoring(cf, identifier)


def _monitoring(cf: Any, identifier: str) -> None:
    try:
        value = cf.get_monitoring_subscription(DistributionId=identifier)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "NoSuchMonitoringSubscription":
            raise FreeRefused("reason=monitoring_read") from None
        return
    status = value["MonitoringSubscription"].get("RealtimeMetricsSubscriptionConfig", {})
    _require(status.get("RealtimeMetricsSubscriptionStatus") in {None, "Disabled"}, "paid_metrics")


def _exact(subscription: dict[str, Any], binding: dict[str, str]) -> None:
    _require(subscription["planFamily"] == "CloudFront", "family")
    _require(subscription["planTier"] == "FREE", "paid_tier")
    _require(subscription["status"] == "ACTIVE", "inactive")
    resources = subscription["resourceArns"]
    _require(
        len(resources) == 2 and set(resources) == set(binding.values()), "conflicting_resources"
    )
    _require(not subscription.get("scheduledChange"), "scheduled_change")
    _require(subscription.get("usageLevel") in {None, "DEFAULT"}, "usage_level")


def _create(ppm: Any, binding: dict[str, str], attempt: Path | None) -> dict[str, Any]:
    token = sha256(json.dumps(binding, sort_keys=True).encode()).hexdigest()
    if attempt is not None:
        try:
            with attempt.open("x", encoding="utf-8") as stream:
                stream.write(json.dumps({"request_sha256": token, "assertion": "create:attempted"}))
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError:
            raise FreeRefused("reason=prior_attempt_requires_readback") from None
    try:
        result = ppm.create_subscription(
            planFamily="CloudFront",
            planTier="FREE",
            resourceArns=list(binding.values()),
            approvalMode="IMMEDIATE",
            clientToken=token,
        )
    except Exception:
        raise FreeRefused("reason=uncertain_create") from None
    return result["subscription"]


def reconcile(binding: dict[str, str], api: Any, attempt: Path | None = None) -> None:
    """Args: binding exato, clientes AWS e journal exclusivo de tentativa.
    Raises: FreeRefused quando elegibilidade, associação ou ACTIVE não forem comprovados.
    """
    account, identifier = _binding(binding)
    _require(api.ppm.meta.region_name == "us-east-1", "region")
    plan = api.freetier.get_account_plan_state()
    _require(plan["accountId"] == account, "account")
    _require(
        plan["accountPlanType"] == "PAID" and plan["accountPlanStatus"] == "ACTIVE",
        "account_ineligible",
    )
    _distribution(api.cloudfront, binding, identifier)
    inventory = _subscriptions(api.ppm)
    matches = [x for x in inventory if set(x["resourceArns"]) & set(binding.values())]
    _require(len(matches) <= 1, "ambiguous")
    if matches:
        subscription = matches[0]
    else:
        _require(
            sum(x["planFamily"] == "CloudFront" and x["planTier"] == "FREE" for x in inventory)
            < FREE_QUOTA,
            "free_quota",
        )
        _require(len(inventory) < TOTAL_QUOTA, "total_quota")
        subscription = _create(api.ppm, binding, attempt)
    _exact(subscription, binding)
    observed = api.ppm.get_subscription(arn=subscription["arn"])["subscription"]
    _require(observed["arn"] == subscription["arn"], "subscription_identity")
    _exact(observed, binding)


def main() -> int:
    """Returns: código de sucesso ou recusa, sem conteúdo privado no log."""
    logging.disable(logging.CRITICAL)
    parser = argparse.ArgumentParser()
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--attempt-journal", type=Path, required=True)
    parser.add_argument("--release-hash", required=True)
    args = parser.parse_args()
    if re.fullmatch(r"[a-f0-9]{64}", args.release_hash) is None:
        return 1
    passed = False
    try:
        session = boto3.Session()
        clients = Clients(
            *(
                session.client(name, region_name="us-east-1", config=NO_RETRY)
                for name in ("pricing-plan-manager", "cloudfront", "freetier")
            )
        )
        reconcile(json.loads(args.binding.read_text()), clients, args.attempt_journal)
        passed = True
    except Exception:
        passed = False
    sys.stdout.write(
        json.dumps(
            {
                "assertion": "cloudfront_free_active:" + ("pass" if passed else "fail"),
                "timestamp": datetime.now(UTC).isoformat(),
                "release_hash": args.release_hash,
            }
        )
        + "\n"
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
