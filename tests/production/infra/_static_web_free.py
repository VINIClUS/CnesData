"""Forma do static-web elegível ao plano Free do CloudFront e execução da Function de headers."""

from __future__ import annotations

import json
import subprocess
from typing import Any

from tests.production.infra.conftest import require_tool

CACHING_OPTIMIZED = "658327ea-f89d-4fab-a63d-7e88639e58f6"
SECURITY_HEADERS = {
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "referrer-policy": "strict-origin-when-cross-origin",
    "permissions-policy": "camera=(), microphone=(), geolocation=()",
}
FUNCTION_CODE_LIMIT = 10240
FUNCTION_ADDRESSES = (
    "module.static_web.aws_cloudfront_function.spa",
    "module.static_web.aws_cloudfront_function.headers",
)
DISTRIBUTION = "module.static_web.aws_cloudfront_distribution.this"
_ORIGIN_HEADERS = {"x-frame-options": {"value": "SAMEORIGIN"}, "server": {"value": "AmazonS3"}}
_BEHAVIOR_ABSENT = (
    "response_headers_policy_id", "origin_request_policy_id", "realtime_log_config_arn",
    "field_level_encryption_id", "forwarded_values",
)

type Node = dict[str, Any]


def run_headers_function(code: str) -> dict[str, str]:
    """Returns:
    Headers da resposta depois da Function viewer-response, sem o header da origem.
    """
    event = {"response": {"statusCode": 200, "headers": _ORIGIN_HEADERS}}
    harness = (
        f"{code}\nconst event = {json.dumps(event)};\n"
        "process.stdout.write(JSON.stringify(handler(event).headers));\n"
    )
    done = subprocess.run(  # noqa: S603
        [require_tool("node"), "-e", harness],
        check=True, capture_output=True, text=True, timeout=30,
    )
    headers = json.loads(done.stdout)
    assert headers.pop("server") == {"value": "AmazonS3"}, "origin_header_dropped"
    return {name: value["value"] for name, value in headers.items()}


def assert_free_distribution(distribution: Node, functions: dict[str, Node]) -> None:
    behavior = distribution["default_cache_behavior"][0]
    assert behavior["cache_policy_id"] == CACHING_OPTIMIZED, "free_cache_policy_not_managed"
    for field in _BEHAVIOR_ABSENT:
        assert not behavior.get(field), f"free_plan_ineligible field={field}"
    for field in ("ordered_cache_behavior", "logging_config"):
        assert not distribution.get(field), f"free_plan_ineligible field={field}"
    events = sorted(item["event_type"] for item in behavior["function_association"])
    assert events == ["viewer-request", "viewer-response"], f"function_events={events}"
    for address, function in functions.items():
        assert not function.get("key_value_store_associations"), f"free_kvs address={address}"
        assert len(function["code"]) < FUNCTION_CODE_LIMIT, f"function_code_size address={address}"


def assert_exclusive_edge(changes: list[Node]) -> None:
    distributions = [c["address"] for c in changes if "cloudfront" in c["type"]
                     and c["type"].endswith("distribution")]
    assert distributions == [DISTRIBUTION], f"free_plan_shared_edge distributions={distributions}"
    associations = [c["address"] for c in changes if c["type"] == "aws_wafv2_web_acl_association"]
    assert not associations, f"free_plan_shared_edge associations={associations}"
