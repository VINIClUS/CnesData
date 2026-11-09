"""Smoke manual: probes reais com saída fechada e ausência de prova como falha."""

import argparse
import configparser
import json
import logging
import re
import shlex
import sys
from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import boto3
import httpx
from botocore.exceptions import ClientError, CredentialRetrievalError
from cryptography import x509

from cnes_infra.auth.oidc import OidcVerifier
from scripts.promotion.reconcile_cloudfront_free import NO_RETRY
from tests.integration.aws_capability._login_claims import aud_label, claims_of, scope_has_api
from tests.integration.aws_capability._login_pkce import challenge_of
from tests.integration.aws_capability._login_ports import API_ORIGIN

ASSERTIONS: tuple[str, ...] = (
    "cognito_pkce_resource",
    "aud_refresh",
    "memberships_isolation",
    "cors",
    "sfn_trust_task",
    "serving_envelope_body",
    "object_lock_compliance",
    "roles_anywhere_crl",
)
WEB = "https://cnesdata.com.br"
SERVING = "/api/v1/dashboard/serving/cnes/overview"


def evidence(name: str, passed: bool, release: str, now: datetime) -> dict[str, str]:
    """Returns: somente nome fechado de asserção, timestamp UTC e hash da release."""
    if name not in ASSERTIONS or re.fullmatch(r"[a-f0-9]{64}", release) is None:
        raise ValueError("reason=evidence_schema")
    if now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise ValueError("reason=evidence_clock")
    return {
        "assertion": f"{name}:{'pass' if passed else 'fail'}",
        "timestamp": now.isoformat(),
        "release_hash": release,
    }


def run_smoke(
    release: str, probes: Mapping[str, Callable[[], object]], now: datetime | None = None
) -> tuple[list[dict[str, str]], bool]:
    """Args: hash aprovado e probes; ausência ou exceção nunca produz sucesso.
    Returns: evidências sem dados privados e sucesso conjunto.
    """
    rows, results = [], []
    for name in ASSERTIONS:
        passed = False
        try:
            passed = probes[name]() is True
        except Exception:
            passed = False
        rows.append(evidence(name, passed, release, now or datetime.now(UTC)))
        results.append(passed)
    return rows, all(results)


def _require(value: bool) -> None:
    if not value:
        raise ValueError("reason=smoke_assertion")


def _https(value: str, host: str | None = None) -> None:
    parsed = urlsplit(value)
    _require(parsed.scheme == "https" and not parsed.username and not parsed.password)
    _require(not parsed.fragment and parsed.port in {None, 443})
    _require(host is None or parsed.hostname == host)


def _grant(config: dict[str, Any], name: str) -> dict[str, str]:
    grant = config[name]
    url = urlsplit(grant["authorize_url"])
    _require(f"{url.scheme}://{url.netloc}" == config["domain"])
    _require(url.path == "/oauth2/authorize")
    query = parse_qs(url.query, strict_parsing=True)
    _require(query["client_id"] == [config["client_id"]])
    _require(query["redirect_uri"] == [config["redirect_uri"]])
    _require(query["response_type"] == ["code"] and query["code_challenge_method"] == ["S256"])
    _require(query["code_challenge"] == [challenge_of(grant["verifier"])])
    _require(query["state"] == [grant["callback_state"]] and bool(grant["callback_state"]))
    expected = None if name == "without_resource" else [API_ORIGIN]
    _require(query.get("resource") == expected)
    return {
        "grant_type": "authorization_code",
        "client_id": config["client_id"],
        "redirect_uri": config["redirect_uri"],
        "code": grant["code"],
        "code_verifier": grant["verifier"],
    }


def _exchange(http: httpx.Client, config: dict[str, Any], name: str) -> httpx.Response:
    form = _grant(config, name)
    if name == "wrong_verifier":
        _require(form["code_verifier"] != "x" * 64)
        form["code_verifier"] = "x" * 64
    return http.post(config["domain"] + "/oauth2/token", data=form)


def check_refresh(http: httpx.Client, config: dict[str, Any], access: dict[str, str]) -> bool:
    """Returns: refresh verificado criptograficamente com audience e subject preservados."""
    response = http.post(
        config["domain"] + "/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "client_id": config["client_id"],
            "refresh_token": access["refresh_token"],
        },
    )
    _require(response.status_code == 200)
    token = response.json()["access_token"]
    verifier = OidcVerifier(config["issuer"], API_ORIGIN, http)
    original, refreshed = verifier.verify(access["access_token"]), verifier.verify(token)
    _require(original.subject == refreshed.subject)
    _require(aud_label(claims_of(token)) == API_ORIGIN and scope_has_api(claims_of(token)))
    return True


class ProductionSmoke:
    def __init__(self, config: dict[str, Any], http: httpx.Client, session: Any) -> None:
        self.config, self.http, self.session = config, http, session
        self.access: dict[str, str] = {}

    def login(self) -> bool:
        cfg = self.config["cognito"]
        _https(cfg["domain"])
        _require(
            re.fullmatch(r"https://[a-z0-9-]+\.auth\.[a-z0-9-]+\.amazoncognito\.com", cfg["domain"])
            is not None
        )
        _https(cfg["issuer"])
        _require(cfg["redirect_uri"] == WEB + "/auth/callback")
        _require(cfg["logout_uri"] == WEB)
        discovery = self.http.get(cfg["issuer"] + "/.well-known/openid-configuration")
        _require(discovery.status_code == 200)
        doc = discovery.json()
        _require(doc["issuer"] == cfg["issuer"])
        _require(doc["authorization_endpoint"] == cfg["domain"] + "/oauth2/authorize")
        _require(doc["token_endpoint"] == cfg["domain"] + "/oauth2/token")
        negative = _exchange(self.http, cfg, "wrong_verifier")
        _require(negative.status_code == 400 and negative.json().get("error") == "invalid_grant")
        absent = _exchange(self.http, cfg, "without_resource")
        _require(absent.status_code == 200)
        _require(aud_label(claims_of(absent.json()["access_token"])) == "absent")
        response = _exchange(self.http, cfg, "primary")
        _require(response.status_code == 200)
        access = response.json()
        principal = OidcVerifier(cfg["issuer"], API_ORIGIN, self.http).verify(
            access["access_token"]
        )
        _require(principal.subject == self.config["demo_user"])
        _require(scope_has_api(claims_of(access["access_token"])))
        logout = self.http.get(
            cfg["domain"] + "/logout",
            params={"client_id": cfg["client_id"], "logout_uri": cfg["logout_uri"]},
        )
        _require(logout.status_code == 302 and logout.headers["location"] == cfg["logout_uri"])
        self.access = access
        return True

    def refresh(self) -> bool:
        return check_refresh(self.http, self.config["cognito"], self.access)

    def _headers(self, tenant: str | None = None) -> dict[str, str]:
        headers = {"Authorization": "Bearer " + self.access["access_token"]}
        if tenant is not None:
            headers["X-Tenant-Id"] = tenant
        return headers

    def memberships(self) -> bool:
        me = self.http.get(API_ORIGIN + "/api/v1/dashboard/auth/me", headers=self._headers())
        _require(me.status_code == 200 and me.json()["subject"] == self.config["demo_user"])
        response = self.http.get(
            API_ORIGIN + "/api/v1/dashboard/memberships", headers=self._headers()
        )
        _require(response.status_code == 200)
        _require([x["tenant_id"] for x in response.json()] == [self.config["demo_tenant"]])
        _require(self.config["other_tenant"] != self.config["demo_tenant"])
        denied = self.http.get(
            API_ORIGIN + SERVING, headers=self._headers(self.config["other_tenant"])
        )
        _require(denied.status_code == 403)
        anonymous = self.http.get(API_ORIGIN + SERVING)
        _require(anonymous.status_code == 401)
        return True

    def cors(self) -> bool:
        cases = [
            (WEB, "GET", "authorization,x-tenant-id", True),
            ("https://invalid.example", "GET", "authorization", False),
            (WEB, "TRACE", "authorization", False),
            (WEB, "GET", "x-forbidden", False),
        ]
        for origin, method, headers, allowed in cases:
            response = self.http.options(
                API_ORIGIN + SERVING,
                headers={
                    "Origin": origin,
                    "Access-Control-Request-Method": method,
                    "Access-Control-Request-Headers": headers,
                },
            )
            if allowed:
                _require(response.status_code == 200)
                _require(response.headers.get("access-control-allow-origin") == WEB)
            else:
                _require(response.status_code == 400)
        return True

    def serving(self) -> bool:
        cfg = self.config["serving"]
        response = self.http.get(
            API_ORIGIN + SERVING, headers=self._headers(self.config["demo_tenant"])
        )
        _require(response.status_code == 200 and "location" not in response.headers)
        _require(response.headers["cache-control"] == "private, no-store")
        envelope = response.json()
        _require(set(envelope) == {"url", "version_id", "expires_in"})
        _require(envelope["version_id"] == cfg["version_id"])
        _require(response.headers["x-dataset-version"] == cfg["version_id"])
        _require(type(envelope["expires_in"]) is int and 0 < envelope["expires_in"] <= 900)
        _https(envelope["url"], cfg["bucket"] + ".s3.us-east-2.amazonaws.com")
        url = urlsplit(envelope["url"])
        _require(
            unquote(url.path)
            == f"/serving/{self.config['demo_tenant']}/{cfg['version_id']}/overview.json"
        )
        query = parse_qs(url.query)
        _require(query.get("X-Amz-Algorithm") == ["AWS4-HMAC-SHA256"])
        _require(query.get("X-Amz-SignedHeaders") == ["host"])
        _require(int(query["X-Amz-Expires"][0]) == envelope["expires_in"])
        request = httpx.Request("GET", envelope["url"])
        body = self.http.send(request, follow_redirects=False)
        _require(body.status_code == 200 and "location" not in body.headers)
        _require(sha256(body.content).hexdigest() == cfg["body_sha256"])
        _require(body.json()["tenant_id"] == self.config["demo_tenant"])
        _require(body.json()["run_id"] == cfg["version_id"])
        return True

    def sfn(self) -> bool:
        cfg = self.config["sfn"]
        iam, sfn = (
            self.session.client("iam", config=NO_RETRY),
            self.session.client("stepfunctions", config=NO_RETRY),
        )
        machine = sfn.describe_state_machine(stateMachineArn=cfg["state_machine_arn"])
        _require(machine["roleArn"] == cfg["role_arn"])
        _require(json.loads(machine["definition"]) == cfg["definition"])
        role = iam.get_role(RoleName=cfg["role_arn"].split("/")[-1])["Role"]
        expected = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "sts:AssumeRole",
                    "Principal": {"Service": "states.amazonaws.com"},
                    "Condition": {
                        "StringEquals": {
                            "aws:SourceAccount": cfg["role_arn"].split(":")[4],
                            "aws:SourceArn": cfg["state_machine_arn"],
                        }
                    },
                }
            ],
        }
        _require(role["Arn"] == cfg["role_arn"] and role["AssumeRolePolicyDocument"] == expected)
        _role_policies(iam, cfg)
        execution = sfn.describe_execution(executionArn=cfg["execution_arn"])
        _require(execution["stateMachineArn"] == cfg["state_machine_arn"])
        _require(execution["status"] == "SUCCEEDED")
        _require(timedelta(0) <= datetime.now(UTC) - execution["stopDate"] <= timedelta(hours=1))
        history = sfn.get_execution_history(executionArn=cfg["execution_arn"], maxResults=1000)
        _require(not history.get("nextToken"))
        _map_tasks(history, cfg, execution)
        return True

    def object_lock(self) -> bool:
        cfg = self.config["object_lock"]
        s3 = self.session.client("s3", config=NO_RETRY)
        audit = s3.get_object_lock_configuration(Bucket=cfg["audit_bucket"])[
            "ObjectLockConfiguration"
        ]
        _require(
            audit
            == {
                "ObjectLockEnabled": "Enabled",
                "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 365}},
            }
        )
        _require(cfg["approved_synthetic_fixture"] is True)
        _require(cfg["bucket"].startswith("cnesdata-cap-") and cfg["bucket"] != cfg["audit_bucket"])
        args = {"Bucket": cfg["bucket"], "Key": cfg["key"], "VersionId": cfg["version_id"]}
        retention = s3.get_object_retention(**args)["Retention"]
        _require(retention["Mode"] == "COMPLIANCE")
        _require(retention["RetainUntilDate"] > datetime.now(UTC) + timedelta(minutes=10))
        _denied(lambda: s3.delete_object(**args, BypassGovernanceRetention=True))
        _denied(
            lambda: s3.put_object_retention(
                **args,
                BypassGovernanceRetention=True,
                Retention={
                    "Mode": "COMPLIANCE",
                    "RetainUntilDate": datetime.now(UTC) + timedelta(minutes=1),
                },
            )
        )
        _require(s3.get_object_retention(**args)["Retention"] == retention)
        return True

    def roles_anywhere(self) -> bool:
        cfg = self.config["roles_anywhere"]
        ra = self.session.client("rolesanywhere", config=NO_RETRY)
        crl = ra.get_crl(crlId=cfg["crl_id"])["crl"]
        _require(crl["enabled"] is True and crl["trustAnchorArn"] == cfg["trust_anchor_arn"])
        _require(sha256(crl["crlData"]).hexdigest() == cfg["crl_sha256"])
        parsed = x509.load_der_x509_crl(crl["crlData"])
        _require(parsed.next_update_utc is not None and parsed.next_update_utc > datetime.now(UTC))
        _require(
            parsed.get_revoked_certificate_by_serial_number(int(cfg["revoked_serial"], 16))
            is not None
        )
        valid = _ra_session(cfg, "valid").client("sts", config=NO_RETRY)
        _require(valid.get_caller_identity()["Arn"] == cfg["expected_session_arn"])
        try:
            _ra_session(cfg, "revoked").client("sts", config=NO_RETRY).get_caller_identity()
        except CredentialRetrievalError as error:
            _require("AccessDenied" in str(error))
        else:
            raise ValueError("reason=revoked_certificate_accepted")
        return True

    def probes(self) -> dict[str, Callable[[], object]]:
        """Returns: mapa fechado de operações concretas."""
        return dict(
            zip(
                ASSERTIONS,
                (
                    self.login,
                    self.refresh,
                    self.memberships,
                    self.cors,
                    self.sfn,
                    self.serving,
                    self.object_lock,
                    self.roles_anywhere,
                ),
                strict=True,
            )
        )


def _unit_parameters(cfg: dict[str, Any], unit: str) -> dict[str, Any]:
    parameters = deepcopy(
        cfg["definition"]["States"]["RunUnits"]["ItemProcessor"]["States"]["RunProcessor"][
            "Parameters"
        ]
    )
    _require(parameters["TaskDefinition"] == cfg["task_definition_arn"])
    context = {
        "$." + key: cfg["execution_input"][key]
        for key in ("tenant_id", "run_id", "wave_id", "dispatch_id")
    }
    _require(all(isinstance(value, str) and bool(value) for value in context.values()))
    context |= {"$.unit_id": unit, "$$.Execution.Id": cfg["execution_arn"]}
    for variable in parameters["Overrides"]["ContainerOverrides"][0]["Environment"]:
        if "Value.$" in variable:
            variable["Value"] = context[variable.pop("Value.$")]
    return parameters


def _map_tasks(history: dict[str, Any], cfg: dict[str, Any], execution: dict[str, Any]) -> None:
    _require(json.loads(execution["input"]) == cfg["execution_input"])
    units = cfg["execution_input"]["unit_ids"]
    _require(isinstance(units, list) and bool(units))
    _require(all(isinstance(unit, str) and bool(unit) for unit in units))
    _require(len(set(units)) == len(units))
    seen = set()
    for event in history["events"]:
        if event["type"] != "TaskScheduled":
            continue
        detail = event["taskScheduledEventDetails"]
        _require(detail["resourceType"] == "ecs" and detail["resource"] == "runTask.sync")
        params = json.loads(detail["parameters"])
        environment = params["Overrides"]["ContainerOverrides"][0]["Environment"]
        unit = [entry["Value"] for entry in environment if entry["Name"] == "UNIT_ID"]
        _require(len(unit) == 1 and unit[0] in units)
        _require(params == _unit_parameters(cfg, unit[0]))
        seen.add(unit[0])
    _require(seen == set(units))


def _role_policies(iam: Any, cfg: dict[str, Any]) -> None:
    role = cfg["role_arn"].split("/")[-1]
    inline = iam.list_role_policies(RoleName=role)
    attached = iam.list_attached_role_policies(RoleName=role)
    _require(not inline.get("IsTruncated") and not attached.get("IsTruncated"))
    _require(attached["AttachedPolicies"] == [])
    _require(set(inline["PolicyNames"]) == set(cfg["role_policies"]))
    _require(bool(cfg["role_policies"]))
    for name, expected in cfg["role_policies"].items():
        _require(iam.get_role_policy(RoleName=role, PolicyName=name)["PolicyDocument"] == expected)


def _ra_session(cfg: dict[str, Any], kind: str) -> Any:
    parser = configparser.ConfigParser(interpolation=None)
    _require(parser.read(cfg["aws_config"]) == [cfg["aws_config"]])
    section = dict(parser["profile " + cfg[kind + "_profile"]])
    _require(set(section) == {"credential_process", "region"})
    _require(section["region"] == "us-east-2")
    expected = [
        cfg["helper"],
        "credential-process",
        "--certificate",
        cfg[kind + "_certificate"],
        "--private-key",
        cfg[kind + "_key"],
        "--trust-anchor-arn",
        cfg["trust_anchor_arn"],
        "--profile-arn",
        cfg["profile_arn"],
        "--role-arn",
        cfg["role_arn"],
    ]
    _require(shlex.split(section["credential_process"]) == expected)
    cert = x509.load_pem_x509_certificate(Path(cfg[kind + "_certificate"]).read_bytes())
    revoked = int(cfg["revoked_serial"], 16)
    _require((cert.serial_number == revoked) == (kind == "revoked"))
    _require(cert.not_valid_before_utc <= datetime.now(UTC) < cert.not_valid_after_utc)
    import os

    _require(os.environ.get("AWS_CONFIG_FILE") == cfg["aws_config"])
    _require(os.environ.get("AWS_SHARED_CREDENTIALS_FILE") == os.devnull)
    return boto3.Session(profile_name=cfg[kind + "_profile"])


def _denied(action: Callable[[], Any]) -> None:
    try:
        action()
    except ClientError as error:
        _require(error.response.get("Error", {}).get("Code") == "AccessDenied")
    else:
        raise ValueError("reason=negative_probe_succeeded")


def main() -> int:
    """Returns: status; stdout contém apenas a evidência redigida."""
    logging.disable(logging.CRITICAL)
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--release-hash", required=True)
    args = parser.parse_args()
    if re.fullmatch(r"[a-f0-9]{64}", args.release_hash) is None:
        return 1
    try:
        config = json.loads(args.inputs.read_text())
        with httpx.Client(timeout=20, follow_redirects=False, trust_env=False) as http:
            session = boto3.Session(region_name="us-east-2")
            probes = ProductionSmoke(config, http, session).probes()
            rows, ok = run_smoke(args.release_hash, probes)
    except Exception:
        rows, ok = run_smoke(args.release_hash, {})
    sys.stdout.write(json.dumps(rows, separators=(",", ":")) + "\n")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
