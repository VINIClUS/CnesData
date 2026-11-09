"""Fixtures HTTP/JWT/ASL reais para os probes de smoke."""

import json
from base64 import urlsafe_b64encode
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock
from urllib.parse import parse_qs, urlencode

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jose import jwt

from tests.integration.aws_capability._login_pkce import challenge_of


def jwt_materials():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = key.public_key().public_numbers()

    def number(value):
        return (
            urlsafe_b64encode(value.to_bytes((value.bit_length() + 7) // 8, "big"))
            .rstrip(b"=")
            .decode()
        )

    jwk = {
        "kty": "RSA",
        "kid": "demo",
        "alg": "RS256",
        "use": "sig",
        "n": number(public.n),
        "e": number(public.e),
    }
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    return pem, jwk


def _sfn_trust(cfg):
    return {
        "Role": {
            "Arn": cfg["role_arn"],
            "AssumeRolePolicyDocument": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "sts:AssumeRole",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Condition": {
                            "StringEquals": {
                                "aws:SourceAccount": "123456789012",
                                "aws:SourceArn": cfg["state_machine_arn"],
                            }
                        },
                    }
                ],
            },
        }
    }


def _definition(task):
    root = Path(__file__).resolve().parents[3]
    template = (root / "infra/opentofu/modules/processing/state_machine.asl.json").read_text()
    bindings = {
        "cluster_arn": "synthetic-cluster",
        "task_definition_arn": task,
        "public_subnet_ids": '["synthetic-subnet"]',
        "security_group_id": "synthetic-sg",
    }
    for key, value in bindings.items():
        template = template.replace("${" + key + "}", value)
    return json.loads(template)


def _scheduled(cfg, unit):
    parameters = deepcopy(
        cfg["definition"]["States"]["RunUnits"]["ItemProcessor"]["States"]["RunProcessor"][
            "Parameters"
        ]
    )
    context = {"$." + key: value for key, value in cfg["execution_input"].items()}
    context |= {"$.unit_id": unit, "$$.Execution.Id": cfg["execution_arn"]}
    for variable in parameters["Overrides"]["ContainerOverrides"][0]["Environment"]:
        if "Value.$" in variable:
            variable["Value"] = context[variable.pop("Value.$")]
    return {
        "type": "TaskScheduled",
        "taskScheduledEventDetails": {
            "resourceType": "ecs",
            "resource": "runTask.sync",
            "parameters": json.dumps(parameters),
        },
    }


def sfn_fixture():
    cfg = {
        "state_machine_arn": "arn:aws:states:us-east-2:123456789012:stateMachine:synthetic",
        "role_arn": "arn:aws:iam::123456789012:role/step-functions",
        "execution_arn": "arn:aws:states:us-east-2:123456789012:execution:synthetic:demo",
        "task_definition_arn": "arn:aws:ecs:us-east-2:123456789012:task-definition/demo:1",
        "role_policies": {"unit": {"Statement": []}},
        "execution_input": {
            "tenant_id": "demo",
            "run_id": "run",
            "wave_id": "wave",
            "dispatch_id": "dispatch",
            "unit_ids": ["unit-one", "unit-two"],
            "max_concurrency": 1,
        },
    }
    cfg["definition"] = _definition(cfg["task_definition_arn"])
    iam, sfn, session = Mock(), Mock(), Mock()
    session.client.side_effect = lambda name, **kw: iam if name == "iam" else sfn
    iam.get_role.return_value = _sfn_trust(cfg)
    iam.list_role_policies.return_value = {"PolicyNames": ["unit"], "IsTruncated": False}
    iam.list_attached_role_policies.return_value = {"AttachedPolicies": [], "IsTruncated": False}
    iam.get_role_policy.return_value = {"PolicyDocument": {"Statement": []}}
    sfn.describe_state_machine.return_value = {
        "roleArn": cfg["role_arn"],
        "definition": json.dumps(cfg["definition"]),
    }
    sfn.describe_execution.return_value = {
        "stateMachineArn": cfg["state_machine_arn"],
        "status": "SUCCEEDED",
        "stopDate": datetime.now(UTC),
        "input": json.dumps(cfg["execution_input"]),
    }
    sfn.get_execution_history.return_value = {
        "events": [_scheduled(cfg, unit) for unit in cfg["execution_input"]["unit_ids"]]
    }
    return cfg, session, iam, sfn


def _grants(cfg):
    for name in ("primary", "without_resource", "wrong_verifier"):
        verifier = name.replace("_", "a") + "a" * 64
        query = {
            "client_id": cfg["client_id"],
            "redirect_uri": cfg["redirect_uri"],
            "response_type": "code",
            "code_challenge_method": "S256",
            "code_challenge": challenge_of(verifier),
            "state": "synthetic-state",
        }
        if name != "without_resource":
            query["resource"] = "https://api.cnesdata.com.br"
        cfg[name] = {
            "authorize_url": cfg["domain"] + "/oauth2/authorize?" + urlencode(query),
            "verifier": verifier,
            "callback_state": "synthetic-state",
            "code": name,
        }


def _login_tokens(cfg, pem):
    now = int(datetime.now(UTC).timestamp())
    claims = {
        "iss": cfg["issuer"],
        "aud": "https://api.cnesdata.com.br",
        "sub": "synthetic-user",
        "iat": now,
        "exp": now + 600,
        "scope": "https://api.cnesdata.com.br/api.access",
    }
    signed = jwt.encode(claims, pem, algorithm="RS256", headers={"kid": "demo"})
    del claims["aud"]
    absent = jwt.encode(claims, pem, algorithm="RS256", headers={"kid": "demo"})
    return signed, absent


def _token_response(request, cfg, tokens):
    form = parse_qs(request.content.decode())
    if form["grant_type"] == ["refresh_token"]:
        return httpx.Response(200, json={"access_token": tokens[0]})
    code = form["code"][0]
    if code == "wrong_verifier":
        assert form["code_verifier"] == ["x" * 64]
        return httpx.Response(400, json={"error": "invalid_grant"})
    assert form["code_verifier"] == [cfg[code]["verifier"]]
    return httpx.Response(
        200,
        json={
            "access_token": tokens[1] if code == "without_resource" else tokens[0],
            "refresh_token": "synthetic",
        },
    )


def login_fixture(material):
    pem, jwk = material
    cfg = {
        "domain": "https://synthetic.auth.us-east-2.amazoncognito.com",
        "issuer": "https://cognito-idp.us-east-2.amazonaws.com/synthetic",
        "client_id": "synthetic",
        "redirect_uri": "https://cnesdata.com.br/auth/callback",
        "logout_uri": "https://cnesdata.com.br",
    }
    _grants(cfg)
    tokens = _login_tokens(cfg, pem)
    replies = {"logout": cfg["logout_uri"]}

    def handler(request):
        if request.url.path.endswith("openid-configuration"):
            return httpx.Response(
                200,
                json={
                    "issuer": cfg["issuer"],
                    "jwks_uri": cfg["issuer"] + "/jwks",
                    "authorization_endpoint": cfg["domain"] + "/oauth2/authorize",
                    "token_endpoint": cfg["domain"] + "/oauth2/token",
                },
            )
        if request.url.path.endswith("/jwks"):
            return httpx.Response(200, json={"keys": [jwk]})
        if request.url.path == "/oauth2/token":
            return _token_response(request, cfg, tokens)
        if request.url.path == "/logout":
            assert request.url.params["logout_uri"] == cfg["logout_uri"]
            return httpx.Response(302, headers={"location": replies["logout"]})
        raise AssertionError("reason=unexpected_http_request")

    return {"cognito": cfg, "demo_user": "synthetic-user"}, handler, replies
