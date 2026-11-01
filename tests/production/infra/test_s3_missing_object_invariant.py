"""Invariante #441: GetObject de dados coberto pelo ListBucket condicionado."""

import json

import pytest

from tests.production.infra._step_functions_iam_contracts import (
    Grants,
    Node,
    literal_grants,
    policy_grants,
    resource_at,
)
from tests.production.infra.test_step_functions_iam import ROOT_RUN, NativePlans

DATA_ADDRESS = "module.data_buckets.aws_s3_bucket.data"
POLICY_ADDRESSES = {
    "unit": 'module.runtime_iam.aws_iam_role_policy.runtime["unit"]',
    "recovery": 'module.runtime_iam.aws_iam_role_policy.runtime["recovery"]',
    "api": "module.runtime_iam.aws_iam_role_policy.api[0]",
}
DATA = "arn:aws:s3:::fixture-data"
AUDIT = "arn:aws:s3:::fixture-audit"
PREFIX_KEY = "s3:prefix"


def _get_prefixes(grants: Grants, data_arn: str) -> tuple[str, ...]:
    head = f"{data_arn}/"
    return tuple(sorted(
        resource.removeprefix(head)
        for action, resource, _ in grants
        if action == "s3:GetObject" and resource.startswith(head)
    ))


def _list_patterns(grants: Grants, data_arn: str) -> tuple[str, ...]:
    patterns: list[str] = []
    for action, resource, encoded in grants:
        if action != "s3:ListBucket" or resource != data_arn:
            continue
        condition = json.loads(encoded)
        if set(condition) != {"StringLike"} or set(condition["StringLike"]) != {PREFIX_KEY}:
            continue
        value = condition["StringLike"][PREFIX_KEY]
        patterns.extend([value] if isinstance(value, str) else value)
    return tuple(patterns)


def _validated(pattern: str) -> str:
    body = pattern[:-1] if pattern.endswith("*") else pattern
    if "*" in body or "?" in body:
        raise AssertionError(f"invariant unsupported_pattern={pattern}")
    return pattern


def _covers(pattern: str, prefix: str) -> bool:
    if pattern.endswith("*"):
        return prefix.startswith(pattern[:-1])
    return pattern == prefix


def uncovered_get_prefixes(grants: Grants, data_arn: str) -> tuple[str, ...]:
    """Args: Grants observados e ARN do data bucket.
    Returns: Prefixos de GetObject sem cobertura de ListBucket StringLike s3:prefix.
    Raises: AssertionError para wildcard fora de um unico '*' final.
    """
    patterns = tuple(_validated(item) for item in _list_patterns(grants, data_arn))
    return tuple(
        prefix
        for prefix in _get_prefixes(grants, data_arn)
        if not any(_covers(pattern, prefix) for pattern in patterns)
    )


def _list(bucket: str, values: list[str], operator: str = "StringLike") -> Grants:
    return literal_grants(("s3:ListBucket",), (bucket,), {operator: {PREFIX_KEY: values}})


def _get(bucket: str, *prefixes: str) -> Grants:
    return literal_grants(
        ("s3:GetObject",), tuple(f"{bucket}/{item}" for item in prefixes), {}
    )


@pytest.mark.parametrize("mode", ["unit", "recovery", "api"])
def test_get_object_de_cada_role_fica_coberto_pelo_list_bucket_condicionado(
    processing_native: NativePlans, mode: str
) -> None:
    plan = processing_native.plans[ROOT_RUN]
    data_arn = resource_at(plan, DATA_ADDRESS)["change"]["after"]["arn"]
    policy = resource_at(plan, POLICY_ADDRESSES[mode])["change"]["after"]["policy"]
    grants = policy_grants(policy)

    assert _get_prefixes(grants, data_arn), f"invariant role={mode} get_prefixes=empty"
    uncovered = uncovered_get_prefixes(grants, data_arn)
    assert uncovered == (), f"invariant role={mode} uncovered={uncovered}"


def test_oraculo_aceita_cobertura_exata() -> None:
    grants = _get(DATA, "normalized/*", "tmp/*") | _list(DATA, ["normalized/*", "tmp/*"])
    assert uncovered_get_prefixes(grants, DATA) == ()


def test_oraculo_aceita_get_mais_estreito_coberto_por_wildcard_final() -> None:
    grants = _get(DATA, "normalized/sub/*") | _list(DATA, ["normalized/*"])
    assert uncovered_get_prefixes(grants, DATA) == ()


def test_oraculo_aceita_valor_unico_como_string() -> None:
    document = json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"{DATA}/serving/*"},
        {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": DATA,
         "Condition": {"StringLike": {PREFIX_KEY: "serving/*"}}},
    ]})
    assert uncovered_get_prefixes(policy_grants(document), DATA) == ()


def test_oraculo_reporta_get_sem_list_bucket() -> None:
    grants = _get(DATA, "normalized/*", "extra/*") | _list(DATA, ["normalized/*"])
    assert uncovered_get_prefixes(grants, DATA) == ("extra/*",)


@pytest.mark.parametrize(
    ("listed", "read"),
    [("serving/*", "serv/*"), ("serv/*", "serving/*"), ("serving/*", "serving*")],
)
def test_oraculo_nao_trata_prefixos_irmaos_como_cobertos(listed: str, read: str) -> None:
    grants = _get(DATA, read) | _list(DATA, [listed])
    assert uncovered_get_prefixes(grants, DATA) == (read,)


def test_oraculo_nao_conta_operador_diferente_de_string_like() -> None:
    grants = _get(DATA, "tmp/*") | _list(DATA, ["tmp/*"], "StringLikeIfExists")
    assert uncovered_get_prefixes(grants, DATA) == ("tmp/*",)


def test_oraculo_nao_conta_condicao_com_chave_extra() -> None:
    condition: Node = {"StringLike": {PREFIX_KEY: ["tmp/*"], "s3:delimiter": ["/"]}}
    grants = _get(DATA, "tmp/*") | literal_grants(("s3:ListBucket",), (DATA,), condition)
    assert uncovered_get_prefixes(grants, DATA) == ("tmp/*",)


def test_oraculo_nao_conta_list_bucket_de_outro_bucket() -> None:
    grants = _get(DATA, "tmp/*") | _list(AUDIT, ["tmp/*"])
    assert uncovered_get_prefixes(grants, DATA) == ("tmp/*",)


def test_oraculo_nao_conta_list_bucket_sem_condicao() -> None:
    grants = _get(DATA, "tmp/*") | literal_grants(("s3:ListBucket",), (DATA,), {})
    assert uncovered_get_prefixes(grants, DATA) == ("tmp/*",)


def test_oraculo_ignora_get_object_de_outro_bucket() -> None:
    grants = _get(AUDIT, "audit/*") | _get(DATA, "tmp/*") | _list(DATA, ["tmp/*"])
    assert uncovered_get_prefixes(grants, DATA) == ()


@pytest.mark.parametrize("pattern", ["t*p/*", "tmp/?", "tmp/**", "tm?/*"])
def test_oraculo_falha_fechado_com_wildcard_nao_suportado(pattern: str) -> None:
    grants = _get(DATA, "tmp/*") | _list(DATA, [pattern])
    with pytest.raises(AssertionError, match="invariant unsupported_pattern="):
        uncovered_get_prefixes(grants, DATA)
