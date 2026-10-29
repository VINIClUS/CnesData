"""Remoção preserva snapshot fresco e exige ausência de filhos e proteção INACTIVE."""

from dataclasses import replace
from typing import cast

import pytest

from tests.integration.aws_capability._service_cognito_root_testing import (
    ROOT,
    RootLab,
    assert_private_channels,
    assert_trace,
    queue_trace,
    root_lab,
)
from tests.integration.aws_capability._service_read_assertions import assert_rejected
from tests.integration.aws_capability.contracts import RequestContext, ServiceFailure

__all__ = ["root_lab"]

EMPTY_CHILDREN = [
    ["D", "pool_active"],
    ["T", "tags_owner"],
    ["D", "pool_active"],
    ["D", "pool_active"],
    ["T", "tags_owner"],
    ["LC", "clients_empty"],
    ["D", "pool_active"],
    ["T", "tags_owner"],
    ["LS", "servers_empty"],
    ["D", "pool_active"],
    ["T", "tags_owner"],
    ["LU", "users_empty"],
]


@pytest.mark.parametrize(
    ("snapshot", "update", "readback"),
    [
        ("pool_active", "U", "pool_inactive"),
        ("pool_fresh_active", "U2", "pool_fresh_inactive"),
        ("pool_safe_active", "U_SAFE", "pool_safe_inactive"),
    ],
    ids=["padrao", "fresco", "extras_seguros"],
)
def test_preserva_snapshot_fresco(
    root_lab: RootLab, snapshot: str, update: str, readback: str
) -> None:
    tags = "tags_extra" if snapshot == "pool_safe_active" else "tags_owner"
    steps = EMPTY_CHILDREN + [
        ["D", snapshot],
        ["T", tags],
        [update, "ok_empty"],
        ["D", readback],
        ["T", tags],
        ["X", "ok_empty"],
    ]
    queue_trace(root_lab, steps)
    context = RequestContext(root_lab.context.budget, "cleanup")
    try:
        result = root_lab.services.delete(ROOT, True, context)
    except ServiceFailure as error:
        assert_private_channels(root_lab, error)
        assert_rejected(error)
        raise
    assert result is None
    assert_trace(root_lab, steps, ["cleanup"] * 18)
    assert root_lab.context.budget.work_used == 0


def test_ja_inactive_nao_repete_update(root_lab: RootLab) -> None:
    steps = EMPTY_CHILDREN + [["D", "pool_inactive"], ["T", "tags_owner"], ["X", "ok_empty"]]
    queue_trace(root_lab, steps)
    context = RequestContext(root_lab.context.budget, "cleanup")
    try:
        result = root_lab.services.delete(ROOT, True, context)
    except ServiceFailure as error:
        assert_private_channels(root_lab, error)
        assert_rejected(error)
        raise
    assert result is None
    assert_trace(root_lab, steps, ["cleanup"] * 15)


@pytest.mark.parametrize(
    "case", ["antes_update", "antes_delete"], ids=["antes_update", "antes_delete"]
)
def test_owner_mudou_antes_mutacao(root_lab: RootLab, case: str) -> None:
    steps = EMPTY_CHILDREN + [["D", "pool_active"], ["T", "tags_run"]]
    if case == "antes_delete":
        steps = EMPTY_CHILDREN + [
            ["D", "pool_active"],
            ["T", "tags_owner"],
            ["U", "ok_empty"],
            ["D", "pool_inactive"],
            ["T", "tags_run"],
        ]
    queue_trace(root_lab, steps)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.delete(ROOT, True, RequestContext(root_lab.context.budget, "cleanup"))
    assert_trace(root_lab, steps, ["cleanup"] * (14 if case == "antes_update" else 17))
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)
    assert error.value.args != ("probe=not_implemented",)


@pytest.mark.parametrize(
    ("readback", "tags"),
    [
        ("pool_active", True),
        ("pool_fresh_inactive", True),
        ("error_rnf", False),
        ("error_denied", False),
    ],
    ids=["ainda_active", "policy_alterada", "rnf", "denied"],
)
def test_update_nao_prova_protecao(root_lab: RootLab, readback: str, tags: bool) -> None:
    steps = EMPTY_CHILDREN + [
        ["D", "pool_active"],
        ["T", "tags_owner"],
        ["U", "ok_empty"],
        ["D", readback],
    ]
    if tags:
        steps += [["T", "tags_owner"]]
    queue_trace(root_lab, steps)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.delete(ROOT, True, RequestContext(root_lab.context.budget, "cleanup"))
    assert_trace(root_lab, steps, ["cleanup"] * (17 if tags else 16))
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)


@pytest.mark.parametrize(
    "snapshot",
    [
        "pool_unknown",
        "pool_policy_missing",
        "pool_sms",
        "pool_trigger",
        "pool_plus",
        "pool_kms",
        "pool_issuer_updated",
        "pool_acr",
    ],
    ids=[
        "desconhecido",
        "policy_ausente",
        "sms",
        "trigger",
        "plus",
        "kms",
        "issuer_updated",
        "acr",
    ],
)
def test_snapshot_inseguro_falha_fechado(root_lab: RootLab, snapshot: str) -> None:
    steps = EMPTY_CHILDREN + [["D", snapshot], ["T", "tags_owner"]]
    queue_trace(root_lab, steps)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.delete(ROOT, True, RequestContext(root_lab.context.budget, "cleanup"))
    assert_trace(root_lab, steps, ["cleanup"] * 14)
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)


@pytest.mark.parametrize(
    ("index", "step"),
    [
        (2, ["D", "pool_domain"]),
        (5, ["LC", "clients_present"]),
        (8, ["LS", "servers_present"]),
        (11, ["LU", "users_present"]),
    ],
    ids=["domain", "client", "server", "user"],
)
def test_filhos_impedem_root(root_lab: RootLab, index: int, step: list[str]) -> None:
    steps = [
        ["D", "pool_active"],
        ["T", "tags_owner"],
        ["D", "pool_active"],
        ["D", "pool_active"],
        ["T", "tags_owner"],
        ["LC", "clients_empty"],
        ["D", "pool_active"],
        ["T", "tags_owner"],
        ["LS", "servers_empty"],
        ["D", "pool_active"],
        ["T", "tags_owner"],
        ["LU", "users_empty"],
    ]
    steps[index] = step
    queue_trace(root_lab, steps)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.delete(ROOT, True, RequestContext(root_lab.context.budget, "cleanup"))
    assert_trace(root_lab, steps, ["cleanup"] * 12)
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)


@pytest.mark.parametrize(
    "reply", ["error_denied", "clients_page1"], ids=["pagina2_denied", "cursor_repetido"]
)
def test_lista_filhos_incompleta(root_lab: RootLab, reply: str) -> None:
    steps = [
        ["D", "pool_active"],
        ["T", "tags_owner"],
        ["D", "pool_active"],
        ["D", "pool_active"],
        ["T", "tags_owner"],
        ["LC", "clients_page1"],
        ["D", "pool_active"],
        ["T", "tags_owner"],
        ["LC2", reply],
    ]
    queue_trace(root_lab, steps)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.delete(ROOT, True, RequestContext(root_lab.context.budget, "cleanup"))
    assert_trace(root_lab, steps, ["cleanup"] * 9)
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)


@pytest.mark.parametrize(
    "case", ["lane", "nome", "child", "bypass_tipo"], ids=["lane", "nome", "child", "bypass_tipo"]
)
def test_rejeita_entrada_de_delete(root_lab: RootLab, case: str) -> None:
    resource, bypass = ROOT, True
    context = RequestContext(root_lab.context.budget, "cleanup")
    if case == "lane":
        context = root_lab.context
    elif case == "nome":
        resource = replace(ROOT, name="cnesdata-cap-a1b2c3d4-suffix")
    elif case == "child":
        resource = replace(
            ROOT,
            resource_id=(
                '{"child_id":{"username":"synthetic-user"},"kind":"cognito.user",'
                '"parent_id":"us-east-2_PoolRoot123","root_id":"us-east-2_PoolRoot123",'
                '"run":"a1b2c3d4","service":"cognito","v":1}'
            ),
        )
    else:
        bypass = cast("bool", 1)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.delete(resource, bypass, context)
    assert_trace(root_lab, [], [])
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)


def test_update_com_objeto_inesperado_falha(root_lab: RootLab) -> None:
    steps = EMPTY_CHILDREN + [["D", "pool_active"], ["T", "tags_owner"], ["U", "ok_unexpected"]]
    queue_trace(root_lab, steps)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.delete(ROOT, True, RequestContext(root_lab.context.budget, "cleanup"))
    assert_trace(root_lab, steps, ["cleanup"] * 15)
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)
