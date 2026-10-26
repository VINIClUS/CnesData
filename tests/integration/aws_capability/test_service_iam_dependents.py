"""Descoberta IAM valida paginas inteiras e preserva dependencias externas."""

import json

import pytest

from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._inventory import dependents
from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_roots_testing import ReadLab, respond
from tests.integration.aws_capability._testing import ProcessBoundary
from tests.integration.aws_capability.contracts import (
    Limits,
    Page,
    Policy,
    Ports,
    Resource,
    ServiceFailure,
)
from tests.integration.aws_capability.test_service_iam_children_lookup import (
    CURSORS,
    assert_failure,
    enqueue,
    read_lab,
    remote_error,
    resource,
    trace,
)
from tests.integration.aws_capability.test_service_s3_children_lookup import (
    enqueue as enqueue_s3,
)
from tests.integration.aws_capability.test_service_s3_children_lookup import (
    resource as s3_resource,
)
from tests.integration.aws_capability.test_service_s3_children_lookup import trace as s3_trace

__all__ = ["read_lab"]


def cleanup(lab: ReadLab) -> Cleanup:
    ports = Ports(
        lab.services,
        ProcessBoundary([]),
        lambda: 1000,
        Policy("111111111111", frozenset({"us-east-2"})),
        lab.context.budget,
    )
    return Cleanup(Journal(lab.services.spec), ports)


def test_policies_emitem_somente_envelopes_inline_e_transicao_sem_antecipar(
    read_lab: ReadLab,
) -> None:
    enqueue(read_lab, "root", "tags", "policies")
    page = read_lab.services.dependents(resource("role"), None, read_lab.context)
    assert page == Page((resource("policy"),), CURSORS["cursors"]["attachments_start"])
    assert_process(read_lab, trace("root", "tags", "policies"))


@pytest.mark.parametrize("collection", ["attachments", "profiles"])
def test_dependencia_externa_presente_bloqueia_sem_detach_ou_filho_falso(
    read_lab: ReadLab, collection: str
) -> None:
    enqueue(read_lab, "root", "tags", "attachment" if collection == "attachments" else "profile")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(
            resource("role"), CURSORS["cursors"][collection + "_start"], read_lab.context
        )
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=unexpected_external_dependency",)
    assert_process(read_lab, trace("root", "tags", collection))


@pytest.mark.parametrize(
    ("collection", "reply"),
    [
        ("policies", {}),
        ("policies", {"PolicyNames": None}),
        ("policies", {"PolicyNames": ["Read_Private+=,.@-", None]}),
        ("policies", {"PolicyNames": ["Read_Private+=,.@-", "with space"]}),
        ("attachments", {}),
        ("attachments", {"AttachedPolicies": {}}),
        ("attachments", {"AttachedPolicies": [None]}),
        ("profiles", {}),
        ("profiles", {"InstanceProfiles": "bad"}),
        ("profiles", {"InstanceProfiles": [None]}),
    ],
)
def test_pagina_inteira_malformada_nunca_emite_inline_ou_vazio(
    read_lab: ReadLab, collection: str, reply: object
) -> None:
    enqueue(read_lab, "root", "tags")
    respond(read_lab, reply)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(
            resource("role"), CURSORS["cursors"][collection + "_start"], read_lab.context
        )
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=inventory_invalid",)
    assert_process(read_lab, trace("root", "tags", collection))


def test_tags_paginadas_antes_da_colecao_atual_sao_debitos_explicitos(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags_first", "tags_last", "policies_empty")
    page = read_lab.services.dependents(resource("role"), None, read_lab.context)
    assert page == Page((), CURSORS["cursors"]["attachments_start"])
    assert_process(read_lab, trace("root", "tags", "tags_next", "policies"))


def test_tag_removida_entre_colecoes_bloqueia_antes_de_listar_attachments(
    read_lab: ReadLab,
) -> None:
    enqueue(read_lab, "root", "tags", "policies_empty", "root")
    respond(read_lab, {"Tags": []})
    page = read_lab.services.dependents(resource("role"), None, read_lab.context)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("role"), page.cursor, read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=not_owned",)
    assert_process(read_lab, trace("root", "tags", "policies", "root", "tags"))


def test_erro_remoto_de_colecao_nao_e_fim_vazio(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags")
    remote_error(read_lab, "NoSuchEntity")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("role"), None, read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root", "tags", "policies"))


def test_folha_policy_exige_posse_fresca_antes_de_page_vazia(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags", "policy")
    assert read_lab.services.dependents(resource("policy"), None, read_lab.context) == Page()
    assert_process(read_lab, trace("root", "tags", "policy"))


def test_inventory_dependents_real_percorre_tres_colecoes_vazias(read_lab: ReadLab) -> None:
    enqueue(
        read_lab,
        "root",
        "tags",
        "policies_empty",
        "root",
        "tags",
        "attachments_empty",
        "root",
        "tags",
        "profiles_empty",
    )
    assert dependents(resource("role"), cleanup(read_lab).ports, read_lab.context) == []
    assert_process(
        read_lab,
        trace(
            "root", "tags", "policies", "root", "tags", "attachments", "root", "tags", "profiles"
        ),
    )


def test_removal_order_iam_real_coloca_policy_antes_do_role_em_17_debitos(
    read_lab: ReadLab,
) -> None:
    names = [
        "root",
        "tags",
        "root",
        "tags",
        "policies",
        "root",
        "tags",
        "attachments_empty",
        "root",
        "tags",
        "profiles_empty",
        "root",
        "tags",
        "policy",
        "root",
        "tags",
        "policy",
    ]
    enqueue(read_lab, *names)
    janitor = cleanup(read_lab)
    assert janitor.removal_order(resource("role")) == [resource("policy"), resource("role")]
    assert_process(
        read_lab,
        trace(
            "root",
            "tags",
            "root",
            "tags",
            "policies",
            "root",
            "tags",
            "attachments",
            "root",
            "tags",
            "profiles",
            "root",
            "tags",
            "policy",
            "root",
            "tags",
            "policy",
        ),
    )
    assert read_lab.context.budget.used == 17
    assert read_lab.context.budget.work_used == 0
    assert read_lab.context.budget.limits == Limits()
    assert janitor.journal.created == janitor.journal.baseline == []
    assert janitor.attempted == janitor.absent == set()


def test_removal_order_cognito_real_preserva_ordem_literal_e_29_debitos(read_lab: ReadLab) -> None:
    from tests.integration.aws_capability.test_service_cognito_children_lookup import (
        DATA as COGNITO,
    )
    from tests.integration.aws_capability.test_service_cognito_children_lookup import (
        enqueue as enqueue_cognito,
    )
    from tests.integration.aws_capability.test_service_cognito_children_lookup import (
        resource as cognito_resource,
    )
    from tests.integration.aws_capability.test_service_cognito_children_lookup import (
        trace as cognito_trace,
    )

    enqueue_cognito(read_lab, *COGNITO["expected"]["graph_replies"])
    janitor = cleanup(read_lab)
    assert janitor.removal_order(cognito_resource("pool")) == [
        cognito_resource("branding"),
        cognito_resource("client"),
        cognito_resource("pool"),
    ]
    assert_process(read_lab, cognito_trace(*COGNITO["expected"]["graph_trace"]))
    assert read_lab.context.budget.used == 29
    assert read_lab.context.budget.work_used == 0
    assert read_lab.context.budget.limits == Limits()


def test_removal_order_s3_real_coloca_versao_antes_de_bucket_em_14_debitos(
    read_lab: ReadLab,
) -> None:
    enqueue_s3(
        read_lab,
        "location",
        "tags",
        "location",
        "tags",
        "one",
        "location",
        "tags",
        "empty",
        "location",
        "tags",
        "one",
        "location",
        "tags",
        "one",
    )
    janitor = cleanup(read_lab)
    assert janitor.removal_order(s3_resource("bucket")) == [
        s3_resource("version"),
        s3_resource("bucket"),
    ]
    assert_process(
        read_lab,
        s3_trace(
            "location",
            "tags",
            "location",
            "tags",
            "versions",
            "location",
            "tags",
            "uploads",
            "location",
            "tags",
            "versions",
            "location",
            "tags",
            "versions",
        ),
    )
    assert read_lab.context.budget.used == 14
    assert read_lab.context.budget.work_used == 0
    assert read_lab.context.budget.limits == Limits()


def test_grafo_iam_tag_ausente_bloqueia_antes_de_descoberta(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root")
    respond(read_lab, {"Tags": []})
    with pytest.raises(SafetyFailure) as error:
        cleanup(read_lab).removal_order(resource("role"))
    assert_failure(error.value, read_lab)
    assert error.value.reason == "ownership_mismatch"
    assert_process(read_lab, trace("root", "tags"))


def test_grafo_s3_de_outra_regiao_preserva_bucket_sem_listar_filhos(read_lab: ReadLab) -> None:
    respond(read_lab, {"LocationConstraint": "us-west-1"})
    enqueue_s3(read_lab, "tags")
    with pytest.raises(SafetyFailure) as error:
        cleanup(read_lab).removal_order(s3_resource("bucket"))
    assert_failure(error.value, read_lab)
    assert error.value.reason == "ownership_mismatch"
    assert_process(read_lab, s3_trace("location", "tags"))


def test_grafo_cognito_com_pai_trocado_recusa_envelope_antes_de_io(read_lab: ReadLab) -> None:
    from tests.integration.aws_capability.test_service_cognito_children_lookup import (
        resource as cognito_resource,
    )

    child = cognito_resource("client")
    payload = json.loads(child.resource_id)
    payload["parent_id"] = "us-east-2_other"
    with pytest.raises(ServiceFailure) as error:
        cleanup(read_lab).removal_order(Resource("cognito", json.dumps(payload), child.name))
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=resource_invalid",)
    assert_process(read_lab, [])


def test_ddb_continua_folha_com_inspecao_e_tags_reais(read_lab: ReadLab) -> None:
    from tests.integration.aws_capability._service_read_testing import fixture_data

    root = Resource("dynamodb", "cnesdata-cap-a1b2c3d4", "cnesdata-cap-a1b2c3d4")
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_complete"])
    assert read_lab.services.dependents(root, None, read_lab.context) == Page()
    assert_process(
        read_lab,
        [
            ("dynamodb", "describe-table", "us-east-2", {"TableName": "cnesdata-cap-a1b2c3d4"}),
            (
                "dynamodb",
                "list-tags-of-resource",
                "us-east-2",
                {
                    "ResourceArn": (
                        "arn:aws:dynamodb:us-east-2:111111111111:table/cnesdata-cap-a1b2c3d4"
                    ),
                },
            ),
        ],
    )


@pytest.mark.parametrize("collection", ["attachments", "profiles"])
def test_linha_malformada_apos_dependencia_valida_nao_encurta_validacao(
    read_lab: ReadLab, collection: str
) -> None:
    from tests.integration.aws_capability.test_service_iam_children_lookup import DATA

    key = "AttachedPolicies" if collection == "attachments" else "InstanceProfiles"
    fixture = "attachment" if collection == "attachments" else "profile"
    enqueue(read_lab, "root", "tags")
    respond(read_lab, {key: [*DATA["replies"][fixture][key], None]})
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(
            resource("role"), CURSORS["cursors"][collection + "_start"], read_lab.context
        )
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=inventory_invalid",)
    assert_process(read_lab, trace("root", "tags", collection))


def test_policy_duplicada_entre_paginas_nao_confirma_inventario(read_lab: ReadLab) -> None:
    from tests.integration.aws_capability.test_service_iam_children_lookup import DATA

    enqueue(read_lab, "root", "tags")
    respond(
        read_lab, dict(DATA["replies"]["policies"], IsTruncated=True, Marker="policy-C4-private")
    )
    enqueue(
        read_lab,
        "root",
        "tags",
        "policies",
        "root",
        "tags",
        "attachments_empty",
        "root",
        "tags",
        "profiles_empty",
    )
    with pytest.raises(SafetyFailure) as error:
        dependents(resource("role"), cleanup(read_lab).ports, read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.reason == "inventory_incomplete"
    assert_process(
        read_lab,
        trace(
            "root",
            "tags",
            "policies",
            "root",
            "tags",
            "policies_next",
            "root",
            "tags",
            "attachments",
            "root",
            "tags",
            "profiles",
        ),
    )
