"""Descoberta e travessia S3 preservam filhos, pares e provas frescas."""

import json
from copy import deepcopy

import pytest

from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._inventory import dependents
from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_roots_testing import ReadLab, respond
from tests.integration.aws_capability._testing import ProcessBoundary
from tests.integration.aws_capability.contracts import Limits, Page, Policy, Ports, ServiceFailure
from tests.integration.aws_capability.test_service_s3_children_lookup import (
    CURSORS,
    DATA,
    assert_failure,
    assert_prefix,
    enqueue,
    read_lab,
    resource,
    trace,
)

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


def test_versions_e_markers_saem_juntos_e_upload_so_na_chamada_seguinte(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags", "versions")
    assert read_lab.services.dependents(resource("bucket"), None, read_lab.context) == Page(
        (resource("version"), resource("marker")), CURSORS["cursors"]["uploads_start"]
    )
    assert_process(read_lab, trace("location", "tags", "versions"))
    enqueue(read_lab, "location", "tags", "uploads")
    assert read_lab.services.dependents(
        resource("bucket"), CURSORS["cursors"]["uploads_start"], read_lab.context
    ) == Page((resource("upload"),))
    assert_process(read_lab, trace("location", "tags", "versions", "location", "tags", "uploads"))


def test_ausencia_modelada_de_colecoes_com_flag_terminal_e_vazio_legitimo(
    read_lab: ReadLab,
) -> None:
    enqueue(read_lab, "location", "tags", "empty")
    assert read_lab.services.dependents(resource("bucket"), None, read_lab.context) == Page(
        (), CURSORS["cursors"]["uploads_start"]
    )
    assert_process(read_lab, trace("location", "tags", "versions"))


@pytest.mark.parametrize("collection", ["versions", "uploads"])
def test_nome_opcional_ausente_preserva_request_escopada_e_owner(
    read_lab: ReadLab, collection: str
) -> None:
    enqueue(read_lab, "location", "tags")
    reply = deepcopy(DATA["replies"][collection])
    del reply["Name" if collection == "versions" else "Bucket"]
    respond(read_lab, reply)
    start = None if collection == "versions" else CURSORS["cursors"]["uploads_start"]
    page = read_lab.services.dependents(resource("bucket"), start, read_lab.context)
    assert page.resources == (
        (resource("version"), resource("marker"))
        if collection == "versions"
        else (resource("upload"),)
    )
    assert_process(read_lab, trace("location", "tags", collection))


@pytest.mark.parametrize("collection", ["versions", "uploads"])
def test_nome_retornado_divergente_recusa_pagina(read_lab: ReadLab, collection: str) -> None:
    enqueue(read_lab, "location", "tags")
    reply = deepcopy(DATA["replies"][collection])
    reply["Name" if collection == "versions" else "Bucket"] = "foreign-bucket"
    respond(read_lab, reply)
    start = None if collection == "versions" else CURSORS["cursors"]["uploads_start"]
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("bucket"), start, read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags", collection))


def test_key_unicode_e_ids_com_separadores_preservam_envelope_literal(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags", "special")
    assert read_lab.services.dependents(resource("bucket"), None, read_lab.context) == Page(
        (resource("special"),), CURSORS["cursors"]["uploads_start"]
    )
    assert_process(read_lab, trace("location", "tags", "versions"))


@pytest.mark.parametrize("kind", ["version", "marker", "upload"])
def test_folha_repete_busca_fresca_antes_de_devolver_vazio(read_lab: ReadLab, kind: str) -> None:
    collection = "parts" if kind == "upload" else "versions"
    enqueue(read_lab, "location", "tags", collection)
    assert read_lab.services.dependents(resource(kind), None, read_lab.context) == Page()
    assert_process(read_lab, trace("location", "tags", collection))


def test_folha_nao_existe_so_por_envelope_quando_busca_inconclusiva(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags", "empty")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("version"), None, read_lab.context)
    assert error.value.args == ("service=lookup_inconclusive",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags", "versions"))


def test_cursor_de_folha_recusa_antes_de_io(read_lab: ReadLab) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(
            resource("upload"), CURSORS["cursors"]["uploads_next"], read_lab.context
        )
    assert error.value.args == ("service=cursor_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


def test_tags_sao_relidas_antes_da_segunda_colecao(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags", "empty", "location")
    respond(read_lab, {"TagSet": []})
    read_lab.services.dependents(resource("bucket"), None, read_lab.context)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(
            resource("bucket"), CURSORS["cursors"]["uploads_start"], read_lab.context
        )
    assert error.value.args == ("service=not_owned",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags", "versions", "location", "tags"))


@pytest.mark.parametrize(
    ("collection", "field", "value"),
    [
        ("Versions", "VersionId", None),
        ("DeleteMarkers", "VersionId", None),
        ("Versions", "Key", ""),
        ("Uploads", "UploadId", None),
    ],
)
def test_identidade_da_linha_invalida_nao_vira_filho(
    read_lab: ReadLab, collection: str, field: str, value: object
) -> None:
    group = "uploads" if collection == "Uploads" else "versions"
    enqueue(read_lab, "location", "tags")
    reply = deepcopy(DATA["replies"][group])
    reply[collection][0][field] = value
    respond(read_lab, reply)
    start = CURSORS["cursors"]["uploads_start"] if group == "uploads" else None
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("bucket"), start, read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags", group))


@pytest.mark.parametrize(
    "field_value", [("Versions", {}), ("DeleteMarkers", None), ("Uploads", "bad")]
)
def test_colecao_malformada_nao_e_vazio(read_lab: ReadLab, field_value: tuple) -> None:
    field, value = field_value
    group = "uploads" if field == "Uploads" else "versions"
    enqueue(read_lab, "location", "tags")
    respond(read_lab, {"IsTruncated": False, field: value})
    start = CURSORS["cursors"]["uploads_start"] if group == "uploads" else None
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("bucket"), start, read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags", group))


def test_removal_order_real_coloca_filho_antes_do_bucket_em_14_calls(read_lab: ReadLab) -> None:
    enqueue(read_lab, *DATA["expected"]["graph_replies"])
    janitor = cleanup(read_lab)
    order = janitor.removal_order(resource("bucket"))
    assert set(order) == {resource("bucket"), resource("version")}
    assert order.index(resource("version")) < order.index(resource("bucket"))
    assert_process(read_lab, trace(*DATA["expected"]["graph_trace"]))
    assert read_lab.context.budget.used == DATA["expected"]["graph_calls"] == 14
    assert read_lab.context.budget.work_used == 0
    assert read_lab.context.budget.limits == Limits()
    assert janitor.attempted == janitor.absent == set()
    assert list(read_lab.directory.iterdir()) == []


@pytest.mark.parametrize("mode", ["same_page", "across_pages"])
def test_colisao_logica_version_marker_recusa_travessia_real(read_lab: ReadLab, mode: str) -> None:
    replies = ["location", "tags", "collision", "location", "tags", "empty"]
    expected = trace("location", "tags", "versions", "location", "tags", "uploads")
    allowed_counts = (3, 6)
    if mode == "across_pages":
        replies = [
            "location",
            "tags",
            "collision_first",
            "location",
            "tags",
            "collision_last",
            "location",
            "tags",
            "empty",
        ]
        expected = trace(
            "location",
            "tags",
            "versions",
            "location",
            "tags",
            "versions_next",
            "location",
            "tags",
            "uploads",
        )
        allowed_counts = (6, 9)
    enqueue(read_lab, *replies)
    with pytest.raises(SafetyFailure) as error:
        dependents(resource("bucket"), cleanup(read_lab).ports, read_lab.context)
    assert error.value.reason == "inventory_incomplete"
    assert_failure(error.value, read_lab)
    count = len(read_lab.process.calls)
    assert count in allowed_counts
    assert_prefix(read_lab, expected[:count])
    assert read_lab.process.outcomes == [
        (0, json.dumps(DATA["replies"][name]), "") for name in replies[count:]
    ]


def test_grafo_preserva_bucket_de_outra_regiao_sem_descobrir_ou_deletar(read_lab: ReadLab) -> None:
    respond(read_lab, {"LocationConstraint": "us-west-1"})
    enqueue(read_lab, "tags")
    with pytest.raises(SafetyFailure) as error:
        cleanup(read_lab).removal_order(resource("bucket"))
    assert error.value.reason == "ownership_mismatch"
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags"))
