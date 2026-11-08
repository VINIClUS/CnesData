"""Cursores S3 preservam duplas de markers e transicoes com prova fresca."""

import json

import pytest

from tests.integration.aws_capability._inventory import pages
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_roots_testing import ReadLab, respond
from tests.integration.aws_capability.contracts import Limits, Page, ServiceFailure
from tests.integration.aws_capability.test_service_s3_children_lookup import (
    CURSORS,
    DATA,
    assert_failure,
    enqueue,
    read_lab,
    remote_error,
    resource,
    trace,
)

__all__ = ["read_lab"]


def test_versions_preservam_dupla_e_ausencia_independente_do_segundo_marker(
    read_lab: ReadLab,
) -> None:
    enqueue(read_lab, "location", "tags", "versions_first")
    assert read_lab.services.dependents(resource("bucket"), None, read_lab.context) == Page(
        (), CURSORS["cursors"]["versions_next"]
    )
    assert_process(read_lab, trace("location", "tags", "versions"))
    enqueue(read_lab, "location", "tags", "versions_key")
    assert read_lab.services.dependents(
        resource("bucket"), CURSORS["cursors"]["versions_next"], read_lab.context
    ) == Page((), CURSORS["cursors"]["versions_key"])
    enqueue(read_lab, "location", "tags", "empty")
    final = read_lab.services.dependents(
        resource("bucket"), CURSORS["cursors"]["versions_key"], read_lab.context
    )
    assert final.resources == ()
    assert final.cursor == CURSORS["cursors"]["uploads_from_key"]
    assert_process(
        read_lab,
        trace(
            "location",
            "tags",
            "versions",
            "location",
            "tags",
            "versions_next",
            "location",
            "tags",
            "versions_key",
        ),
    )


def test_uploads_preservam_dupla_e_nao_inventam_marker_secundario(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags", "uploads_first")
    assert read_lab.services.dependents(
        resource("bucket"), CURSORS["cursors"]["uploads_start"], read_lab.context
    ) == Page((), CURSORS["cursors"]["uploads_next"])
    enqueue(read_lab, "location", "tags", "uploads_key")
    assert read_lab.services.dependents(
        resource("bucket"), CURSORS["cursors"]["uploads_next"], read_lab.context
    ) == Page((), CURSORS["cursors"]["uploads_key"])
    enqueue(read_lab, "location", "tags", "empty")
    assert (
        read_lab.services.dependents(
            resource("bucket"), CURSORS["cursors"]["uploads_key"], read_lab.context
        )
        == Page()
    )
    assert_process(
        read_lab,
        trace(
            "location",
            "tags",
            "uploads",
            "location",
            "tags",
            "uploads_next",
            "location",
            "tags",
            "uploads_key",
        ),
    )


def test_ecos_vazios_na_pagina_terminal_nao_sao_next_markers(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags")
    respond(read_lab, {"IsTruncated": False, "KeyMarker": "", "VersionIdMarker": ""})
    assert read_lab.services.dependents(resource("bucket"), None, read_lab.context) == Page(
        (), CURSORS["cursors"]["uploads_start"]
    )
    assert_process(read_lab, trace("location", "tags", "versions"))


@pytest.mark.parametrize(
    ("collection", "reply"),
    [
        ("versions", {}),
        ("versions", {"IsTruncated": None}),
        ("versions", {"IsTruncated": 1}),
        ("uploads", {"IsTruncated": "false"}),
        ("versions", {"IsTruncated": True}),
        ("versions", {"IsTruncated": True, "NextKeyMarker": ""}),
        ("versions", {"IsTruncated": True, "NextKeyMarker": "key-A", "NextVersionIdMarker": None}),
        ("versions", {"IsTruncated": False, "NextKeyMarker": "key-A"}),
        ("versions", {"IsTruncated": False, "NextVersionIdMarker": "version-A"}),
        ("uploads", {"IsTruncated": True, "KeyMarker": "key-U", "UploadIdMarker": "upload-A"}),
        ("uploads", {"IsTruncated": True, "NextKeyMarker": "key-U", "NextUploadIdMarker": 3}),
        ("uploads", {"IsTruncated": False, "NextUploadIdMarker": False}),
        ("versions", {"IsTruncated": True, "NextKeyMarker": None}),
        ("versions", {"IsTruncated": True, "NextKeyMarker": 3}),
        ("versions", {"IsTruncated": True, "NextKeyMarker": "key-A", "NextVersionIdMarker": ""}),
        ("versions", {"IsTruncated": True, "NextKeyMarker": "key-A", "NextVersionIdMarker": 3}),
        ("uploads", {"IsTruncated": True, "NextKeyMarker": "key-U", "NextUploadIdMarker": None}),
        ("uploads", {"IsTruncated": True, "NextKeyMarker": "key-U", "NextUploadIdMarker": ""}),
    ],
)
def test_flag_ou_next_incoerentes_nao_viram_fim(
    read_lab: ReadLab, collection: str, reply: object
) -> None:
    enqueue(read_lab, "location", "tags")
    respond(read_lab, reply)
    start = None if collection == "versions" else CURSORS["cursors"]["uploads_start"]
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("bucket"), start, read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags", collection))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("v", True),
        ("v", 1.0),
        ("run", "foreign"),
        ("parent", "other-bucket"),
        ("service", "cognito"),
        ("kind", "s3.buckets"),
        ("markers", {"VersionIdMarker": "version-A"}),
        ("markers", {"KeyMarker": "key-A", "UploadIdMarker": "upload-A"}),
        ("markers", {"KeyMarker": ""}),
        ("markers", {"KeyMarker": "key-A", "extra": "x"}),
        ("markers", {}),
        ("seen", []),
        ("seen", ["z" * 64]),
        ("extra", "x"),
    ],
)
def test_cursor_fechado_rejeita_escopo_e_markers_invalidos_sem_io(
    read_lab: ReadLab, field: str, value: object
) -> None:
    invalid = json.loads(CURSORS["cursors"]["versions_next"])
    invalid[field] = value
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("bucket"), json.dumps(invalid), read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


def test_cursor_com_chave_duplicada_recusa_antes_de_io(read_lab: ReadLab) -> None:
    invalid = CURSORS["cursors"]["versions_next"].replace('"v":1', '"v":1,"v":1')
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("bucket"), invalid, read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


@pytest.mark.parametrize("start", ["versions_next", "versions_key"])
def test_repeticao_e_ciclo_a_b_a_dos_markers_falham(read_lab: ReadLab, start: str) -> None:
    enqueue(read_lab, "location", "tags", "versions_cycle")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(
            resource("bucket"), CURSORS["cursors"][start], read_lab.context
        )
    assert error.value.args == ("service=cursor_cycle",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags", start))


def test_replay_do_cursor_repete_location_tags_e_pagina(read_lab: ReadLab) -> None:
    expected = []
    for _ in range(2):
        enqueue(read_lab, "location", "tags", "empty")
        assert read_lab.services.dependents(
            resource("bucket"), CURSORS["cursors"]["versions_next"], read_lab.context
        ) == Page((), CURSORS["cursors"]["uploads_from_versions"])
        expected.extend(trace("location", "tags", "versions_next"))
    assert_process(read_lab, expected)


def test_erro_na_pagina_seguinte_recusa_sem_retry(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags")
    remote_error(read_lab, "AccessDenied")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(
            resource("bucket"), CURSORS["cursors"]["uploads_next"], read_lab.context
        )
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags", "uploads_next"))


def test_pages_real_limita_oito_paginas_sem_ignorar_excecao_privada(read_lab: ReadLab) -> None:
    expected = []
    replies = [*DATA["lookup_prefix_replies"], {"IsTruncated": True, "NextKeyMarker": "page-H"}]
    for reply, call in zip(replies, DATA["lookup_calls"], strict=True):
        enqueue(read_lab, "location", "tags")
        respond(read_lab, reply)
        expected.extend(trace("location", "tags"))
        expected.append(tuple(call))
    with pytest.raises(SafetyFailure) as error:
        pages(
            lambda value: read_lab.services.dependents(resource("bucket"), value, read_lab.context),
            8,
        )
    assert error.value.reason == "inventory_incomplete"
    assert_failure(error.value, read_lab)
    assert_process(read_lab, expected)
    assert read_lab.context.budget.used == 24
    assert read_lab.context.budget.limits == Limits()


@pytest.mark.parametrize("collection", ["versions", "uploads"])
def test_ecos_nao_vazios_diferentes_nao_substituem_next_na_segunda_request(
    read_lab: ReadLab, collection: str
) -> None:
    enqueue(read_lab, "location", "tags")
    if collection == "versions":
        reply = {
            "IsTruncated": True,
            "KeyMarker": "key-B",
            "VersionIdMarker": "version-C",
            "NextKeyMarker": "key-A",
            "NextVersionIdMarker": "version-A",
        }
        start = None
        next_name, end_name = "versions_next", "uploads_from_versions"
    else:
        reply = {
            "IsTruncated": True,
            "KeyMarker": "key-V",
            "UploadIdMarker": "upload-private-1",
            "NextKeyMarker": "key-U",
            "NextUploadIdMarker": "upload-A",
        }
        start = CURSORS["cursors"]["uploads_start"]
        next_name, end_name = "uploads_next", None
    respond(read_lab, reply)
    first = read_lab.services.dependents(resource("bucket"), start, read_lab.context)
    assert first == Page((), CURSORS["cursors"][next_name])
    assert_process(read_lab, trace("location", "tags", collection))
    enqueue(read_lab, "location", "tags", "empty")
    second = read_lab.services.dependents(resource("bucket"), first.cursor, read_lab.context)
    assert second == Page((), None if end_name is None else CURSORS["cursors"][end_name])
    assert_process(read_lab, trace("location", "tags", collection, "location", "tags", next_name))
