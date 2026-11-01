"""Listagens S3 reais vazias e nao truncadas trazem next markers vazios ou ausentes."""

from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._aws_fake_types_testing import FakeBucket, FakeVersion
from tests.integration.aws_capability._b1_testing import NAME
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._probe_testing import (
    BUCKET,
    janitor_ports,
    make_probe_lab,
)
from tests.integration.aws_capability._safety import tags
from tests.integration.aws_capability._service_children_cursor import S3ChildrenCursor
from tests.integration.aws_capability._service_s3_children import _next_state, _validate_page
from tests.integration.aws_capability.contracts import ServiceFailure

UPLOADS = {
    "Bucket": "b",
    "KeyMarker": "",
    "UploadIdMarker": "",
    "NextKeyMarker": "",
    "NextUploadIdMarker": "",
    "MaxUploads": 1000,
    "IsTruncated": False,
}
VERSIONS = {
    "EncodingType": "url",
    "IsTruncated": False,
    "KeyMarker": "",
    "MaxKeys": 1000,
    "Name": "b",
    "Prefix": "",
    "VersionIdMarker": "",
}


def test_aceita_uploads_reais_vazios_com_next_markers_vazios() -> None:
    _validate_page(UPLOADS, "s3.uploads")
    assert _next_state(UPLOADS, S3ChildrenCursor("s3.uploads", {}, ())) is None


def test_aceita_versoes_reais_vazias_sem_next_markers() -> None:
    _validate_page(VERSIONS, "s3.versions")
    state = _next_state(VERSIONS, S3ChildrenCursor("s3.versions", {}, ()))
    assert state is not None
    assert state.kind == "s3.uploads"


def test_aceita_versoes_nao_truncadas_com_next_markers_vazios() -> None:
    page = {**VERSIONS, "NextKeyMarker": "", "NextVersionIdMarker": ""}
    _validate_page(page, "s3.versions")


@pytest.mark.parametrize(
    ("kind", "base", "changes"),
    [
        ("s3.uploads", UPLOADS, {"NextKeyMarker": "k"}),
        ("s3.uploads", UPLOADS, {"NextUploadIdMarker": "u"}),
        ("s3.uploads", UPLOADS, {"NextKeyMarker": None}),
        ("s3.uploads", UPLOADS, {"NextUploadIdMarker": 7}),
        ("s3.versions", VERSIONS, {"NextKeyMarker": "k"}),
        ("s3.versions", VERSIONS, {"NextVersionIdMarker": "v"}),
        ("s3.versions", VERSIONS, {"NextVersionIdMarker": 0}),
    ],
)
def test_rejeita_next_marker_nao_vazio_ou_nao_string_em_pagina_nao_truncada(
    kind: str, base: dict, changes: dict
) -> None:
    with pytest.raises(ServiceFailure) as error:
        _validate_page({**base, **changes}, kind)
    assert error.value.args == ("service=inventory_invalid",)


def test_fake_zero_estado_devolve_as_formas_reais_de_listagem_vazia(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path / "private", "s3-hold")
    uploads = lab.aws._s3api_list_multipart_uploads({"Bucket": NAME}, "operator").data
    versions = lab.aws._s3api_list_object_versions({"Bucket": NAME}, "operator").data
    assert uploads == {**UPLOADS, "Bucket": NAME}
    assert versions == {**VERSIONS, "Name": NAME}


def test_cleanup_remove_bucket_cuja_ultima_pagina_de_uploads_tem_markers_vazios(
    tmp_path: Path,
) -> None:
    lab = make_probe_lab(tmp_path / "private", "s3-hold", seeded=False)
    prepare_directory(lab.spec.journal_dir)
    bucket = FakeBucket(tags(lab.spec))
    bucket.versions = [FakeVersion("k", "V1", None, 0.0)]
    lab.aws.buckets[NAME] = bucket
    journal = Journal(lab.spec)
    journal.created = [BUCKET]
    journal.phase = "cleanup_requested"
    journal.save()
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.buckets == {}
    assert "delete-bucket" in lab.aws.operations()
