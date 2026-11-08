"""Na cleanup, listagem completa sem o par prova a ausência; fora dela segue inconclusivo."""

import logging
from pathlib import Path

import pytest

from tests.integration.aws_capability._b1_testing import NAME, TAG_SET, b1_lab, cleanup_context, ok
from tests.integration.aws_capability._resource_ids import s3_child
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab
from tests.integration.aws_capability.contracts import Resource, ServiceFailure

ROOT = Resource("s3", NAME, NAME)


@pytest.fixture
def lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> ReadLab:
    caplog.set_level(logging.DEBUG)
    return b1_lab(make_read_lab(monkeypatch, tmp_path), "s3-retention")


def child(lab: ReadLab, kind: str) -> Resource:
    return s3_child(ROOT, lab.services.spec, kind, {"key": "k", "version_id": "V1"})


def listing(**changes: object) -> dict[str, object]:
    return {
        "Name": NAME,
        "IsTruncated": False,
        "Versions": [{"Key": "outra", "VersionId": "V9"}],
        "DeleteMarkers": [],
        **changes,
    }


def owner(lab: ReadLab) -> None:
    ok(lab, {"LocationConstraint": "us-east-2"})
    ok(lab, {"TagSet": TAG_SET})


@pytest.mark.parametrize("kind", ["s3.version", "s3.marker"])
def test_cleanup_com_listagem_completa_sem_o_par_retorna_none(lab: ReadLab, kind: str) -> None:
    owner(lab)
    ok(lab, listing())
    assert lab.services.lookup(child(lab, kind), cleanup_context(lab)) is None


@pytest.mark.parametrize("kind", ["s3.version", "s3.marker"])
def test_fora_da_cleanup_a_listagem_completa_continua_inconclusiva(
    lab: ReadLab, kind: str
) -> None:
    owner(lab)
    ok(lab, listing())
    with pytest.raises(ServiceFailure) as error:
        lab.services.lookup(child(lab, kind), lab.context)
    assert error.value.args == ("service=lookup_inconclusive",)


def test_cleanup_encontra_o_par_em_pagina_posterior(lab: ReadLab) -> None:
    owner(lab)
    ok(lab, listing(IsTruncated=True, NextKeyMarker="outra", NextVersionIdMarker="V9"))
    ok(lab, listing(Versions=[{"Key": "k", "VersionId": "V1"}]))
    assert lab.services.lookup(child(lab, "s3.version"), cleanup_context(lab)) is not None


def test_cleanup_com_listagem_truncada_sem_fim_continua_inconclusiva(lab: ReadLab) -> None:
    owner(lab)
    for index in range(8):
        marker = f"m{index}"
        ok(lab, listing(IsTruncated=True, NextKeyMarker=marker, NextVersionIdMarker=marker))
    with pytest.raises(ServiceFailure) as error:
        lab.services.lookup(child(lab, "s3.version"), cleanup_context(lab))
    assert error.value.args == ("service=lookup_inconclusive",)
