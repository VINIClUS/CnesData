"""Exclusão de versões, markers e bucket com legal hold desligado e bypass explícito."""

import logging
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from tests.integration.aws_capability._b1_testing import (
    ACCOUNT,
    NAME,
    TAG_SET,
    b1_lab,
    cleanup_context,
    fail,
    ok,
)
from tests.integration.aws_capability._resource_ids import s3_child
from tests.integration.aws_capability._service_read_assertions import assert_private, assert_process
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab
from tests.integration.aws_capability.contracts import RequestContext, Resource, ServiceFailure

ROOT = Resource("s3", NAME, NAME)
OWNER = {"Bucket": NAME, "ExpectedBucketOwner": ACCOUNT}
LOCATION = ("s3api", "get-bucket-location", "us-east-2", OWNER)
TAGGING = ("s3api", "get-bucket-tagging", "us-east-2", OWNER)
OWNERSHIP = [LOCATION, TAGGING]
OBJECT = {"Bucket": NAME, "Key": "k", "VersionId": "V1_CANARY", "ExpectedBucketOwner": ACCOUNT}
HOLD = ("s3api", "get-object-legal-hold", "us-east-2", OBJECT)
HOLD_OFF = (
    "s3api",
    "put-object-legal-hold",
    "us-east-2",
    {**OBJECT, "LegalHold": {"Status": "OFF"}},
)
DELETE = (
    "s3api",
    "delete-object",
    "us-east-2",
    {**OBJECT, "BypassGovernanceRetention": True},
)
DELETE_BUCKET = ("s3api", "delete-bucket", "us-east-2", OWNER)


@pytest.fixture
def lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> ReadLab:
    caplog.set_level(logging.DEBUG)
    return b1_lab(make_read_lab(monkeypatch, tmp_path), "s3-retention", [])


def version(lab: ReadLab, kind: str = "s3.version") -> Resource:
    child = {"key": "k", "version_id": "V1_CANARY"}
    return s3_child(ROOT, lab.services.spec, kind, child)


def owner_replies(lab: ReadLab, tags: list | None = None) -> None:
    ok(lab, {"LocationConstraint": "us-east-2"})
    ok(lab, {"TagSet": TAG_SET if tags is None else tags})


def test_remove_versao_sem_hold_com_bypass(lab: ReadLab) -> None:
    owner_replies(lab)
    ok(lab, {"LegalHold": {"Status": "OFF"}})
    ok(lab, {"VersionId": "V1_CANARY"})
    lab.services.delete(version(lab), True, cleanup_context(lab))
    assert_process(lab, [*OWNERSHIP, HOLD, DELETE])


def test_desliga_hold_ligado_antes_de_remover(lab: ReadLab) -> None:
    owner_replies(lab)
    ok(lab, {"LegalHold": {"Status": "ON"}})
    ok(lab)
    ok(lab, {"VersionId": "V1_CANARY"})
    lab.services.delete(version(lab), True, cleanup_context(lab))
    assert_process(lab, [*OWNERSHIP, HOLD, HOLD_OFF, DELETE])


def test_versao_sem_configuracao_de_hold_conta_como_desligada(lab: ReadLab) -> None:
    owner_replies(lab)
    fail(lab, "NoSuchObjectLockConfiguration")
    ok(lab, {})
    lab.services.delete(version(lab), True, cleanup_context(lab))
    assert_process(lab, [*OWNERSHIP, HOLD, DELETE])


def test_status_de_hold_desconhecido_falha_sem_remover(lab: ReadLab) -> None:
    owner_replies(lab)
    ok(lab, {"LegalHold": {"Status": "WEIRD"}})
    with pytest.raises(ServiceFailure) as error:
        lab.services.delete(version(lab), True, cleanup_context(lab))
    assert error.value.args == ("service=delete_failed",)
    assert_process(lab, [*OWNERSHIP, HOLD])


def test_marker_pula_leitura_de_hold(lab: ReadLab) -> None:
    owner_replies(lab)
    ok(lab, {})
    lab.services.delete(version(lab, "s3.marker"), True, cleanup_context(lab))
    assert_process(lab, [*OWNERSHIP, DELETE])


def test_sem_bypass_nao_envia_cabecalho_de_bypass(lab: ReadLab) -> None:
    owner_replies(lab)
    ok(lab, {"LegalHold": {"Status": "OFF"}})
    ok(lab, {})
    lab.services.delete(version(lab), False, cleanup_context(lab))
    plain = {key: value for key, value in DELETE[3].items() if key != "BypassGovernanceRetention"}
    assert_process(lab, [*OWNERSHIP, HOLD, (*DELETE[:3], plain)])


@pytest.mark.parametrize("probe", ["all", "s3-hold", "ddb-ttl-gsi", "iam-trust"])
def test_remocao_funciona_em_qualquer_perfil(lab: ReadLab, probe: str) -> None:
    lab = b1_lab(lab, probe)
    owner_replies(lab)
    ok(lab, {"LegalHold": {"Status": "OFF"}})
    ok(lab, {})
    lab.services.delete(version(lab), True, cleanup_context(lab))
    assert len(lab.process.calls) == 4


@pytest.mark.parametrize("failing", ["hold_read", "hold_off", "delete"])
def test_falha_remota_em_cada_passo_interrompe_e_nao_vaza(
    lab: ReadLab, failing: str, caplog: pytest.LogCaptureFixture
) -> None:
    owner_replies(lab)
    if failing == "hold_read":
        fail(lab, "AccessDenied")
    else:
        ok(lab, {"LegalHold": {"Status": "ON"}})
        if failing == "hold_off":
            fail(lab, "AccessDenied")
        else:
            ok(lab)
            fail(lab, "AccessDenied")
    with pytest.raises(ServiceFailure) as error:
        lab.services.delete(version(lab), True, cleanup_context(lab))
    assert error.value.args != ("service=not_implemented",)
    assert_private(error.value, lab, caplog.text)


def test_nao_remove_com_root_de_outra_posse(lab: ReadLab) -> None:
    owner_replies(lab, tags=TAG_SET[:3])
    with pytest.raises(ServiceFailure) as error:
        lab.services.delete(version(lab), True, cleanup_context(lab))
    assert error.value.args == ("service=not_owned",)
    assert_process(lab, OWNERSHIP)


def test_root_ausente_falha_como_nao_encontrado(lab: ReadLab) -> None:
    fail(lab, "NoSuchBucket")
    with pytest.raises(ServiceFailure) as error:
        lab.services.delete(version(lab), True, cleanup_context(lab))
    assert error.value.args == ("service=not_found",)


def _invalid_delete(lab: ReadLab, case: str) -> tuple[Resource, bool, RequestContext]:
    other = replace(lab.services.spec, run_id="ffffffff")
    upload = s3_child(ROOT, lab.services.spec, "s3.upload", {"key": "k", "upload_id": "U1"})
    foreign = s3_child(ROOT, other, "s3.version", {"key": "k", "version_id": "v"})
    cases = {
        "work": (version(lab), True, lab.context),
        "bypass_int": (version(lab), cast("bool", 1), cleanup_context(lab)),
        "upload": (upload, True, cleanup_context(lab)),
        "outro_run": (foreign, True, cleanup_context(lab)),
        "outro_root": (Resource("s3", "outro-bucket", "outro-bucket"), True, cleanup_context(lab)),
    }
    return cases[case]


@pytest.mark.parametrize("case", ["work", "bypass_int", "upload", "outro_run", "outro_root"])
def test_recusa_entrada_invalida_sem_io(lab: ReadLab, case: str) -> None:
    resource, bypass, context = _invalid_delete(lab, case)
    with pytest.raises(ServiceFailure) as error:
        lab.services.delete(resource, bypass, context)
    assert error.value.args != ("service=not_implemented",)
    assert_process(lab, [])


def test_remove_bucket_vazio_e_confirma_ausencia(lab: ReadLab) -> None:
    pauses: list[float] = []
    lab = b1_lab(lab, "s3-retention", pauses)
    owner_replies(lab)
    ok(lab)
    fail(lab, "NoSuchBucket")
    lab.services.delete(ROOT, True, cleanup_context(lab))
    assert_process(lab, [*OWNERSHIP, DELETE_BUCKET, LOCATION])
    assert pauses == []


def test_aguarda_ausencia_do_bucket_de_forma_limitada(lab: ReadLab) -> None:
    pauses: list[float] = []
    lab = b1_lab(lab, "s3-retention", pauses)
    owner_replies(lab)
    ok(lab)
    for _ in range(2):
        ok(lab, {"LocationConstraint": "us-east-2"})
    fail(lab, "NoSuchBucket")
    lab.services.delete(ROOT, True, cleanup_context(lab))
    assert pauses == [1.0, 1.0]


def test_bucket_que_continua_visivel_esgota_a_espera(lab: ReadLab) -> None:
    pauses: list[float] = []
    lab = b1_lab(lab, "s3-retention", pauses)
    owner_replies(lab)
    ok(lab)
    for _ in range(10):
        ok(lab, {"LocationConstraint": "us-east-2"})
    with pytest.raises(ServiceFailure) as error:
        lab.services.delete(ROOT, True, cleanup_context(lab))
    assert error.value.args == ("service=delete_pending",)
    assert len(pauses) == 4
    assert len(lab.process.calls) == 3 + 5


def test_falha_no_delete_bucket_nao_consulta_ausencia(lab: ReadLab) -> None:
    owner_replies(lab)
    fail(lab, "BucketNotEmpty")
    with pytest.raises(ServiceFailure):
        lab.services.delete(ROOT, True, cleanup_context(lab))
    assert len(lab.process.calls) == 3
