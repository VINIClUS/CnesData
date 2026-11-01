"""Criação do bucket raiz com Object Lock, tags e readback sem retenção padrão."""

import logging
from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability._b1_testing import (
    ACCOUNT,
    NAME,
    TAG_SET,
    TAGS,
    b1_lab,
    creation_for,
    fail,
    ok,
)
from tests.integration.aws_capability._service_read_assertions import (
    assert_private,
    assert_process,
    assert_rejected,
)
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    ServiceFailure,
)

ROOT = Resource("s3", NAME, NAME)
OWNER = {"Bucket": NAME, "ExpectedBucketOwner": ACCOUNT}
CREATE = (
    "s3api",
    "create-bucket",
    "us-east-2",
    {
        "Bucket": NAME,
        "CreateBucketConfiguration": {"LocationConstraint": "us-east-2"},
        "ObjectLockEnabledForBucket": True,
    },
)
TAGGING = (
    "s3api",
    "put-bucket-tagging",
    "us-east-2",
    {**OWNER, "Tagging": {"TagSet": TAG_SET}},
)
READBACK = [
    ("s3api", "get-bucket-location", "us-east-2", OWNER),
    ("s3api", "get-bucket-tagging", "us-east-2", OWNER),
    ("s3api", "get-bucket-versioning", "us-east-2", OWNER),
    ("s3api", "get-object-lock-configuration", "us-east-2", OWNER),
]
LOCK = {"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}}


@pytest.fixture
def lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> ReadLab:
    caplog.set_level(logging.DEBUG)
    return b1_lab(make_read_lab(monkeypatch, tmp_path), "s3-hold")


def script(lab: ReadLab, **changes: object) -> None:
    replies: dict[str, object] = {
        "create": {"Location": f"http://{NAME}.s3.amazonaws.com/"},
        "tag": None,
        "location": {"LocationConstraint": "us-east-2"},
        "tags": {"TagSet": TAG_SET},
        "versioning": {"Status": "Enabled"},
        "lock": LOCK,
        **changes,
    }
    for key in ("create", "tag", "location", "tags", "versioning", "lock"):
        ok(lab, replies[key])


def test_cria_bucket_com_object_lock_tags_e_readback_completo(lab: ReadLab) -> None:
    script(lab)
    resource = lab.services.create("s3", creation_for(lab), lab.context)
    assert resource == ROOT
    assert_process(lab, [CREATE, TAGGING, *READBACK])
    operations = [call[0][2] for call in lab.process.calls]
    assert "put-object-lock-configuration" not in operations
    assert lab.context.budget.work_used == 6


@pytest.mark.parametrize("probe", ["s3-hold", "s3-retention"])
def test_habilita_criacao_de_s3_nos_perfis_s3(lab: ReadLab, probe: str) -> None:
    lab = b1_lab(lab, probe)
    script(lab)
    assert lab.services.create("s3", creation_for(lab), lab.context) == ROOT


@pytest.mark.parametrize("probe", ["all", "iam-s3", "iam-trust", "iam-leadingkeys", "ddb-ttl-gsi"])
def test_outros_perfis_preservam_stub_de_s3_sem_io(lab: ReadLab, probe: str) -> None:
    lab = b1_lab(lab, probe)
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("s3", creation_for(lab), lab.context)
    assert error.value.args == ("service=not_implemented",)
    assert_process(lab, [])


@pytest.mark.parametrize(
    "case", ["spec", "name", "tags", "account", "lane"]
)
def test_recusa_criacao_divergente_sem_io(lab: ReadLab, case: str) -> None:
    creation = creation_for(lab)
    context = lab.context
    if case == "spec":
        creation = replace(creation, spec=replace(creation.spec, run_id="ffffffff"))
    elif case == "name":
        creation = replace(creation, name="cnesdata-cap-ffffffff")
    elif case == "tags":
        creation = replace(creation, tags={**TAGS, "Run": "ffffffff"})
    elif case == "account":
        creation = replace(creation, spec=replace(creation.spec, account="222222222222"))
    else:
        context = RequestContext(lab.context.budget, "cleanup")
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("s3", creation, context)
    assert_rejected(error.value)
    assert_process(lab, [])


def test_falha_do_create_bucket_nao_chama_mais_nada(lab: ReadLab) -> None:
    fail(lab, "BucketAlreadyExists")
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("s3", creation_for(lab), lab.context)
    assert_rejected(error.value)
    assert len(lab.process.calls) == 1
    assert lab.process.outcomes == []


def test_falha_ao_etiquetar_interrompe_antes_do_readback(lab: ReadLab) -> None:
    ok(lab, {"Location": "x"})
    fail(lab, "AccessDenied")
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("s3", creation_for(lab), lab.context)
    assert_rejected(error.value)
    assert len(lab.process.calls) == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"location": {"LocationConstraint": "us-west-2"}},
        {"tags": {"TagSet": TAG_SET[:3]}},
        {"tags": {"TagSet": [*TAG_SET[:3], {"Key": "ExpiresAt", "Value": "9"}]}},
        {"tags": {"TagSet": [*TAG_SET, {"Key": "Extra", "Value": "x"}]}},
        {"versioning": {"Status": "Suspended"}},
        {"versioning": {}},
        {"lock": {"ObjectLockConfiguration": {"ObjectLockEnabled": "Disabled"}}},
        {"lock": {"ObjectLockConfiguration": {}}},
        {"lock": {}},
        {
            "lock": {
                "ObjectLockConfiguration": {
                    "ObjectLockEnabled": "Enabled",
                    "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}},
                }
            }
        },
    ],
    ids=[
        "regiao", "tag_ausente", "tag_trocada", "tag_extra", "versionamento_suspenso",
        "versionamento_ausente", "lock_desabilitado", "lock_vazio", "lock_ausente",
        "retencao_padrao",
    ],
)
def test_readback_divergente_falha_depois_de_ler_tudo(lab: ReadLab, changes: dict) -> None:
    script(lab, **changes)
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("s3", creation_for(lab), lab.context)
    assert error.value.args == ("service=create_invalid",)
    assert lab.process.outcomes == []


@pytest.mark.parametrize("step", ["location", "tags", "versioning", "lock"])
def test_erro_remoto_em_leitura_do_readback_falha(lab: ReadLab, step: str) -> None:
    ok(lab, {"Location": "x"})
    ok(lab, None)
    sequence = ["location", "tags", "versioning", "lock"]
    good = {
        "location": {"LocationConstraint": "us-east-2"},
        "tags": {"TagSet": TAG_SET},
        "versioning": {"Status": "Enabled"},
        "lock": LOCK,
    }
    for name in sequence:
        if name == step:
            fail(lab, "ObjectLockConfigurationNotFoundError")
            break
        ok(lab, good[name])
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("s3", creation_for(lab), lab.context)
    assert_rejected(error.value)


def test_nao_vaza_canary_nem_cria_arquivos(lab: ReadLab, caplog: pytest.LogCaptureFixture) -> None:
    script(lab)
    lab.services.create("s3", creation_for(lab), lab.context)
    assert list(lab.directory.iterdir()) == []
    assert_private(ServiceFailure("service=read_ok"), lab, caplog.text)
