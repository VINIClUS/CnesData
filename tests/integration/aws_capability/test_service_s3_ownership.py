"""Ownership S3 exige conta na request, região real e tags frescas."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.integration.aws_capability._safety import owned
from tests.integration.aws_capability._service_read_assertions import assert_private, assert_process
from tests.integration.aws_capability._service_roots_testing import (
    LOCATION,
    ROOT,
    TAGS,
    ReadLab,
    fixture_data,
    make_read_lab,
    respond,
    respond_error,
)
from tests.integration.aws_capability.contracts import Ownership, Resource, ServiceFailure


@pytest.fixture
def read_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> Iterator[ReadLab]:
    lab = make_read_lab(monkeypatch, tmp_path, caplog, capsys)
    yield lab
    lab.check_private()


@pytest.mark.parametrize(
    ("case", "region"),
    [
        ("location_null", "us-east-1"),
        ("location_eu", "eu-west-1"),
        ("location_east", "us-east-2"),
        ("location_west", "us-west-2"),
    ],
)
def test_inspect_normaliza_apenas_aliases_sem_inventar_account_na_resposta(
    read_lab: ReadLab,
    case: str,
    region: str,
) -> None:
    respond(read_lab, fixture_data("s3")[case])
    respond(read_lab, fixture_data("s3")["tags"])
    owner = read_lab.services.inspect(ROOT, read_lab.context)
    assert owner == Ownership(
        "111111111111",
        {
            "Project": "CnesData",
            "Purpose": "aws-capability",
            "Run": "a1b2c3d4",
            "ExpiresAt": "1060",
        },
        region,
    )
    assert_process(read_lab, [LOCATION, TAGS])


def test_bucket_baseline_sem_prefixo_ou_run_permanece_legivel(read_lab: ReadLab) -> None:
    resource = Resource("s3", "legacy-bucket", "legacy-bucket")
    respond(read_lab, fixture_data("s3")["location_east"])
    respond(read_lab, fixture_data("s3")["tags_empty"])
    owner = read_lab.services.lookup(resource, read_lab.context)
    assert owner is not None
    assert owner == Ownership("111111111111", {}, "us-east-2")
    assert not owned(resource, owner, read_lab.services.spec)
    assert_process(
        read_lab,
        [
            (
                "s3api",
                "get-bucket-location",
                "us-east-2",
                {"Bucket": "legacy-bucket", "ExpectedBucketOwner": "111111111111"},
            ),
            (
                "s3api",
                "get-bucket-tagging",
                "us-east-2",
                {"Bucket": "legacy-bucket", "ExpectedBucketOwner": "111111111111"},
            ),
        ],
    )


def test_bucket_cross_region_legivel_fica_owned_false_com_regiao_explicita(
    read_lab: ReadLab,
) -> None:
    respond(read_lab, fixture_data("s3")["location_west"])
    respond(read_lab, fixture_data("s3")["tags"])
    owner = read_lab.services.lookup(ROOT, read_lab.context)
    assert owner is not None
    assert owner.account == "111111111111"
    assert owner.region == "us-west-2"
    assert dict(owner.tags) == {
        "Project": "CnesData",
        "Purpose": "aws-capability",
        "Run": "a1b2c3d4",
        "ExpiresAt": "1060",
    }
    assert not owned(ROOT, owner, read_lab.services.spec)
    assert_process(read_lab, [LOCATION, TAGS])


def test_bucket_da_regiao_e_tags_corretas_tem_owned_positivo(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["location_east"])
    respond(read_lab, fixture_data("s3")["tags"])
    owner = read_lab.services.lookup(ROOT, read_lab.context)
    assert owner is not None
    assert owned(ROOT, owner, read_lab.services.spec)
    assert_process(read_lab, [LOCATION, TAGS])


def test_lookup_e_inspect_releem_location_e_tags_removidas_sem_cache(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["location_east"])
    respond(read_lab, fixture_data("s3")["tags"])
    first = read_lab.services.lookup(ROOT, read_lab.context)
    assert first is not None
    assert owned(ROOT, first, read_lab.services.spec)
    respond(read_lab, fixture_data("s3")["location_west"])
    respond(read_lab, fixture_data("s3")["tags_removed"])
    second = read_lab.services.inspect(ROOT, read_lab.context)
    assert second.region == "us-west-2"
    assert dict(second.tags) == {
        "Project": "CnesData",
        "Purpose": "aws-capability",
        "ExpiresAt": "1060",
    }
    assert not owned(ROOT, second, read_lab.services.spec)
    assert_process(read_lab, [LOCATION, TAGS, LOCATION, TAGS])


def test_no_such_tag_set_remoto_preserva_bucket_com_tags_vazias(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["location_east"])
    respond_error(read_lab, "remote_no_tags")
    owner = read_lab.services.lookup(ROOT, read_lab.context)
    assert owner == Ownership("111111111111", {}, "us-east-2")
    assert owner is not None
    read_lab.check_private(owner)
    assert not owned(ROOT, owner, read_lab.services.spec)
    assert_process(read_lab, [LOCATION, TAGS])


def test_inspect_preserva_presenca_quando_tag_set_remoto_nao_existe(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["location_east"])
    respond_error(read_lab, "remote_no_tags")
    owner = read_lab.services.inspect(ROOT, read_lab.context)
    assert owner == Ownership(
        "111111111111",
        {},
        "us-east-2",
    )
    read_lab.check_private(owner)
    assert_process(read_lab, [LOCATION, TAGS])


@pytest.mark.parametrize(
    "case",
    [
        "location_missing",
        "location_list",
        "location_empty",
        "location_number",
        "location_boolean",
        "location_array",
        "location_object",
        "location_private",
    ],
)
def test_location_invalida_falha_antes_de_ler_tags(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond(read_lab, fixture_data("s3")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    expected = "service=describe_failed" if case == "location_list" else "service=describe_invalid"
    assert error.value.args == (expected,)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [LOCATION])


@pytest.mark.parametrize(
    "case",
    [
        "tags_missing",
        "tags_map",
        "tags_null",
        "tags_entry",
        "tags_missing_key",
        "tags_missing_value",
        "tags_number_key",
        "tags_number_value",
        "tags_null_value",
        "tags_duplicate",
        "tags_extra_entry",
    ],
)
def test_tag_set_invalido_ou_duplicado_falha_fechado(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond(read_lab, fixture_data("s3")["location_east"])
    respond(read_lab, fixture_data("s3")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert error.value.args == ("service=tags_invalid",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [LOCATION, TAGS])


@pytest.mark.parametrize(
    "resource",
    [
        Resource("s3", "cnesdata-cap-a1b2c3d4", "other-bucket"),
        Resource("s3", "", ""),
        Resource("s3", "legacy-bucket", ""),
        Resource("s3", "", "legacy-bucket"),
    ],
    ids=["id_diverge_nome", "ambos_vazios", "nome_vazio", "id_vazio"],
)
def test_recurso_s3_invalido_e_rejeitado_sem_processo(
    read_lab: ReadLab, resource: Resource
) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource, read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [])


def test_owner_divergente_e_rejeitado_pelo_expected_bucket_owner_remoto(read_lab: ReadLab) -> None:
    respond_error(read_lab, "remote_denied")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert error.value.args == ("service=describe_failed",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [LOCATION])
