"""Lookup S3 prova pares exatos por enumeracao fresca sem inventar ausencia."""

import json
import traceback
from collections.abc import Iterator, Sequence
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import (
    FIXED_FLAGS,
    ExpectedCall,
    assert_environment,
    assert_process,
)
from tests.integration.aws_capability._service_roots_testing import (
    ReadLab,
    load_fixture,
    make_read_lab,
    respond,
)
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Limits,
    Ownership,
    Resource,
    ServiceFailure,
)
from tests.integration.aws_capability.test_service_cognito_client_safety import (
    _assert_channels,
    _channels,
)

DATA = load_fixture(Path(__file__).parent / "fixtures/cli/children/s3.json")
CURSORS = load_fixture(Path(__file__).parent / "fixtures/cli/children/s3_cursors.json")


@pytest.fixture
def read_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> Iterator[ReadLab]:
    lab = make_read_lab(monkeypatch, tmp_path, caplog, capsys)
    yield lab
    assert_failure(None, lab)


def assert_failure(error: BaseException | None, lab: ReadLab) -> None:
    public = _channels(lab.channels.caplog, lab.channels.capsys)
    _assert_channels(error, lab, public)
    if error is not None:
        public += "".join(traceback.format_exception(error)) + repr(error)
    public += repr(lab.services) + repr(lab.services.ports) + repr(lab.services.ports.commands)
    for path in lab.directory.rglob("*"):
        public += str(path.relative_to(lab.directory))
        if path.is_file() and not path.is_symlink():
            public += path.read_text(errors="replace")
    assert all(value not in public for value in DATA["private_values"])


def resource(name: str) -> Resource:
    return Resource(**DATA["resources"][name])


def trace(*names: str) -> list[ExpectedCall]:
    return [tuple(DATA["calls"][name]) for name in names]


def enqueue(lab: ReadLab, *names: str) -> None:
    for name in names:
        respond(lab, DATA["replies"].get(name, CURSORS["replies"].get(name)))


def remote_error(lab: ReadLab, code: str) -> None:
    lab.process.outcomes.append(
        (254, "", json.dumps({"Code": code, "Message": "R1_PRIVATE_CANARY"}))
    )


def owner() -> Ownership:
    return Ownership(
        "111111111111",
        {
            "Project": "CnesData",
            "Purpose": "aws-capability",
            "Run": "a1b2c3d4",
            "ExpiresAt": "1060",
        },
        "us-east-2",
    )


def assert_prefix(lab: ReadLab, expected: Sequence[ExpectedCall], offset: int = 0) -> None:
    assert len(lab.process.calls) == len(expected)
    assert lab.context.budget.used == offset + len(expected)
    assert lab.process.observed_debits == list(range(offset + 1, offset + len(expected) + 1))
    for (argv, options), (service, operation, region, payload) in zip(
        lab.process.calls, expected, strict=True
    ):
        assert argv == [
            "aws",
            service,
            operation,
            "--region",
            region,
            *FIXED_FLAGS,
            "7",
            "--cli-read-timeout",
            "7",
        ]
        assert json.loads(options["input"]) == payload
        assert_environment(options["env"], region)
        assert options["timeout"] == 7
        assert options["shell"] is False
        assert options["capture_output"] is True
        assert options["text"] is True
        assert options["check"] is False


@pytest.mark.parametrize("kind", ["version", "marker", "upload"])
@pytest.mark.parametrize("method", ["lookup", "inspect"])
def test_par_exato_exige_root_fresco_e_colecao_correta(
    read_lab: ReadLab, kind: str, method: str
) -> None:
    collection = "uploads" if kind == "upload" else "versions"
    enqueue(read_lab, "location", "tags", collection)
    assert getattr(read_lab.services, method)(resource(kind), read_lab.context) == owner()
    assert_process(read_lab, trace("location", "tags", collection))


@pytest.mark.parametrize("reply", ["wrong_pair", "wrong_kind", "empty"])
def test_key_igual_kind_trocado_ou_omissao_nao_confirma_versao(
    read_lab: ReadLab, reply: str
) -> None:
    enqueue(read_lab, "location", "tags", reply)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource("version"), read_lab.context)
    assert error.value.args == ("service=lookup_inconclusive",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags", "versions"))


@pytest.mark.parametrize("kind", ["marker", "upload"])
def test_lista_terminal_vazia_nao_autoriza_none_do_par(read_lab: ReadLab, kind: str) -> None:
    enqueue(read_lab, "location", "tags", "empty")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource(kind), read_lab.context)
    assert error.value.args == ("service=lookup_inconclusive",)
    assert_failure(error.value, read_lab)
    assert_process(
        read_lab, trace("location", "tags", "uploads" if kind == "upload" else "versions")
    )


def test_lookup_consume_segunda_pagina_com_os_dois_markers(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags", "versions_first", "one")
    assert read_lab.services.lookup(resource("version"), read_lab.context) == owner()
    assert_process(read_lab, trace("location", "tags", "versions", "versions_next"))


def test_lookup_upload_consume_sua_segunda_pagina_sem_enumerar_versions(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags", "uploads_first", "uploads")
    assert read_lab.services.lookup(resource("upload"), read_lab.context) == owner()
    assert_process(read_lab, trace("location", "tags", "uploads", "uploads_next"))


def test_lookup_preserva_key_unicode_aspas_e_separadores_sem_normalizar(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags", "special")
    assert read_lab.services.inspect(resource("special"), read_lab.context) == owner()
    assert_process(read_lab, trace("location", "tags", "versions"))


@pytest.mark.parametrize("phase", ["location", "tags", "versions"])
def test_no_such_bucket_nao_prova_ausencia_de_filho(read_lab: ReadLab, phase: str) -> None:
    script = ["location", "tags", "versions"]
    cut = script.index(phase)
    enqueue(read_lab, *script[:cut])
    remote_error(read_lab, "NoSuchBucket")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource("version"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace(*script[: cut + 1]))


@pytest.mark.parametrize("failure", ["denied", "local404", "json", "timeout"])
def test_falha_na_segunda_pagina_nao_e_ausencia_ou_retry(read_lab: ReadLab, failure: str) -> None:
    import subprocess

    enqueue(read_lab, "location", "tags", "versions_first")
    if failure == "denied":
        remote_error(read_lab, "AccessDenied")
    elif failure == "local404":
        read_lab.process.outcomes.append((252, "", '{"Code":"404","Message":"R1_PRIVATE_CANARY"}'))
    elif failure == "json":
        read_lab.process.outcomes.append((0, "R1_PRIVATE_CANARY", ""))
    else:
        read_lab.process.outcomes.append(subprocess.TimeoutExpired("aws", 7))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource("version"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags", "versions", "versions_next"))


def test_segunda_busca_rele_tags_e_recusa_dono_removido(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags", "one", "location")
    respond(read_lab, {"TagSet": []})
    assert read_lab.services.lookup(resource("version"), read_lab.context) == owner()
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource("version"), read_lab.context)
    assert error.value.args == ("service=not_owned",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags", "versions", "location", "tags"))


@pytest.mark.parametrize("scenario", ["region", "tags"])
def test_root_estrangeiro_impede_enumerar_filho(read_lab: ReadLab, scenario: str) -> None:
    respond(
        read_lab,
        {"LocationConstraint": "us-west-1"}
        if scenario == "region"
        else DATA["replies"]["location"],
    )
    respond(read_lab, {"TagSet": []} if scenario == "tags" else DATA["replies"]["tags"])
    child = resource("version")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(child, read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("v", True),
        ("v", 1.0),
        ("run", "foreign"),
        ("service", "iam"),
        ("kind", "s3.unknown"),
        ("parent_id", "other"),
        ("root_id", "other"),
        ("child_id", {"key": "private/key|one", "version_id": None}),
        ("child_id", {"key": "private/key|one", "version_id": 1}),
        ("child_id", {"key": "private/key|one", "version_id": "x", "extra": "x"}),
        ("extra", "x"),
        ("child_id", None),
        ("child_id", []),
        ("child_id", {"key": 1, "version_id": "version-private-1"}),
    ],
)
def test_envelope_fechado_recusa_invalido_sem_io(
    read_lab: ReadLab, field: str, value: object
) -> None:
    child = resource("version")
    invalid = json.loads(child.resource_id)
    invalid[field] = value
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(Resource("s3", json.dumps(invalid), child.name), read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


def test_chave_duplicada_de_envelope_recusa_sem_io(read_lab: ReadLab) -> None:
    child = resource("upload")
    invalid = child.resource_id.replace('"v":1', '"v":1,"v":1')
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(Resource("s3", invalid, child.name), read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


@pytest.mark.parametrize("last", ["one", "empty", "more"])
def test_busca_limitada_usa_oito_paginas_e_dez_debitos_sem_none(
    read_lab: ReadLab, last: str
) -> None:
    enqueue(read_lab, "location", "tags")
    for reply in DATA["lookup_prefix_replies"]:
        respond(read_lab, reply)
    enqueue(read_lab, last)
    if last == "one":
        assert read_lab.services.lookup(resource("version"), read_lab.context) == owner()
    else:
        with pytest.raises(ServiceFailure) as error:
            read_lab.services.lookup(resource("version"), read_lab.context)
        assert error.value.args == ("service=lookup_inconclusive",)
        assert_failure(error.value, read_lab)
    assert_process(
        read_lab, [*trace("location", "tags"), *(tuple(call) for call in DATA["lookup_calls"])]
    )
    assert read_lab.context.budget.used == DATA["expected"]["lookup_max_calls"] == 10
    assert read_lab.context.budget.limits == Limits()


@pytest.mark.parametrize("cut", [0, 1, 2])
def test_deadline_bloqueia_proximo_processo_da_busca(read_lab: ReadLab, cut: int) -> None:
    script = ["location", "tags", "versions"]
    enqueue(read_lab, *script[:cut])
    read_lab.context.budget.now = lambda: 1060 if len(read_lab.process.calls) >= cut else 1000
    with pytest.raises(BudgetExhausted) as error:
        read_lab.services.lookup(resource("version"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace(*script[:cut]))


def test_reserva_real_interrompe_antes_de_listar_filho(read_lab: ReadLab) -> None:
    for _ in range(38):
        read_lab.context.budget.acquire("work")
    enqueue(read_lab, "location", "tags")
    with pytest.raises(BudgetExhausted) as error:
        read_lab.services.lookup(resource("version"), read_lab.context)
    assert error.value.args == ("budget=requests",)
    assert_failure(error.value, read_lab)
    assert_prefix(read_lab, trace("location", "tags"), 38)
    assert read_lab.process.outcomes == []
    assert read_lab.context.budget.work_used == 40
    assert read_lab.context.budget.limits == Limits()


@pytest.mark.parametrize("kind_reply", [("marker", "wrong_marker"), ("upload", "wrong_upload")])
def test_mesma_key_nao_substitui_versionid_ou_uploadid_exato(
    read_lab: ReadLab, kind_reply: tuple[str, str]
) -> None:
    kind, reply = kind_reply
    enqueue(read_lab, "location", "tags", reply)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource(kind), read_lab.context)
    assert error.value.args == ("service=lookup_inconclusive",)
    assert_failure(error.value, read_lab)
    assert_process(
        read_lab, trace("location", "tags", "uploads" if kind == "upload" else "versions")
    )


def test_busca_apos_omissao_refaz_prova_e_pode_encontrar_versao(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags", "empty", "location", "tags", "one")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource("version"), read_lab.context)
    assert error.value.args == ("service=lookup_inconclusive",)
    assert_failure(error.value, read_lab)
    assert read_lab.services.lookup(resource("version"), read_lab.context) == owner()
    assert_process(read_lab, trace("location", "tags", "versions") * 2)


def test_loop_de_marker_na_busca_falha_sem_nona_request(read_lab: ReadLab) -> None:
    enqueue(read_lab, "location", "tags", "versions_first", "versions_first")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource("version"), read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("location", "tags", "versions", "versions_next"))


@pytest.mark.parametrize(
    ("kind", "field"),
    [
        ("version", "child_id"),
        ("version", "root_id"),
        ("version", "parent_id"),
        ("version", "key"),
        ("version", "version_id"),
        ("upload", "upload_id"),
    ],
)
def test_campo_estrutural_ou_membro_obrigatorio_ausente_recusa_sem_io(
    read_lab: ReadLab, kind: str, field: str
) -> None:
    child = resource(kind)
    invalid = json.loads(child.resource_id)
    if field in {"key", "version_id", "upload_id"}:
        del invalid["child_id"][field]
    else:
        del invalid[field]
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(Resource("s3", json.dumps(invalid), child.name), read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


@pytest.mark.parametrize(
    ("kind", "method", "reply"),
    [
        (
            "version",
            "lookup",
            {
                "IsTruncated": False,
                "Versions": [{"Key": "private/key|one", "VersionId": "version-private-1"}],
                "DeleteMarkers": "bad",
            },
        ),
        (
            "marker",
            "lookup",
            {
                "IsTruncated": False,
                "DeleteMarkers": [{"Key": "private/deleted", "VersionId": "null"}],
                "Versions": "bad",
            },
        ),
        (
            "version",
            "lookup",
            {
                "IsTruncated": False,
                "Versions": [
                    {"Key": "private/key|one", "VersionId": "version-private-1"},
                    {"Key": "private/key|one"},
                ],
            },
        ),
        (
            "upload",
            "lookup",
            {
                "IsTruncated": False,
                "Uploads": [
                    {"Key": "private/upload", "UploadId": "upload-private-1"},
                    {"Key": 1, "UploadId": None},
                ],
            },
        ),
        (
            "version",
            "inspect",
            {"Versions": [{"Key": "private/key|one", "VersionId": "version-private-1"}]},
        ),
        (
            "upload",
            "lookup",
            {
                "IsTruncated": True,
                "Uploads": [{"Key": "private/upload", "UploadId": "upload-private-1"}],
                "NextKeyMarker": "key-U",
                "NextUploadIdMarker": None,
            },
        ),
    ],
    ids=[
        "version_grupo_marker",
        "marker_grupo_version",
        "version_linha_posterior",
        "upload_linha_posterior",
        "inspect_flag_ausente",
        "upload_next_invalido",
    ],
)
def test_target_presente_nao_autoriza_pagina_parcialmente_malformada(
    read_lab: ReadLab, kind: str, method: str, reply: object
) -> None:
    enqueue(read_lab, "location", "tags")
    respond(read_lab, reply)
    with pytest.raises(ServiceFailure) as error:
        getattr(read_lab.services, method)(resource(kind), read_lab.context)
    assert error.value.args == ("service=inventory_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(
        read_lab, trace("location", "tags", "uploads" if kind == "upload" else "versions")
    )
