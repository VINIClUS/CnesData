"""Phase A P-C: objetos privados no S3 sob o role de escrita e artefato público sem ARN."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pytest

from release.verify_candidate import canonical
from scripts.promotion.phase_a_cli import Seams, main
from scripts.promotion.phase_a_private import OBJECTS, stage_private, upload_private
from scripts.promotion.promote_ecr import promote_candidate
from tests.production.delivery import test_candidate_manifest as candidate_fixtures
from tests.production.delivery._phase_a_fixtures import (
    ACCOUNT,
    NOW,
    REGION,
    RegistryBoundary,
    _candidate_reference,
    _creator_pin,
)

github = candidate_fixtures.github
SOURCE = "a" * 40
IDENTITY = {"source_sha": SOURCE, "run_id": 987654321, "run_attempt": 2}
BUCKET = "evidence-bucket-test"
PREFIX = "cnesdata/delivery/"
KMS = f"arn:aws:kms:{REGION}:{ACCOUNT}:key/11111111-2222-3333-4444-555555555555"
WRITER = f"arn:aws:sts::{ACCOUNT}:assumed-role/cnesdata-prod-phase-inputs-writer/session"
TARGET = {"bucket": BUCKET, "prefix": PREFIX, "kms_key_arn": KMS}
ARN = f"arn:aws:iam::{ACCOUNT}:role/x"


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _promotion(source: str = SOURCE) -> dict[str, Any]:
    candidate = {"source_sha": source, "images": {"arn": ARN}}
    return {
        "candidate": candidate,
        "binding": {"candidate_sha256": _sha(canonical(candidate))},
        "arn": ARN,
    }


def _directory(tmp_path, promotion: dict[str, Any] | None = None):
    (tmp_path / "promotion.json").write_bytes(canonical(promotion or _promotion()))
    (tmp_path / "phase-inputs.json").write_bytes(canonical({"role": ARN}))
    return tmp_path


class Client:
    def __init__(self) -> None:
        self.puts: list[dict[str, Any]] = []
        self.refuse = False

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        if self.refuse:
            raise ValueError("s3=PreconditionFailed")
        self.puts.append(kwargs)
        return {}

    def get_caller_identity(self) -> dict[str, str]:
        return {"Account": ACCOUNT, "Arn": WRITER}


class Session:
    def __init__(self, client: Client, arn: str = WRITER) -> None:
        self.client_ = client
        self.arn = arn
        self.calls: list[str] = []

    def client(self, name: str) -> Any:
        self.calls.append(name)
        if name == "sts":
            return type(
                "Sts",
                (),
                {"get_caller_identity": lambda _s: {"Account": ACCOUNT, "Arn": self.arn}},
            )()
        return self.client_


def test_stage_grava_o_candidate_byte_a_byte_do_recibo_autenticado(tmp_path):
    promotion = _promotion()
    directory = _directory(tmp_path, promotion)
    stage_private(directory, IDENTITY)
    assert (directory / "candidate.json").read_bytes() == canonical(promotion["candidate"])
    assert (directory / "candidate.json").read_bytes().endswith(b"\n")


def test_stage_com_o_recibo_real_reproduz_o_sha_do_binding(github, tmp_path):
    registry = RegistryBoundary()
    reference = _candidate_reference(github, registry)
    governed = {
        "account_id": ACCOUNT,
        "region": REGION,
        "now": NOW,
        "candidate_creator": _creator_pin(),
    }
    promotion = promote_candidate(reference, github, registry, governed)
    directory = _directory(tmp_path, promotion)
    identity = IDENTITY | {"source_sha": promotion["candidate"]["source_sha"]}
    stage_private(directory, identity)
    digest = _sha((directory / "candidate.json").read_bytes())
    assert digest == promotion["binding"]["candidate_sha256"]


def test_stage_recusa_candidate_com_sha_diferente_do_binding(tmp_path):
    promotion = _promotion()
    promotion["binding"]["candidate_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="candidate=binding"):
        stage_private(_directory(tmp_path, promotion), IDENTITY)
    assert not (tmp_path / "phase-a-public.json").exists()


def test_stage_recusa_source_sha_divergente_do_candidate(tmp_path):
    with pytest.raises(ValueError, match="candidate=source_sha"):
        stage_private(_directory(tmp_path, _promotion("b" * 40)), IDENTITY)


def test_artefato_publico_tem_so_o_conjunto_fechado_e_nenhum_arn(tmp_path):
    directory = _directory(tmp_path)
    stage_private(directory, IDENTITY)
    raw = (directory / "phase-a-public.json").read_bytes()
    public = json.loads(raw)
    assert set(public) == {"objects", "source_sha", "run_id", "run_attempt"}
    assert public["source_sha"] == SOURCE
    assert (public["run_id"], public["run_attempt"]) == (987654321, 2)
    assert [item["name"] for item in public["objects"]] == sorted(OBJECTS)
    for item in public["objects"]:
        assert set(item) == {"name", "sha256", "size"}
        data = (directory / item["name"]).read_bytes()
        assert (item["sha256"], item["size"]) == (_sha(data), len(data))
    assert b"arn:" not in raw
    assert ACCOUNT.encode() not in raw


def test_objetos_privados_sao_exatamente_tres_com_estes_nomes():
    assert sorted(OBJECTS) == ["candidate.json", "phase-inputs.json", "promotion.json"]


def test_upload_grava_as_chaves_exatas_com_sse_kms_e_if_none_match(tmp_path):
    directory = _directory(tmp_path)
    stage_private(directory, IDENTITY)
    client = Client()
    upload_private(directory, TARGET | {"identity": IDENTITY}, client)
    base = f"{PREFIX}phase-inputs/{SOURCE}/987654321/2/"
    assert sorted(put["Key"] for put in client.puts) == [base + name for name in sorted(OBJECTS)]
    for put in client.puts:
        name = put["Key"].rsplit("/", 1)[-1]
        assert put["Bucket"] == BUCKET
        assert put["Body"] == (directory / name).read_bytes()
        assert put["ServerSideEncryption"] == "aws:kms"
        assert put["SSEKMSKeyId"] == KMS
        assert put["IfNoneMatch"] == "*"


def test_upload_nao_grava_o_artefato_publico_nem_chave_de_ensaio(tmp_path):
    directory = _directory(tmp_path)
    stage_private(directory, IDENTITY)
    client = Client()
    upload_private(directory, TARGET | {"identity": IDENTITY}, client)
    assert not [put for put in client.puts if "public" in put["Key"] or "rehearsal" in put["Key"]]


def test_upload_recusa_objeto_alterado_depois_do_stage(tmp_path):
    directory = _directory(tmp_path)
    stage_private(directory, IDENTITY)
    (directory / "promotion.json").write_bytes(b"{}\n")
    client = Client()
    with pytest.raises(ValueError, match=r"private=sha256 name=promotion\.json"):
        upload_private(directory, TARGET | {"identity": IDENTITY}, client)
    assert client.puts == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("bucket", ""),
        ("bucket", "UPPER_bucket"),
        ("prefix", ""),
        ("prefix", "sem-barra-final"),
        ("prefix", "/absoluto/"),
        ("prefix", "a/../b/"),
        ("kms_key_arn", ""),
        ("kms_key_arn", "arn:aws:kms:us-east-2:123:key/x"),
        ("kms_key_arn", f"arn:aws:kms:{REGION}:{ACCOUNT}:alias/x"),
    ],
)
def test_upload_recusa_variavel_vazia_ou_malformada_antes_de_qualquer_chamada(
    tmp_path, field, value
):
    directory = _directory(tmp_path)
    stage_private(directory, IDENTITY)
    client = Client()
    with pytest.raises(ValueError, match=f"evidence_{field}=invalid"):
        upload_private(directory, TARGET | {field: value, "identity": IDENTITY}, client)
    assert client.puts == []


def _arguments(directory, target: dict[str, str] | None = None) -> list[str]:
    values = target or {"bucket": BUCKET, "prefix": PREFIX, "kms-key-arn": KMS}
    return [
        "upload-private",
        "--directory",
        str(directory),
        "--source-sha",
        SOURCE,
        "--run-id",
        "987654321",
        "--run-attempt",
        "2",
        "--account-id",
        ACCOUNT,
        *[item for key, value in values.items() for item in (f"--{key}", value)],
    ]


def test_cli_upload_exige_o_role_de_escrita_antes_de_gravar(tmp_path):
    directory = _directory(tmp_path)
    stage_private(directory, IDENTITY)
    client = Client()
    other = f"arn:aws:sts::{ACCOUNT}:assumed-role/cnesdata-prod-ecr-promotion/s"
    seams = Seams(session_factory=lambda: Session(client, other))
    assert main(_arguments(directory), seams=seams) == 2
    assert client.puts == []


def test_cli_upload_grava_com_o_role_de_escrita(tmp_path):
    directory = _directory(tmp_path)
    stage_private(directory, IDENTITY)
    client = Client()
    assert main(_arguments(directory), seams=Seams(session_factory=lambda: Session(client))) == 0
    assert len(client.puts) == 3


def test_cli_upload_recusa_variavel_malformada_sem_criar_sessao(tmp_path):
    directory = _directory(tmp_path)
    stage_private(directory, IDENTITY)
    created: list[int] = []

    def factory() -> Any:
        created.append(1)
        return Session(Client())

    bad = {"bucket": "", "prefix": PREFIX, "kms-key-arn": KMS}
    assert main(_arguments(directory, bad), seams=Seams(session_factory=factory)) == 2
    assert created == []


def test_cli_upload_devolve_2_quando_o_objeto_ja_existe(tmp_path):
    directory = _directory(tmp_path)
    stage_private(directory, IDENTITY)
    client = Client()
    client.refuse = True
    seams = Seams(session_factory=lambda: Session(client))
    assert main(_arguments(directory), seams=seams) == 2


def test_cli_stage_private_grava_candidate_e_publico(tmp_path):
    directory = _directory(tmp_path)
    arguments = [
        "stage-private",
        "--directory",
        str(directory),
        "--source-sha",
        SOURCE,
        "--run-id",
        "987654321",
        "--run-attempt",
        "2",
    ]
    assert main(arguments) == 0
    assert (directory / "phase-a-public.json").is_file()
    assert (directory / "candidate.json").is_file()
