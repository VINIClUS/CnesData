"""Matrizes IAM no Floci: role de dispatch por dynamodb:LeadingKeys e roles por prefixo S3."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from cnes_infra.control_plane.dynamodb_keys import outbox_key, unit_key
from tests.integration.floci._dynamodb import create_canonical_table, delete_table
from tests.integration.floci._floci import ADMIN, http_client, outcome, unique_name
from tests.integration.floci._iam import (
    ASSUME_ANY_ROLE,
    IamUser,
    assume,
    create_role,
    create_user,
    delete_role,
    delete_user,
    policy,
    put_role_policy,
    statement,
)
from tests.integration.floci._s3 import create_bucket, delete_bucket

if TYPE_CHECKING:
    from contextlib import ExitStack

    from tests.integration.floci._floci import FlociContext

pytestmark = [pytest.mark.floci]

_DENIED = "AccessDeniedException"
_PENDING = "OUTBOX#PENDING"
_OUTBOX = outbox_key("evt-cap")
_CURSOR = ("OUTBOX#CURSOR", "CURSOR")
_FENCE = ("FENCE#prod", "FENCE")
_SEMAPHORE = ("SEMAPHORE#runtime", "SEMAPHORE")
_RUN = unit_key("354130", "run-cap", "unit-01")
_SERVING = "serving/relatorio.json"
_RAW = "raw/arquivo.json"
_AUDIT = "audit/semente.json"


@dataclass(frozen=True, slots=True)
class _Matrix:
    floci: FlociContext
    iam: Any
    operator: IamUser
    table: str
    table_arn: str
    bucket: str


@dataclass(frozen=True, slots=True)
class _Session:
    dynamodb: Any
    s3: Any


def _key(key: tuple[str, str]) -> dict[str, dict[str, str]]:
    return {"pk": {"S": key[0]}, "sk": {"S": key[1]}}


def _index(name: str, partition: str) -> dict[str, dict[str, str]]:
    return {f"{name}pk": {"S": partition}, f"{name}sk": {"S": "2026-01-01T00:00:00"}}


def _seed(matrix: _Matrix) -> None:
    dynamodb = matrix.floci.client("dynamodb")
    outbox = {**_key(_OUTBOX), **_index("gsi6", _PENDING), **_index("gsi1", _PENDING)}
    fence = {**_key(_FENCE), **_index("gsi6", "FENCE#PENDING")}
    for item in (outbox, fence, _key(_CURSOR), _key(_SEMAPHORE), _key(_RUN)):
        dynamodb.put_item(TableName=matrix.table, Item={**item, "v": {"N": "1"}})
    s3 = matrix.floci.client("s3")
    for key in (_SERVING, _RAW):
        s3.put_object(Bucket=matrix.bucket, Key=key, Body=b"{}")
    s3.put_object(
        Bucket=matrix.bucket, Key=_AUDIT, Body=b"{}", ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=datetime.now(UTC) + timedelta(hours=1),
    )


@pytest.fixture(scope="module")
def matrix(floci: FlociContext, module_cleanup: ExitStack) -> _Matrix:
    iam, dynamodb, s3 = (floci.client(name) for name in ("iam", "dynamodb", "s3"))
    table, bucket = unique_name(), unique_name()
    table_arn = create_canonical_table(dynamodb, table)
    module_cleanup.callback(delete_table, dynamodb, table)
    create_bucket(s3, bucket)
    module_cleanup.callback(delete_bucket, s3, bucket)
    operator = create_user(iam, unique_name("operador"), [ASSUME_ANY_ROLE])
    module_cleanup.callback(delete_user, iam, operator.name)
    created = _Matrix(floci, iam, operator, table, table_arn, bucket)
    _seed(created)
    return created


def _session(matrix: _Matrix, cleanup: ExitStack, statements: list[dict[str, Any]]) -> _Session:
    role_name = unique_name("role")
    role_arn = create_role(matrix.iam, role_name, matrix.operator.arn)
    cleanup.callback(delete_role, matrix.iam, role_name)
    put_role_policy(matrix.iam, role_name, statements)
    sts = matrix.floci.client("sts", matrix.operator.credentials)
    credentials = assume(sts, role_arn, "sessao-cap")
    return _Session(
        matrix.floci.client("dynamodb", credentials), matrix.floci.client("s3", credentials),
    )


def _leading(*partitions: str) -> dict[str, Any]:
    return {"ForAllValues:StringEquals": {"dynamodb:LeadingKeys": list(partitions)}}


def _dispatch_statements(matrix: _Matrix) -> list[dict[str, Any]]:
    bucket = f"arn:aws:s3:::{matrix.bucket}"
    items = statement("Allow", ["dynamodb:GetItem", "dynamodb:UpdateItem"], [matrix.table_arn])
    pending = statement("Allow", ["dynamodb:Query"], [f"{matrix.table_arn}/index/gsi6"])
    return [
        {**items, "Condition": _leading(_OUTBOX[0], _CURSOR[0])},
        {**pending, "Condition": _leading(_PENDING)},
        statement("Allow", ["s3:GetBucketObjectLockConfiguration"], [bucket]),
        statement("Allow", ["s3:GetObject", "s3:PutObject", "s3:PutObjectRetention"], [
            f"{bucket}/audit/*",
        ]),
    ]


@pytest.fixture(scope="module")
def dispatch(matrix: _Matrix, module_cleanup: ExitStack) -> _Session:
    return _session(matrix, module_cleanup, _dispatch_statements(matrix))


@pytest.fixture(scope="module")
def api(matrix: _Matrix, module_cleanup: ExitStack) -> _Session:
    serving = statement("Allow", ["s3:GetObject"], [f"arn:aws:s3:::{matrix.bucket}/serving/*"])
    return _session(matrix, module_cleanup, [serving])


def _get(matrix: _Matrix, session: _Session, key: tuple[str, str]) -> str:
    return outcome(session.dynamodb.get_item, TableName=matrix.table, Key=_key(key))


def _update(matrix: _Matrix, session: _Session, key: tuple[str, str]) -> str:
    return outcome(
        session.dynamodb.update_item, TableName=matrix.table, Key=_key(key),
        UpdateExpression="SET v = :v", ExpressionAttributeValues={":v": {"N": "2"}},
    )


def _transact_update(matrix: _Matrix, session: _Session, key: tuple[str, str]) -> str:
    update = {
        "TableName": matrix.table, "Key": _key(key), "UpdateExpression": "SET v = :v",
        "ExpressionAttributeValues": {":v": {"N": "3"}},
    }
    return outcome(session.dynamodb.transact_write_items, TransactItems=[{"Update": update}])


def _query(matrix: _Matrix, session: _Session, index: str, partition: str) -> str:
    return outcome(
        session.dynamodb.query, TableName=matrix.table, IndexName=index,
        KeyConditionExpression=f"{index}pk = :p",
        ExpressionAttributeValues={":p": {"S": partition}},
    )


@pytest.mark.parametrize("key", [_OUTBOX, _CURSOR], ids=["outbox", "cursor"])
def test_role_de_dispatch_le_e_atualiza_particoes_de_outbox_e_cursor(
    matrix: _Matrix, dispatch: _Session, key: tuple[str, str],
) -> None:
    assert (_get(matrix, dispatch, key), _update(matrix, dispatch, key)) == ("OK", "OK")


def test_role_de_dispatch_nao_le_chave_de_fence(matrix: _Matrix, dispatch: _Session) -> None:
    assert (_get(matrix, dispatch, _FENCE), _update(matrix, dispatch, _FENCE)) == (_DENIED, _DENIED)


@pytest.mark.parametrize("key", [_SEMAPHORE, _RUN], ids=["semaforo", "run"])
def test_role_de_dispatch_nao_le_nem_atualiza_semaforo_ou_run(
    matrix: _Matrix, dispatch: _Session, key: tuple[str, str],
) -> None:
    assert (_get(matrix, dispatch, key), _update(matrix, dispatch, key)) == (_DENIED, _DENIED)


def test_role_de_dispatch_nao_cria_nao_apaga_nem_varre(matrix: _Matrix, dispatch: _Session) -> None:
    dynamodb = dispatch.dynamodb
    created = {"pk": {"S": _OUTBOX[0]}, "sk": {"S": "OUTBOX#novo"}}
    base_query = {"KeyConditionExpression": "pk = :p", "ExpressionAttributeValues": {
        ":p": {"S": _OUTBOX[0]},
    }}

    assert outcome(dynamodb.put_item, TableName=matrix.table, Item=created) == _DENIED
    assert outcome(dynamodb.delete_item, TableName=matrix.table, Key=_key(_OUTBOX)) == _DENIED
    assert outcome(dynamodb.scan, TableName=matrix.table) == _DENIED
    assert outcome(dynamodb.query, TableName=matrix.table, **base_query) == _DENIED


def test_floci_nega_query_no_gsi_exato_concedido_pelo_arn_do_indice(
    matrix: _Matrix, dispatch: _Session,
) -> None:
    assert _query(matrix, dispatch, "gsi6", _PENDING) == _DENIED


def test_floci_nega_transacao_da_role_de_dispatch_em_particao_permitida(
    matrix: _Matrix, dispatch: _Session,
) -> None:
    assert _update(matrix, dispatch, _OUTBOX) == "OK"
    assert _transact_update(matrix, dispatch, _OUTBOX) == _DENIED


def test_floci_autoriza_query_de_indice_pelo_arn_da_tabela(
    matrix: _Matrix, cleanup: ExitStack,
) -> None:
    table_query = statement("Allow", ["dynamodb:Query"], [matrix.table_arn])
    session = _session(matrix, cleanup, [{**table_query, "Condition": _leading(_PENDING)}])

    assert _query(matrix, session, "gsi6", _PENDING) == "OK"
    assert _query(matrix, session, "gsi6", "FENCE#PENDING") == _DENIED
    assert _query(matrix, session, "gsi1", _PENDING) == "OK"


def test_floci_nao_aplica_deny_por_leading_keys_a_transacoes(
    matrix: _Matrix, cleanup: ExitStack,
) -> None:
    deny_fence = statement("Deny", ["dynamodb:*"], [matrix.table_arn])
    guardrail = {**deny_fence, "Condition": {
        "ForAnyValue:StringLike": {"dynamodb:LeadingKeys": ["FENCE#*"]},
    }}
    session = _session(matrix, cleanup, [
        statement("Allow", ["dynamodb:*"], [matrix.table_arn]), guardrail,
    ])

    assert _update(matrix, session, _FENCE) == _DENIED
    assert _transact_update(matrix, session, _FENCE) == "OK"


def test_role_de_dispatch_grava_le_e_estende_retencao_em_audit(
    matrix: _Matrix, dispatch: _Session,
) -> None:
    s3, bucket, key = dispatch.s3, matrix.bucket, f"audit/{uuid4().hex}.json"
    until = datetime.now(UTC).replace(microsecond=0) + timedelta(hours=1)
    version = s3.put_object(
        Bucket=bucket, Key=key, Body=b"{}", ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=until,
    )["VersionId"]
    extended = {"Mode": "GOVERNANCE", "RetainUntilDate": until + timedelta(hours=1)}
    s3.put_object_retention(Bucket=bucket, Key=key, VersionId=version, Retention=extended)

    lock = s3.get_object_lock_configuration(Bucket=bucket)["ObjectLockConfiguration"]
    stored = matrix.floci.client("s3", ADMIN).get_object_retention(
        Bucket=bucket, Key=key, VersionId=version,
    )["Retention"]

    assert lock["ObjectLockEnabled"] == "Enabled"
    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == b"{}"
    assert stored == extended


def test_role_de_dispatch_nao_apaga_audit_nem_faz_bypass(
    matrix: _Matrix, dispatch: _Session,
) -> None:
    s3, bucket = dispatch.s3, matrix.bucket
    version = matrix.floci.client("s3").head_object(Bucket=bucket, Key=_AUDIT)["VersionId"]
    target = {"Bucket": bucket, "Key": _AUDIT}

    assert outcome(s3.delete_object, **target) == "AccessDenied"
    assert outcome(s3.delete_object, **target, VersionId=version) == "AccessDenied"
    assert outcome(
        s3.delete_object, **target, VersionId=version, BypassGovernanceRetention=True,
    ) == "AccessDenied"
    assert outcome(s3.list_objects_v2, Bucket=bucket) == "AccessDenied"


@pytest.mark.parametrize("key", [_RAW, _SERVING], ids=["raw", "serving"])
def test_role_de_dispatch_nao_le_fora_de_audit(
    matrix: _Matrix, dispatch: _Session, key: str,
) -> None:
    assert outcome(dispatch.s3.get_object, Bucket=matrix.bucket, Key=key) == "AccessDenied"


@pytest.mark.parametrize(
    ("key", "expected"),
    [(_SERVING, "OK"), (_RAW, "AccessDenied"), (_AUDIT, "AccessDenied")],
    ids=["serving", "raw", "audit"],
)
def test_role_da_api_le_somente_serving(
    matrix: _Matrix, api: _Session, key: str, expected: str,
) -> None:
    assert outcome(api.s3.get_object, Bucket=matrix.bucket, Key=key) == expected


def test_role_da_api_nao_grava_em_serving(matrix: _Matrix, api: _Session) -> None:
    written = outcome(api.s3.put_object, Bucket=matrix.bucket, Key="serving/novo.json", Body=b"{}")

    assert written == "AccessDenied"


@pytest.mark.parametrize(("key", "status"), [(_SERVING, 200), (_RAW, 403)], ids=["serving", "raw"])
def test_url_assinada_da_role_da_api_so_serve_o_prefixo_serving(
    matrix: _Matrix, api: _Session, key: str, status: int,
) -> None:
    url = api.s3.generate_presigned_url(
        "get_object", Params={"Bucket": matrix.bucket, "Key": key}, ExpiresIn=60,
    )

    with http_client() as http:
        assert http.get(url).status_code == status


def _policy_bucket(matrix: _Matrix, cleanup: ExitStack) -> tuple[Any, str]:
    s3, bucket = matrix.floci.client("s3"), unique_name()
    create_bucket(s3, bucket, object_lock=False)
    cleanup.callback(delete_bucket, s3, bucket)
    s3.put_object(Bucket=bucket, Key=_SERVING, Body=b"{}")
    return s3, bucket


def _principal_s3(
    matrix: _Matrix, cleanup: ExitStack, statements: list[dict[str, Any]],
) -> tuple[Any, str]:
    user = create_user(matrix.iam, unique_name(), statements)
    cleanup.callback(delete_user, matrix.iam, user.name)
    return matrix.floci.client("s3", user.credentials), user.arn


def test_floci_avalia_deny_da_bucket_policy_sobre_allow_de_identidade(
    matrix: _Matrix, cleanup: ExitStack,
) -> None:
    admin, bucket = _policy_bucket(matrix, cleanup)
    reader, arn = _principal_s3(matrix, cleanup, [
        statement("Allow", ["s3:GetObject"], [f"arn:aws:s3:::{bucket}/*"]),
    ])
    before = outcome(reader.get_object, Bucket=bucket, Key=_SERVING)
    deny = statement("Deny", ["s3:GetObject"], [f"arn:aws:s3:::{bucket}/*"])
    admin.put_bucket_policy(Bucket=bucket, Policy=policy({**deny, "Principal": {"AWS": arn}}))

    assert before == "OK"
    assert outcome(reader.get_object, Bucket=bucket, Key=_SERVING) == "AccessDenied"
    assert outcome(admin.get_object, Bucket=bucket, Key=_SERVING) == "OK"


def test_floci_concede_acesso_pela_bucket_policy_sem_politica_de_identidade(
    matrix: _Matrix, cleanup: ExitStack,
) -> None:
    admin, bucket = _policy_bucket(matrix, cleanup)
    writer, arn = _principal_s3(matrix, cleanup, [])
    grant = statement("Allow", ["s3:PutObject"], [f"arn:aws:s3:::{bucket}/*"])
    admin.put_bucket_policy(Bucket=bucket, Policy=policy({**grant, "Principal": {"AWS": arn}}))

    assert outcome(writer.put_object, Bucket=bucket, Key="serving/novo.json", Body=b"{}") == "OK"
    assert outcome(writer.get_object, Bucket=bucket, Key=_SERVING) == "AccessDenied"
