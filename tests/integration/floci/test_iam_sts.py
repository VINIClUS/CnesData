"""IAM/STS no Floci com enforcement: trust exato, duração de sessão e validação de credenciais."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

from tests.integration.floci._floci import ACCOUNT_ID, ADMIN, Credentials, outcome, unique_name
from tests.integration.floci._iam import (
    ASSUME_ANY_ROLE,
    IamUser,
    assume,
    create_role,
    create_user,
    delete_role,
    delete_user,
    policy,
    statement,
)

if TYPE_CHECKING:
    from contextlib import ExitStack

    from tests.integration.floci._floci import FlociContext

pytestmark = [pytest.mark.floci]

_SESSION = "sessao-cap"
_LIST_TABLES = statement("Allow", ["dynamodb:ListTables"], ["*"])
_FORGED = "material-forjado"


@dataclass(frozen=True, slots=True)
class _Trust:
    floci: FlociContext
    iam: Any
    trusted: IamUser
    outsider: IamUser
    role_name: str
    role_arn: str

    def sts(self, credentials: Credentials) -> Any:
        return self.floci.client("sts", credentials)


@pytest.fixture(scope="module")
def trust(floci: FlociContext, module_cleanup: ExitStack) -> _Trust:
    iam = floci.client("iam")
    trusted = create_user(iam, unique_name("confiavel"), [ASSUME_ANY_ROLE])
    module_cleanup.callback(delete_user, iam, trusted.name)
    outsider = create_user(iam, unique_name("intruso"), [ASSUME_ANY_ROLE])
    module_cleanup.callback(delete_user, iam, outsider.name)
    role_name = unique_name("dispatch")
    role_arn = create_role(iam, role_name, trusted.arn)
    module_cleanup.callback(delete_role, iam, role_name)
    return _Trust(floci, iam, trusted, outsider, role_name, role_arn)


def _seconds_until(expiration: datetime) -> float:
    return (expiration - datetime.now(UTC)).total_seconds()


def _user(trust: _Trust, cleanup: ExitStack, statements: list[dict[str, Any]]) -> IamUser:
    user = create_user(trust.iam, unique_name(), statements)
    cleanup.callback(delete_user, trust.iam, user.name)
    return user


def test_assume_role_com_trust_exato_aceita_o_principal_nomeado(trust: _Trust) -> None:
    credentials = assume(trust.sts(trust.trusted.credentials), trust.role_arn, _SESSION)

    identity = trust.sts(credentials).get_caller_identity()

    assert credentials.access_key_id.startswith("ASIA")
    assert identity["Arn"] == f"arn:aws:sts::{ACCOUNT_ID}:assumed-role/{trust.role_name}/{_SESSION}"
    assert identity["Account"] == ACCOUNT_ID


def test_assume_role_de_principal_fora_do_trust_e_negado(trust: _Trust) -> None:
    sts = trust.sts(trust.outsider.credentials)

    assert outcome(sts.assume_role, RoleArn=trust.role_arn, RoleSessionName=_SESSION) == (
        "AccessDenied"
    )


@pytest.mark.parametrize(
    ("resource", "expected"),
    [(None, "AccessDenied"), ("role", "AccessDenied"), ("*", "OK")],
    ids=["sem_politica", "recurso_arn_da_role", "recurso_curinga"],
)
def test_floci_exige_assume_role_na_identidade_avaliado_contra_curinga(
    trust: _Trust, cleanup: ExitStack, resource: str | None, expected: str,
) -> None:
    identity_statements = [] if resource is None else [
        statement("Allow", ["sts:AssumeRole"], [trust.role_arn if resource == "role" else "*"]),
    ]
    user = _user(trust, cleanup, identity_statements)
    role_name = unique_name("trust-exato")
    role_arn = create_role(trust.iam, role_name, user.arn)
    cleanup.callback(delete_role, trust.iam, role_name)

    sts = trust.sts(user.credentials)

    assert outcome(sts.assume_role, RoleArn=role_arn, RoleSessionName=_SESSION) == expected


def test_duracao_igual_ao_max_session_duration_e_aceita(trust: _Trust) -> None:
    sts = trust.sts(trust.trusted.credentials)

    response = sts.assume_role(
        RoleArn=trust.role_arn, RoleSessionName=_SESSION, DurationSeconds=3600,
    )

    assert 3540 < _seconds_until(response["Credentials"]["Expiration"]) <= 3600


@pytest.mark.parametrize("duration", [3601, 43200])
def test_floci_aceita_duration_seconds_acima_do_max_session_duration(
    trust: _Trust, duration: int,
) -> None:
    sts = trust.sts(trust.trusted.credentials)
    role = trust.iam.get_role(RoleName=trust.role_name)["Role"]

    response = sts.assume_role(
        RoleArn=trust.role_arn, RoleSessionName=_SESSION, DurationSeconds=duration,
    )

    assert role["MaxSessionDuration"] == 3600
    assert duration - 60 < _seconds_until(response["Credentials"]["Expiration"]) <= duration


def test_floci_aceita_session_tags_e_source_identity_sem_permissao_no_trust(
    trust: _Trust,
) -> None:
    sts = trust.sts(trust.trusted.credentials)
    tags = [{"Key": "tenant", "Value": "354130"}]

    tagged = outcome(sts.assume_role, RoleArn=trust.role_arn, RoleSessionName=_SESSION, Tags=tags)
    sourced = outcome(
        sts.assume_role, RoleArn=trust.role_arn, RoleSessionName=_SESSION,
        SourceIdentity="operador",
    )

    assert (tagged, sourced) == ("OK", "OK")


def test_politica_iam_vale_imediatamente_sem_atraso_de_propagacao(
    trust: _Trust, cleanup: ExitStack,
) -> None:
    user = _user(trust, cleanup, [])
    dynamodb = trust.floci.client("dynamodb", user.credentials)

    before = outcome(dynamodb.list_tables)
    trust.iam.put_user_policy(
        UserName=user.name, PolicyName="lista", PolicyDocument=policy(_LIST_TABLES),
    )
    granted = outcome(dynamodb.list_tables)
    trust.iam.delete_user_policy(UserName=user.name, PolicyName="lista")
    revoked = outcome(dynamodb.list_tables)

    assert (before, granted, revoked) == (
        "AccessDeniedException", "OK", "AccessDeniedException",
    )


def test_chave_test_ignora_o_enforcement_que_nega_usuario_sem_politica(
    trust: _Trust, cleanup: ExitStack,
) -> None:
    user = _user(trust, cleanup, [])

    denied = outcome(trust.floci.client("dynamodb", user.credentials).list_tables)
    bypassed = outcome(trust.floci.client("dynamodb", ADMIN).list_tables)

    assert (denied, bypassed) == ("AccessDeniedException", "OK")


def test_chave_desconhecida_e_rejeitada_exceto_no_get_caller_identity(trust: _Trust) -> None:
    unknown = Credentials("chave-ficticia-desconhecida", "inexistente")
    floci = trust.floci

    identity = floci.client("sts", unknown).get_caller_identity()

    assert outcome(floci.client("dynamodb", unknown).list_tables) == "UnrecognizedClientException"
    assert outcome(floci.client("s3", unknown).list_buckets) == "InvalidAccessKeyId"
    assert outcome(floci.client("iam", unknown).list_users) == "InvalidClientTokenId"
    assert identity["Arn"] == f"arn:aws:iam::{ACCOUNT_ID}:root"


def test_floci_verifica_assinatura_sigv4_somente_no_s3(trust: _Trust, cleanup: ExitStack) -> None:
    allowed = statement("Allow", ["dynamodb:ListTables", "s3:ListAllMyBuckets"], ["*"])
    user = _user(trust, cleanup, [allowed])
    forged = replace(user.credentials, secret_access_key=_FORGED)
    floci = trust.floci

    assert outcome(floci.client("s3", user.credentials).list_buckets) == "OK"
    assert outcome(floci.client("s3", forged).list_buckets) == "SignatureDoesNotMatch"
    assert outcome(floci.client("dynamodb", forged).list_tables) == "OK"
    assert floci.client("sts", forged).get_caller_identity()["Arn"] == user.arn


def test_floci_aceita_sessao_assumida_sem_token_ou_com_token_forjado_fora_do_s3(
    trust: _Trust, cleanup: ExitStack,
) -> None:
    trust.iam.put_role_policy(
        RoleName=trust.role_name, PolicyName="lista", PolicyDocument=policy(_LIST_TABLES),
    )
    cleanup.callback(trust.iam.delete_role_policy, RoleName=trust.role_name, PolicyName="lista")
    session = assume(trust.sts(trust.trusted.credentials), trust.role_arn, _SESSION)
    tokenless = replace(session, session_token=None)
    forged = replace(session, session_token=_FORGED)
    floci = trust.floci

    assert outcome(floci.client("dynamodb", session).list_tables) == "OK"
    assert outcome(floci.client("dynamodb", tokenless).list_tables) == "OK"
    assert outcome(floci.client("dynamodb", forged).list_tables) == "OK"
    assert outcome(floci.client("s3", tokenless).list_buckets) == "InvalidAccessKeyId"


def test_floci_aceita_chave_inativa_fora_do_s3(trust: _Trust, cleanup: ExitStack) -> None:
    allowed = statement("Allow", ["dynamodb:ListTables", "s3:ListAllMyBuckets"], ["*"])
    user = _user(trust, cleanup, [allowed])
    trust.iam.update_access_key(
        UserName=user.name, AccessKeyId=user.credentials.access_key_id, Status="Inactive",
    )

    assert outcome(trust.floci.client("dynamodb", user.credentials).list_tables) == "OK"
    assert outcome(trust.floci.client("s3", user.credentials).list_buckets) == "InvalidAccessKeyId"
