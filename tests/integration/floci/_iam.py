"""IAM/STS efêmeros dos cenários: usuários com chave, roles com trust exato e sessões assumidas."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from tests.integration.floci._floci import Credentials

if TYPE_CHECKING:
    from collections.abc import Sequence

ASSUME_ANY_ROLE = {"Effect": "Allow", "Action": "sts:AssumeRole", "Resource": "*"}
_POLICY_NAME = "cenario"


def policy(*statements: dict[str, Any]) -> str:
    """Returns: documento de política IAM serializado com as declarações dadas."""
    return json.dumps({"Version": "2012-10-17", "Statement": list(statements)})


def statement(effect: str, actions: Sequence[str], resources: Sequence[str]) -> dict[str, Any]:
    """Returns: declaração IAM com efeito, ações e recursos explícitos."""
    return {"Effect": effect, "Action": list(actions), "Resource": list(resources)}


@dataclass(frozen=True, slots=True)
class IamUser:
    name: str
    arn: str
    credentials: Credentials


def create_user(iam: Any, name: str, statements: Sequence[dict[str, Any]] = ()) -> IamUser:
    """Cria usuário com chave de acesso e, se houver declarações, uma política inline.

    Returns: ARN e credenciais do usuário criado.
    """
    arn = iam.create_user(UserName=name)["User"]["Arn"]
    try:
        key = iam.create_access_key(UserName=name)["AccessKey"]
        if statements:
            iam.put_user_policy(
                UserName=name, PolicyName=_POLICY_NAME, PolicyDocument=policy(*statements),
            )
    except BaseException:
        delete_user(iam, name)
        raise
    return IamUser(name, arn, Credentials(key["AccessKeyId"], key["SecretAccessKey"]))


def delete_user(iam: Any, name: str) -> None:
    """Remove chaves, políticas inline e o usuário."""
    for key in iam.list_access_keys(UserName=name)["AccessKeyMetadata"]:
        iam.delete_access_key(UserName=name, AccessKeyId=key["AccessKeyId"])
    for policy_name in iam.list_user_policies(UserName=name)["PolicyNames"]:
        iam.delete_user_policy(UserName=name, PolicyName=policy_name)
    iam.delete_user(UserName=name)


def create_role(iam: Any, name: str, trusted_arn: str, max_session_seconds: int = 3600) -> str:
    """Cria role cujo trust aceita só o ARN informado em `sts:AssumeRole`.

    Returns: ARN da role.
    """
    trust = policy(
        {"Effect": "Allow", "Principal": {"AWS": trusted_arn}, "Action": "sts:AssumeRole"},
    )
    role = iam.create_role(
        RoleName=name, AssumeRolePolicyDocument=trust, MaxSessionDuration=max_session_seconds,
    )
    return role["Role"]["Arn"]


def put_role_policy(iam: Any, name: str, statements: Sequence[dict[str, Any]]) -> None:
    """Substitui a política inline do cenário na role."""
    iam.put_role_policy(RoleName=name, PolicyName=_POLICY_NAME, PolicyDocument=policy(*statements))


def delete_role(iam: Any, name: str) -> None:
    """Remove políticas inline e a role."""
    for policy_name in iam.list_role_policies(RoleName=name)["PolicyNames"]:
        iam.delete_role_policy(RoleName=name, PolicyName=policy_name)
    iam.delete_role(RoleName=name)


def assume(sts: Any, role_arn: str, session_name: str, **options: Any) -> Credentials:
    """Returns: credenciais temporárias da sessão assumida."""
    response = sts.assume_role(RoleArn=role_arn, RoleSessionName=session_name, **options)
    issued = response["Credentials"]
    return Credentials(issued["AccessKeyId"], issued["SecretAccessKey"], issued["SessionToken"])
