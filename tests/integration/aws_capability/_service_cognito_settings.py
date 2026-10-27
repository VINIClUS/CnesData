"""Validação e preservação fechada das configurações Cognito."""

from collections.abc import Mapping

from tests.integration.aws_capability._safety import tags
from tests.integration.aws_capability._service_commands import (
    call_command,
    require_data,
    require_error,
)
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

_IMMUTABLE = {"Id", "Name", "Arn", "UsernameAttributes", "UsernameConfiguration"}
_READ_ONLY = {
    "CreationDate",
    "LastModifiedDate",
    "Status",
    "SchemaAttributes",
    "EstimatedNumberOfUsers",
}
_UPDATE = {
    "Policies",
    "LambdaConfig",
    "AutoVerifiedAttributes",
    "SmsVerificationMessage",
    "EmailVerificationMessage",
    "EmailVerificationSubject",
    "VerificationMessageTemplate",
    "SmsAuthenticationMessage",
    "UserAttributeUpdateSettings",
    "MfaConfiguration",
    "DeviceConfiguration",
    "EmailConfiguration",
    "AdminCreateUserConfig",
    "AccountRecoverySetting",
    "UserPoolTier",
    "UserPoolTags",
    "KeyConfiguration",
    "IssuerConfiguration",
}
_KNOWN = (
    _IMMUTABLE
    | _READ_ONLY
    | _UPDATE
    | {
        "DeletionProtection",
        "KeyConfiguration",
        "IssuerConfiguration",
        "AcrConfiguration",
        "Domain",
        "CustomDomain",
        "SmsConfiguration",
        "UserPoolAddOns",
    }
)


def read_snapshot(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> tuple[Mapping[str, object], dict[str, str]]:
    reply = call_command(
        commands,
        Request("cognito-idp", "describe-user-pool", {"UserPoolId": resource.resource_id}),
        context,
    )
    if not reply.ok:
        require_error(reply)
        raise ServiceFailure("service=cognito_describe_failed")
    data = require_data(reply)
    pool = data.get("UserPool")
    if "UserPool" not in data:
        raise ServiceFailure("service=not_implemented")
    if not isinstance(pool, Mapping):
        raise ServiceFailure("service=cognito_describe_invalid")
    _identity(pool, spec, resource)
    arn = pool["Arn"]
    tag_reply = call_command(
        commands, Request("cognito-idp", "list-tags-for-resource", {"ResourceArn": arn}), context
    )
    tag_data = require_data(tag_reply)
    raw_tags = tag_data.get("Tags")
    if not isinstance(raw_tags, Mapping) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in raw_tags.items()
    ):
        raise ServiceFailure("service=cognito_tags_invalid")
    fresh_tags = dict(raw_tags)
    described_tags = pool.get("UserPoolTags")
    if "UserPoolTags" in pool and described_tags != fresh_tags:
        raise ServiceFailure("service=cognito_tags_mismatch")
    if any(fresh_tags.get(key) != value for key, value in tags(spec).items()):
        raise ServiceFailure("service=cognito_owner_invalid")
    return pool, fresh_tags


def validate_snapshot(
    pool: Mapping[str, object], tags_value: Mapping[str, str], spec: RunSpec, protection: str
) -> None:
    if set(pool) - _KNOWN or pool.get("UserPoolTier") != "ESSENTIALS":
        raise ServiceFailure("service=cognito_settings_invalid")
    if pool.get("DeletionProtection") != protection or pool.get("MfaConfiguration") != "OFF":
        raise ServiceFailure("service=cognito_settings_invalid")
    if pool.get("UsernameAttributes") != ["email"] or not _case_sensitive(pool):
        raise ServiceFailure("service=cognito_settings_invalid")
    if not _admin_only(pool) or not _recovery(pool) or not _policy(pool):
        raise ServiceFailure("service=cognito_settings_invalid")
    if pool.get("LambdaConfig") != {} or not _email_default(pool):
        raise ServiceFailure("service=cognito_settings_invalid")
    message = pool.get("VerificationMessageTemplate")
    if not isinstance(message, Mapping) or message.get("DefaultEmailOption") != "CONFIRM_WITH_CODE":
        raise ServiceFailure("service=cognito_settings_invalid")
    if any(
        pool.get(key) not in (None, {}, [])
        for key in ("Domain", "CustomDomain", "SmsConfiguration")
    ):
        raise ServiceFailure("service=cognito_settings_invalid")
    _special_fields(pool)
    if "UserPoolTags" in pool and dict(tags_value) != pool["UserPoolTags"]:
        raise ServiceFailure("service=cognito_tags_mismatch")
    if any(tags_value.get(key) != value for key, value in tags(spec).items()):
        raise ServiceFailure("service=cognito_owner_invalid")


def update_payload(
    pool: Mapping[str, object], tags_value: Mapping[str, str], resource: Resource
) -> dict[str, object]:
    result: dict[str, object] = {"UserPoolId": resource.resource_id}
    for key in _UPDATE:
        if key == "UserPoolTags":
            result[key] = dict(tags_value)
        elif key in pool:
            result[key] = pool[key]
    result["DeletionProtection"] = "INACTIVE"
    return result


def preserve(before: Mapping[str, object], after: Mapping[str, object]) -> None:
    for key in _UPDATE:
        if key in before and (key not in after or after[key] != before[key]):
            raise ServiceFailure("service=cognito_settings_changed")


def _identity(pool: Mapping[str, object], spec: RunSpec, resource: Resource) -> None:
    expected = f"arn:aws:cognito-idp:{spec.region}:{spec.account}:userpool/{resource.resource_id}"
    if pool.get("Id") != resource.resource_id or pool.get("Name") != resource.name:
        raise ServiceFailure("service=cognito_identity_invalid")
    if pool.get("Arn") != expected:
        raise ServiceFailure("service=cognito_identity_invalid")


def _case_sensitive(pool: Mapping[str, object]) -> bool:
    value = pool.get("UsernameConfiguration")
    return (
        isinstance(value, Mapping)
        and type(value.get("CaseSensitive")) is bool
        and not value["CaseSensitive"]
    )


def _admin_only(pool: Mapping[str, object]) -> bool:
    value = pool.get("AdminCreateUserConfig")
    return (
        isinstance(value, Mapping)
        and type(value.get("AllowAdminCreateUserOnly")) is bool
        and value["AllowAdminCreateUserOnly"]
    )


def _recovery(pool: Mapping[str, object]) -> bool:
    value = pool.get("AccountRecoverySetting")
    methods = value.get("RecoveryMechanisms") if isinstance(value, Mapping) else None
    return methods == [{"Name": "verified_email", "Priority": 1}]


def _policy(pool: Mapping[str, object]) -> bool:
    value = pool.get("Policies")
    policy = value.get("PasswordPolicy") if isinstance(value, Mapping) else None
    if not isinstance(policy, Mapping):
        return False
    required = {"RequireLowercase", "RequireUppercase", "RequireNumbers", "RequireSymbols"}
    if (
        not isinstance(policy.get("MinimumLength"), int)
        or policy["MinimumLength"] < 14
        or any(type(policy.get(key)) is not bool or not policy[key] for key in required)
    ):
        return False
    temporary = policy.get("TemporaryPasswordValidityDays")
    return temporary is None or type(temporary) is int


def _email_default(pool: Mapping[str, object]) -> bool:
    value = pool.get("EmailConfiguration")
    return isinstance(value, Mapping) and value.get("EmailSendingAccount") == "COGNITO_DEFAULT"


def _special_fields(pool: Mapping[str, object]) -> None:
    key = pool.get("KeyConfiguration")
    if key is not None and key != {"KeyType": "AWS_OWNED_KEY"}:
        raise ServiceFailure("service=cognito_settings_invalid")
    issuer = pool.get("IssuerConfiguration")
    if issuer is not None and issuer != {"Type": "ORIGINAL"}:
        raise ServiceFailure("service=cognito_settings_invalid")
    if pool.get("AcrConfiguration") not in (None, {}):
        raise ServiceFailure("service=cognito_settings_invalid")
    if pool.get("UserPoolAddOns") not in (None, {}):
        raise ServiceFailure("service=cognito_settings_invalid")
