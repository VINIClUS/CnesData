"""Contratos públicos de ID remoto, tags opcionais e policy por fase."""

import json
from typing import Any

import pytest

from tests.integration.aws_capability._service_cognito_root_testing import (
    ROOT,
    RootLab,
    assert_private_channels,
    literal_reply,
    load_replies,
    load_requests,
    root_lab,
)
from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability.contracts import (
    Creation,
    RequestContext,
    Resource,
    ServiceFailure,
)

__all__ = ["root_lab"]

LITERALS = r"""{
"requests":{
  "D_C":{"service":"cognito-idp","operation":"describe-user-pool","region":"us-east-2",
    "stdin":{"UserPoolId":"us-east-2_a1b2c3d4"}},
  "T_C":{"service":"cognito-idp","operation":"list-tags-for-resource","region":"us-east-2",
    "stdin":{
      "ResourceArn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_a1b2c3d4"}},
  "LC_C":{"service":"cognito-idp","operation":"list-user-pool-clients","region":"us-east-2",
    "stdin":{"UserPoolId":"us-east-2_a1b2c3d4","MaxResults":60}},
  "LS_C":{"service":"cognito-idp","operation":"list-resource-servers","region":"us-east-2",
    "stdin":{"UserPoolId":"us-east-2_a1b2c3d4","MaxResults":50}},
  "LU_C":{"service":"cognito-idp","operation":"list-users","region":"us-east-2",
    "stdin":{"UserPoolId":"us-east-2_a1b2c3d4","Limit":60}},
  "U_C":{"service":"cognito-idp","operation":"update-user-pool","region":"us-east-2",
    "stdin":{
      "UserPoolId":"us-east-2_a1b2c3d4","UserPoolTier":"ESSENTIALS",
      "DeletionProtection":"INACTIVE","MfaConfiguration":"OFF",
      "AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
      "AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
      "Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,
        "RequireUppercase":true,"RequireNumbers":true,"RequireSymbols":true,
        "TemporaryPasswordValidityDays":7}},
      "LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
      "VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
      "UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability",
        "Run":"a1b2c3d4","ExpiresAt":"1060"}}},
  "X_C":{"service":"cognito-idp","operation":"delete-user-pool","region":"us-east-2",
    "stdin":{"UserPoolId":"us-east-2_a1b2c3d4"}},
  "U15":{"service":"cognito-idp","operation":"update-user-pool","region":"us-east-2",
    "stdin":{
      "UserPoolId":"us-east-2_PoolRoot123","UserPoolTier":"ESSENTIALS",
      "DeletionProtection":"INACTIVE","MfaConfiguration":"OFF",
      "AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
      "AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
      "Policies":{"PasswordPolicy":{"MinimumLength":15,"RequireLowercase":true,
        "RequireUppercase":true,"RequireNumbers":true,"RequireSymbols":true,
        "TemporaryPasswordValidityDays":7}},
      "LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
      "VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
      "UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability",
        "Run":"a1b2c3d4","ExpiresAt":"1060"}}}
},
"replies":{
  "coincident_create":{"UserPool":{
    "Id":"us-east-2_a1b2c3d4","Name":"cnesdata-cap-a1b2c3d4",
    "Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_a1b2c3d4"}},
  "coincident_active":{"UserPool":{
    "Id":"us-east-2_a1b2c3d4","Name":"cnesdata-cap-a1b2c3d4",
    "Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_a1b2c3d4",
    "UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
    "UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
    "AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
    "AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
    "Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,
      "RequireUppercase":true,"RequireNumbers":true,"RequireSymbols":true,
      "TemporaryPasswordValidityDays":7}},
    "LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
    "VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
    "UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability",
      "Run":"a1b2c3d4","ExpiresAt":"1060"}}},
  "coincident_inactive":{"UserPool":{
    "Id":"us-east-2_a1b2c3d4","Name":"cnesdata-cap-a1b2c3d4",
    "Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_a1b2c3d4",
    "UserPoolTier":"ESSENTIALS","DeletionProtection":"INACTIVE","MfaConfiguration":"OFF",
    "UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
    "AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
    "AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
    "Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,
      "RequireUppercase":true,"RequireNumbers":true,"RequireSymbols":true,
      "TemporaryPasswordValidityDays":7}},
    "LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
    "VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
    "UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability",
      "Run":"a1b2c3d4","ExpiresAt":"1060"}}},
  "describe_no_tags_active":{"UserPool":{
    "Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
    "Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
    "UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
    "UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
    "AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
    "AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
    "Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,
      "RequireUppercase":true,"RequireNumbers":true,"RequireSymbols":true,
      "TemporaryPasswordValidityDays":7}},
    "LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
    "VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"}}},
  "describe_no_tags_inactive":{"UserPool":{
    "Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
    "Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
    "UserPoolTier":"ESSENTIALS","DeletionProtection":"INACTIVE","MfaConfiguration":"OFF",
    "UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
    "AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
    "AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
    "Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,
      "RequireUppercase":true,"RequireNumbers":true,"RequireSymbols":true,
      "TemporaryPasswordValidityDays":7}},
    "LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
    "VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"}}},
  "minimum15_active":{"UserPool":{
    "Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
    "Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
    "UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
    "UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
    "AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
    "AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
    "Policies":{"PasswordPolicy":{"MinimumLength":15,"RequireLowercase":true,
      "RequireUppercase":true,"RequireNumbers":true,"RequireSymbols":true,
      "TemporaryPasswordValidityDays":7}},
    "LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
    "VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
    "UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability",
      "Run":"a1b2c3d4","ExpiresAt":"1060"}}},
  "minimum15_inactive":{"UserPool":{
    "Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
    "Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
    "UserPoolTier":"ESSENTIALS","DeletionProtection":"INACTIVE","MfaConfiguration":"OFF",
    "UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
    "AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
    "AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
    "Policies":{"PasswordPolicy":{"MinimumLength":15,"RequireLowercase":true,
      "RequireUppercase":true,"RequireNumbers":true,"RequireSymbols":true,
      "TemporaryPasswordValidityDays":7}},
    "LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
    "VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
    "UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability",
      "Run":"a1b2c3d4","ExpiresAt":"1060"}}}
},
"traces":{
  "remote_id_accepted":{"calls":19,"work":1,"cleanup":18,"steps":[
    ["C","coincident_create"],
    ["D_C","coincident_active"],["T_C","tags_owner"],["D_C","coincident_active"],
    ["D_C","coincident_active"],["T_C","tags_owner"],["LC_C","clients_empty"],
    ["D_C","coincident_active"],["T_C","tags_owner"],["LS_C","servers_empty"],
    ["D_C","coincident_active"],["T_C","tags_owner"],["LU_C","users_empty"],
    ["D_C","coincident_active"],["T_C","tags_owner"],["U_C","ok_empty"],
    ["D_C","coincident_inactive"],["T_C","tags_owner"],["X_C","ok_empty"]]},
  "optional_tags_prepare":{"calls":2,"work":2,"cleanup":0,"steps":[
    ["D","describe_no_tags_active"],["T","tags_owner"]]},
  "optional_tags_cleanup":{"calls":18,"work":0,"cleanup":18,"steps":[
    ["D","pool_active"],["T","tags_owner"],["D","pool_active"],
    ["D","pool_active"],["T","tags_owner"],["LC","clients_empty"],
    ["D","pool_active"],["T","tags_owner"],["LS","servers_empty"],
    ["D","pool_active"],["T","tags_owner"],["LU","users_empty"],
    ["D","describe_no_tags_active"],["T","tags_owner"],["U","ok_empty"],
    ["D","describe_no_tags_inactive"],["T","tags_owner"],["X","ok_empty"]]},
  "policy15_work":{"calls":2,"work":2,"cleanup":0,"steps":[
    ["D","minimum15_active"],["T","tags_owner"]]},
  "policy15_cleanup":{"calls":18,"work":0,"cleanup":18,"steps":[
    ["D","pool_active"],["T","tags_owner"],["D","pool_active"],
    ["D","pool_active"],["T","tags_owner"],["LC","clients_empty"],
    ["D","pool_active"],["T","tags_owner"],["LS","servers_empty"],
    ["D","pool_active"],["T","tags_owner"],["LU","users_empty"],
    ["D","minimum15_active"],["T","tags_owner"],["U15","ok_empty"],
    ["D","minimum15_inactive"],["T","tags_owner"],["X","ok_empty"]]}
}
}"""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for key, value in pairs:
        if key in data:
            raise ValueError(f"duplicate_key={key}")
        data[key] = value
    return data


def _literal_records() -> dict[str, Any]:
    return json.loads(LITERALS, object_pairs_hook=_unique_object)


def _queue_literals(lab: RootLab, name: str) -> None:
    data = _literal_records()
    for _, reply in data["traces"][name]["steps"]:
        outcome = (
            (0, json.dumps(data["replies"][reply]), "")
            if reply in data["replies"]
            else literal_reply(reply)
        )
        lab.process.outcomes.append(outcome)


def _assert_literal_trace(lab: RootLab, name: str) -> None:
    data = _literal_records()
    trace = data["traces"][name]
    shared = load_requests()
    records = [
        data["requests"][key] if key in data["requests"] else shared[key]
        for key, _ in trace["steps"]
    ]
    expected = [(r["service"], r["operation"], r["region"], r["stdin"]) for r in records]
    assert_process(lab, expected)
    for (_, options), record in zip(lab.process.calls, records, strict=True):
        assert json.dumps(json.loads(options["input"]), sort_keys=True) == json.dumps(
            record["stdin"], sort_keys=True
        )
    assert lab.context.budget.used == trace["calls"]
    assert lab.context.budget.work_used == trace["work"]
    assert lab.lanes == ["work"] * trace["work"] + ["cleanup"] * trace["cleanup"]
    assert list(lab.directory.iterdir()) == []


def test_remove_id_remoto_aceito_igual_ao_run(root_lab: RootLab) -> None:
    _queue_literals(root_lab, "remote_id_accepted")
    creation = Creation(root_lab.services.spec, ROOT.name, load_replies()["tags_owner"]["Tags"])
    resource = root_lab.services.create("cognito", creation, root_lab.context)
    assert resource == Resource("cognito", "us-east-2_a1b2c3d4", "cnesdata-cap-a1b2c3d4")
    assert len(root_lab.process.calls) == 1
    assert root_lab.context.budget.work_used == 1
    observed = None
    result = None
    try:
        result = root_lab.services.delete(
            resource, True, RequestContext(root_lab.context.budget, "cleanup")
        )
    except ServiceFailure as error:
        observed = error
    assert_private_channels(root_lab, observed)
    assert observed is None, f"contract=accepted_remote_id_cleanup error={observed}"
    assert result is None
    _assert_literal_trace(root_lab, "remote_id_accepted")


@pytest.mark.parametrize("phase", ["preparo", "cleanup"], ids=["preparo", "cleanup"])
def test_describe_sem_tags_usa_list_tags(root_lab: RootLab, phase: str) -> None:
    name = "optional_tags_prepare" if phase == "preparo" else "optional_tags_cleanup"
    _queue_literals(root_lab, name)
    if phase == "preparo":
        with pytest.raises(ServiceFailure) as error:
            root_lab.services.probe([ROOT], root_lab.services.spec, root_lab.context)
        assert_private_channels(root_lab, error.value)
        _assert_literal_trace(root_lab, name)
        assert error.value.args == ("probe=not_implemented",), "contract=describe_tags_optional"
    else:
        observed = None
        result = None
        try:
            result = root_lab.services.delete(
                ROOT, True, RequestContext(root_lab.context.budget, "cleanup")
            )
        except ServiceFailure as error:
            observed = error
        assert_private_channels(root_lab, observed)
        assert observed is None, f"contract=describe_tags_optional_cleanup error={observed}"
        assert result is None
        _assert_literal_trace(root_lab, name)


@pytest.mark.parametrize(
    "phase", ["work_rejeita", "cleanup_preserva"], ids=["work_rejeita", "cleanup_preserva"]
)
def test_minimo15_tem_contrato_por_fase(root_lab: RootLab, phase: str) -> None:
    name = "policy15_work" if phase == "work_rejeita" else "policy15_cleanup"
    _queue_literals(root_lab, name)
    if phase == "work_rejeita":
        with pytest.raises(ServiceFailure) as error:
            root_lab.services.probe([ROOT], root_lab.services.spec, root_lab.context)
        assert_private_channels(root_lab, error.value)
        _assert_literal_trace(root_lab, name)
        assert error.value.args not in {("probe=not_implemented",), ("service=not_implemented",)}, (
            "contract=work_minimum_exact14"
        )
    else:
        result = root_lab.services.delete(
            ROOT, True, RequestContext(root_lab.context.budget, "cleanup")
        )
        assert result is None
        _assert_literal_trace(root_lab, name)
