"""Operações IAM da fronteira com estado: role, trust, boundary e inline policies."""

import json
from collections.abc import Callable

from tests.integration.aws_capability._aws_fake_types_testing import (
    FakeRole,
    denied,
    good,
)
from tests.integration.aws_capability._b1_testing import (
    ACCOUNT,
    BOUNDARY_ARN,
    NAME,
    OPERATOR_ARN,
    ROLE_ARN,
)
from tests.integration.aws_capability._service_contracts import Reply


class IamHandlers:
    roles: dict[str, FakeRole]
    counter: int
    policy_lag: int
    malformed_creates: int
    trust_lag: int
    allowed: Callable[[str, str], bool]

    def _role_row(self, name: str) -> dict:
        role = self.roles[name]
        return {
            "RoleName": name,
            "Path": "/cnesdata-cap/",
            "Arn": f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/{name}",
            "AssumeRolePolicyDocument": role.newest_trust(),
            "PermissionsBoundary": {
                "PermissionsBoundaryType": "Policy",
                "PermissionsBoundaryArn": BOUNDARY_ARN,
            },
            "MaxSessionDuration": 3600,
        }

    def _iam_create_role(self, payload: dict, who: str) -> Reply:
        if self.malformed_creates > 0:
            self.malformed_creates -= 1
            return denied("MalformedPolicyDocument")
        trust = json.loads(payload["AssumeRolePolicyDocument"])
        principal = trust["Statement"][0]["Principal"]["AWS"]
        assert principal == (OPERATOR_ARN if payload["RoleName"] == NAME else ROLE_ARN)
        assert payload["PermissionsBoundary"] == BOUNDARY_ARN
        assert payload["MaxSessionDuration"] == 3600
        if payload["RoleName"] in self.roles:
            return denied("EntityAlreadyExists")
        if principal == ROLE_ARN and NAME not in self.roles:
            return denied("MalformedPolicyDocument")
        tags = {item["Key"]: item["Value"] for item in payload["Tags"]}
        self.roles[payload["RoleName"]] = FakeRole(tags, trust)
        return good({"Role": self._role_row(payload["RoleName"])})

    def _iam_update_assume_role_policy(self, payload: dict, who: str) -> Reply:
        role = self.roles.get(payload["RoleName"])
        if role is None:
            return denied("NoSuchEntity")
        document = json.loads(payload["PolicyDocument"])
        role.pending_trust = (self.counter + self.trust_lag, document)
        return good()

    def _iam_get_role(self, payload: dict, who: str) -> Reply:
        if payload["RoleName"] not in self.roles:
            return denied("NoSuchEntity")
        return good({"Role": self._role_row(payload["RoleName"])})

    def _iam_list_role_tags(self, payload: dict, who: str) -> Reply:
        role = self.roles.get(payload["RoleName"])
        if role is None:
            return denied("NoSuchEntity")
        tags = [{"Key": key, "Value": value} for key, value in role.tags.items()]
        return good({"Tags": tags, "IsTruncated": False})

    def _iam_list_roles(self, payload: dict, who: str) -> Reply:
        rows = [{"RoleName": name, "Path": "/cnesdata-cap/"} for name in self.roles]
        return good({"Roles": rows, "IsTruncated": False})

    def _iam_put_role_policy(self, payload: dict, who: str) -> Reply:
        role = self.roles.get(payload["RoleName"])
        if role is None:
            return denied("NoSuchEntity")
        document = json.loads(payload["PolicyDocument"])
        actions = frozenset(
            action for statement in document["Statement"] for action in statement["Action"]
        )
        role.policies[payload["PolicyName"]] = (actions, self.counter + self.policy_lag, document)
        return good()

    def _iam_get_role_policy(self, payload: dict, who: str) -> Reply:
        role = self.roles.get(payload["RoleName"])
        stored = None if role is None else role.policies.get(payload["PolicyName"])
        if stored is None:
            return denied("NoSuchEntity")
        return good({**payload, "PolicyDocument": stored[2]})

    def _iam_list_role_policies(self, payload: dict, who: str) -> Reply:
        role = self.roles.get(payload["RoleName"])
        if role is None:
            return denied("NoSuchEntity")
        return good({"PolicyNames": list(role.policies), "IsTruncated": False})

    def _iam_list_attached_role_policies(self, payload: dict, who: str) -> Reply:
        return good({"AttachedPolicies": [], "IsTruncated": False})

    def _iam_list_instance_profiles_for_role(self, payload: dict, who: str) -> Reply:
        return good({"InstanceProfiles": [], "IsTruncated": False})

    def _iam_delete_role_policy(self, payload: dict, who: str) -> Reply:
        role = self.roles.get(payload["RoleName"])
        if role is None or payload["PolicyName"] not in role.policies:
            return denied("NoSuchEntity")
        del role.policies[payload["PolicyName"]]
        return good()

    def _iam_delete_role(self, payload: dict, who: str) -> Reply:
        role = self.roles.get(payload["RoleName"])
        if role is None:
            return denied("NoSuchEntity")
        if role.policies:
            return denied("DeleteConflict")
        del self.roles[payload["RoleName"]]
        return good()
