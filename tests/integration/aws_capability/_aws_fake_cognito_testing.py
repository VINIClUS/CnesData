"""Cognito com estado para o ciclo de login: pool, filhos, usuário e InitiateAuth."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from tests.integration.aws_capability._aws_fake_types_testing import denied, good
from tests.integration.aws_capability._b1_testing import ACCOUNT
from tests.integration.aws_capability._service_contracts import Reply

REGION = "us-east-2"
MISSING = "ResourceNotFoundException"


@dataclass
class FakePool:
    pool_id: str
    name: str
    tags: dict[str, str]
    config: dict[str, object]
    protection: str
    servers: dict[str, dict[str, object]] = field(default_factory=dict)
    clients: dict[str, dict[str, object]] = field(default_factory=dict)
    users: dict[str, dict[str, object]] = field(default_factory=dict)
    brandings: dict[str, str] = field(default_factory=dict)
    domain: str | None = None
    domain_version: int | None = None

    @property
    def arn(self) -> str:
        return f"arn:aws:cognito-idp:{REGION}:{ACCOUNT}:userpool/{self.pool_id}"

    def described(self) -> dict[str, object]:
        pool: dict[str, object] = {
            **self.config,
            "Id": self.pool_id,
            "Name": self.name,
            "Arn": self.arn,
            "DeletionProtection": self.protection,
            "UserPoolTags": dict(self.tags),
            "LambdaConfig": {},
            "EmailConfiguration": {"EmailSendingAccount": "COGNITO_DEFAULT"},
            "VerificationMessageTemplate": {"DefaultEmailOption": "CONFIRM_WITH_CODE"},
        }
        if self.domain is not None:
            pool["Domain"] = self.domain
        return pool


_CONFIG_KEYS = (
    "UserPoolTier",
    "MfaConfiguration",
    "UsernameAttributes",
    "UsernameConfiguration",
    "AdminCreateUserConfig",
    "AccountRecoverySetting",
    "Policies",
)


class CognitoHandlers:
    pools: dict[str, FakePool]
    counter: int
    idp: "Callable[[Mapping[str, object]], Reply] | None"

    def _pool(self, payload: dict) -> FakePool | None:
        return self.pools.get(payload.get("UserPoolId", ""))

    def _cognito_idp_list_user_pools(self, payload: dict, who: str) -> Reply:
        rows = [{"Id": p.pool_id, "Name": p.name} for p in self.pools.values()]
        return good({"UserPools": rows})

    def _cognito_idp_create_user_pool(self, payload: dict, who: str) -> Reply:
        pool_id = f"{REGION}_CAP{self.counter:04d}"
        config = {key: payload[key] for key in _CONFIG_KEYS}
        self.pools[pool_id] = FakePool(
            pool_id,
            payload["PoolName"],
            dict(payload["UserPoolTags"]),
            config,
            payload["DeletionProtection"],
        )
        pool = self.pools[pool_id]
        return good({"UserPool": {"Id": pool_id, "Name": pool.name, "Arn": pool.arn}})

    def _cognito_idp_describe_user_pool(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        if pool is None:
            return denied(MISSING)
        return good({"UserPool": pool.described()})

    def _cognito_idp_list_tags_for_resource(self, payload: dict, who: str) -> Reply:
        for pool in self.pools.values():
            if pool.arn == payload["ResourceArn"]:
                return good({"Tags": dict(pool.tags)})
        return denied(MISSING)

    def _cognito_idp_update_user_pool(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        if pool is None:
            return denied(MISSING)
        pool.protection = payload["DeletionProtection"]
        return good()

    def _cognito_idp_delete_user_pool(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        if pool is None:
            return denied(MISSING)
        if pool.protection == "ACTIVE" or pool.domain is not None:
            return denied("InvalidParameterException")
        del self.pools[pool.pool_id]
        return good()

    def _cognito_idp_create_resource_server(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        if pool is None:
            return denied(MISSING)
        row = {
            "UserPoolId": pool.pool_id,
            "Identifier": payload["Identifier"],
            "Name": payload["Name"],
            "Scopes": payload["Scopes"],
        }
        pool.servers[payload["Identifier"]] = row
        return good({"ResourceServer": row})

    def _cognito_idp_describe_resource_server(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        row = pool.servers.get(payload["Identifier"]) if pool else None
        return denied(MISSING) if row is None else good({"ResourceServer": row})

    def _cognito_idp_list_resource_servers(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        rows = [] if pool is None else list(pool.servers.values())
        return good({"ResourceServers": rows})

    def _cognito_idp_delete_resource_server(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        if pool is None or pool.servers.pop(payload["Identifier"], None) is None:
            return denied(MISSING)
        return good()

    def _cognito_idp_create_user_pool_client(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        if pool is None:
            return denied(MISSING)
        client_id = f"client{self.counter:04d}"
        row = {**payload, "ClientId": client_id}
        pool.clients[client_id] = row
        return good({"UserPoolClient": row})

    def _cognito_idp_describe_user_pool_client(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        row = pool.clients.get(payload["ClientId"]) if pool else None
        return denied(MISSING) if row is None else good({"UserPoolClient": row})

    def _cognito_idp_list_user_pool_clients(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        rows = [] if pool is None else [_client_row(c) for c in pool.clients.values()]
        return good({"UserPoolClients": rows})

    def _cognito_idp_delete_user_pool_client(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        if pool is None or pool.clients.pop(payload["ClientId"], None) is None:
            return denied(MISSING)
        for key, client_id in list(pool.brandings.items()):
            if client_id == payload["ClientId"]:
                del pool.brandings[key]
        return good()

    def _cognito_idp_create_user_pool_domain(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        if pool is None:
            return denied(MISSING)
        pool.domain = payload["Domain"]
        pool.domain_version = payload.get("ManagedLoginVersion")
        return good({"ManagedLoginVersion": pool.domain_version, "CloudFrontDomain": "cf"})

    def _cognito_idp_describe_user_pool_domain(self, payload: dict, who: str) -> Reply:
        for pool in self.pools.values():
            if pool.domain == payload["Domain"]:
                row = {
                    "UserPoolId": pool.pool_id,
                    "Domain": pool.domain,
                    "AWSAccountId": ACCOUNT,
                    "Status": "ACTIVE",
                }
                return good({"DomainDescription": row})
        return good({"DomainDescription": {}})

    def _cognito_idp_delete_user_pool_domain(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        if pool is None or pool.domain != payload["Domain"]:
            return denied(MISSING)
        pool.domain = None
        return good()

    def _cognito_idp_create_managed_login_branding(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        if pool is None or payload["ClientId"] not in pool.clients:
            return denied(MISSING)
        branding_id = f"brand{self.counter:04d}"
        pool.brandings[branding_id] = payload["ClientId"]
        row = {"ManagedLoginBrandingId": branding_id, "UserPoolId": pool.pool_id}
        return good({"ManagedLoginBranding": row})

    def _cognito_idp_describe_managed_login_branding_by_client(
        self, payload: dict, who: str
    ) -> Reply:
        pool = self._pool(payload)
        if pool is None:
            return denied(MISSING)
        for branding_id, client_id in pool.brandings.items():
            if client_id == payload["ClientId"]:
                row = {"ManagedLoginBrandingId": branding_id, "UserPoolId": pool.pool_id}
                return good({"ManagedLoginBranding": row})
        return denied(MISSING)

    def _cognito_idp_delete_managed_login_branding(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        if pool is None or pool.brandings.pop(payload["ManagedLoginBrandingId"], None) is None:
            return denied(MISSING)
        return good()

    def _cognito_idp_admin_create_user(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        if pool is None:
            return denied(MISSING)
        sub = f"sub-{self.counter:04d}"
        pool.users[sub] = {"email": payload["Username"], "password": None}
        return good({"User": {"Username": sub, "UserStatus": "FORCE_CHANGE_PASSWORD"}})

    def _user(self, payload: dict) -> tuple[FakePool | None, str | None]:
        pool = self._pool(payload)
        if pool is None:
            return None, None
        for sub, row in pool.users.items():
            if payload["Username"] in {sub, row["email"]}:
                return pool, sub
        return pool, None

    def _cognito_idp_admin_set_user_password(self, payload: dict, who: str) -> Reply:
        pool, sub = self._user(payload)
        if pool is None or sub is None:
            return denied("UserNotFoundException")
        pool.users[sub]["password"] = payload["Password"]
        return good()

    def _cognito_idp_admin_get_user(self, payload: dict, who: str) -> Reply:
        pool, sub = self._user(payload)
        if pool is None or sub is None:
            return denied("UserNotFoundException")
        return good({"Username": sub, "UserStatus": "CONFIRMED"})

    def _cognito_idp_list_users(self, payload: dict, who: str) -> Reply:
        pool = self._pool(payload)
        rows = [] if pool is None else [{"Username": sub} for sub in pool.users]
        return good({"Users": rows})

    def _cognito_idp_admin_delete_user(self, payload: dict, who: str) -> Reply:
        pool, sub = self._user(payload)
        if pool is None or sub is None:
            return denied("UserNotFoundException")
        del pool.users[sub]
        return good()

    def _cognito_idp_initiate_auth(self, payload: dict, who: str) -> Reply:
        if self.idp is None:
            return denied("InvalidParameterException")
        return self.idp(payload)


def _client_row(client: Mapping[str, object]) -> dict[str, object]:
    return {
        "UserPoolId": client["UserPoolId"],
        "ClientId": client["ClientId"],
        "ClientName": client["ClientName"],
    }
