"""Estado do perfil fargate-write e operações ECR, Logs e EC2 da fronteira com estado."""

import base64
from dataclasses import dataclass, field
from fnmatch import fnmatchcase

from tests.integration.aws_capability._aws_fake_types_testing import denied, good
from tests.integration.aws_capability._b1_testing import ACCOUNT
from tests.integration.aws_capability._service_contracts import Reply

REGION = "us-east-2"
DEFAULT_VPC = "vpc-0default00000001"
OTHER_VPC = "vpc-0other000000002"
SUBNETS = ("subnet-0aaa1110", "subnet-0bbb2220", "subnet-0ccc3330")
TOKEN_CANARY = "ECR_TOKEN_CANARY_do_not_log"  # noqa: S105
STOPPED_CANARY = "STOPPED_REASON_CANARY"
LOG_CANARY = "LOG_LINE_CANARY"
ALL_EGRESS = {"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}
TLS_EGRESS = {
    "IpProtocol": "tcp", "FromPort": 443, "ToPort": 443, "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
}


def repo_uri(name: str) -> str:
    return f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/{name}"


def repo_arn(name: str) -> str:
    return f"arn:aws:ecr:{REGION}:{ACCOUNT}:repository/{name}"


def group_arn(name: str) -> str:
    return f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:{name}"


def cluster_arn(name: str) -> str:
    return f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/{name}"


def pairs(items: list[dict], key: str = "Key", value: str = "Value") -> dict[str, str]:
    return {item[key]: item[value] for item in items}


@dataclass
class FakeRepo:
    tags: dict[str, str]
    mutability: str = "MUTABLE"
    images: dict[str, str] = field(default_factory=dict)


@dataclass
class FakeLogGroup:
    tags: dict[str, str]
    retention: int | None = None
    streams: dict[str, tuple[float, list[str]]] = field(default_factory=dict)


@dataclass
class FakeCluster:
    tags: dict[str, str]
    status: str = "ACTIVE"


@dataclass
class FakeRevision:
    family: str
    number: int
    definition: dict
    status: str = "ACTIVE"

    @property
    def arn(self) -> str:
        return f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{self.family}:{self.number}"


@dataclass
class FakeGroup:
    name: str
    vpc: str
    tags: dict[str, str]
    ingress: list[dict] = field(default_factory=list)
    egress: list[dict] = field(default_factory=lambda: [dict(ALL_EGRESS)])


@dataclass
class FakeTask:
    arn: str
    cluster: str
    group_id: str
    start: float
    stop: float
    container_started: bool
    exit_code: int | None
    reason: str | None
    stopped_by_request: bool = False


@dataclass
class FargateState:
    repos: dict[str, FakeRepo] = field(default_factory=dict)
    log_groups: dict[str, FakeLogGroup] = field(default_factory=dict)
    clusters: dict[str, FakeCluster] = field(default_factory=dict)
    revisions: list[FakeRevision] = field(default_factory=list)
    tasks: dict[str, FakeTask] = field(default_factory=dict)
    groups: dict[str, FakeGroup] = field(default_factory=dict)
    writable: bool = False
    task_seconds: float = 60.0
    provision_seconds: float = 6.0
    stop_seconds: float = 6.0
    log_lag: float = 8.0
    sg_violations: int = 0
    revoke_error: str | None = None
    recovery_denials: int = 0
    probe_lines: list[str] | None = None
    recovery_lines: list[str] | None = None
    probe_exit: int = 0
    log_page_size: int = 100
    no_logs: set[str] = field(default_factory=set)
    page_size: int = 0
    describe_missing: int = 0
    run_failures: int = 0
    delete_definition_failures: int = 0
    deregister_denied: bool = False
    empty_first_log_page: bool = False
    force_pull_failure: bool = False
    counter: int = 0
    task_counter: int = 0

    def paged(self, rows: list, token: str | None) -> tuple[list, str | None]:
        if self.page_size <= 0:
            return rows, None
        start = int(token or 0)
        end = start + self.page_size
        return rows[start:end], (str(end) if end < len(rows) else None)

    def next_group_id(self) -> str:
        self.counter += 1
        return f"sg-0cap{self.counter:012d}"


class FargateHandlers:
    fargate: FargateState

    def _repo(self, payload: dict) -> FakeRepo | None:
        return self.fargate.repos.get(payload["repositoryName"])

    def _ecr_create_repository(self, payload: dict, who: str) -> Reply:
        name = payload["repositoryName"]
        if name in self.fargate.repos:
            return denied("RepositoryAlreadyExistsException")
        tags = pairs(payload.get("tags", []))
        mutability = payload.get("imageTagMutability", "MUTABLE")
        self.fargate.repos[name] = FakeRepo(tags, mutability)
        return good({"repository": self._repo_row(name)})

    def _repo_row(self, name: str) -> dict:
        return {
            "repositoryName": name,
            "repositoryArn": repo_arn(name),
            "repositoryUri": repo_uri(name),
            "registryId": ACCOUNT,
            "imageTagMutability": self.fargate.repos[name].mutability,
        }

    def _ecr_describe_repositories(self, payload: dict, who: str) -> Reply:
        names = payload.get("repositoryNames")
        if names and any(name not in self.fargate.repos for name in names):
            return denied("RepositoryNotFoundException")
        token = None
        if not names:
            names, token = self.fargate.paged(list(self.fargate.repos), payload.get("nextToken"))
        data: dict = {"repositories": [self._repo_row(name) for name in names]}
        if token:
            data["nextToken"] = token
        return good(data)

    def _ecr_list_tags_for_resource(self, payload: dict, who: str) -> Reply:
        name = payload["resourceArn"].rsplit("/", 1)[1]
        repo = self.fargate.repos.get(name)
        if repo is None:
            return denied("RepositoryNotFoundException")
        return good({"tags": [{"Key": k, "Value": v} for k, v in repo.tags.items()]})

    def _ecr_get_authorization_token(self, payload: dict, who: str) -> Reply:
        token = base64.b64encode(f"AWS:{TOKEN_CANARY}".encode()).decode()
        host = f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com"
        data = {"authorizationToken": token, "proxyEndpoint": f"https://{host}"}
        return good({"authorizationData": [data]})

    def _ecr_describe_images(self, payload: dict, who: str) -> Reply:
        repo = self._repo(payload)
        if repo is None:
            return denied("RepositoryNotFoundException")
        tag = payload["imageIds"][0]["imageTag"]
        if tag not in repo.images:
            return denied("ImageNotFoundException")
        return good({"imageDetails": [{"imageDigest": repo.images[tag], "imageTags": [tag]}]})

    def _ecr_delete_repository(self, payload: dict, who: str) -> Reply:
        repo = self._repo(payload)
        if repo is None:
            return denied("RepositoryNotFoundException")
        if repo.images and payload.get("force") is not True:
            return denied("RepositoryNotEmptyException")
        del self.fargate.repos[payload["repositoryName"]]
        return good({"repository": {"repositoryName": payload["repositoryName"]}})

    def _logs_create_log_group(self, payload: dict, who: str) -> Reply:
        name = payload["logGroupName"]
        if name in self.fargate.log_groups:
            return denied("ResourceAlreadyExistsException")
        self.fargate.log_groups[name] = FakeLogGroup(dict(payload.get("tags", {})))
        return good()

    def _logs_put_retention_policy(self, payload: dict, who: str) -> Reply:
        group = self.fargate.log_groups.get(payload["logGroupName"])
        if group is None:
            return denied("ResourceNotFoundException")
        group.retention = payload["retentionInDays"]
        return good()

    def _logs_describe_log_groups(self, payload: dict, who: str) -> Reply:
        prefix = payload.get("logGroupNamePrefix", "")
        rows = [
            {"logGroupName": name, "arn": group_arn(name) + ":*", "retentionInDays": g.retention}
            for name, g in self.fargate.log_groups.items()
            if name.startswith(prefix)
        ]
        rows, token = self.fargate.paged(rows, payload.get("nextToken"))
        return good({"logGroups": rows, **({"nextToken": token} if token else {})})

    def _logs_list_tags_log_group(self, payload: dict, who: str) -> Reply:
        group = self.fargate.log_groups.get(payload["logGroupName"])
        if group is None:
            return denied("ResourceNotFoundException")
        return good({"tags": dict(group.tags)})

    def _logs_list_tags_for_resource(self, payload: dict, who: str) -> Reply:
        name = payload["resourceArn"].removesuffix(":*").rsplit(":", 1)[1]
        group = self.fargate.log_groups.get(name)
        if group is None:
            return denied("ResourceNotFoundException")
        return good({"tags": dict(group.tags)})

    def _logs_delete_log_group(self, payload: dict, who: str) -> Reply:
        if payload["logGroupName"] not in self.fargate.log_groups:
            return denied("ResourceNotFoundException")
        del self.fargate.log_groups[payload["logGroupName"]]
        return good()

    def _logs_describe_log_streams(self, payload: dict, who: str) -> Reply:
        group = self.fargate.log_groups.get(payload["logGroupName"])
        if group is None:
            return denied("ResourceNotFoundException")
        rows = [{"logStreamName": name} for name in group.streams]
        return good({"logStreams": rows})

    def _logs_get_log_events(self, payload: dict, who: str) -> Reply:
        group = self.fargate.log_groups.get(payload["logGroupName"])
        entry = None if group is None else group.streams.get(payload["logStreamName"])
        if entry is None or self.clock() < entry[0]:  # type: ignore[attr-defined]
            return denied("ResourceNotFoundException")
        lines = entry[1]
        token = payload.get("nextToken")
        size = min(payload.get("limit", 10000), self.fargate.log_page_size)
        if token is None and not payload.get("startFromHead", False):
            start = max(len(lines) - size, 0)
            return good(self._events(lines, start, len(lines)))
        if token is None and self.fargate.empty_first_log_page:
            return good({"events": [], "nextForwardToken": "f/0"})
        start = int(str(token or "f/0").split("/")[1])
        return good(self._events(lines, start, min(start + size, len(lines))))

    def _events(self, lines: list[str], start: int, end: int) -> dict:
        events = [{"message": line, "timestamp": 1} for line in lines[start:end]]
        return {"events": events, "nextForwardToken": f"f/{end}", "nextBackwardToken": f"b/{start}"}

    def _ec2_describe_vpcs(self, payload: dict, who: str) -> Reply:
        rows = [
            {"VpcId": OTHER_VPC, "IsDefault": False},
            {"VpcId": DEFAULT_VPC, "IsDefault": True},
        ]
        for item in payload.get("Filters", []):
            if item["Name"] == "isDefault":
                rows = [r for r in rows if str(r["IsDefault"]).lower() in item["Values"]]
        return good({"Vpcs": rows})

    def _ec2_describe_subnets(self, payload: dict, who: str) -> Reply:
        rows = [{"SubnetId": s, "VpcId": DEFAULT_VPC} for s in SUBNETS]
        rows.append({"SubnetId": "subnet-0other999", "VpcId": OTHER_VPC})
        for item in payload.get("Filters", []):
            if item["Name"] == "vpc-id":
                rows = [r for r in rows if r["VpcId"] in item["Values"]]
        return good({"Subnets": rows})

    def _ec2_create_security_group(self, payload: dict, who: str) -> Reply:
        if not payload.get("Description"):
            return denied("MissingParameter")
        specs = payload.get("TagSpecifications", [])
        if any(spec.get("ResourceType") != "security-group" for spec in specs):
            return denied("InvalidParameterValue")
        if any(g.name == payload["GroupName"] for g in self.fargate.groups.values()):
            return denied("InvalidGroup.Duplicate")
        tags = pairs(specs[0]["Tags"]) if specs else {}
        identifier = self.fargate.next_group_id()
        self.fargate.groups[identifier] = FakeGroup(payload["GroupName"], payload["VpcId"], tags)
        return good({"GroupId": identifier})

    def _group_row(self, identifier: str) -> dict:
        group = self.fargate.groups[identifier]
        return {
            "GroupId": identifier,
            "GroupName": group.name,
            "VpcId": group.vpc,
            "OwnerId": ACCOUNT,
            "Tags": [{"Key": k, "Value": v} for k, v in group.tags.items()],
            "IpPermissions": [dict(rule) for rule in group.ingress],
            "IpPermissionsEgress": [dict(rule) for rule in group.egress],
        }

    def _ec2_describe_security_groups(self, payload: dict, who: str) -> Reply:
        wanted = payload.get("GroupIds")
        if wanted is not None:
            if any(identifier not in self.fargate.groups for identifier in wanted):
                return denied("InvalidGroup.NotFound")
            return good({"SecurityGroups": [self._group_row(i) for i in wanted]})
        names = [v for f in payload.get("Filters", []) if f["Name"] == "group-name"
                 for v in f["Values"]]
        rows = [
            self._group_row(identifier)
            for identifier, group in self.fargate.groups.items()
            if not names or any(fnmatchcase(group.name, n) for n in names)
        ]
        rows, token = self.fargate.paged(rows, payload.get("NextToken"))
        return good({"SecurityGroups": rows, **({"NextToken": token} if token else {})})

    def _ec2_revoke_security_group_egress(self, payload: dict, who: str) -> Reply:
        if self.fargate.revoke_error is not None:
            return denied(self.fargate.revoke_error)
        group = self.fargate.groups.get(payload["GroupId"])
        if group is None:
            return denied("InvalidGroup.NotFound")
        before = len(group.egress)
        for rule in payload["IpPermissions"]:
            group.egress = [r for r in group.egress if _rule_key(r) != _rule_key(rule)]
        if len(group.egress) == before:
            return denied("InvalidPermission.NotFound")
        return good({"Return": True})

    def _ec2_authorize_security_group_egress(self, payload: dict, who: str) -> Reply:
        group = self.fargate.groups.get(payload["GroupId"])
        if group is None:
            return denied("InvalidGroup.NotFound")
        for rule in payload["IpPermissions"]:
            if any(_rule_key(r) == _rule_key(rule) for r in group.egress):
                return denied("InvalidPermission.Duplicate")
            group.egress.append(dict(rule))
        return good({"Return": True})

    def _ec2_delete_security_group(self, payload: dict, who: str) -> Reply:
        identifier = payload["GroupId"]
        if identifier not in self.fargate.groups:
            return denied("InvalidGroup.NotFound")
        if self._eni_holds(identifier):
            return denied("DependencyViolation")
        del self.fargate.groups[identifier]
        return good({"Return": True})

    def _eni_holds(self, identifier: str) -> bool:
        now = self.clock()  # type: ignore[attr-defined]
        if any(t.group_id == identifier and now < t.stop for t in self.fargate.tasks.values()):
            return True
        if self.fargate.sg_violations > 0:
            self.fargate.sg_violations -= 1
            return True
        return False


def _rule_key(rule: dict) -> tuple:
    ranges = tuple(sorted(item["CidrIp"] for item in rule.get("IpRanges", [])))
    return (rule["IpProtocol"], rule.get("FromPort"), rule.get("ToPort"), ranges)
