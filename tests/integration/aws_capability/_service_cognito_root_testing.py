"""Literais independentes e fronteiras offline do root Cognito."""

import json
import logging
import os
import stat
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pytest

from tests.integration.aws_capability._cli_testing import capture_input
from tests.integration.aws_capability._durability import DurabilityTrace, assert_durable
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._service_cognito_testing import load_fixture
from tests.integration.aws_capability._service_read_assertions import (
    FIXED_FLAGS,
    assert_environment,
)
from tests.integration.aws_capability._service_read_testing import CANARY, ReadLab, make_read_lab
from tests.integration.aws_capability._service_testing import ProcessScript
from tests.integration.aws_capability._testing import ProcessBoundary
from tests.integration.aws_capability.contracts import Policy, Ports, Resource

ROOT = Resource("cognito", "us-east-2_PoolRoot123", "cnesdata-cap-a1b2c3d4")
FIXTURES = Path(__file__).parent / "fixtures" / "cli" / "b3"


@dataclass
class RootLab(ReadLab):
    process: ProcessScript = field(repr=False)
    monkeypatch: pytest.MonkeyPatch
    channels: tuple[pytest.LogCaptureFixture, pytest.CaptureFixture[str]]
    durability: DurabilityTrace = field(default_factory=DurabilityTrace)
    lanes: list[str] = field(default_factory=list)
    last_work: int = 0
    at_request: Any = None
    clock: list[float] = field(default_factory=lambda: [1000])
    ports: Ports = field(init=False)


@pytest.fixture
def root_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> Iterator[RootLab]:
    directory = tmp_path / "private"
    caplog.set_level(logging.DEBUG)
    prepare_directory(directory)
    base = make_read_lab(monkeypatch, directory)
    lab = RootLab(
        base.services, base.process, base.context, base.directory, monkeypatch, (caplog, capsys)
    )
    lab.context.budget.limits = PROFILES["cognito-sdk"].limits
    lab.context.budget.now = lambda: lab.clock[0]
    lab.services = replace(
        base.services,
        spec=replace(base.services.spec, probe="cognito-sdk", allow_governance_bypass=True),
    )
    processes = ProcessBoundary([])
    processes.ack = {
        "state": "ready",
        "account": "111111111111",
        "region": "us-east-2",
        "run_id": "a1b2c3d4",
        "journal": str(directory / "a1b2c3d4.json"),
        "worker_pid": os.getpid(),
    }
    policy = Policy("111111111111", frozenset({"us-east-2"}), lab.context.budget.limits)
    lab.ports = Ports(lab.services, processes, lab.context.budget.now, policy, lab.context.budget)
    lab.durability.install(monkeypatch)

    def recorded(argv: Any, **options: Any) -> Any:
        work = lab.context.budget.work_used
        lab.lanes.append("work" if work > lab.last_work else "cleanup")
        lab.last_work = work
        if lab.at_request is not None:
            lab.at_request(*capture_input(argv, options))
        return lab.process.run(argv, **options)

    monkeypatch.setattr(subprocess, "run", recorded)
    yield lab
    assert_private_channels(lab)


def load_requests() -> dict[str, Any]:
    return load_fixture(FIXTURES / "cognito_root_requests.json")


def load_replies() -> dict[str, Any]:
    return load_fixture(FIXTURES / "cognito_root_replies.json")


def load_trace(name: str) -> Any:
    return load_fixture(FIXTURES / "cognito_root_traces.json")[name]


def literal_reply(name: str) -> tuple[int, str, str] | BaseException:
    data = json.loads(globals()[name]) if name in globals() else load_replies()[name]
    if "exception" in data:
        return subprocess.TimeoutExpired("aws", data["timeout"], output=CANARY, stderr=CANARY)
    if "exit" in data:
        return data["exit"], data["stdout"], data["stderr"]
    return 0, json.dumps(data), ""


def queue_trace(lab: RootLab, steps: list[list[str]]) -> None:
    aliases = load_trace("aliases")
    for _, reply in steps:
        lab.process.outcomes.append(literal_reply(aliases.get(reply, reply)))


def assert_trace(lab: RootLab, steps: list[list[str]], lanes: list[str]) -> None:
    assert len(lab.process.calls) == len(steps), "stage=physical_trace"
    requests = load_requests()
    for (argv, options), (name, _) in zip(lab.process.calls, steps, strict=True):
        expected = requests[name]
        assert argv == [
            "aws",
            expected["service"],
            expected["operation"],
            "--region",
            expected["region"],
            *FIXED_FLAGS,
            "7",
            "--cli-read-timeout",
            "7",
        ]
        assert json.dumps(json.loads(options["input"]), sort_keys=True) == json.dumps(
            expected["stdin"], sort_keys=True
        )
        assert_environment(options["env"], expected["region"])
        assert options["timeout"] == 7
        assert options["shell"] is False
        assert options["capture_output"] is True
        assert options["text"] is True
        assert options["check"] is False
    assert lab.lanes == lanes
    assert lab.process.observed_debits == list(range(1, len(steps) + 1))
    assert lab.context.budget.used == len(steps)
    assert lab.process.outcomes == []


def assert_journal(lab: RootLab, name: str) -> bytes:
    path = lab.directory / "a1b2c3d4.json"
    raw = path.read_bytes()
    assert json.loads(raw) == load_trace(name)
    assert path.stat().st_mode & 0o777 == 0o600
    assert lab.directory.stat().st_mode & 0o777 == 0o700
    fresh = Journal(lab.services.spec)
    fresh.load()
    assert json.loads(fresh.content()) == load_trace(name)
    return raw


def observe_durability(lab: RootLab, name: str) -> None:
    assert_journal(lab, name)
    assert_durable(lab.durability, lab.directory / "a1b2c3d4.json")


def install_fault(lab: RootLab, kind: str, when: int = 1, interruption: bool = False) -> None:
    if kind == "process":
        lab.process.outcomes[when - 1] = KeyboardInterrupt()
        return
    original_sync, original_replace = os.fsync, os.replace
    seen = [0]

    def fail() -> None:
        seen[0] += 1
        if seen[0] == when:
            raise KeyboardInterrupt if interruption else OSError("native=journal_fault")

    def synced(fd: int) -> None:
        directory = stat.S_ISDIR(os.fstat(fd).st_mode)
        if kind == ("diretorio_fsync" if directory else "arquivo_fsync"):
            fail()
        original_sync(fd)

    def replaced(source: Any, target: Any) -> None:
        if kind == "replace":
            fail()
        original_replace(source, target)

    lab.monkeypatch.setattr(os, "fsync", synced)
    lab.monkeypatch.setattr(os, "replace", replaced)


def public_channels(lab: RootLab) -> str:
    caplog, capsys = lab.channels
    captured = capsys.readouterr()
    records = caplog.get_records("setup") + caplog.get_records("call")
    records += caplog.get_records("teardown")
    return captured.out + captured.err + caplog.text + "".join(r.getMessage() for r in records)


def assert_private_channels(lab: RootLab, observed: object = None) -> None:
    public = public_channels(lab) + repr(lab.services) + repr(lab.services.ports) + repr(observed)
    for path in lab.directory.rglob("*"):
        if path.is_file() and not path.is_symlink():
            public += path.read_text(errors="replace")
    assert CANARY not in public


pool_lite = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"LITE","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_plus = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"PLUS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_no_protection = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_bool_protection = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":true,"MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_mfa_on = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"ON",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_username_phone = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["phone_number"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_case_true = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":true},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_case_int = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":0},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_admin_false = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":false},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_admin_int = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":1},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_recovery_phone = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_phone_number","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_minimum13 = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":13,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_lower_false = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":false,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_upper_false = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":false,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_numbers_false = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":false,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_symbols_false = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":false,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_trigger = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{"PreSignUp":"arn:aws:lambda:us-east-2:111111111111:function:synthetic"},
"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_sms = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"SmsConfiguration":{"SnsCallerArn":"arn:aws:iam::111111111111:role/synthetic"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""

pool_issuer_updated = """{"UserPool":{
"Id":"us-east-2_PoolRoot123","Name":"cnesdata-cap-a1b2c3d4",
"Arn":"arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_PoolRoot123",
"UserPoolTier":"ESSENTIALS","DeletionProtection":"ACTIVE","MfaConfiguration":"OFF",
"UsernameAttributes":["email"],"UsernameConfiguration":{"CaseSensitive":false},
"AdminCreateUserConfig":{"AllowAdminCreateUserOnly":true},
"AccountRecoverySetting":{"RecoveryMechanisms":[{"Name":"verified_email","Priority":1}]},
"Policies":{"PasswordPolicy":{"MinimumLength":14,"RequireLowercase":true,"RequireUppercase":true,
"RequireNumbers":true,"RequireSymbols":true,"TemporaryPasswordValidityDays":7}},
"LambdaConfig":{},"EmailConfiguration":{"EmailSendingAccount":"COGNITO_DEFAULT"},
"IssuerConfiguration":{"Type":"UPDATED"},
"VerificationMessageTemplate":{"DefaultEmailOption":"CONFIRM_WITH_CODE"},
"UserPoolTags":{"Project":"CnesData","Purpose":"aws-capability","Run":"a1b2c3d4","ExpiresAt":"1060"}
}}"""
