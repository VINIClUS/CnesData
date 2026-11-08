"""Probe iam-s3: prefixos e retenção, cada negação ao lado de um sucesso da mesma ação."""

import json
from pathlib import Path

import pytest

from tests.integration.aws_capability import _matrix_policies as policies
from tests.integration.aws_capability._aws_fake_types_testing import parse_iso
from tests.integration.aws_capability._b1_testing import ACCOUNT, NAME
from tests.integration.aws_capability._matrix_testing import Call, covered, dims, record
from tests.integration.aws_capability._probe_testing import ProbeLab, make_probe_lab
from tests.integration.aws_capability.contracts import ServiceFailure

BUCKET_ARN = "arn:aws:s3:::" + NAME
AUDIT, API = f"{NAME}-audit", f"{NAME}-api"
DENIED = "AccessDenied"
SEEDS = ["serving/x", "serving/blocked/x", "raw/x", "reconciliation/x", "tmp/x", "normalized/x"]
UNIT_NAMES = ("raw", "tmp", "normalized", "reconciliation", "serving")
RECOVERY_NAMES = UNIT_NAMES[1:]
SECONDS = {
    "assume_audit_seconds", "audit_put_ready_seconds", "audit_retention_extend_ready_seconds",
    "audit_get_ready_seconds", "assume_unit_seconds", "unit_get_ready_seconds",
    "assume_recovery_seconds", "recovery_get_ready_seconds", "assume_api_seconds",
    "api_get_ready_seconds", "api_list_ready_seconds", "blocked_flip_seconds",
}
CODES = {
    "audit_put_lock": "ok",
    "audit_lock_config": "ok",
    "audit_retention_shorten": DENIED,
    "audit_get_raw": "403",
    "audit_get_missing": "403",
    "audit_get_retention": "ok",
    "audit_delete_version_ungranted": DENIED,
    "audit_list_ungranted": DENIED,
    **{f"unit_get_missing_{name}": "404" for name in UNIT_NAMES},
    **{f"recovery_get_missing_{name}": "404" for name in RECOVERY_NAMES},
    "recovery_get_missing_raw": "403",
    "api_get_missing": "404",
    "api_get_missing_reconciliation": "404",
    "api_get_raw": "403",
    "api_list_raw": DENIED,
    "api_put_object_ungranted": DENIED,
    "blocked_before": "ok",
    "policy_put_attempts": 1,
    "blocked_after": "403",
    "serving_after_flip": "ok",
}
LOCK, EXTEND, SHORTEN = 300, 540, 360


@pytest.fixture
def lab(tmp_path: Path) -> ProbeLab:
    return make_probe_lab(tmp_path, "iam-s3", seeded=True)


def run(lab: ProbeLab) -> dict:
    return dict(lab.services.probe(lab.resources, lab.spec, lab.context))


def row(call: Call) -> tuple:
    payload, operation = call.payload, call.operation
    if operation == "assume-role":
        return call.who, operation, str(payload["RoleArn"]).rsplit("-", 1)[1]
    if operation == "put-role-policy":
        role = str(payload["RoleName"]).rsplit("-", 1)[1]
        return call.who, operation, role, payload["PolicyName"]
    if operation in {"get-object-lock-configuration", "put-bucket-policy"}:
        return call.who, operation
    found = dims(call)
    return call.who, operation, found["key"]


STEPS = [
    *[("operator", "put-object", key) for key in SEEDS],
    ("operator", "assume-role", "audit"),
    ("operator", "put-role-policy", "audit", "cap-audit-s3"),
    ("role-audit", "put-object", "audit/a"),
    ("role-audit", "put-object", "audit/b"),
    ("role-audit", "get-object-lock-configuration"),
    ("role-audit", "put-object-retention", "audit/b"),
    ("role-audit", "put-object-retention", "audit/b"),
    ("role-audit", "head-object", "audit/a"),
    ("role-audit", "head-object", "raw/x"),
    ("role-audit", "head-object", "audit/missing"),
    ("role-audit", "get-object-retention", "audit/b"),
    ("role-audit", "head-object", "audit/a"),
    ("role-audit", "delete-object", "audit/a"),
    ("role-audit", "list-objects-v2", "audit/"),
    ("operator", "assume-role", "unit"),
    ("operator", "put-role-policy", "unit", "cap-unit-s3"),
    *[
        ("role-unit", "head-object", f"{name}/{leaf}")
        for name in UNIT_NAMES for leaf in ("x", "missing")
    ],
    ("operator", "assume-role", "recovery"),
    ("operator", "put-role-policy", "recovery", "cap-recovery-s3"),
    *[
        ("role-recovery", "head-object", f"{name}/{leaf}")
        for name in RECOVERY_NAMES for leaf in ("x", "missing")
    ],
    ("role-recovery", "head-object", "serving/x"),
    ("role-recovery", "head-object", "raw/missing"),
    ("operator", "assume-role", "api"),
    ("operator", "put-role-policy", "api", "cap-api-s3"),
    ("role-api", "head-object", "serving/x"),
    ("role-api", "head-object", "serving/missing"),
    ("role-api", "head-object", "reconciliation/x"),
    ("role-api", "head-object", "reconciliation/missing"),
    ("role-api", "head-object", "raw/x"),
    ("role-api", "list-objects-v2", "serving/"),
    ("role-api", "list-objects-v2", "raw/"),
    ("role-api", "head-object", "serving/x"),
    ("role-api", "put-object", "serving/y"),
    ("role-api", "head-object", "serving/blocked/x"),
    ("operator", "put-bucket-policy"),
    ("role-api", "head-object", "serving/blocked/x"),
    ("role-api", "head-object", "serving/x"),
]


def test_observa_codigos_e_segundos(lab: ProbeLab) -> None:
    observed = run(lab)
    assert {k: v for k, v in observed.items() if k not in SECONDS} == CODES
    assert set(observed) == set(CODES) | SECONDS
    assert all(isinstance(observed[key], float) and observed[key] >= 0 for key in SECONDS)


def test_ordem_e_chave_de_cada_passo_com_controle_antes_da_negacao(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    assert [row(call) for call in calls] == STEPS


def test_negacoes_de_prefixo_e_de_retencao_tem_controle_anterior_com_uma_unica_troca(
    lab: ProbeLab,
) -> None:
    calls = record(lab)
    run(lab)
    denials = [
        i for i, c in enumerate(calls)
        if not c.ok and c.who != "operator" and "missing" not in str(dims(c).get("key"))
        and dims(c).get("key") != "serving/blocked/x"
        and c.operation not in {"delete-object", "put-object"}
        and not (c.operation == "list-objects-v2" and c.who == "role-audit")
    ]
    assert [calls[i].operation for i in denials] == [
        "put-object-retention", "head-object", "head-object", "list-objects-v2",
    ]
    assert [i for i in denials if not covered(calls, i)] == []


def test_retencao_encurtada_so_depois_da_estendida_e_so_difere_na_data(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    first, second = [c for c in calls if c.operation == "put-object-retention"]
    assert first.ok
    assert not second.ok
    assert (first.payload["Key"], first.payload["VersionId"]) == (
        second.payload["Key"], second.payload["VersionId"],
    )
    first_until = first.payload["Retention"]["RetainUntilDate"]  # type: ignore[index]
    second_until = second.payload["Retention"]["RetainUntilDate"]  # type: ignore[index]
    extend = parse_iso(first_until) - first.at
    shorten = parse_iso(second_until) - second.at
    assert (extend, shorten) == (EXTEND, SHORTEN)
    assert "BypassGovernanceRetention" not in second.payload


def test_toda_retencao_e_governance_e_no_maximo_dez_minutos(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    stamps = []
    for call in calls:
        text = json.dumps(dict(call.payload)).lower()
        assert "compliance" not in text
        if call.operation == "put-object" and "ObjectLockMode" in call.payload:
            assert call.payload["ObjectLockMode"] == "GOVERNANCE"
            stamps.append(parse_iso(call.payload["ObjectLockRetainUntilDate"]) - call.at)
        if call.operation == "put-object-retention":
            retention = call.payload["Retention"]
            assert retention["Mode"] == "GOVERNANCE"  # type: ignore[index]
            stamps.append(parse_iso(retention["RetainUntilDate"]) - call.at)  # type: ignore[index]
    assert stamps == [LOCK, EXTEND, SHORTEN]
    assert max(stamps) <= 600


def test_acoes_nao_concedidas_vem_logo_depois_de_um_head_ok_da_mesma_sessao(
    lab: ProbeLab,
) -> None:
    calls = record(lab)
    observed = run(lab)
    assert {k for k in observed if k.endswith("_ungranted")} == {
        "audit_delete_version_ungranted", "audit_list_ungranted", "api_put_object_ungranted",
    }
    for who, operation, key in (
        ("role-audit", "delete-object", "audit/a"),
        ("role-api", "put-object", "serving/y"),
    ):
        index = next(
            i for i, c in enumerate(calls) if c.who == who and dims(c).get("key") == key
            and c.operation == operation
        )
        control = calls[index - 1]
        assert (control.who, control.operation, control.ok) == (who, "head-object", True)
        assert dims(control)["key"] == ("audit/a" if who == "role-audit" else "serving/x")


def test_delete_ungranted_usa_a_versao_do_objeto_sem_retencao(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    put = next(
        c for c in calls
        if c.who == "role-audit" and c.operation == "put-object" and c.payload["Key"] == "audit/a"
    )
    delete = next(c for c in calls if c.operation == "delete-object")
    assert delete.payload["Key"] == "audit/a"
    assert delete.payload["VersionId"] == put.data["VersionId"]
    assert "BypassGovernanceRetention" not in delete.payload
    assert delete.payload["VersionId"] != next(
        c.data["VersionId"] for c in calls if dims(c).get("key") == "audit/b" and c.ok
    )


def test_sementes_sao_gravadas_pelo_operador_sem_lock(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    seeds = [c for c in calls if c.who == "operator" and c.operation == "put-object"]
    assert [c.payload["Key"] for c in seeds] == SEEDS
    assert all("ObjectLockMode" not in c.payload for c in seeds)


def test_policies_inline_espelham_task_roles_com_o_arn_do_bucket(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    puts = {c.payload["RoleName"]: c.payload for c in calls if c.operation == "put-role-policy"}
    assert json.loads(puts[AUDIT]["PolicyDocument"]) == {  # type: ignore[arg-type]
        "Version": "2012-10-17", "Statement": policies.audit_s3(BUCKET_ARN),
    }
    assert json.loads(puts[API]["PolicyDocument"]) == {  # type: ignore[arg-type]
        "Version": "2012-10-17", "Statement": policies.api_s3(BUCKET_ARN),
    }
    for role in ("unit", "recovery"):
        document = json.loads(puts[f"{NAME}-{role}"]["PolicyDocument"])  # type: ignore[arg-type]
        statements = getattr(policies, f"{role}_s3")(BUCKET_ARN)
        assert document == {"Version": "2012-10-17", "Statement": statements}


def test_prefixos_de_get_saem_do_espelho_em_ordem() -> None:
    assert policies.get_prefixes(policies.unit_s3(BUCKET_ARN), BUCKET_ARN) == (
        "raw/", "tmp/", "normalized/", "reconciliation/", "serving/",
    )
    assert policies.get_prefixes(policies.recovery_s3(BUCKET_ARN), BUCKET_ARN) == (
        "tmp/", "normalized/", "reconciliation/", "serving/",
    )
    assert policies.get_prefixes(policies.api_s3(BUCKET_ARN), BUCKET_ARN) == (
        "reconciliation/", "serving/",
    )
    assert policies.get_prefixes(policies.audit_s3(BUCKET_ARN), BUCKET_ARN) == ("audit/",)
    assert policies.get_prefixes(policies.unit_s3(BUCKET_ARN), BUCKET_ARN + "2") == ()


def test_prefixos_de_get_ignoram_chave_exata_sem_wildcard() -> None:
    statements = [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"{BUCKET_ARN}/audit/a"},
        {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": [f"{BUCKET_ARN}/serving/*"]},
    ]
    assert policies.get_prefixes(statements, BUCKET_ARN) == ("serving/",)


def test_cada_prefixo_de_get_de_cada_espelho_recebe_head_de_chave_ausente_apos_controle(
    lab: ProbeLab,
) -> None:
    calls = record(lab)
    run(lab)
    for role in ("unit", "recovery", "api"):
        statements = getattr(policies, f"{role}_s3")(BUCKET_ARN)
        prefixes = policies.get_prefixes(statements, BUCKET_ARN)
        assert prefixes
        heads = [
            (c.ok, dims(c)["key"])
            for c in calls if c.who == f"role-{role}" and c.operation == "head-object"
        ]
        for prefix in prefixes:
            missing = heads.index((False, f"{prefix}missing"))
            assert heads[missing - 1] == (True, f"{prefix}x")


def test_todas_as_chamadas_de_objeto_levam_o_dono_esperado_do_bucket(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    objects = [c for c in calls if c.request.service == "s3api"]
    assert objects
    assert all(c.payload["ExpectedBucketOwner"] == ACCOUNT for c in objects)
    assert all(c.payload["Bucket"] == NAME for c in objects)


def test_so_o_operador_e_as_quatro_sessoes_chamam(lab: ProbeLab) -> None:
    run(lab)
    assert {who for _, _, who in lab.aws.calls} == {
        "operator", "role-audit", "role-unit", "role-recovery", "role-api",
    }


@pytest.mark.parametrize("option", [{"policy_lag": 10**6}])
def test_controle_que_nunca_fica_verde_falha_sem_gravar_negacao(
    tmp_path: Path, option: dict
) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3", **option)
    calls = record(lab)
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=control_failed step=audit_put",)
    audit = [c for c in calls if c.who == "role-audit"]
    assert {dims(c)["key"] for c in audit} == {"audit/a"}
    assert len(audit) == 12


@pytest.mark.parametrize(("role", "first"), [("unit", "raw/x"), ("recovery", "tmp/x")])
def test_espelho_sem_get_falha_fechado_sem_head_de_chave_ausente(
    tmp_path: Path, role: str, first: str
) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3")
    original = lab.aws.permits

    def no_get(who: str, action: str, resource: str, keys: object = None) -> bool:
        if who == f"role-{role}" and action == "s3:GetObject":
            return False
        return original(who, action, resource, keys)  # type: ignore[arg-type]

    lab.aws.permits = no_get  # type: ignore[method-assign]
    calls = record(lab)
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == (f"probe=control_failed step={role}_get",)
    assert {dims(c)["key"] for c in calls if c.who == f"role-{role}"} == {first}


@pytest.mark.parametrize(
    "case",
    [
        ("unit", "tmp/x", 1, "unit_get_control", "tmp/missing"),
        ("recovery", "serving/x", 2, "recovery_live_control", "raw/missing"),
        ("api", "reconciliation/x", 1, "api_reconciliation_control", "reconciliation/missing"),
    ],
    ids=["unit", "recovery", "api"],
)
def test_controle_posterior_negado_falha_fechado_sem_head_de_chave_ausente(
    tmp_path: Path, case: tuple[str, str, int, str, str]
) -> None:
    role, key, nth, step, skipped = case
    lab = make_probe_lab(tmp_path, "iam-s3")
    original, seen = lab.aws.permits, [0]

    def deny_nth(who: str, action: str, resource: str, keys: object = None) -> bool:
        if who == f"role-{role}" and action == "s3:GetObject" and resource.endswith(f"/{key}"):
            seen[0] += 1
            if seen[0] >= nth:
                return False
        return original(who, action, resource, keys)  # type: ignore[arg-type]

    lab.aws.permits = deny_nth  # type: ignore[method-assign]
    calls = record(lab)
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == (f"probe=step_failed step={step}",)
    heads = [(c.ok, dims(c)["key"]) for c in calls if c.who == f"role-{role}"]
    assert heads[-1] == (False, key)
    assert skipped not in {called for _, called in heads}


def test_lock_negado_registra_o_codigo_e_pula_a_retencao(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3")
    original = lab.aws.permits

    def no_retention(who: str, action: str, resource: str, keys: object = None) -> bool:
        if action == "s3:PutObjectRetention":
            return False
        return original(who, action, resource, keys)  # type: ignore[arg-type]

    lab.aws.permits = no_retention  # type: ignore[method-assign]
    calls = record(lab)
    observed = run(lab)
    assert observed["audit_put_lock"] == DENIED
    for key in ("audit_retention_shorten", "audit_get_retention"):
        assert observed[key] == "skipped"
    assert observed["audit_retention_extend_ready_seconds"] == "skipped"
    assert not [c for c in calls if c.operation in {"put-object-retention", "get-object-retention"}]


def test_recursos_divergentes_sao_recusados(lab: ProbeLab) -> None:
    with pytest.raises(ServiceFailure) as error:
        lab.services.probe(list(reversed(lab.resources)), lab.spec, lab.context)
    assert error.value.args == ("probe=iam_s3_invalid",)
    assert lab.aws.calls == []
