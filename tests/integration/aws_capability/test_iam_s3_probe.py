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
SEEDS = ["serving/x", "serving/blocked/x", "raw/x", "reconciliation/x"]
SECONDS = {
    "assume_audit_seconds", "audit_put_ready_seconds", "audit_retention_extend_ready_seconds",
    "audit_get_ready_seconds", "assume_api_seconds", "api_get_ready_seconds",
    "api_list_ready_seconds",
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
    "api_get_missing": "404",
    "api_get_raw": "403",
    "api_list_raw": DENIED,
    "api_put_object_ungranted": DENIED,
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
    if operation == "get-object-lock-configuration":
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
    ("operator", "assume-role", "api"),
    ("operator", "put-role-policy", "api", "cap-api-s3"),
    ("role-api", "head-object", "serving/x"),
    ("role-api", "head-object", "serving/missing"),
    ("role-api", "head-object", "raw/x"),
    ("role-api", "list-objects-v2", "serving/"),
    ("role-api", "list-objects-v2", "raw/"),
    ("role-api", "head-object", "serving/x"),
    ("role-api", "put-object", "serving/y"),
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


def test_todas_as_chamadas_de_objeto_levam_o_dono_esperado_do_bucket(lab: ProbeLab) -> None:
    calls = record(lab)
    run(lab)
    objects = [c for c in calls if c.request.service == "s3api"]
    assert objects
    assert all(c.payload["ExpectedBucketOwner"] == ACCOUNT for c in objects)
    assert all(c.payload["Bucket"] == NAME for c in objects)


def test_so_o_operador_e_as_duas_sessoes_chamam(lab: ProbeLab) -> None:
    run(lab)
    assert {who for _, _, who in lab.aws.calls} == {"operator", "role-audit", "role-api"}


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
