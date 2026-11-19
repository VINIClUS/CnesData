"""Probe iam-trust: cada negação gravada fica ao lado de um sucesso da mesma ação."""

import json
import traceback
from pathlib import Path

import pytest

from tests.integration.aws_capability._aws_fake_types_testing import (
    ACCESS_KEY,
    SECRET,
    TOKEN,
    denied,
    good,
)
from tests.integration.aws_capability._b1_testing import NAME, OPERATOR_ARN, ROLE_ARN
from tests.integration.aws_capability._probe_attempt import Attempt
from tests.integration.aws_capability._probe_testing import (
    ROLE,
    ROLE2,
    ProbeLab,
    make_probe_lab,
    payload_text,
)
from tests.integration.aws_capability._trust_probe import duration_category
from tests.integration.aws_capability.contracts import RequestContext, ServiceFailure

R2 = f"{NAME}-r2"
BASE = {"RoleSessionName": "cap-a1b2c3d4", "DurationSeconds": 900}
TAG_SET = [{"Key": "cap-tag", "Value": "cap-tag-value"}]
SOURCE = "cap-source"
SECONDS = (
    "assume_seconds",
    "chain_seconds",
    "session_tags_ready_seconds",
    "source_identity_ready_seconds",
)
EXPECTED = {
    "self_assume": "AccessDenied",
    "duration_3601": "ValidationError",
    "duration_3601_category": "role_chaining",
    "session_tags_before": "AccessDenied",
    "source_identity_before": "AccessDenied",
    "session_tags_after": "ok",
    "source_identity_after": "ok",
}
ROLE_CHAINING = (
    "The requested DurationSeconds exceeds the 1 hour session limit "
    "for roles assumed by role chaining."
)
MAX_SESSION = "The requested DurationSeconds exceeds the MaxSessionDuration set for this role."


@pytest.fixture
def lab(tmp_path: Path) -> ProbeLab:
    return make_probe_lab(tmp_path, "iam-trust")


def run(lab: ProbeLab) -> dict:
    return dict(lab.services.probe(lab.resources, lab.spec, lab.context))


def role_of(arn: object) -> str:
    return "R" if str(arn).endswith(NAME) else str(arn).rsplit("-", 1)[1].upper()


def timeline(lab: ProbeLab) -> list[tuple]:
    rows: list[tuple] = []
    for (_, operation, who), request in zip(lab.aws.calls, lab.aws.requests, strict=True):
        payload = dict(request.payload)
        if operation == "assume-role":
            extra = {k: v for k, v in payload.items() if k not in BASE and k != "RoleArn"}
            role, seconds = role_of(payload["RoleArn"]), payload["DurationSeconds"]
            rows.append((who, "assume", role, seconds, extra))
        elif operation in {"put-role-policy", "update-assume-role-policy"}:
            rows.append((who, operation))
    return rows


def assumes(lab: ProbeLab) -> list[dict]:
    return [dict(r.payload) for r in lab.aws.requests if r.operation == "assume-role"]


STEPS = [
    ("operator", "assume", "R", 900, {}),
    ("operator", "put-role-policy"),
    ("role", "assume", "R2", 900, {}),
    ("role", "assume", "R", 900, {}),
    ("operator", "assume", "R", 3601, {}),
    ("operator", "assume", "R", 900, {"Tags": TAG_SET}),
    ("operator", "assume", "R", 900, {"SourceIdentity": SOURCE}),
    ("operator", "update-assume-role-policy"),
    ("operator", "assume", "R", 900, {"Tags": TAG_SET}),
    ("operator", "assume", "R", 900, {"SourceIdentity": SOURCE}),
]


def test_observa_codigos_categoria_e_segundos(lab: ProbeLab) -> None:
    observed = run(lab)
    assert {k: v for k, v in observed.items() if k not in SECONDS} == EXPECTED
    assert set(observed) == set(EXPECTED) | set(SECONDS)
    assert all(isinstance(observed[key], float) and observed[key] >= 0 for key in SECONDS)


def test_ordem_e_payloads_de_cada_passo_com_controle_antes_ou_depois_da_negacao(
    lab: ProbeLab,
) -> None:
    run(lab)
    assert timeline(lab) == STEPS


def test_cada_passo_difere_do_controle_do_passo_1_em_exatamente_uma_condicao(
    lab: ProbeLab,
) -> None:
    run(lab)
    sent = assumes(lab)
    assert sent[0] == {"RoleArn": ROLE_ARN, **BASE}
    assert sent[1] == {**sent[0], "RoleArn": f"{ROLE_ARN}-r2"}
    assert sent[2] == sent[0]
    assert sent[3] == {**sent[0], "DurationSeconds": 3601}
    assert sent[4] == {**sent[0], "Tags": TAG_SET}
    assert sent[5] == {**sent[0], "SourceIdentity": SOURCE}
    assert sent[6] == sent[4]
    assert sent[7] == sent[5]
    for flipped in sent[3:6]:
        assert len({k for k in flipped if flipped[k] != sent[0].get(k)}) == 1


def test_apenas_o_operador_e_a_sessao_de_r_chamam_e_r2_nunca_vira_sessao(lab: ProbeLab) -> None:
    run(lab)
    assert {who for _, _, who in lab.aws.calls} == {"operator", "role"}
    role_ops = [op for _, op, who in lab.aws.calls if who == "role"]
    assert role_ops == ["assume-role", "assume-role"]


def test_policy_de_r_cobre_r_e_r2_so_com_assume_role_e_e_aditiva(lab: ProbeLab) -> None:
    run(lab)
    (put,) = [r for r in lab.aws.requests if r.operation == "put-role-policy"]
    assert put.payload["RoleName"] == NAME
    assert put.payload["PolicyName"] == "cap-assume"
    document = json.loads(str(put.payload["PolicyDocument"]))
    assert document == {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["sts:AssumeRole"],
                "Resource": f"arn:aws:iam::111111111111:role/cnesdata-cap/{NAME}*",
            }
        ],
    }
    assert "delete-role-policy" not in lab.aws.operations()


def test_widening_mantem_o_principal_e_so_acrescenta_acoes(lab: ProbeLab) -> None:
    run(lab)
    (update,) = [r for r in lab.aws.requests if r.operation == "update-assume-role-policy"]
    assert set(update.payload) == {"RoleName", "PolicyDocument"}
    assert update.payload["RoleName"] == NAME
    after = json.loads(str(update.payload["PolicyDocument"]))
    before = lab.aws.roles[NAME].trust
    assert isinstance(before, dict)
    assert len(after["Statement"]) == len(before["Statement"]) == 1
    assert after["Version"] == before["Version"]
    statement = after["Statement"][0]
    assert statement["Effect"] == "Allow"
    assert statement["Principal"] == {"AWS": OPERATOR_ARN}
    assert set(statement["Action"]) == {"sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity"}


def test_trust_de_r2_nunca_e_tocado(lab: ProbeLab) -> None:
    run(lab)
    updates = [r for r in lab.aws.requests if r.operation == "update-assume-role-policy"]
    assert [r.payload["RoleName"] for r in updates] == [NAME]
    assert lab.aws.roles[R2].pending_trust is None


def test_propagacao_da_policy_repete_so_o_controle_da_cadeia_e_registra_segundos(
    tmp_path: Path,
) -> None:
    lab = make_probe_lab(tmp_path, "iam-trust", policy_lag=5)
    observed = run(lab)
    steps = timeline(lab)
    chain = [row for row in steps if row[:3] == ("role", "assume", "R2")]
    assert len(chain) == 5
    assert steps.index(("role", "assume", "R", 900, {})) > steps.index(chain[-1])
    assert float(str(observed["chain_seconds"])) > float(str(observed["assume_seconds"]))
    assert observed["self_assume"] == "AccessDenied"
    assert lab.pauses


def test_propagacao_do_trust_repete_o_passo_5_ate_ok_e_so_depois_executa_o_6(
    tmp_path: Path,
) -> None:
    lab = make_probe_lab(tmp_path, "iam-trust", trust_lag=4)
    observed = run(lab)
    steps = timeline(lab)
    update = steps.index(("operator", "update-assume-role-policy"))
    after = steps[update + 1 :]
    assert after[:-1] == [("operator", "assume", "R", 900, {"Tags": TAG_SET})] * 4
    assert after[-1] == ("operator", "assume", "R", 900, {"SourceIdentity": SOURCE})
    assert float(str(observed["session_tags_ready_seconds"])) > 3
    assert observed["session_tags_after"] == observed["source_identity_after"] == "ok"


def test_assume_de_r_pelo_operador_lento_repete_com_limite(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-trust", assume_denials=3)
    observed = run(lab)
    assert float(str(observed["assume_seconds"])) > 0
    assert len([r for r in timeline(lab) if r[:3] == ("operator", "assume", "R")]) >= 4


@pytest.mark.parametrize(
    ("options", "step"),
    [
        ({"assume_denials": 10**6}, "assume_r"),
        ({"policy_lag": 10**6}, "assume_r2"),
        ({"trust_lag": 10**6}, "tags_flip"),
    ],
)
def test_controle_que_nunca_fica_verde_falha_com_limite(
    tmp_path: Path, options: dict, step: str
) -> None:
    lab = make_probe_lab(tmp_path, "iam-trust", **options)
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == (f"probe=control_failed step={step}",)
    assert len(lab.pauses) <= 11 * 3


def test_negacao_nao_e_executada_enquanto_o_controle_anterior_nao_fica_verde(
    tmp_path: Path,
) -> None:
    lab = make_probe_lab(tmp_path, "iam-trust", policy_lag=10**6)
    with pytest.raises(ServiceFailure):
        run(lab)
    steps = timeline(lab)
    assert ("role", "assume", "R", 900, {}) not in steps
    assert all(row[3] == 900 and row[4] == {} for row in steps if row[1] == "assume")
    assert len([row for row in steps if row[:3] == ("role", "assume", "R2")]) == 12


def test_widening_ineficaz_falha_no_passo_5_pareado(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-trust")
    lab.aws._iam_update_assume_role_policy = lambda payload, who: good()  # type: ignore[method-assign]
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=control_failed step=tags_flip",)


def test_widening_negado_falha_no_proprio_passo_sem_assumir_depois(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-trust")
    lab.aws._iam_update_assume_role_policy = lambda payload, who: denied()  # type: ignore[method-assign]
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=step_failed step=widen_trust",)
    operations = [operation for _, operation, _ in lab.aws.calls]
    after = operations[operations.index("update-assume-role-policy") + 1 :]
    assert "assume-role" not in after

def test_widening_so_com_tag_session_falha_no_passo_6_pareado(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-trust")
    original = lab.aws._iam_update_assume_role_policy

    def tags_only(payload: dict, who: str):
        document = json.loads(payload["PolicyDocument"])
        document["Statement"][0]["Action"] = ["sts:AssumeRole", "sts:TagSession"]
        return original({**payload, "PolicyDocument": json.dumps(document)}, who)

    lab.aws._iam_update_assume_role_policy = tags_only  # type: ignore[method-assign]
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=control_failed step=source_flip",)


@pytest.mark.parametrize(
    ("message", "category"),
    [
        (ROLE_CHAINING, "role_chaining"),
        (MAX_SESSION, "max_session_duration"),
        ("Other text", "other"),
    ],
)
def test_categoria_da_duracao_sai_da_mensagem_e_a_mensagem_nao_vaza(
    tmp_path: Path, message: str, category: str
) -> None:
    lab = make_probe_lab(tmp_path, "iam-trust", duration_message=message)
    observed = run(lab)
    assert observed["duration_3601_category"] == category
    assert message not in json.dumps(observed)
    assert "limit" not in json.dumps(observed)


@pytest.mark.parametrize(
    ("message", "category"),
    [
        (ROLE_CHAINING, "role_chaining"),
        (MAX_SESSION, "max_session_duration"),
        ("exceeds the maximum session duration", "max_session_duration"),
        ("", "other"),
        ("unrelated", "other"),
    ],
)
def test_duration_category_mapeia_mensagens_conhecidas(message: str, category: str) -> None:
    assert duration_category(message) == category


def test_mensagem_do_attempt_nao_aparece_em_repr_nem_str() -> None:
    attempt = Attempt(False, "ValidationError", {}, "MESSAGE_CANARY")
    assert "MESSAGE_CANARY" not in repr(attempt) + str(attempt)


def test_constantes_sinteticas_de_tag_e_source_nunca_viram_evidencia(lab: ProbeLab) -> None:
    observed = run(lab)
    text = json.dumps(observed)
    assert "cap-tag" not in text
    assert SOURCE not in text
    assert "cap-tag" in payload_text(lab)


def test_credenciais_de_todas_as_sessoes_ficam_so_na_memoria(lab: ProbeLab) -> None:
    observed = run(lab)
    secrets = (ACCESS_KEY, SECRET, TOKEN)
    public = json.dumps(observed) + payload_text(lab) + repr(lab.services) + repr(lab.pauses)
    public += repr(lab.aws.calls) + repr(lab.services.ports)
    assert not any(secret in public for secret in secrets)


def test_falha_nao_vaza_credenciais(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-trust", trust_lag=10**6)
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    text = "".join(traceback.format_exception(error.value)) + repr(error.value)
    assert not any(secret in text for secret in (ACCESS_KEY, SECRET, TOKEN))


@pytest.mark.parametrize("case", ["lane", "extra", "ordem", "vazio"])
def test_recusa_recursos_ou_lane_invalidos_sem_io(lab: ProbeLab, case: str) -> None:
    resources = {
        "lane": lab.resources,
        "extra": [*lab.resources, ROLE],
        "ordem": [ROLE2, ROLE],
        "vazio": [],
    }[case]
    context = RequestContext(lab.budget, "cleanup") if case == "lane" else lab.context
    with pytest.raises(ServiceFailure) as error:
        lab.services.probe(resources, lab.spec, context)
    assert error.value.args == ("probe=trust_invalid",)
    assert lab.aws.calls == []


@pytest.mark.parametrize("missing", ["session", "operator"])
def test_recusa_portas_ausentes_sem_io(lab: ProbeLab, missing: str) -> None:
    ports = lab.services.ports
    without = ports.__class__(
        ports.commands,
        ports.now,
        ports.pause,
        None if missing == "session" else ports.session,
        None if missing == "operator" else ports.operator,
    )
    services = lab.services.__class__(lab.spec, without)
    with pytest.raises(ServiceFailure) as error:
        services.probe(lab.resources, lab.spec, lab.context)
    assert error.value.args == ("probe=session_unavailable",)
    assert lab.aws.calls == []
