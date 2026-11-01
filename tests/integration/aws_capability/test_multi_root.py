"""Múltiplas raízes de um serviço: sufixo fechado, journal antes de criar e cleanup completo."""

import json
from copy import deepcopy
from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._aws_fake_types_testing import FakeRole
from tests.integration.aws_capability._b1_testing import (
    NAME,
    OPERATOR_ARN,
    ROLE_ARN,
    TRUST,
    TRUST2,
)
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._probe_testing import (
    ROLE,
    ROLE2,
    ProbeLab,
    janitor_ports,
    make_probe_lab,
)
from tests.integration.aws_capability._profiles import PROFILES, Root
from tests.integration.aws_capability._safety import owned, root_name, tags
from tests.integration.aws_capability.contracts import (
    Creation,
    RequestContext,
    Resource,
    ServiceFailure,
)

WIDENED = deepcopy(TRUST)
WIDENED["Statement"][0]["Action"] = ["sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity"]
ZZ = Resource("iam", f"{NAME}-zz", f"{NAME}-zz")


def cleanup_context(lab: ProbeLab) -> RequestContext:
    return RequestContext(lab.budget, "cleanup")


def test_sufixo_vazio_e_o_nome_do_run_e_sufixo_valido_o_estende(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-trust")
    assert root_name(lab.spec, "") == NAME
    assert root_name(lab.spec, "r2") == f"{NAME}-r2"
    assert root_name(lab.spec, "a1b2c3d4") == f"{NAME}-a1b2c3d4"


@pytest.mark.parametrize("suffix", ["R2", "a-b", "a_b", "abcdefghi", "é", "a b", " ", "r2\n"])
def test_sufixo_fora_do_alfabeto_fechado_e_recusado(suffix: str) -> None:
    with pytest.raises(ValueError, match="root=invalid_suffix"):
        Root("iam", suffix)


def test_perfis_declaram_raizes_e_o_padrao_e_uma_por_servico() -> None:
    assert PROFILES["iam-trust"].planned_roots() == (Root("iam"), Root("iam", "r2"))
    assert PROFILES["s3-retention"].planned_roots() == (Root("s3"), Root("iam"))
    assert PROFILES["all"].planned_roots() == tuple(
        Root(service) for service in ("cognito", "dynamodb", "s3", "iam")
    )


def test_worker_grava_intencao_antes_de_cada_raiz_e_cleanup_remove_todas(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path / "private", "iam-trust", seeded=False)
    seen: list[dict] = []
    original = lab.aws._iam_create_role

    def recording(payload: dict, who: str):
        seen.append(json.loads(lab.spec.journal_dir.joinpath("a1b2c3d4.json").read_bytes()))
        return original(payload, who)

    lab.aws._iam_create_role = recording  # type: ignore[method-assign]
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    creates = [r.payload for r in lab.aws.requests if r.operation == "create-role"]
    assert [p["RoleName"] for p in creates] == [NAME, f"{NAME}-r2"]
    principals = [
        json.loads(str(p["AssumeRolePolicyDocument"]))["Statement"][0]["Principal"]["AWS"]
        for p in creates
    ]
    assert principals == [OPERATOR_ARN, ROLE_ARN]
    assert [(row["pending_service"], row["operation_state"]) for row in seen] == [
        ("iam", "unknown"),
        ("iam", "unknown"),
    ]
    assert [len(row["created"]) for row in seen] == [0, 1]
    assert seen[1]["created"][0]["name"] == NAME
    assert lab.aws.roles == {}
    assert outcome.evidence["final_counts_all_cap"] == {
        "cognito": 0, "dynamodb": 0, "s3": 0, "iam": 0,
    }
    journal = json.loads(lab.spec.journal_dir.joinpath("a1b2c3d4.json").read_bytes())
    assert journal["created"] == []


def test_crash_com_r2_remoto_e_journal_incerto_o_janitor_remove_as_duas_raizes(
    tmp_path: Path,
) -> None:
    lab = make_probe_lab(tmp_path / "private", "iam-trust", seeded=False)
    prepare_directory(lab.spec.journal_dir)
    lab.aws.roles[NAME] = FakeRole(tags(lab.spec), TRUST)
    lab.aws.roles[ROLE2.name] = FakeRole(tags(lab.spec), TRUST2)
    policy = (frozenset({"sts:AssumeRole"}), 0, {"Statement": []})
    lab.aws.roles[NAME].policies["cap-trust"] = policy
    journal = Journal(lab.spec)
    journal.created = [ROLE]
    journal.phase = "creating"
    journal.pending_service = "iam"
    journal.operation_state = "unknown"
    journal.save()
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.roles == {}
    deleted = [str(r.payload["RoleName"]) for r in lab.aws.requests if r.operation == "delete-role"]
    assert sorted(deleted) == sorted([NAME, ROLE2.name])


@pytest.mark.parametrize("widened", [False, True])
def test_trust_ampliado_continua_dono_e_e_removido_sem_ownership_mismatch(
    tmp_path: Path, widened: bool
) -> None:
    lab = make_probe_lab(tmp_path / "private", "iam-trust", seeded=False)
    prepare_directory(lab.spec.journal_dir)
    role = FakeRole(tags(lab.spec), TRUST)
    if widened:
        role.pending_trust = (0, WIDENED)
    lab.aws.roles[NAME] = role
    owner = lab.services.inspect(ROLE, cleanup_context(lab))
    assert owned(ROLE, owner, lab.spec)
    journal = Journal(lab.spec)
    journal.created = [ROLE]
    journal.phase = "cleanup_requested"
    journal.save()
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.roles == {}


@pytest.mark.parametrize(
    ("probe", "resource", "allowed"),
    [
        ("iam-trust", ROLE2, True),
        ("iam-trust", ZZ, False),
        ("s3-retention", ROLE2, False),
    ],
)
def test_delete_so_remove_raizes_declaradas_pelo_perfil(
    tmp_path: Path, probe: str, resource: Resource, allowed: bool
) -> None:
    lab = make_probe_lab(tmp_path / "private", probe, seeded=False)
    lab.aws.roles[resource.name] = FakeRole(tags(lab.spec), TRUST)
    if allowed:
        lab.services.delete(resource, True, cleanup_context(lab))
        assert lab.aws.roles == {}
        return
    with pytest.raises(ServiceFailure) as error:
        lab.services.delete(resource, True, cleanup_context(lab))
    assert error.value.args == ("service=iam_delete_invalid",)
    assert lab.aws.calls == []
    assert resource.name in lab.aws.roles


@pytest.mark.parametrize(
    ("probe", "name", "allowed"),
    [
        ("iam-trust", f"{NAME}-r2", True),
        ("iam-trust", f"{NAME}-zz", False),
        ("s3-retention", f"{NAME}-r2", False),
    ],
)
def test_create_so_aceita_nomes_de_raiz_declarados_pelo_perfil(
    tmp_path: Path, probe: str, name: str, allowed: bool
) -> None:
    lab = make_probe_lab(tmp_path / "private", probe, seeded=False)
    lab.aws.roles[NAME] = FakeRole(tags(lab.spec), TRUST)
    creation = Creation(lab.spec, name, tags(lab.spec))
    if allowed:
        created = lab.services.create("iam", creation, lab.context)
        assert created == Resource("iam", name, name)
        return
    before = len(lab.aws.calls)
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("iam", creation, lab.context)
    assert error.value.args == ("service=creation_invalid",)
    assert len(lab.aws.calls) == before
