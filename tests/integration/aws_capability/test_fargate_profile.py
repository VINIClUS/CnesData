"""Perfil fargate-write: raízes fechadas, serviços do inventário e perfis existentes intactos."""

from pathlib import Path

import pytest

from tests.integration.aws_capability._b1_testing import ACCOUNT, NAME
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._profiles import PROFILES, Root
from tests.integration.aws_capability._safety import (
    FARGATE_SERVICES,
    SERVICES,
    SafetyFailure,
    owned,
    services_for,
    tags,
)
from tests.integration.aws_capability.contracts import Ownership, Resource, RunSpec

FARGATE = "fargate-write"
SPEC = RunSpec(ACCOUNT, "us-east-2", "a1b2c3d4", 1060, Path("/nonexistent"), probe=FARGATE)
FROZEN = {
    "cognito-resource-server": (("cognito",), (512, 8, 2, 240, 256), 180.0, ()),
    "all": (("cognito", "dynamodb", "s3", "iam"), (80, 8, 2, 30, 40), 180.0, ()),
    "cognito-login": (("cognito",), (512, 8, 2, 360, 256), 1100.0, ()),
    "ddb-transactions": (("dynamodb",), (192, 8, 2, 150, 96), 180.0, ()),
    "ddb-conditions": (("dynamodb",), (256, 8, 2, 150, 128), 180.0, ()),
    "ddb-ttl-gsi": (("dynamodb",), (256, 8, 2, 150, 128), 180.0, ()),
    "ddb-throughput": (("dynamodb",), (128, 8, 2, 150, 64), 140.0, ()),
    "s3-retention": (("s3", "iam"), (384, 8, 2, 240, 192), 180.0, ()),
    "s3-hold": (("s3",), (256, 8, 2, 120, 128), 180.0, ()),
    "iam-trust": (("iam",), (176, 8, 2, 150, 104), 180.0, ("iam", "iam:r2")),
    "iam-leadingkeys": (
        ("dynamodb", "iam"), (270, 8, 2, 285, 150), 330.0, ("dynamodb", "iam:audit", "iam:api"),
    ),
    "iam-s3": (
        ("s3", "iam"), (550, 8, 2, 630, 380), 425.0,
        ("s3", "iam:audit", "iam:unit", "iam:recovery", "iam:api"),
    ),
}


def _shape(name: str) -> tuple[object, ...]:
    profile = PROFILES[name]
    limits = profile.limits
    numbers = (limits.calls, limits.pages, limits.attempts, limits.cleanup_seconds)
    roots = tuple(
        f"{r.service}:{r.suffix}" if r.suffix else r.service
        for r in profile.roots
    )
    return (profile.services, (*numbers, limits.cleanup_reserve), profile.window, roots)


@pytest.mark.parametrize("name", sorted(FROZEN))
def test_perfis_existentes_mantem_servicos_limites_janela_e_raizes(name: str) -> None:
    assert _shape(name) == FROZEN[name]


def test_registro_de_perfis_so_ganha_o_fargate_write() -> None:
    assert set(PROFILES) == set(FROZEN) | {FARGATE}


def test_perfil_declara_as_seis_raizes_na_ordem_de_criacao() -> None:
    profile = PROFILES[FARGATE]
    assert profile.planned_roots() == (
        Root("ecr"), Root("logs"), Root("iam", "exec"), Root("iam", "task"),
        Root("ec2"), Root("ecs"),
    )
    assert set(profile.services) == {"iam", "ecr", "ecs", "logs", "ec2"}
    assert profile.children == ()


def test_services_continua_intacto_e_fargate_services_o_estende_sem_perder_nada() -> None:
    assert SERVICES == ("cognito", "dynamodb", "s3", "iam")
    assert set(FARGATE_SERVICES) == set(SERVICES) | {"ecr", "ecs", "logs", "ec2"}
    assert len(FARGATE_SERVICES) == len(set(FARGATE_SERVICES))


def test_ordem_do_inventario_fargate_poe_ecs_antes_de_ec2_logs_ecr_e_iam() -> None:
    order = {service: index for index, service in enumerate(FARGATE_SERVICES)}
    assert order["ecs"] < order["ec2"]
    assert all(order["ecs"] < order[other] for other in ("logs", "ecr", "iam"))


@pytest.mark.parametrize("name", sorted(FROZEN))
def test_services_for_devolve_o_conjunto_atual_para_todo_perfil_existente(name: str) -> None:
    assert services_for(name) == SERVICES


def test_services_for_do_fargate_write_devolve_o_conjunto_ampliado() -> None:
    assert services_for(FARGATE) == FARGATE_SERVICES


def _owner(spec: RunSpec, **changes: object) -> Ownership:
    values = {"account": ACCOUNT, "tags": tags(spec), "region": "us-east-2"}
    return Ownership(**{**values, **changes})  # type: ignore[arg-type]


@pytest.mark.parametrize("service", ["ecr", "ecs", "logs", "ec2"])
def test_owned_reconhece_servico_fargate_so_no_perfil_fargate(service: str) -> None:
    resource = Resource(service, NAME, NAME)
    assert owned(resource, _owner(SPEC), SPEC) is True
    other = RunSpec(ACCOUNT, "us-east-2", "a1b2c3d4", 1060, Path("/x"), probe="s3-hold")
    assert owned(resource, _owner(other), other) is False


@pytest.mark.parametrize(
    "changes",
    [
        {"account": "222222222222"},
        {"region": "us-east-1"},
        {"tags": {**tags(SPEC), "Purpose": "other"}},
        {"tags": {**tags(SPEC), "Run": "ffffffff"}},
        {"tags": {}},
    ],
)
def test_owned_recusa_conta_regiao_ou_tag_divergente_no_fargate(changes: dict) -> None:
    resource = Resource("ecs", NAME, NAME)
    assert owned(resource, _owner(SPEC), SPEC) is True
    assert owned(resource, _owner(SPEC, **changes), SPEC) is False


def test_owned_recusa_nome_de_outro_run_no_fargate() -> None:
    resource = Resource("ecr", "cnesdata-cap-ffffffff", "cnesdata-cap-ffffffff")
    assert owned(Resource("ecr", NAME, NAME), _owner(SPEC), SPEC) is True
    assert owned(resource, _owner(SPEC), SPEC) is False


def test_journal_do_fargate_persiste_e_recarrega_recursos_dos_servicos_novos(
    tmp_path: Path,
) -> None:
    spec = RunSpec(ACCOUNT, "us-east-2", "a1b2c3d4", 1060, tmp_path / "j", probe=FARGATE)
    prepare_directory(spec.journal_dir)
    journal = Journal(spec)
    journal.created = [
        Resource("ecr", NAME, NAME), Resource("ec2", "sg-0cap1", NAME),
        Resource("ecs", NAME, NAME), Resource("logs", NAME, NAME),
    ]
    journal.phase = "cleanup_requested"
    journal.save()
    reloaded = Journal(spec)
    reloaded.load()
    assert reloaded.created == journal.created


def test_journal_de_perfil_antigo_continua_recusando_servico_fargate(tmp_path: Path) -> None:
    spec = RunSpec(ACCOUNT, "us-east-2", "a1b2c3d4", 1060, tmp_path / "j", probe="s3-hold")
    prepare_directory(spec.journal_dir)
    journal = Journal(spec)
    journal.created = [Resource("ecr", NAME, NAME)]
    journal.save()
    with pytest.raises(SafetyFailure) as error:
        Journal(spec).load()
    assert error.value.reason == "journal_corrupt"
