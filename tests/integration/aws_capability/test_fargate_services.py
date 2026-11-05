"""Operações por serviço do fargate-write: criação fechada, posse, delete e ausência."""

from pathlib import Path

import pytest

from tests.integration.aws_capability._aws_fake_fargate_testing import (
    DEFAULT_VPC,
    TLS_EGRESS,
    FakeRevision,
    FakeTask,
)
from tests.integration.aws_capability._aws_fake_types_testing import denied
from tests.integration.aws_capability._b1_testing import NAME
from tests.integration.aws_capability._fargate_testing import (
    CLUSTER,
    ECR,
    EXEC,
    GROUP,
    LOGS,
    OTHER_RUN,
    FargateLab,
    first,
    make_fargate_lab,
    operations,
    seed_foreign,
)
from tests.integration.aws_capability._safety import owned, tags
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Creation,
    Page,
    RequestContext,
    Resource,
    ServiceFailure,
)

FAR_FUTURE = 10.0**9
LEAF_ROOTS = [ECR, LOGS, GROUP, CLUSTER]


@pytest.fixture
def lab(tmp_path: Path) -> FargateLab:
    return make_fargate_lab(tmp_path / "j", seeded=False)


@pytest.fixture
def seeded(tmp_path: Path) -> FargateLab:
    return make_fargate_lab(tmp_path / "j")


def cleanup(lab: FargateLab) -> RequestContext:
    return RequestContext(lab.budget, "cleanup")


def creation(lab: FargateLab, name: str = NAME) -> Creation:
    return Creation(lab.spec, name, tags(lab.spec))


def test_cria_repositorio_imutavel_com_tags_do_run(lab: FargateLab) -> None:
    created = lab.services.create("ecr", creation(lab), lab.context)
    assert (created.service, created.name) == ("ecr", NAME)
    repo = lab.state.repos[NAME]
    assert repo.mutability == "IMMUTABLE"
    assert repo.tags == tags(lab.spec)


def test_cria_log_group_com_retencao_de_um_dia_e_tags_do_run(lab: FargateLab) -> None:
    created = lab.services.create("logs", creation(lab), lab.context)
    assert (created.service, created.name) == ("logs", NAME)
    group = lab.state.log_groups[NAME]
    assert group.retention == 1
    assert group.tags == tags(lab.spec)


def test_cria_cluster_com_tags_do_run(lab: FargateLab) -> None:
    created = lab.services.create("ecs", creation(lab), lab.context)
    assert (created.service, created.name) == ("ecs", NAME)
    assert lab.state.clusters[NAME].tags == tags(lab.spec)
    assert lab.state.clusters[NAME].status == "ACTIVE"


def test_cria_security_group_na_vpc_default_sem_ingress_e_so_egress_tls(lab: FargateLab) -> None:
    created = lab.services.create("ec2", creation(lab), lab.context)
    assert (created.service, created.name) == ("ec2", NAME)
    group = lab.state.groups[created.resource_id]
    assert group.vpc == DEFAULT_VPC
    assert group.ingress == []
    assert group.egress == [TLS_EGRESS]
    assert group.tags == tags(lab.spec)
    assert group.name == NAME


def test_security_group_com_revogacao_negada_falha_sem_dar_egress_total_por_sucesso(
    lab: FargateLab,
) -> None:
    control = make_fargate_lab(lab.spec.journal_dir.parent / "control", seeded=False)
    made = control.services.create("ec2", creation(control), control.context)
    assert control.state.groups[made.resource_id].egress == [TLS_EGRESS]
    lab.state.revoke_error = "UnauthorizedOperation"
    with pytest.raises(ServiceFailure):
        lab.services.create("ec2", creation(lab), lab.context)
    assert all(g.egress != [TLS_EGRESS] for g in lab.state.groups.values())


@pytest.mark.parametrize(
    ("service", "name"),
    [
        ("ecr", f"{NAME}-zz"),
        ("ecr", "cnesdata-cap-ffffffff"),
        ("ecr", "cnesdata-prod-processor"),
        ("logs", f"{NAME}-exec"),
        ("ecs", "cnesdata-cap-ffffffff"),
        ("ec2", f"{NAME}-task"),
    ],
)
def test_create_so_aceita_o_nome_de_raiz_declarado_pelo_perfil(
    lab: FargateLab, service: str, name: str
) -> None:
    lab.services.create("ecr", creation(lab), lab.context)
    before = len(lab.aws.calls)
    with pytest.raises(ServiceFailure):
        lab.services.create(service, creation(lab, name), lab.context)
    assert len(lab.aws.calls) == before


def test_create_recusa_tags_diferentes_das_do_run_e_lane_de_cleanup(lab: FargateLab) -> None:
    lab.services.create("logs", creation(lab), lab.context)
    before = len(lab.aws.calls)
    wrong = Creation(lab.spec, NAME, {**tags(lab.spec), "Run": "ffffffff"})
    with pytest.raises(ServiceFailure):
        lab.services.create("ecs", wrong, lab.context)
    with pytest.raises(ServiceFailure):
        lab.services.create("ecs", creation(lab), cleanup(lab))
    assert len(lab.aws.calls) == before
    assert NAME not in lab.state.clusters


@pytest.mark.parametrize("resource", LEAF_ROOTS + [EXEC], ids=lambda r: r.service)
def test_inspect_e_lookup_devolvem_posse_do_run_para_cada_raiz(
    seeded: FargateLab, resource: Resource
) -> None:
    for read in (seeded.services.inspect, seeded.services.lookup):
        owner = read(resource, cleanup(seeded))
        assert owner is not None
        assert owned(resource, owner, seeded.spec)


@pytest.mark.parametrize("resource", LEAF_ROOTS, ids=lambda r: r.service)
def test_inspect_expoe_tag_divergente_e_a_posse_deixa_de_valer(
    seeded: FargateLab, resource: Resource
) -> None:
    assert seeded.services.lookup(resource, cleanup(seeded)) is not None
    diverge(seeded, resource.service)
    owner = seeded.services.inspect(resource, cleanup(seeded))
    assert not owned(resource, owner, seeded.spec)


def diverge(lab: FargateLab, service: str) -> None:
    state = lab.state
    target = {
        "ecr": lambda: state.repos[NAME].tags,
        "logs": lambda: state.log_groups[NAME].tags,
        "ecs": lambda: state.clusters[NAME].tags,
        "ec2": lambda: next(iter(state.groups.values())).tags,
    }[service]()
    target["Run"] = "ffffffff"


@pytest.mark.parametrize("resource", LEAF_ROOTS, ids=lambda r: r.service)
def test_lookup_devolve_none_quando_o_recurso_nao_existe(
    seeded: FargateLab, resource: Resource
) -> None:
    assert seeded.services.lookup(resource, cleanup(seeded)) is not None
    state = seeded.state
    {"ecr": state.repos, "logs": state.log_groups, "ecs": state.clusters,
     "ec2": state.groups}[resource.service].clear()
    assert seeded.services.lookup(resource, cleanup(seeded)) is None


def test_lookup_trata_cluster_inativo_como_ausente(seeded: FargateLab) -> None:
    assert seeded.services.lookup(CLUSTER, cleanup(seeded)) is not None
    seeded.state.clusters[NAME].status = "INACTIVE"
    assert seeded.services.lookup(CLUSTER, cleanup(seeded)) is None


@pytest.mark.parametrize("service", ["ecr", "logs", "ecs", "ec2"])
def test_inventario_lista_o_recurso_do_run_e_o_alheio_com_nome_cap(
    seeded: FargateLab, service: str
) -> None:
    seed_foreign(seeded.aws, seeded.spec)
    page = seeded.services.inventory(service, None, cleanup(seeded))
    names = {resource.name for resource in page.resources}
    assert {NAME, OTHER_RUN} <= names
    assert page.cursor is None
    assert {resource.service for resource in page.resources} == {service}


def test_inventario_de_ecs_nao_lista_cluster_inativo(seeded: FargateLab) -> None:
    seeded.state.clusters[NAME].status = "INACTIVE"
    page = seeded.services.inventory("ecs", None, cleanup(seeded))
    assert NAME not in {resource.name for resource in page.resources}


@pytest.mark.parametrize("resource", LEAF_ROOTS, ids=lambda r: r.service)
def test_dependents_de_raiz_ociosa_e_vazio(seeded: FargateLab, resource: Resource) -> None:
    assert seeded.services.dependents(resource, None, cleanup(seeded)) == Page()


def test_delete_do_repositorio_usa_force_e_remove_imagens(seeded: FargateLab) -> None:
    seeded.state.repos[NAME].images["cap"] = "sha256:" + "1" * 64
    seeded.services.delete(ECR, True, cleanup(seeded))
    assert NAME not in seeded.state.repos
    deletes = [r for r in seeded.aws.requests if r.operation == "delete-repository"]
    assert [dict(r.payload).get("force") for r in deletes] == [True]


def test_delete_do_log_group_remove_so_o_grupo_do_run(seeded: FargateLab) -> None:
    seed_foreign(seeded.aws, seeded.spec)
    seeded.services.delete(LOGS, True, cleanup(seeded))
    assert set(seeded.state.log_groups) == {OTHER_RUN}


@pytest.mark.parametrize("resource", LEAF_ROOTS, ids=lambda r: r.service)
def test_delete_com_posse_divergente_recusa_sem_nenhuma_chamada_de_remocao(
    seeded: FargateLab, resource: Resource
) -> None:
    diverge(seeded, resource.service)
    with pytest.raises(ServiceFailure) as error:
        seeded.services.delete(resource, True, cleanup(seeded))
    assert error.value.args == ("service=not_owned",)
    removals = [op for op in operations(seeded) if "delete" in op or "stop-task" in op]
    assert removals == []
    state = seeded.state
    assert (len(state.repos), len(state.groups), len(state.clusters)) == (1, 1, 1)
    assert len(state.log_groups) == 1


def test_delete_recusa_lane_de_trabalho_e_bypass_nao_booleano(seeded: FargateLab) -> None:
    seeded.services.delete(LOGS, True, cleanup(seeded))
    with pytest.raises(ServiceFailure):
        seeded.services.delete(ECR, True, seeded.context)
    with pytest.raises(ServiceFailure):
        seeded.services.delete(ECR, "yes", cleanup(seeded))  # type: ignore[arg-type]
    assert NAME in seeded.state.repos


def _add_revision(lab: FargateLab, family: str, status: str = "ACTIVE") -> FakeRevision:
    revision = FakeRevision(family, 1, {}, status)
    lab.state.revisions.append(revision)
    return revision


def _add_running_task(lab: FargateLab, cluster: str = NAME) -> FakeTask:
    task = FakeTask(
        f"arn:aws:ecs:us-east-2:111111111111:task/{cluster}/t1", cluster, "sg-0cap000000000001",
        0.0, FAR_FUTURE, True, 0, None,
    )
    lab.state.tasks[task.arn] = task
    return task


def test_delete_do_cluster_para_task_espera_parar_apaga_revisoes_do_run_e_depois_o_cluster(
    seeded: FargateLab,
) -> None:
    seed_foreign(seeded.aws, seeded.spec)
    own = [_add_revision(seeded, f"{NAME}-probe"), _add_revision(seeded, f"{NAME}-recovery")]
    prod = _add_revision(seeded, "cnesdata-prod-processor-recovery")
    other = _add_revision(seeded, f"{OTHER_RUN}-probe")
    task = _add_running_task(seeded)
    seeded.services.delete(CLUSTER, True, cleanup(seeded))
    assert seeded.state.clusters[NAME].status == "INACTIVE"
    assert task.stopped_by_request is True
    assert [r.status for r in own] == ["DELETE_IN_PROGRESS"] * 2
    assert (prod.status, other.status) == ("ACTIVE", "ACTIVE")
    assert seeded.state.clusters[OTHER_RUN].status == "ACTIVE"
    ops = operations(seeded)
    delete_cluster = first(seeded, "ecs.delete-cluster")
    assert first(seeded, "ecs.stop-task") < delete_cluster
    assert ops.index("ecs.deregister-task-definition") < ops.index("ecs.delete-task-definitions")
    assert ops.index("ecs.delete-task-definitions") < delete_cluster
    assert ops.index("ecs.stop-task") < max(
        i for i, op in enumerate(ops) if op == "ecs.describe-tasks" and i < delete_cluster
    )


def test_delete_do_cluster_nao_chega_ao_cluster_se_a_task_nao_parar(seeded: FargateLab) -> None:
    control = make_fargate_lab(seeded.spec.journal_dir.parent / "control")
    _add_running_task(control)
    control.services.delete(CLUSTER, True, cleanup(control))
    assert "ecs.delete-cluster" in operations(control)
    seeded.state.stop_seconds = FAR_FUTURE
    _add_running_task(seeded)
    with pytest.raises((ServiceFailure, BudgetExhausted)):
        seeded.services.delete(CLUSTER, True, cleanup(seeded))
    assert "ecs.delete-cluster" not in operations(seeded)
    assert seeded.state.clusters[NAME].status == "ACTIVE"
    assert operations(seeded).count("ecs.describe-tasks") <= 60


def test_delete_do_cluster_sem_task_nem_revisao_so_remove_o_cluster(seeded: FargateLab) -> None:
    seeded.services.delete(CLUSTER, True, cleanup(seeded))
    assert seeded.state.clusters[NAME].status == "INACTIVE"
    assert "ecs.stop-task" not in operations(seeded)


def test_delete_do_security_group_repete_dependency_violation_ate_a_eni_sair(
    seeded: FargateLab,
) -> None:
    seeded.state.sg_violations = 4
    seeded.services.delete(GROUP, True, cleanup(seeded))
    assert seeded.state.groups == {}
    assert operations(seeded).count("ec2.delete-security-group") == 5
    assert len(seeded.pauses) >= 4


def test_delete_do_security_group_tem_limite_e_deixa_o_recurso(seeded: FargateLab) -> None:
    seeded.state.sg_violations = 10**6
    with pytest.raises((ServiceFailure, BudgetExhausted)):
        seeded.services.delete(GROUP, True, cleanup(seeded))
    assert len(seeded.state.groups) == 1
    assert 2 <= operations(seeded).count("ec2.delete-security-group") <= 60


def test_delete_do_security_group_so_repete_dependency_violation(seeded: FargateLab) -> None:
    seeded.aws._ec2_delete_security_group = (  # type: ignore[method-assign]
        lambda payload, who: denied("UnauthorizedOperation")
    )
    with pytest.raises(ServiceFailure):
        seeded.services.delete(GROUP, True, cleanup(seeded))
    assert operations(seeded).count("ec2.delete-security-group") == 1
