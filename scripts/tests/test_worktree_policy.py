"""Valida a política executável de worktrees do backlog CND."""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
ISSUE_FORM = ROOT / ".github/ISSUE_TEMPLATE/cnesdata-implementation.yml"
POLICY = ROOT / "docs/development/worktree-ownership.md"
BRANCH_PATTERN = r"^(feat|fix|test|docs)/cnd-[0-9]{3}-[a-z0-9-]+$"
EXPECTED_IDS = {
    "logical_id",
    "depends_on",
    "allowed_paths",
    "forbidden_shared_paths",
    "interfaces_consumed",
    "interfaces_produced",
    "verification_commands",
}
EXPECTED_LABELS = {
    "allowed_paths": "Caminhos permitidos",
    "forbidden_shared_paths": "Caminhos compartilhados proibidos",
}
BRANCH_EXAMPLES = (
    "feat/cnd-020-sqlite-control-plane",
    "fix/cnd-021-control-plane-retry",
    "test/cnd-022-control-plane-race",
    "docs/cnd-003-worktree-ownership",
)
CONTROLLER_ROWS = (
    "| Dependency manifests | Root and package `pyproject.toml` files |",
    (
        "| Locks | `uv.lock`, Go module lock changes spanning another task, "
        "and frontend lockfiles |"
    ),
    "| Shared package exports | Package `__init__.py` exports used by multiple tasks |",
    "| Composition roots | Application bootstrap and dependency-injection composition roots |",
    "| Generated contracts | Generated OpenAPI and JSON Schema artifacts |",
    "| Root planning docs | Root documentation indexes and roadmap |",
    "| Delivery configuration | Docker Compose, CI workflows, and deployment-wide configuration |",
)
DEFINITION_OF_READY = """A task is ready for dispatch only when:

- all `Depends on` tasks are merged into `develop`;
- consumed interfaces exist at the documented signatures;
- allowed paths do not overlap another active task;
- the baseline test command is known and runnable;
- fixtures or external emulators required by the task are available;
- the acceptance criteria can be verified without another unmerged branch."""
DEFINITION_OF_DONE = """A task is done only when:

- behavior and negative tests pass;
- relevant contract, property, race, security, and recovery tests pass;
- lint, type, coverage, and build gates pass;
- no forbidden dependency or legacy coupling was introduced;
- generated artifacts are updated by the integration lane when applicable;
- the PR documents consumed and produced interfaces;
- the wave-level suite remains green after integration.

A phase is done only when its final gate is green on integrated `develop`, not when
individual worktrees pass in isolation."""


def _load_issue_form() -> dict[str, object]:
    return yaml.safe_load(ISSUE_FORM.read_text(encoding="utf-8"))


def _load_policy() -> str:
    return POLICY.read_text(encoding="utf-8")


def test_define_metadados_minimos_sem_automacoes() -> None:
    issue_form = _load_issue_form()

    assert set(issue_form) == {"name", "description", "body"}
    assert issue_form["name"]
    assert issue_form["description"]


def test_exige_sete_campos_com_ids_unicos() -> None:
    body = _load_issue_form()["body"]
    ids = [field["id"] for field in body]

    assert len(body) == 7
    assert len(ids) == len(set(ids))
    assert set(ids) == EXPECTED_IDS
    assert all(field["validations"] == {"required": True} for field in body)


def test_define_tipos_dos_campos_e_renderizacao_shell() -> None:
    fields = {field["id"]: field for field in _load_issue_form()["body"]}

    assert fields["logical_id"]["type"] == "input"
    assert all(fields[field_id]["type"] == "textarea" for field_id in EXPECTED_IDS - {"logical_id"})
    assert fields["verification_commands"]["attributes"]["render"] == "shell"


def test_exibe_campos_de_caminhos_em_portugues() -> None:
    fields = {field["id"]: field for field in _load_issue_form()["body"]}
    labels = {
        field_id: fields[field_id]["attributes"]["label"] for field_id in EXPECTED_LABELS
    }

    assert labels == EXPECTED_LABELS
    assert "faixa de controle" in fields["forbidden_shared_paths"]["attributes"]["description"]


def test_documenta_regex_e_exemplo_valido_para_cada_prefixo() -> None:
    policy = _load_policy()

    assert f"`{BRANCH_PATTERN}`" in policy
    assert {example.split("/", maxsplit=1)[0] for example in BRANCH_EXAMPLES} == {
        "feat",
        "fix",
        "test",
        "docs",
    }
    assert all(re.fullmatch(BRANCH_PATTERN, example) for example in BRANCH_EXAMPLES)
    assert all(f"`{example}`" in policy for example in BRANCH_EXAMPLES)


def test_limita_concorrencia_e_reserva_superficies_para_controller_lane() -> None:
    policy = _load_policy()

    assert "At most three feature worktrees may run at once." in policy
    assert "One controller lane is retained" in policy
    assert all(row in policy for row in CONTROLLER_ROWS)
    assert "unless an issue explicitly grants ownership" in policy


def test_exige_dependencias_integradas_no_ultimo_develop_verde() -> None:
    policy = _load_policy()

    assert "latest green `develop` commit containing all declared dependencies" in policy
    assert "Dependent work never starts from an unmerged branch" in policy


def test_fixa_fila_da_controller_lane() -> None:
    policy = _load_policy()
    normalized_policy = " ".join(policy.split())

    expected_queue = (
        "`CND-064` → AWS Task 8 → Billing Task 6 → Source Task 4 → "
        "Billing Task 13 → Billing Task 17"
    )
    assert expected_queue in normalized_policy
    assert "only after the previous item is on green `develop`" in normalized_policy


def test_reproduz_definitions_of_ready_e_done_literalmente() -> None:
    policy = _load_policy()

    assert DEFINITION_OF_READY in policy
    assert DEFINITION_OF_DONE in policy
