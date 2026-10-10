"""Inventário de audit events de billing contra os produtores no código."""

import ast
import re
from collections.abc import Mapping
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SCANNED_GLOBS = (
    "packages/cnes_domain/src/cnes_domain/billing/*.py",
    "packages/cnes_infra/src/cnes_infra/billing/*.py",
    "packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_billing.py",
    "apps/central_api/src/central_api/routes/billing.py",
    "apps/central_api/src/central_api/services/serving_entitlement.py",
)
EVENT_PATTERN = re.compile(
    r"^(billing_account|checkout|billing|entitlement|subscription|quota|run|run_execution|serving)"
    r"\.[a-z_]+(\.[a-z_]+)?$"
)
LOG_PATTERN = re.compile(r"billing_audit event_type=([a-z_.]+)")
EVENT_CONSTANT_NAME = re.compile(r"(^|_)EVENT(_TYPES?)?$")
POSITIONAL_HELPERS = frozenset({"_audit", "quota_event"})

AUDIT_EVENT_INVENTORY: Mapping[str, str] = {
    "billing_account.created": "dynamodb_catalog: criação de conta",
    "billing_account.customer_attached": "dynamodb_catalog: Stripe Customer anexado",
    "billing_account.transferred": "dynamodb_catalog: transferência de conta",
    "checkout.session_created": "central_api routes/billing: checkout",
    "billing.webhook_failed_final": "webhook_inbox: falha final de webhook",
    "subscription.status_changed": "projector: mudança de status da assinatura",
    "entitlement.changed": "projector: mudança de entitlement",
    "quota.reserved": "dynamodb_quota/dynamodb_quota_capacity: reserva",
    "quota.consumed": "dynamodb_quota_settlement/capacity: consumo",
    "quota.released": "dynamodb_quota_settlement/capacity: liberação",
    "run.authorized": "control_plane/dynamodb_billing: autorização de Run",
    "entitlement.revoked": "revocation: revogação administrativa",
    "run.cancel_requested": "revocation: cancelamento solicitado ao executor",
    "run.canceled": "revocation/dynamodb_revocation_units: Run cancelado",
    "billing.reconciliation_drift": "reconciliation: drift detectado",
    "billing.reconciliation_corrected": "reconciliation: snapshot corrigido",
    "run_execution.bind_failed": "execution_policy: falha de vinculação da execução",
    "entitlement.shadow_denied": "shadow: negação hipotética dos gates de API",
    "serving.denied": "central_api serving_entitlement: negação de serving",
    "entitlement.shadow_access_loss": "enforcement: perda de acesso observada em shadow",
    "run.failed": "revocation: publicação negada após revogação",
}
LOG_ONLY_EVENTS: frozenset[str] = frozenset()


def _trees() -> dict[Path, ast.Module]:
    paths = sorted(path for pattern in SCANNED_GLOBS for path in ROOT.glob(pattern))
    return {path: ast.parse(path.read_text(encoding="utf-8")) for path in paths}


def _str(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _module_constants(trees: Mapping[Path, ast.Module]) -> dict[str, str]:
    constants: dict[str, str] = {}
    for tree in trees.values():
        for node in tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target, value = node.targets[0], _str(node.value)
                if isinstance(target, ast.Name) and value is not None:
                    constants[target.id] = value
    return constants


def _resolve(node: ast.AST, constants: Mapping[str, str]) -> str | None:
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    return _str(node)


def _call_name(call: ast.Call) -> str:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    return func.attr if isinstance(func, ast.Attribute) else ""


def _call_events(call: ast.Call, constants: Mapping[str, str]) -> set[str]:
    found = {
        _resolve(keyword.value, constants)
        for keyword in call.keywords
        if keyword.arg == "event_type"
    }
    if _call_name(call) in POSITIONAL_HELPERS:
        found |= {_resolve(arg, constants) for arg in call.args}
    return {value for value in found if value and EVENT_PATTERN.match(value)}


def _dict_events(node: ast.Assign) -> set[str]:
    names = {t.id for t in node.targets if isinstance(t, ast.Name)}
    if not any(EVENT_CONSTANT_NAME.search(name.lstrip("_")) for name in names):
        return set()
    if not isinstance(node.value, ast.Dict):
        return set()
    values = {_str(value) for value in node.value.values}
    return {value for value in values if value and EVENT_PATTERN.match(value)}


def _named_constant_events(constants: Mapping[str, str]) -> set[str]:
    return {
        value
        for name, value in constants.items()
        if EVENT_CONSTANT_NAME.search(name.lstrip("_")) and EVENT_PATTERN.match(value)
    }


def _produced_events() -> set[str]:
    trees = _trees()
    constants = _module_constants(trees)
    events = _named_constant_events(constants)
    for tree in trees.values():
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                events |= _call_events(node, constants)
            elif isinstance(node, ast.Assign):
                events |= _dict_events(node)
    return events


def _log_only_literals() -> set[str]:
    found: set[str] = set()
    for tree in _trees().values():
        for node in ast.walk(tree):
            literal = _str(node)
            if literal is not None:
                found.update(LOG_PATTERN.findall(literal))
    return found


def test_todo_evento_produzido_esta_no_inventario() -> None:
    produced = _produced_events()
    missing = sorted(produced - set(AUDIT_EVENT_INVENTORY))
    assert not missing, f"events_sem_inventario={missing}"


def test_inventario_nao_lista_evento_sem_produtor() -> None:
    produced = _produced_events()
    orphans = sorted(set(AUDIT_EVENT_INVENTORY) - produced)
    assert not orphans, f"events_sem_produtor={orphans}"


def test_nenhum_evento_de_audit_e_somente_log() -> None:
    assert _log_only_literals() == set()


def test_inventario_cobre_categorias_do_bil_022() -> None:
    prefixes = {
        "conta": "billing_account.",
        "transferencia": "billing_account.transferred",
        "checkout": "checkout.",
        "webhook": "billing.webhook_",
        "subscription": "subscription.",
        "entitlement": "entitlement.",
        "quota": "quota.",
        "revogacao": "entitlement.revoked",
        "cancelamento_de_run": "run.cancel",
    }
    for categoria, prefixo in prefixes.items():
        covered = any(name.startswith(prefixo) for name in AUDIT_EVENT_INVENTORY)
        assert covered, f"categoria_sem_evento={categoria}"
