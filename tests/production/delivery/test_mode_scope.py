"""resources.scope do mode-canary derivado do owner do keeper e do tenant do secret."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pytest

from scripts.promotion import mode_context_source as source
from scripts.promotion.lifecycle_inputs import inputs_bytes
from scripts.promotion.mode_scope import mode_scope
from scripts.promotion.prepare_mode_canary import check_mode_canary_input
from tests.production.delivery import _ia_lifecycle_consumer as ia
from tests.production.delivery.test_mode_context_source import CONTEXT, MARKER, TARGET

ROOT = Path(__file__).resolve().parents[3]
TENANT = "prod-canary-" + "c" * 32
OWNERS = ("2" * 64, "e" * 64)
TERRAFORM = (ROOT / "infra/opentofu/modules/runtime-iam/variables.tf").read_text()
_AUDIT_RULE = re.search(r'regex\("(\^audit/.*?\$)", key\)', TERRAFORM)
assert _AUDIT_RULE is not None
AUDIT_REGEX = re.compile(_AUDIT_RULE[1].replace("\\\\", "\\"))


def _context(scope: Any) -> str:
    return json.dumps(CONTEXT | {"resources": CONTEXT["resources"] | {"scope": scope}})


def _canary(owner: str) -> dict[str, Any]:
    context = source.mode_context(_context({"tenant_id": TENANT}))
    marker = source.stage_marker(ia.canonical(MARKER | {"owner_sha256": owner}), TARGET)
    return json.loads(source.mode_canary_file(context, marker, TARGET, "a" * 40))


@pytest.mark.parametrize("owner", OWNERS)
def test_run_ids_seguem_o_owner_do_keeper(owner: str) -> None:
    scope = mode_scope(TENANT, owner)
    assert scope["tenant_id"] == TENANT
    assert scope["run_ids"] == sorted(f"{name}-{owner[:16]}"
                                      for name in ("unit", "recovery-a", "recovery-b"))


@pytest.mark.parametrize("owner", OWNERS)
def test_chaves_seguem_o_layout_do_produto(owner: str) -> None:
    scope = mode_scope(TENANT, owner)
    assert scope["raw_object_keys"]
    assert all(key.startswith(f"raw/{TENANT}/") for key in scope["raw_object_keys"])
    assert all(owner[:16] in key for key in scope["raw_object_keys"])
    assert all(any(key.startswith(f"outputs/{TENANT}/{run}/") for run in scope["run_ids"])
               for key in scope["output_object_keys"])
    for name in ("run_ids", "raw_object_keys", "output_object_keys", "audit_object_keys"):
        assert scope[name] == sorted(set(scope[name]))


@pytest.mark.parametrize("owner", OWNERS)
def test_chaves_de_audit_casam_com_a_regex_do_produto(owner: str) -> None:
    keys = mode_scope(TENANT, owner)["audit_object_keys"]
    assert len(keys) == 3
    assert all(AUDIT_REGEX.fullmatch(key) for key in keys)
    event = f"reconciliation.published:{TENANT}:unit-{owner[:16]}"
    assert f"audit/.event-id/{hashlib.sha256(event.encode()).hexdigest()}.json" in keys


def test_owners_distintos_nao_compartilham_chaves() -> None:
    first, second = (mode_scope(TENANT, owner) for owner in OWNERS)
    for name in ("run_ids", "raw_object_keys", "output_object_keys", "audit_object_keys"):
        assert not set(first[name]) & set(second[name])


@pytest.mark.parametrize("owner", OWNERS)
def test_input_do_run_leva_o_scope_derivado_e_passa_nos_dois_oraculos(owner: str) -> None:
    value = _canary(owner)
    assert value["resources"]["scope"] == mode_scope(TENANT, owner)
    check_mode_canary_input(value)
    ia.decode_mode_context(inputs_bytes({"region": "us-east-2"}, mode_canary=value))


def test_resources_sha256_cobre_o_scope_completo() -> None:
    value = _canary(OWNERS[0])
    manifest = json.loads(value["context_manifest"]["canonical_utf8"])
    assert manifest["resources_sha256"] == hashlib.sha256(
        ia.canonical(value["resources"])).hexdigest()


@pytest.mark.parametrize("scope", [
    {"tenant_id": TENANT, "run_ids": ["unit-" + "2" * 16]},
    {"tenant_id": TENANT, "raw_object_keys": []},
    {"tenant_id": "prod-canary-" + "C" * 32},
    {"tenant_id": "prod-canary-" + "c" * 31},
    {"tenant_id": "tenant-1"},
    {},
    "tenant",
])
def test_recusa_scope_pre_computado_ou_tenant_fora_da_forma(scope: Any) -> None:
    with pytest.raises(ValueError, match="mode_context field=resources"):
        source.mode_context(_context(scope))


def test_recusa_contexto_sem_scope() -> None:
    raw = json.dumps(CONTEXT | {"resources": {k: v for k, v in CONTEXT["resources"].items()
                                              if k != "scope"}})
    with pytest.raises(ValueError, match="mode_context field=resources"):
        source.mode_context(raw)


def test_admite_scope_so_com_tenant() -> None:
    assert source.mode_context(_context({"tenant_id": TENANT}))["resources"]["scope"] == {
        "tenant_id": TENANT}

