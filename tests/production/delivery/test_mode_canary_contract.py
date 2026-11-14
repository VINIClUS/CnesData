"""Codecs fechados do canary de modo: sessões, context manifest e registro."""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import Any

import pytest

from cnes_infra.aws.mode_canary import canonical_text
from scripts.promotion._canonical import canonical_bytes
from scripts.promotion.mode_canary_contract import (
    ContractError,
    parse_context_manifest,
    record_bytes,
    session_binding_bytes,
    session_name,
    session_names,
)

_BASE: dict[str, Any] = {
    "activation_sha256": "3" * 64,
    "context_manifest_sha256": "5" * 64,
    "generation": 7,
    "owner_sha256": "4" * 64,
    "product_source_closure_sha256": "1" * 64,
    "product_source_sha": "d58a7e4fd17c8a8128841e545428ab020ada1f88",
    "release_id": "r-vector-499",
    "repository": "VINIClUS/CnesData",
    "run_attempt": 1,
    "run_id": 4990001,
    "schema_version": 2,
    "shared_source_closure_sha256": "2" * 64,
    "shared_source_sha": "2e51aaa560372679a54002788d80d46ad4fee5b3",
}
_SESSION_VECTORS = (
    ("unit-seed", "us", "unit", "promotion",
     "48b7d736784048cbc38012c80ccfca7b9d4b4882637997e54ec28b4fcc8ffbab"),
    ("unit-start", "ux", "unit", "promotion",
     "bef725d100de2341734a37bba58e966a10251fd841ec95fe1487b2630dc81609"),
    ("unit-observe", "uo", "unit", "promotion",
     "bc5abb91dec9c2e5711c7806eb673258de3682fe3ec16cb6b2e8be6409105216"),
    ("recovery-seed", "rs", "recovery", "promotion",
     "286077e480bfd6c77c80e132d9e950cd2c07d10580770eec4bdcd9eca223f0e2"),
    ("recovery-start", "rx", "recovery", "promotion",
     "c1c6960e3e734ad331bfb47528906086ee4ca3bc6312b76636ae0bb4d3b3bad2"),
    ("recovery-observe", "ro", "recovery", "promotion",
     "af7e35d324b6a430776faf51f32c112db8aed9604b1ff69d4419fb78fc33e4b4"),
    ("audit-start", "ax", "audit", "promotion",
     "cd2004f9e918765cf56d5797f0b3101f9433b8882b8a7d09fbf7f9df7450c7bd"),
    ("audit-observe", "ao", "audit", "promotion",
     "12ee7d2484ce90df9bc40584f3e56779440bf97a13950753480cadf012aa9d1c"),
    ("drill-seed", "ds", "readiness", "readiness",
     "e6ebde6489261e90128fed2f1f93d7d13b17988880682f685ce5570220aff82b"),
    ("drill-unit-start", "dx", "readiness", "readiness",
     "e5378128c0d476162d552926b77f224fcd0d74caa97493dfb66cc429f3ff7aee"),
    ("drill-recovery-start", "dr", "readiness", "readiness",
     "8b56470988e738bdb25672fe033332012284dd72336258f454c57184b490734a"),
    ("drill-hang-first", "h1", "readiness", "readiness",
     "4944bc2baa8170d2afe9667ada4fd2b79bb3b6c46cd45120f3357277fa6555c3"),
    ("drill-hang-second", "h2", "readiness", "readiness",
     "927b46e5ff825cef9dca498c5388b0f1483f301c200143840a10d9329639227f"),
    ("drill-observe", "do", "readiness", "readiness",
     "43d216179aad340e4e25cd046e4d3d88e3242cd2db665b2f0c1a45e6fb7ee441"),
)
_RECORD: dict[str, Any] = {
    "schema": "cnesdata.mode-canary/v2", "activation_sha256": "a" * 64,
    "candidate_sha256": "b" * 64, "source_sha": "c" * 40, "release_id": "liberação-é",
    "canary_id": "0123456789abcdef0123456789abcdef", "owner_sha256": "d" * 64,
    "promotion_id": "promoção-1", "promotion_generation": 3, "phase_receipt_sha256": "e" * 64,
    "machine_arn": "arn:aws:states:us-east-2:111122223333:stateMachine:m",
    "revisions": {"unit": "1", "recovery": "2", "audit": "3"}, "run_ids": ["r1", "r2"],
    "fixture_manifest_sha256": "1" * 64, "seed_manifest_sha256": "2" * 64,
    "fixed_deadline": "2026-11-13T12:00:00+00:00", "stage_deadline": "2026-11-13T12:30:00+00:00",
    "canary_deadline": "2026-11-13T11:45:00+00:00", "phase": "unit",
    "freeze_policy_sha256": "3" * 64, "expires_at": "2026-11-13T13:00:00+00:00",
    "context_kind": "promotion", "context_manifest_sha256": "4" * 64, "scenario": "unit_wave",
    "fault": "none", "mode": "unit",
}
_RECORD_SHA256 = "09ea5e922b648bfeedb1cbb5fd8ea724202707b888427e1bd9c30d032b5e7063"
_OWNER: dict[str, Any] = {
    "repository": "VINIClUS/CnesData", "run_id": 4990001, "run_attempt": 1, "generation": 7,
    "owner_sha256": "4" * 64,
}


def _manifest(kind: str = "promotion", **overrides: Any) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "schema_version": 2, "context_kind": kind, "owner": dict(_OWNER),
        "product_source_sha": _BASE["product_source_sha"],
        "product_source_closure_sha256": "1" * 64,
        "shared_source_sha": _BASE["shared_source_sha"],
        "shared_source_closure_sha256": "2" * 64, "release_id": "r-vector-499",
        "activation_sha256": "3" * 64, "resources_sha256": "6" * 64,
        "fixture_manifest_sha256": "7" * 64, "scenario_plan_sha256": "8" * 64,
        "predecessor_terminal_sha256": "9" * 64 if kind == "readiness" else None,
        "admitted_at": "2026-11-10T11:00:00Z", "deadline": "2026-11-10T11:15:00Z",
    }
    manifest.update(overrides)
    return manifest


def _wrapper(manifest: dict[str, Any]) -> dict[str, str]:
    text = canonical_bytes(manifest).decode()
    return {"canonical_utf8": text, "sha256": hashlib.sha256(text.encode()).hexdigest()}


@pytest.mark.parametrize(("operation", "code", "phase", "kind", "digest"), _SESSION_VECTORS)
def test_dez_aliases_deterministicos_e_sessions_vetores_v2(
    operation: str, code: str, phase: str, kind: str, digest: str,
) -> None:
    binding = _BASE | {"operation": operation, "phase": phase, "context_kind": kind}

    raw = session_binding_bytes(binding)

    assert hashlib.sha256(raw).hexdigest() == digest
    assert raw.endswith(b"}\n")
    assert raw.isascii()
    assert session_name(binding) == f"mc-{code}-{digest[:40]}"


@pytest.mark.parametrize(
    "mutation",
    [
        {"extra": "x"},
        {"operation": "maintenance"},
        {"phase": "audit"},
        {"context_kind": "readiness"},
        {"run_id": 0},
        {"run_attempt": True},
        {"generation": 1.0},
        {"owner_sha256": "A" * 64},
        {"product_source_sha": "d" * 39},
        {"repository": "VINIClUS/Other"},
        {"schema_version": 1},
        {"release_id": "liberação"},
    ],
)
def test_binding_de_sessao_fora_do_fechado_e_recusado(mutation: dict[str, Any]) -> None:
    binding = _BASE | {"operation": "unit-start", "phase": "unit", "context_kind": "promotion"}

    with pytest.raises(ContractError):
        session_binding_bytes(binding | mutation)


def test_sessoes_derivadas_do_manifest_seguem_o_mapa_de_aliases() -> None:
    wrapper = _wrapper(_manifest())

    sessions = session_names(parse_context_manifest(wrapper))

    assert set(sessions) == {"mode_seed", "mode_dispatch", "mode_audit_launch", "mode_observe"}
    assert [len(sessions[alias]) for alias in sorted(sessions)] == [1, 2, 3, 2]
    assert all(name.startswith("mc-") and len(name) == 46 for names in sessions.values()
               for name in names)
    readiness = session_names(parse_context_manifest(_wrapper(_manifest("readiness"))))
    assert readiness["mode_audit_launch"] == []
    assert len(readiness["mode_dispatch"]) == 4


def test_context_manifest_valido_devolve_campos_e_digest_interno() -> None:
    manifest = _manifest()
    wrapper = _wrapper(manifest)

    parsed = parse_context_manifest(wrapper)

    assert parsed.fields == manifest
    assert parsed.sha256 == wrapper["sha256"]
    assert parsed.canonical_utf8.encode() == canonical_bytes(manifest)


def _pretty(manifest: dict[str, Any]) -> dict[str, str]:
    text = json.dumps(manifest, indent=1) + "\n"
    return {"canonical_utf8": text, "sha256": hashlib.sha256(text.encode()).hexdigest()}


def _duplicate(manifest: dict[str, Any]) -> dict[str, str]:
    text = canonical_bytes(manifest).decode().replace("{", '{"admitted_at":"x",', 1)
    return {"canonical_utf8": text, "sha256": hashlib.sha256(text.encode()).hexdigest()}


@pytest.mark.parametrize(
    "build",
    [
        lambda m: _wrapper(m) | {"extra": "x"},
        lambda m: {"canonical_utf8": _wrapper(m)["canonical_utf8"]},
        lambda m: _wrapper(m) | {"sha256": "0" * 64},
        lambda m: _wrapper(m) | {"canonical_utf8": canonical_bytes(m)},
        lambda m: [_wrapper(m)],
        _pretty,
        _duplicate,
        lambda m: _wrapper(m | {"extra": 1}),
        lambda m: _wrapper({k: v for k, v in m.items() if k != "deadline"}),
        lambda m: _wrapper(m | {"schema_version": 1}),
        lambda m: _wrapper(m | {"owner": m["owner"] | {"extra": 1}}),
        lambda m: _wrapper(m | {"owner": m["owner"] | {"run_id": 0}}),
        lambda m: _wrapper(m | {"owner": m["owner"] | {"repository": "x/y"}}),
        lambda m: _wrapper(m | {"activation_sha256": "3" * 63}),
        lambda m: _wrapper(m | {"product_source_sha": "D" * 40}),
        lambda m: _wrapper(m | {"release_id": "com espaço"}),
        lambda m: _wrapper(m | {"admitted_at": "2026-11-10T11:00:00+00:00"}),
        lambda m: _wrapper(m | {"deadline": "2026-11-10T11:00:00Z"}),
        lambda m: _wrapper(m | {"deadline": "2026-02-30T11:00:00Z"}),
        lambda m: _wrapper(m | {"predecessor_terminal_sha256": "9" * 64}),
        lambda m: _wrapper(m | {"context_kind": "readiness"}),
        lambda m: _wrapper(m | {"context_kind": "other"}),
    ],
)
def test_context_manifest_alternativo_ou_semantica_invalida_e_recusado(build: Any) -> None:
    with pytest.raises(ContractError):
        parse_context_manifest(build(_manifest()))


def test_readiness_exige_predecessor_e_deadline_dentro_de_14400s() -> None:
    parse_context_manifest(_wrapper(_manifest("readiness", deadline="2026-11-10T15:00:00Z")))
    with pytest.raises(ContractError):
        parse_context_manifest(_wrapper(_manifest("readiness", predecessor_terminal_sha256=None)))
    with pytest.raises(ContractError):
        parse_context_manifest(_wrapper(_manifest("readiness", deadline="2026-11-10T15:00:01Z")))


def test_registro_reproduz_o_vetor_byte_a_byte_do_produtor_compartilhado() -> None:
    raw = record_bytes(_RECORD)

    assert hashlib.sha256(raw).hexdigest() == _RECORD_SHA256
    assert raw == canonical_bytes(_RECORD) == canonical_text(_RECORD).encode()
    assert b"\\u" not in raw
    assert "liberação-é".encode() in raw


def test_registro_com_campo_extra_ou_invalido_nao_e_serializado() -> None:
    broken = deepcopy(_RECORD)
    broken["fault"] = "hang_no_dispatch"
    with pytest.raises(ContractError):
        record_bytes(broken)
    with pytest.raises(ContractError):
        record_bytes(_RECORD | {"extra": 1})
