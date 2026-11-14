"""Preâmbulo da promoção: SHA em develop, checks obrigatórios verdes e freeze ausente."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from release.build_candidate import VERIFY_JOBS
from scripts.promotion._api_static_preamble import (
    PreambleError,
    check_preamble,
    check_unfrozen,
    main,
)

if TYPE_CHECKING:
    from pathlib import Path

SHA = "a" * 40
FREEZE = "arn:aws:iam::123456789012:policy/cnesdata-budget-freeze"
UNFROZEN = {"AttachedPolicies": [], "IsTruncated": False}


def _run(name: str, conclusion: str = "success", ident: int = 1, sha: str = SHA) -> dict:
    return {"id": ident, "name": name, "head_sha": sha, "status": "completed",
            "conclusion": conclusion, "app": {"id": 15368}}


def evidence(**changes) -> dict:
    value = {
        "source_sha": SHA,
        "compare": {"status": "identical", "merge_base_commit": {"sha": SHA}},
        "check_runs": {"total_count": len(VERIFY_JOBS),
                       "check_runs": [_run(name, ident=n) for n, name in
                                      enumerate(sorted(VERIFY_JOBS), start=1)]},
    }
    return value | changes


def test_preambulo_aceita_sha_em_develop_com_checks_verdes_sem_freeze() -> None:
    check_preamble(evidence())
    check_preamble(evidence(compare={"status": "ahead", "merge_base_commit": {"sha": SHA}}))
    check_unfrozen(UNFROZEN, FREEZE)


def _with_run(*runs: dict) -> dict:
    base = evidence()["check_runs"]["check_runs"]
    return {"total_count": len(base) + len(runs), "check_runs": [*base, *runs]}


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"compare": {"status": "diverged", "merge_base_commit": {"sha": "b" * 40}}}, "develop"),
        ({"compare": {"status": "behind", "merge_base_commit": {"sha": SHA}}}, "develop"),
        ({"check_runs": _with_run(_run("delivery-contracts", "failure", ident=99))}, "check"),
        ({"check_runs": {"total_count": 1, "check_runs": [_run("dashboard")]}}, "check"),
        ({"check_runs": _with_run(_run("dashboard", ident=97, sha="b" * 40))}, "check"),
        ({"check_runs": {"total_count": 150, "check_runs": []}}, "check"),
        ({"source_sha": "A" * 40}, "source_sha"),
        ({"freeze": {"AttachedPolicies": [{"PolicyArn": FREEZE}], "IsTruncated": False}},
         "freeze"),
        ({"freeze": {"AttachedPolicies": [], "IsTruncated": True}}, "freeze"),
    ],
)
def test_promocao_recusa_sha_fora_de_develop_check_vermelho_e_freeze(
    changes: dict, reason: str
) -> None:
    def attempt() -> None:
        if "freeze" in changes:
            check_unfrozen(changes["freeze"], FREEZE)
        else:
            check_preamble(evidence(**changes))

    with pytest.raises(PreambleError, match=reason):
        attempt()


def test_check_reexecutado_vale_pela_execucao_mais_recente() -> None:
    rerun = _with_run(_run("delivery-contracts", "failure", ident=0))
    check_preamble(evidence(check_runs=rerun))


def test_check_falho_fora_dos_obrigatorios_nao_bloqueia_nova_tentativa() -> None:
    own = (_run("closed-ready", "failure", ident=300), _run("lint-extra", "cancelled", ident=301),
           _run("host-gate / execute", "failure", ident=302, sha="b" * 40))
    check_preamble(evidence(check_runs=_with_run(*own)))


def test_listagem_completa_acima_de_cem_checks_e_aceita() -> None:
    extra = tuple(_run(f"matrix-{n}", ident=1000 + n) for n in range(120))
    check_preamble(evidence(check_runs=_with_run(*extra)))


def test_cli_falha_fechado_sem_evidencia_valida(tmp_path: Path) -> None:
    good, bad = tmp_path / "good.json", tmp_path / "bad.json"
    good.write_text(json.dumps(evidence()), encoding="utf-8")
    bad.write_text(json.dumps(evidence(source_sha="")), encoding="utf-8")
    assert main(["--evidence", str(good)]) == 0
    assert main(["--evidence", str(bad)]) == 1
    assert main(["--evidence", str(tmp_path / "missing.json")]) == 1


def test_check_obrigatorio_de_outro_app_nao_conta_como_verde() -> None:
    foreign = _run("delivery-contracts", ident=200) | {"app": {"id": 999}}
    with pytest.raises(PreambleError, match="check"):
        check_preamble(evidence(check_runs=_with_run(foreign)))


def test_freeze_desconhecido_e_recusado() -> None:
    with pytest.raises(PreambleError, match="freeze"):
        check_unfrozen(UNFROZEN, "arn:aws:iam::123456789012:policy/outra")
