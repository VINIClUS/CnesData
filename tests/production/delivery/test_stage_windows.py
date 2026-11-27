"""Janelas de estágio governadas e prazo fixo do produto dentro dos limites do código (P36b)."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from cnes_infra.control_plane import dynamodb_promotion_snapshot as snapshot
from scripts.promotion import _adapter_abi as abi
from scripts.promotion import _stage_windows as sw
from scripts.promotion import verify_routes
from scripts.promotion._closed_ready import build_closed_ready, object_hash
from scripts.promotion._phase_c_core import PhaseCWatchdog
from scripts.promotion.environment_gate import _fixed_deadline
from tests.production.delivery import _ia_stage_windows_consumer as ia

NOW = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
MINIMUM = {"stages": {name: low for name, (low, _) in ia.STAGES.items()},
           "fixed_deadline_seconds": 1800}
INITIAL = {"stages": {"phase-b": 360, "api": 180, "static": 180, "unit": 180,
                      "phase-c-recovery": 330, "phase-c-audit": 330, "final": 90},
           "fixed_deadline_seconds": 2070}
FENCE = {"environment": "prod", "state": "CLOSED", "promotion_id": "r1", "version": 3,
         "changed_at": "2026-11-09T12:00:00Z"}


def windows(**stages: Any) -> dict[str, Any]:
    merged = MINIMUM["stages"] | {name.replace("_", "-"): value for name, value in stages.items()}
    return {"stages": merged, "fixed_deadline_seconds": sum(merged.values()) + 420}


def utc(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def close(governed: dict[str, Any], fixed_seconds: int, **changes: Any) -> dict[str, Any]:
    expirations = [{"kind": "credential", "object_sha256": "f" * 64,
                    "expires_at": utc(NOW + timedelta(hours=2))}]
    return {"version": 2, "kind": "close-drain", "common": {"caller": {"run_id": 1},
                                                           "promotion_generation": 7},
            "fence": FENCE, "close_confirmed_at": utc(NOW), "drain_observed_at": utc(NOW),
            "frozen_membership_sha256": "0" * 64, "members": [], "proofs": [], "permit": None,
            "input_expirations": expirations,
            "fixed_deadline": utc(NOW + timedelta(seconds=fixed_seconds)), "decision": None,
            "windows": governed} | changes


def _both_refuse(value: Any) -> str:
    with pytest.raises(ia.Refused):
        ia.governed_windows(value)
    with pytest.raises(ValueError, match=r"^windows=") as refused:
        sw.governed_windows(value)
    return str(refused.value)


def test_pin_do_espelho_de_janelas_e_o_commit_do_p36_com_sha256_dos_dois_fontes() -> None:
    assert re.fullmatch(r"[0-9a-f]{40}", ia.IA_PIN)
    assert set(ia.IA_MIRRORED_SOURCES) == {"tools/cnesdata_delivery/lifecycle_contract.py",
                                           "tools/cnesdata_delivery/host_gate.py"}
    assert all(re.fullmatch(r"[0-9a-f]{64}", sha) for sha in ia.IA_MIRRORED_SOURCES.values())
    assert len(set(ia.IA_MIRRORED_SOURCES.values())) == 2


def test_limites_do_produto_sao_os_do_infra_ansible_na_mesma_ordem() -> None:
    assert list(sw.STAGES.items()) == list(ia.STAGES.items())
    assert (sw.FINAL_RESERVE, sw.STAGE_SUM_CEILING) == (ia.FINAL_RESERVE, ia.STAGE_SUM_CEILING)


def test_abi3_estagio_fechado_e_close_drain_versao_2_iguais_ao_infra() -> None:
    assert abi.ABI_VERSION == ia.ADAPTER_ABI_VERSION == 3
    assert frozenset(ia.ADAPTER_STAGE_FIELDS.split()) == abi.STAGE_FIELDS
    assert frozenset(ia.CLOSE_DRAIN_FIELDS.split()) == abi.CLOSE_DRAIN_FIELDS
    assert abi.CLOSE_DRAIN_VERSION == 2


def test_recibo_do_close_nunca_vence_antes_do_maior_prazo_fixo_governavel() -> None:
    assert snapshot.CLOSE_RECEIPT_SECONDS == sw.STAGE_SUM_CEILING + sw.FINAL_RESERVE


@pytest.mark.parametrize("governed", [MINIMUM, INITIAL], ids=["minimas", "iniciais"])
def test_janelas_minimas_e_iniciais_sao_aceitas_pelos_dois_lados(
    governed: dict[str, Any],
) -> None:
    assert sw.governed_windows(governed) == ia.governed_windows(governed) == governed
    for stage in sw.STAGES:
        assert sw.later_seconds(governed, stage) == ia.later_seconds(governed, stage)


def test_minimas_reproduzem_os_prazos_antigos_e_iniciais_seguem_a_decisao_e2() -> None:
    assert sw.later_seconds(MINIMUM, "api") == 840
    assert MINIMUM["stages"]["api"] + sw.later_seconds(MINIMUM, "api") + sw.FINAL_RESERVE == 1440
    assert sw.later_seconds(INITIAL, "api") == 1110


BOUNDS = [(name, value, low <= value <= high)
          for name, (low, high) in ia.STAGES.items()
          for value in (low - 1, low, high, high + 1)]


@pytest.mark.parametrize(("stage", "value", "accepted"), BOUNDS,
                         ids=[f"{name}-{value}" for name, value, _ in BOUNDS])
def test_cada_janela_e_aceita_so_dentro_do_min_e_do_max(stage: str, value: int,
                                                        accepted: bool) -> None:
    governed = windows(**{stage: value})
    if accepted:
        assert sw.governed_windows(governed) == ia.governed_windows(governed)
        return
    low, high = sw.STAGES[stage]
    assert _both_refuse(governed) == (
        f"windows=stage stage={stage} value={value} min={low} max={high}")


def test_soma_no_teto_e_aceita_e_um_segundo_acima_e_recusada() -> None:
    ceiling = windows(phase_b=720, api=270)
    assert sum(ceiling["stages"].values()) == 1830
    assert sw.governed_windows(ceiling) == ia.governed_windows(ceiling)
    assert _both_refuse(windows(phase_b=720, api=271)) == "windows=sum sum=1831 ceiling=1830"


@pytest.mark.parametrize("delta", [-1, 1])
def test_prazo_fixo_incoerente_com_a_soma_e_recusado(delta: int) -> None:
    governed = MINIMUM | {"fixed_deadline_seconds": 1800 + delta}
    assert _both_refuse(governed) == f"windows=fixed value={1800 + delta} expected=1800"


@pytest.mark.parametrize(("value", "reason"), [
    pytest.param(None, "windows=fields", id="ausente"),
    pytest.param({"stages": MINIMUM["stages"]}, "windows=fields", id="sem-prazo-fixo"),
    pytest.param(MINIMUM | {"extra": 1}, "windows=fields", id="chave-a-mais"),
    pytest.param({"stages": {**MINIMUM["stages"], "extra": 60}, "fixed_deadline_seconds": 1860},
                 "windows=stages", id="estagio-a-mais"),
    pytest.param({"stages": {k: v for k, v in MINIMUM["stages"].items() if k != "final"},
                  "fixed_deadline_seconds": 1740}, "windows=stages", id="estagio-a-menos"),
    pytest.param(windows(final=True), "windows=stage stage=final", id="bool"),
    pytest.param(windows(final=60.0), "windows=stage stage=final", id="float"),
    pytest.param(MINIMUM | {"fixed_deadline_seconds": True}, "windows=fixed", id="prazo-bool"),
])
def test_forma_fora_do_contrato_e_recusada_sem_default(value: Any, reason: str) -> None:
    assert _both_refuse(value).startswith(reason)


def test_prazo_fixo_do_close_drain_vem_da_janela_governada_e_das_expiracoes() -> None:
    later = [{"kind": "credential", "object_sha256": "f" * 64,
              "expires_at": utc(NOW + timedelta(hours=1))}]
    earlier = [later[0] | {"expires_at": utc(NOW + timedelta(seconds=2000))}]
    assert _fixed_deadline(NOW, later, INITIAL) == NOW + timedelta(seconds=2070)
    assert _fixed_deadline(NOW, later, MINIMUM) == NOW + timedelta(seconds=1800)
    assert _fixed_deadline(NOW, earlier, INITIAL) == NOW + timedelta(seconds=2000)


@pytest.mark.parametrize("prefix", ["phase_b", "window", "closed_ready"])
def test_close_drain_versao_1_ou_sem_janelas_e_recusado_pelos_leitores(prefix: str) -> None:
    legacy = {key: value for key, value in close(MINIMUM, 1800).items() if key != "windows"}
    with pytest.raises(ValueError, match=rf"^{prefix}=close_drain_version$"):
        sw.close_windows(legacy | {"version": 1}, prefix)
    with pytest.raises(ValueError, match=r"^windows=fields$"):
        sw.close_windows(legacy, prefix)
    assert sw.close_windows(close(INITIAL, 2070), prefix) == INITIAL


@pytest.mark.parametrize(("governed", "seconds", "accepted"), [
    (INITIAL, 2070, True), (INITIAL, 2071, False), (MINIMUM, 1800, True), (MINIMUM, 1801, False),
])
def test_watchdog_da_phase_c_aceita_f_so_ate_o_prazo_governado(
    governed: dict[str, Any], seconds: int, accepted: bool,
) -> None:
    document = close(governed, seconds)
    if accepted:
        watchdog = PhaseCWatchdog.from_close(document)
        assert watchdog.fixed_deadline == NOW + timedelta(seconds=seconds)
        return
    with pytest.raises(ValueError, match=r"^window=fixed$"):
        PhaseCWatchdog.from_close(document)


class _Reached(Exception):
    pass


class _Gate:
    def observe_environment_gate(self, environment: str) -> Any:
        raise _Reached(environment)


def _phase_b(document: dict[str, Any], observed: datetime) -> dict[str, Any]:
    return {"kind": "phase-b", "common": document["common"],
            "close_drain_sha256": object_hash(document), "observed_at": utc(observed)}


def _ready(document: dict[str, Any], admitted: datetime, deadline: datetime) -> None:
    common = document["common"]
    build_closed_ready(document, _phase_b(document, admitted), _Gate(), None, admitted,
                       common["caller"], common["promotion_generation"], admitted, deadline)


@pytest.mark.parametrize(("governed", "api"), [(MINIMUM, 180), (INITIAL, 180),
                                               (windows(api=360), 360)])
def test_closed_ready_admite_api_e_reserva_governados_no_limite_exato(
    governed: dict[str, Any], api: int,
) -> None:
    fixed = governed["fixed_deadline_seconds"]
    budget = api + sw.later_seconds(governed, "api") + sw.FINAL_RESERVE
    document = close(governed, fixed)
    admitted = NOW + timedelta(seconds=fixed - budget)
    with pytest.raises(_Reached):
        _ready(document, admitted, admitted + timedelta(seconds=api))
    with pytest.raises(ValueError, match=r"^closed_ready=stage_deadline$"):
        _ready(document, admitted, admitted + timedelta(seconds=api + 1))
    late = admitted + timedelta(seconds=1)
    with pytest.raises(ValueError, match=r"^closed_ready=stage_deadline$"):
        _ready(document, late, late + timedelta(seconds=api))


def test_closed_ready_recusa_close_drain_versao_1() -> None:
    legacy = {key: value for key, value in close(MINIMUM, 1800).items() if key != "windows"}
    legacy["version"] = 1
    with pytest.raises(ValueError, match=r"^closed_ready=close_drain_version$"):
        _ready(legacy, NOW, NOW + timedelta(seconds=180))


class _ClosedControlPlane:
    def __init__(self, *args: Any) -> None:
        self.args = args

    def observe_environment_gate(self, environment: str) -> Any:
        changed = datetime.fromisoformat(FENCE["changed_at"].replace("Z", "+00:00"))
        fence = SimpleNamespace(state=SimpleNamespace(value="CLOSED"),
                                promotion_id=FENCE["promotion_id"], version=FENCE["version"],
                                changed_at=changed)
        return SimpleNamespace(fence=fence, permit=None)


@pytest.mark.parametrize("governed", [MINIMUM, INITIAL, windows(api=360, final=180)],
                         ids=["minimas", "iniciais", "api-e-final-no-max"])
def test_phase_b_exige_api_mais_later_mais_reserva_governados_ate_f(
    governed: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cnes_infra.control_plane import dynamodb_adapter

    monkeypatch.setattr(dynamodb_adapter, "DynamoDBControlPlane", _ClosedControlPlane)
    native = verify_routes.NativePhaseB(None, {"dynamodb": None},  # type: ignore[arg-type]
                                        {"control_plane_table": "t"})
    need = governed["stages"]["api"] + sw.later_seconds(governed, "api") + sw.FINAL_RESERVE
    now = datetime.now(UTC)
    roomy = close(governed, 0, fixed_deadline=utc(now + timedelta(seconds=need + 30)))
    native.assert_closed(roomy)
    tight = close(governed, 0, fixed_deadline=utc(now + timedelta(seconds=need - 30)))
    with pytest.raises(ValueError, match=r"^phase_b=closed_deadline_readback$"):
        native.assert_closed(tight)
    legacy = {key: value for key, value in roomy.items() if key != "windows"} | {"version": 1}
    with pytest.raises(ValueError, match=r"^phase_b=close_drain_version$"):
        native.assert_closed(legacy)


def _raw(document: dict[str, Any]) -> bytes:
    from scripts.promotion._api_static_records import canonical

    return canonical(document)


@pytest.mark.parametrize("prefix", ["closed_ready", "mode"])
def test_leitor_do_close_drain_exige_versao_2_e_as_janelas_do_request(prefix: str) -> None:
    document = close(INITIAL, 2070)
    assert abi.close_drain_document(_raw(document), prefix, INITIAL) == document
    with pytest.raises(ValueError, match=rf"^{prefix}=windows$"):
        abi.close_drain_document(_raw(document), prefix, MINIMUM)
    with pytest.raises(ValueError, match=rf"^{prefix}=kind name=close-drain$"):
        abi.close_drain_document(_raw(document | {"version": 1}), prefix, INITIAL)
    legacy = {key: value for key, value in document.items() if key != "windows"}
    with pytest.raises(ValueError, match=rf"^{prefix}=fields name=close-drain$"):
        abi.close_drain_document(_raw(legacy | {"version": 1}), prefix, INITIAL)
    outside = document | {"windows": windows(api=361)}
    with pytest.raises(ValueError, match=r"^windows=stage stage=api value=361"):
        abi.close_drain_document(_raw(outside), prefix, windows(api=361))


def _ia_refusal(value: Any) -> str:
    with pytest.raises(ia.Refused) as refused:
        ia.governed_windows(value)
    return str(refused.value)


def test_espelho_recusa_soma_apos_static_acima_de_1200_com_mensagem_chave_valor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    governed = windows(unit=120, phase_c_recovery=660, phase_c_audit=361)
    assert ia.later_seconds(governed, "static") == 1201
    monkeypatch.setattr(ia, "STAGE_SUM_CEILING", 10_000)
    assert _ia_refusal(governed) == "stage_windows post_static=1201 context=1200"
    at_cap = windows(unit=120, phase_c_recovery=660, phase_c_audit=360)
    assert ia.later_seconds(at_cap, "static") == 1200
    assert ia.governed_windows(at_cap)


def test_teto_de_1200_apos_o_static_fica_ocioso_sob_o_teto_de_soma_de_1830() -> None:
    governed = windows(unit=120, phase_c_recovery=660, phase_c_audit=361)
    assert _ia_refusal(governed) == "stage windows sum=1921 ceiling=1830"
    assert _both_refuse(governed) == "windows=sum sum=1921 ceiling=1830"
    largest = windows(phase_b=360, api=180, static=180, unit=360, phase_c_recovery=450,
                      phase_c_audit=450, final=180)
    assert ia.later_seconds(largest, "static") == 1440 > ia.PROMOTION_CONTEXT_SECONDS
    assert sum(largest["stages"].values()) > ia.STAGE_SUM_CEILING


def test_post_static_maximo_alcancavel_sob_o_teto_de_soma_e_aceito_pelos_dois_lados() -> None:
    reachable = windows(unit=360, phase_c_recovery=360, phase_c_audit=330, final=60)
    assert sum(reachable["stages"].values()) == 1830
    assert ia.later_seconds(reachable, "static") == 1110 <= ia.PROMOTION_CONTEXT_SECONDS
    assert sw.governed_windows(reachable) == ia.governed_windows(reachable)


@pytest.mark.parametrize(("changed", "expected"), [
    pytest.param({"api": 181}, "api,fixed_deadline_seconds", id="estagio"),
    pytest.param({"api": 181, "final": 61}, "api,final,fixed_deadline_seconds", id="dois"),
])
def test_espelho_do_close_drain_nomeia_os_campos_de_janela_divergentes(
    changed: dict[str, int], expected: str,
) -> None:
    keeper = {"windows": MINIMUM}
    other = windows(**changed)
    raw = (json.dumps(close(other, 1800), sort_keys=True, ensure_ascii=False,
                      separators=(",", ":")) + "\n").encode()
    with pytest.raises(ia.Refused) as refused:
        ia.close_window(keeper, raw, infra_bound=10**12, now=int(NOW.timestamp()))
    message = f"close-drain windows differ from keeper windows changed={expected}"
    assert str(refused.value) == message


def test_espelho_do_close_drain_recusa_janela_so_com_prazo_fixo_diferente() -> None:
    other = MINIMUM | {"fixed_deadline_seconds": 1801}
    raw = (json.dumps(close(other, 1800), sort_keys=True, ensure_ascii=False,
                      separators=(",", ":")) + "\n").encode()
    with pytest.raises(ia.Refused) as refused:
        ia.close_window({"windows": MINIMUM}, raw, infra_bound=10**12, now=int(NOW.timestamp()))
    assert "fixed_deadline_seconds" in str(refused.value)


def test_pin_do_espelho_e_o_merge_do_p40_com_os_dois_sha256() -> None:
    assert ia.IA_PIN == "82e9ff210cbc0767c88f4f761b09e7ccfc38e50c"
    assert ia.IA_MIRRORED_SOURCES == {
        "tools/cnesdata_delivery/lifecycle_contract.py":
            "2f689fcd2e0c41525ba7e002e45289071af01e1c5c56b9333f23d6fb8424e63f",
        "tools/cnesdata_delivery/host_gate.py":
            "2a0a2eedd524f6dfe2119022797a91becd01670a853befba495e1abecf8e0cb1",
    }


@pytest.mark.ia_tree
def test_fontes_espelhados_conferem_com_a_arvore_do_infra_ansible_no_pin() -> None:
    repository = os.environ.get("INFRA_ANSIBLE_REPO")
    if not repository:
        pytest.skip("INFRA_ANSIBLE_REPO ausente")
    for path, expected in ia.IA_MIRRORED_SOURCES.items():
        data = subprocess.run(  # noqa: S603 -- git fixo sobre o checkout do pin.
            ["git", "-C", repository, "show", f"{ia.IA_PIN}:{path}"],  # noqa: S607 -- git do dev.
            capture_output=True, check=True).stdout
        assert hashlib.sha256(data).hexdigest() == expected, path
