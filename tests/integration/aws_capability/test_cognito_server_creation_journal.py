"""Intenção, recuperação e aposentadoria duráveis do resource server."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import asdict, replace
from typing import TYPE_CHECKING, Any

import pytest

from tests.integration.aws_capability import test_service_cognito_server_create as c
from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._durability import assert_durable
from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._safety import SafetyFailure, validate
from tests.integration.aws_capability._testing import make_lab
from tests.integration.aws_capability._worker import Worker
from tests.integration.aws_capability.contracts import Ownership, RequestContext

if TYPE_CHECKING:
    from tests.integration.aws_capability._service_cognito_root_testing import RootLab
root_lab = c.root_lab
server_lab = c.server_lab
__all__ = ["root_lab", "server_lab"]


def _roundtrip(lab: RootLab, name: str) -> None:
    expected = c.DATA["states"][name]
    if name.startswith("legacy"):
        lab.services = replace(
            lab.services, spec=replace(lab.services.spec, probe=expected.get("probe", "all"))
        )
    else:
        c._ready(lab)
    before = c._seed(lab, name)
    journal = c._disk(lab, name)
    journal.save()
    c._disk(lab, name)
    if name.startswith("legacy"):
        assert journal.path.read_bytes() == before
    else:
        assert ("pending_service" in expected) == (
            "pending_service" in json.loads(journal.content())
        )
        assert ("pending_resource" in expected) == (
            "pending_resource" in json.loads(journal.content())
        )
    assert lab.process.calls == []
    assert lab.context.budget.used == 0


def _change(data: dict[str, Any], key: str, value: Any) -> None:
    if key.startswith("pending."):
        envelope = json.loads(data["pending_resource"]["resource_id"])
        field = key.removeprefix("pending.")
        if field == "identifier":
            envelope["child_id"]["identifier"] = value
        else:
            envelope[field] = value
        data["pending_resource"]["resource_id"] = json.dumps(
            envelope, sort_keys=True, separators=(",", ":")
        )
    elif value == "__omit__":
        del data[key]
    else:
        data[key] = value


def _invalid(lab: RootLab, changes: dict[str, Any], base: str = "child_unknown") -> None:
    c._ready(lab)
    before = c._seed(lab, base)
    journal = c._disk(lab, base)
    previous, events = (deepcopy(vars(journal)), list(lab.durability.events))
    changed = deepcopy(c.DATA["states"][base])
    for key, value in changes.items():
        _change(changed, key, value)
    with pytest.raises(SafetyFailure):
        journal.decode(changed)
    assert vars(journal) == previous, "oracle=non_atomic_decode"
    assert journal.path.read_bytes() == before
    assert lab.durability.events == events
    assert lab.process.calls == []
    assert lab.context.budget.used == 0


def _creation(lab: RootLab, steps: list[str], fault: tuple | None, expected: str) -> None:
    c._ready(lab)
    c._start(lab)
    journal = Journal(lab.services.spec)
    journal.save()
    fired = c._fault(lab, *fault) if fault else None
    observed: dict[str, bytes] = {}

    def inspect(argv: list[str], options: dict[str, Any]) -> None:
        if argv[2] == "describe-user-pool" and (not observed):
            c._disk(lab, "child_unknown")
            observed["intent"] = journal.path.read_bytes()
        if argv[2] == "create-resource-server":
            c._disk(lab, "child_unknown")
            assert_durable(lab.durability, journal.path)
            observed["create"] = journal.path.read_bytes()

    lab.at_request = inspect
    c._queue(lab, ["R", *steps])
    worker = Worker(journal, lab.ports, 9876)
    if fault and (not fault[2]):
        with pytest.raises(OSError, match="native=r3_journal_fault"):
            worker.execute()
    else:
        assert worker.execute() == ("interrupted" if fault else "operation_failed")
    lab.at_request = None
    c.BOUNDARIES[lab.directory]["active"] = False
    assert c.BOUNDARIES[lab.directory]["attempts"] == [], "oracle=mutation_after_failed_save"
    assert fired is None or fired == [True], "oracle=unreached_fault"
    c._trace(lab, ["R", *steps], ["work"] * (1 + len(steps)))
    c._disk(lab, expected)
    _creation_saved(lab, expected, observed)


def _creation_saved(lab: RootLab, expected: str, observed: dict[str, bytes]) -> None:
    journal = Journal(lab.services.spec)
    saved = [json.loads(e[3]) for e in lab.durability.events if e[0] == "file"]
    assert c.DATA["states"]["root_unknown"] in saved
    if expected != "root_unknown":
        assert c.DATA["states"]["root_only"] in saved
    if c.DATA["states"]["child_unknown"] in saved:
        assert saved.index(c.DATA["states"]["root_only"]) < saved.index(
            c.DATA["states"]["child_unknown"]
        )
    if expected == "child_unknown" and "create" in observed:
        assert journal.path.read_bytes() == observed["create"]


def _worker_recovery(lab: RootLab, steps: list[str], fault: tuple, expected: str) -> None:
    _creation(lab, steps, fault, expected)
    c._complete(lab, "C" in steps)


def _recover_intent(lab: RootLab, present: bool) -> None:
    c._ready(lab)
    c._seed(lab, "child_unknown")
    _, _, names = c._complete(lab, present)
    assert names[:3] == ["D", "T", "S"]
    checkpoint = c.DATA["states"]["adopted" if present else "rejected"]
    assert checkpoint in c.BOUNDARIES[lab.directory]["saved"]
    assert names.count("X") == int(present)


def _checkpoint(lab: RootLab, present: bool, boundary: str) -> None:
    c._ready(lab)
    c._seed(lab, "child_unknown")
    target = "adopted" if present else "rejected"
    fired = c._fault(lab, target, boundary)
    remote = {"parent": True, "child": present}
    outcome, _, names = c._run_cleanup(lab, remote)
    assert fired == [True]
    assert outcome.reason == "journal_unavailable"
    assert not set(names) & {"X", "P"}
    assert remote["parent"]
    c._disk(lab, target if boundary == "directory" else "child_unknown")
    _, _, recovered = c._complete(lab, present)
    if boundary == "replace":
        assert recovered[:3] == ["D", "T", "S"]


def _retirement(lab: RootLab, boundary: str | None, interrupt: bool = False) -> None:
    c._ready(lab)
    c._seed(lab, "adopted" if boundary is None else "confirmed")
    fired = c._fault(lab, "retired", boundary, interrupt) if boundary else None
    remote = {"parent": True, "child": boundary is None}
    if interrupt:
        with pytest.raises(KeyboardInterrupt):
            c._run_cleanup(lab, remote)
    else:
        outcome, cleanup, names = c._run_cleanup(lab, remote)
        if boundary is None:
            assert (outcome.status, outcome.reason) == ("succeeded", "complete")
            assert c.CHILD in cleanup.attempted
            assert c.CHILD in cleanup.absent
            assert names.count("X") == 1
            assert names.count("P") == 1
            c._disk(lab, "complete")
            assert c.DATA["states"]["cleanup_empty"] in c.BOUNDARIES[lab.directory]["saved"]
            return
        assert outcome.reason == "journal_unavailable"
    assert fired == [True]
    assert remote["parent"]
    assert not remote["child"]
    c._disk(lab, "retired" if boundary == "directory" else "confirmed")
    _, _, recovered = c._complete(lab, False)
    assert "P" in recovered
    assert "X" not in recovered


def test_c01_confirma_filho_duravel(server_lab: RootLab) -> None:
    _creation(server_lab, ["D", "T", "C", "S"], ("confirmed", "directory", True), "confirmed")


def test_f01_preserva_erro_remoto(server_lab: RootLab) -> None:
    _creation(server_lab, ["D", "T", "C_remote"], None, "child_unknown")


def test_f02_preserva_timeout(server_lab: RootLab) -> None:
    _creation(server_lab, ["D", "T", "C_timeout"], None, "child_unknown")


def test_f03_preserva_stdout_invalido(server_lab: RootLab) -> None:
    _creation(server_lab, ["D", "T", "C_invalid"], None, "child_unknown")


def test_f04_preserva_readback_ausente(server_lab: RootLab) -> None:
    _creation(server_lab, ["D", "T", "C", "S_missing"], None, "child_unknown")


def test_g01_para_fsync_intent(server_lab: RootLab) -> None:
    _worker_recovery(server_lab, [], ("child_unknown", "file", False), "root_only")


def test_g02_para_replace_intent(server_lab: RootLab) -> None:
    _worker_recovery(server_lab, [], ("child_unknown", "replace", False), "root_only")


def test_g03_para_fsync_diretorio(server_lab: RootLab) -> None:
    _worker_recovery(server_lab, [], ("child_unknown", "directory", False), "child_unknown")


def test_g04_recupera_crash_antes_create(server_lab: RootLab) -> None:
    _recover_intent(server_lab, False)


def test_g05_recupera_crash_apos_create(server_lab: RootLab) -> None:
    _recover_intent(server_lab, True)


def test_g06_para_confirmacao_pre_replace(server_lab: RootLab) -> None:
    _worker_recovery(
        server_lab, ["D", "T", "C", "S"], ("confirmed", "replace", False), "child_unknown"
    )


def test_h01_roundtrip_root_unknown(server_lab: RootLab) -> None:
    _roundtrip(server_lab, "root_unknown")


def test_h02_roundtrip_child_unknown(server_lab: RootLab) -> None:
    _roundtrip(server_lab, "child_unknown")


def test_h03_roundtrip_confirmed(server_lab: RootLab) -> None:
    _roundtrip(server_lab, "probing")


def test_h04_roundtrip_rejected(server_lab: RootLab) -> None:
    _roundtrip(server_lab, "rejected")


def test_h05_preserva_schema_um(server_lab: RootLab) -> None:
    _roundtrip(server_lab, "legacy_one")


def test_h06_preserva_schema_dois(server_lab: RootLab) -> None:
    _roundtrip(server_lab, "legacy_two")


def test_h07_rejeita_schema_um_novo(server_lab: RootLab) -> None:
    _invalid(server_lab, {"schema_version": 1})


def test_h08_rejeita_schema_dois_novo(server_lab: RootLab) -> None:
    _invalid(server_lab, {"schema_version": 2})


def test_h09_rejeita_pending_kind(server_lab: RootLab) -> None:
    _invalid(server_lab, {"pending.kind": "cognito.client"})


def test_h10_rejeita_pending_run(server_lab: RootLab) -> None:
    _invalid(server_lab, {"pending.run": "e5f6a7b8"})


def test_h11_rejeita_pending_parent(server_lab: RootLab) -> None:
    _invalid(server_lab, {"pending.parent_id": "us-east-2_Other999"})


def test_h12_rejeita_pending_identifier(server_lab: RootLab) -> None:
    _invalid(server_lab, {"pending.identifier": "https://other.invalid"})


def test_h13_rejeita_pai_ausente(server_lab: RootLab) -> None:
    _invalid(server_lab, {"created": []})


def test_h14_rejeita_pai_baseline(server_lab: RootLab) -> None:
    _invalid(server_lab, {"baseline": [c.DATA["root"]]})


def test_h15_rejeita_pending_confirmado(server_lab: RootLab) -> None:
    _invalid(server_lab, {"operation_state": "confirmed"})


def test_h16_rejeita_pending_complete(server_lab: RootLab) -> None:
    _invalid(server_lab, {"phase": "complete"})


def test_h17_rejeita_pending_servico(server_lab: RootLab) -> None:
    _invalid(server_lab, {"pending_service": "dynamodb"})


def test_i01_adota_por_porta_estrita(server_lab: RootLab) -> None:
    c._strict_control(server_lab)
    _recover_intent(server_lab, True)


def test_i02_rejeita_ausencia_exata(server_lab: RootLab) -> None:
    c._strict_control(server_lab)
    c._queue(server_lab, ["D", "T", "S_missing"])
    assert (
        c._method(server_lab, "lookup_server_exact")(
            c.CHILD, RequestContext(server_lab.context.budget, "cleanup")
        )
        == "exact_absent"
    )
    _recover_intent(server_lab, False)


def test_i03_preserva_pai_ausente(server_lab: RootLab) -> None:
    c._unsafe(server_lab, ["D_missing"], "parent_missing")


def test_i04_preserva_transporte(server_lab: RootLab) -> None:
    c._unsafe(server_lab, ["D", "T", "S_remote"], "transport_failure")


def test_i05_preserva_nome_divergente(server_lab: RootLab) -> None:
    c._unsafe(server_lab, ["D", "T", "S_name"], "configuration_mismatch")


def test_i06_preserva_scope_nome(server_lab: RootLab) -> None:
    c._unsafe(server_lab, ["D", "T", "S_scope_name"], "configuration_mismatch")


def test_i07_preserva_scope_descricao(server_lab: RootLab) -> None:
    c._unsafe(server_lab, ["D", "T", "S_scope_description"], "configuration_mismatch")


def test_i08_preserva_pai_estrangeiro(server_lab: RootLab) -> None:
    c._unsafe(server_lab, ["D", "T_run"], "parent_missing")


def test_i09_nao_adota_listing(server_lab: RootLab) -> None:
    c._strict_control(server_lab)
    before = c._seed(server_lab, "child_unknown")
    journal = c._disk(server_lab, "child_unknown")
    owner = Ownership("111111111111", c.DATA["replies"]["T"]["Tags"], "us-east-2")
    c._queue(server_lab, ["D", "T", "S_remote"])
    try:
        Cleanup(journal, server_lab.ports).confirm_pending([(c.ROOT, owner), (c.CHILD, owner)])
    except SafetyFailure:
        pass
    assert journal.path.read_bytes() == before
    c._disk(server_lab, "child_unknown")
    c._trace(server_lab, ["D", "T", "S", "D", "T", "S_remote"], ["cleanup"] * 6)


def test_i10_para_adocao_pre_replace(server_lab: RootLab) -> None:
    _checkpoint(server_lab, True, "replace")


def test_j01_seleciona_perfil_literal(server_lab: RootLab) -> None:
    c._ready(server_lab)
    profile = PROFILES["cognito-resource-server"]
    assert profile.services == ("cognito",)
    assert profile.version == 1
    assert profile.children == ("cognito.server",)
    assert asdict(profile.limits) == c.DATA["limits"]
    assert profile.snapshot("cognito-resource-server") == {
        "probe": "cognito-resource-server",
        "profile_version": 1,
        "limits": c.DATA["limits"],
    }
    validate(server_lab.services.spec, server_lab.ports)
    native = make_lab(server_lab.directory)
    native.spec = replace(native.spec, probe="cognito-resource-server")
    ports = native.ports()
    assert asdict(ports.policy.limits) == c.DATA["limits"]
    assert asdict(ports.budget.limits) == c.DATA["limits"]
    assert callable(getattr(ports.services, "create_child", None))
    assert callable(getattr(ports.services, "lookup_server_exact", None))
    validate(native.spec, ports)


def test_j02_preserva_perfis(server_lab: RootLab) -> None:
    actual = {
        name: [list(p.services), p.version, list(asdict(p.limits).values())]
        for name, p in PROFILES.items()
        if name != "cognito-resource-server"
    }
    assert actual == c.DATA["profiles"]
    assert {
        name: p.snapshot(name) for name, p in PROFILES.items() if name != "cognito-resource-server"
    } == c.DATA["snapshots"]


def test_j03_aposenta_antes_do_pai(server_lab: RootLab) -> None:
    _retirement(server_lab, None)


def test_j04_probe_readback_exato(server_lab: RootLab) -> None:
    c._control(server_lab)
    c._queue(server_lab, ["D", "T", "S"])
    assert (
        server_lab.services.probe([c.ROOT, c.CHILD], server_lab.services.spec, server_lab.context)
        is None
    )
    c._trace(server_lab, ["D", "T", "C", "S", "D", "T", "S"], ["work"] * 7)


def test_k01_rejeita_root_pending_null(server_lab: RootLab) -> None:
    _invalid(server_lab, {"pending_resource": None}, "root_unknown")


def test_k02_rejeita_service_null(server_lab: RootLab) -> None:
    _invalid(server_lab, {"pending_service": None}, "confirmed")


def test_k03_rejeita_resource_null(server_lab: RootLab) -> None:
    _invalid(server_lab, {"pending_resource": None}, "confirmed")


def test_k04_rejeita_service_omitido(server_lab: RootLab) -> None:
    _invalid(server_lab, {"pending_service": "__omit__"})


def test_k05_rejeita_fallback_root(server_lab: RootLab) -> None:
    _invalid(server_lab, {"pending_resource": "__omit__"})


def test_k06_rejeita_unknown_vazio(server_lab: RootLab) -> None:
    _invalid(server_lab, {"pending_service": "__omit__"}, "root_unknown")


def test_k07_roundtrip_prepared(server_lab: RootLab) -> None:
    _roundtrip(server_lab, "prepared")


def test_k08_roundtrip_root_only(server_lab: RootLab) -> None:
    _roundtrip(server_lab, "root_only")


def test_k09_roundtrip_root_aposentado(server_lab: RootLab) -> None:
    _roundtrip(server_lab, "retired")


def test_k10_roundtrip_cleanup_vazio(server_lab: RootLab) -> None:
    _roundtrip(server_lab, "cleanup_empty")


def test_k11_para_root_pre_replace(server_lab: RootLab) -> None:
    _worker_recovery(server_lab, [], ("root_only", "replace", False), "root_unknown")


def test_k12_recupera_root_pos_replace(server_lab: RootLab) -> None:
    _worker_recovery(server_lab, [], ("root_only", "directory", False), "root_only")


def test_k13_recupera_child_pos_replace(server_lab: RootLab) -> None:
    _worker_recovery(
        server_lab, ["D", "T", "C", "S"], ("confirmed", "directory", False), "confirmed"
    )


def test_k14_recupera_adocao_pos_replace(server_lab: RootLab) -> None:
    _checkpoint(server_lab, True, "directory")


def test_k15_para_rejeicao_pre_replace(server_lab: RootLab) -> None:
    _checkpoint(server_lab, False, "replace")


def test_k16_recupera_rejeicao_pos_replace(server_lab: RootLab) -> None:
    _checkpoint(server_lab, False, "directory")


def test_k21_recupera_crash_retirement(server_lab: RootLab) -> None:
    _retirement(server_lab, "replace", True)


def test_k22_para_retirement_pre_replace(server_lab: RootLab) -> None:
    _retirement(server_lab, "replace")


def test_k23_recupera_retirement_pos_replace(server_lab: RootLab) -> None:
    _retirement(server_lab, "directory")
