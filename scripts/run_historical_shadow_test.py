"""Testes da CLI de equivalencia historica (MIG-010)."""

from __future__ import annotations

import json
import shutil
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from cnes_domain.ports.processing import ExecutionStatus
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.executor.local_pool import LocalWorkerPool
from cnes_infra.object_store import FilesystemObjectStore
from data_processor.migration.publication import Expected
from data_processor.migration.report import aggregate_bytes
from scripts.run_historical_shadow import main

if TYPE_CHECKING:
    from collections.abc import Callable

_ROOT = Path(__file__).resolve().parents[1]
_CONTRACT = _ROOT / "docs/fixtures/migration/equivalence-contract-v1.json"
_TENANT = "354130"
_COMMIT = "0123456789abcdef0123456789abcdef01234567"
_NOW = datetime(2026, 10, 10, 12, tzinfo=UTC)
_COMPETENCIA = {"cnes": "2026-01", "sihd": "2026-01", "bpa": "2026-08", "sia": "2026-01"}
_DATASETS = tuple(_COMPETENCIA)
_WAVES = [["NORMALIZE"], ["RECONCILE"], ["MATERIALIZE"]]
_RUNS = {"2026-01": ("cnes", "sia", "sihd"), "2026-08": ("bpa",)}
_APPROVED_RULES = frozenset({
    "MIG010-RUN-ID", "MIG010-GENERATED-AT", "MIG010-NORMALIZED-AT",
    "MIG010-CNES-NORMALIZED-IDS", "MIG010-CNES-DIVERGENCE-TEXT",
    "MIG010-SIA-RAW-MANIFEST-SHA256",
})
_EVIDENCE = {
    "cnes": (125, 19, "normalized", {
        "MIG010-CNES-DIVERGENCE-TEXT": 3, "MIG010-CNES-NORMALIZED-IDS": 14,
        "MIG010-GENERATED-AT": 1, "MIG010-RUN-ID": 1,
    }),
    "sihd": (248, 13, "reconciliation", {
        "MIG010-GENERATED-AT": 1, "MIG010-NORMALIZED-AT": 11, "MIG010-RUN-ID": 1,
    }),
    "bpa": (319, 16, "reconciliation", {
        "MIG010-GENERATED-AT": 2, "MIG010-NORMALIZED-AT": 12, "MIG010-RUN-ID": 2,
    }),
    "sia": (301, 54, "reconciliation", {
        "MIG010-GENERATED-AT": 2, "MIG010-NORMALIZED-AT": 25, "MIG010-RUN-ID": 2,
        "MIG010-SIA-RAW-MANIFEST-SHA256": 25,
    }),
}


def _argv(work: Path, sources: tuple[str, ...], *overrides: str) -> list[str]:
    (month,) = {_COMPETENCIA[source] for source in sources}
    base = [
        "--tenant", _TENANT, "--from-competencia", month, "--to-competencia", month,
        "--legacy-root", str(_ROOT), "--contract", str(_CONTRACT),
        "--candidate-root", str(work / "candidate"), "--report-root", str(work / "reports"),
    ]
    chosen = [part for source in sources for part in ("--source", source)]
    return [*base, *chosen, *overrides]


def _contract() -> dict[str, Any]:
    return json.loads(_CONTRACT.read_bytes())


def _report(work: Path, dataset: str) -> dict[str, Any]:
    path = work / "reports" / _TENANT / dataset / f"{_COMPETENCIA[dataset]}.json"
    return json.loads(path.read_bytes())


def _aggregate_path(work: Path) -> Path:
    return work / "reports" / _TENANT / "aggregate.json"


def _aggregate(work: Path) -> dict[str, Any]:
    return json.loads(_aggregate_path(work).read_bytes())


def _assert_sem_caminhos_locais(work: Path) -> None:
    for path in (work / "reports").rglob("*.json"):
        text = path.read_text(encoding="utf-8")
        assert str(work) not in text
        assert str(_ROOT) not in text


def _assert_falha(work: Path, dataset: str, error: str) -> None:
    failure = {"dataset": dataset, "competencia": _COMPETENCIA[dataset], "error": error}
    assert _aggregate(work)["failures"] == [failure]
    _assert_sem_caminhos_locais(work)


@pytest.fixture(autouse=True)
def _checkout_identificado(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("scripts.run_historical_shadow._source_commit", lambda: _COMMIT)


@dataclass(frozen=True)
class _Executed:
    works: dict[str, Path]
    exit_codes: dict[str, int]

    def work(self, dataset: str) -> Path:
        return self.works[_COMPETENCIA[dataset]]


@pytest.fixture(scope="module")
def executed(tmp_path_factory: pytest.TempPathFactory) -> _Executed:
    works = {month: tmp_path_factory.mktemp(f"shadow-{month}") for month in _RUNS}
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("scripts.run_historical_shadow._source_commit", lambda: _COMMIT)
        codes = {month: main(_argv(works[month], sources)) for month, sources in _RUNS.items()}
    return _Executed(works, codes)


def test_execucao_completa_cobre_as_quatro_fontes_com_relatorios_imutaveis(
    executed: _Executed,
) -> None:
    for month, sources in _RUNS.items():
        aggregate = _aggregate(executed.works[month])
        reports = executed.works[month] / "reports" / _TENANT

        assert executed.exit_codes[month] == 0
        assert (aggregate["accepted"], aggregate["failures"]) == (True, [])
        assert [(item["dataset"], item["competencia"]) for item in aggregate["covered"]] == [
            (source, month) for source in sources
        ]
        for item in aggregate["covered"]:
            assert sha256((reports / item["report"]).read_bytes()).hexdigest() == item[
                "report_sha256"
            ]
            assert (reports / item["report"]).stat().st_mode & 0o222 == 0


def test_agregado_registra_o_pedido_e_a_janela_inteira_coberta(executed: _Executed) -> None:
    for month, sources in _RUNS.items():
        aggregate = _aggregate(executed.works[month])

        assert aggregate["requested"] == {"from": month, "sources": list(sources), "to": month}
        for source in sources:
            summary = aggregate["datasets"][source]
            assert (summary["covered"], summary["uncovered"]) == ([month], [])


@pytest.mark.parametrize("dataset", _DATASETS)
def test_agregado_declara_proveniencia_e_saidas_afirmadas_por_dataset(
    executed: _Executed, dataset: str
) -> None:
    summary = _aggregate(executed.work(dataset))["datasets"][dataset]
    outputs = _report(executed.work(dataset), dataset)["outputs"]
    asserted = [output["asserted"] for output in outputs]

    assert summary["provenance"] == _report(executed.work(dataset), dataset)["provenance"]
    assert summary["provenance"]["data_nature"] == "synthetic"
    assert summary["outputs"] == {
        "asserted": asserted.count(True), "unasserted": asserted.count(False),
        "unasserted_layers": [_EVIDENCE[dataset][2]],
    }


@pytest.mark.parametrize("dataset", _DATASETS)
def test_fonte_retida_sem_mismatch(executed: _Executed, dataset: str) -> None:
    report = _report(executed.work(dataset), dataset)
    matches, explained, _, per_rule = _EVIDENCE[dataset]
    by_rule = Counter(c["rule_id"] for c in report["comparisons"] if c["status"] == "EXPLAINED")

    assert report["accepted"] is True
    assert report["summary"] == {"MATCH": matches, "EXPLAINED": explained, "MISMATCH": 0}
    assert dict(by_rule) == per_rule
    assert set(by_rule) <= _APPROVED_RULES


@pytest.mark.parametrize("dataset", _DATASETS)
def test_drena_tres_ondas_pelo_ponteiro(executed: _Executed, dataset: str) -> None:
    report = _report(executed.work(dataset), dataset)
    run_id = f"mig010-{dataset}-{_COMPETENCIA[dataset]}"
    job = executed.work(dataset) / "candidate" / f"{dataset}-{_COMPETENCIA[dataset]}"
    pointer = SQLiteControlPlane(job / "state" / "cnesdata.sqlite3", lambda: _NOW)
    published = pointer.get_dataset_pointer(_TENANT, dataset)
    stat = FilesystemObjectStore(job / "objects").stat(report["run_manifest_key"])

    assert report["waves"] == _WAVES
    assert report["run_state"] == "PUBLISHED"
    assert report["candidate_version_id"] == run_id
    assert published is not None
    assert published.version_id == run_id
    assert stat is not None
    assert stat.sha256 == report["run_manifest_sha256"]


@pytest.mark.parametrize("dataset", _DATASETS)
def test_relatorio_marca_so_as_saidas_afirmadas_pelo_contrato(
    executed: _Executed, dataset: str
) -> None:
    declared = {
        (item["candidate"]["layer"], item["candidate"]["leaf"])
        for item in _contract()["datasets"][dataset]["documents"]
    }
    outputs = _report(executed.work(dataset), dataset)["outputs"]
    asserted = {
        (output["layer"], output["object_key"].rsplit("/", 1)[-1])
        for output in outputs
        if output["asserted"]
    }

    assert asserted == declared


@pytest.mark.parametrize(("dataset", "kind"), [
    ("cnes", "independent_frozen"), ("sihd", "reproduction"), ("bpa", "reproduction"),
    ("sia", "reproduction"),
])
def test_relatorio_registra_a_proveniencia_do_oraculo(
    executed: _Executed, dataset: str, kind: str
) -> None:
    provenance = _report(executed.work(dataset), dataset)["provenance"]

    assert (provenance["kind"], provenance["data_nature"]) == (kind, "synthetic")
    assert provenance["frozen_in"]


def test_agregado_e_reproduzivel_e_sem_caminhos_locais(tmp_path: Path) -> None:
    first, second = tmp_path / "a", tmp_path / "b"

    assert main(_argv(first, ("sihd",))) == 0
    assert main(_argv(second, ("sihd",))) == 0

    assert _aggregate_path(first).read_bytes() == _aggregate_path(second).read_bytes()
    assert _report(first, "sihd") == _report(second, "sihd")
    assert _aggregate(first)["git_commit"] == _COMMIT
    _assert_sem_caminhos_locais(first)
    _assert_sem_caminhos_locais(second)


@pytest.mark.parametrize("overrides", [
    ("--from-competencia", "2027-01", "--to-competencia", "2027-03"),
    ("--tenant", "999999"),
])
def test_rejeita_competencia_sem_oraculo(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, overrides: tuple[str, ...]
) -> None:
    assert main(_argv(tmp_path, ("bpa",), *overrides)) == 1

    assert "missing_oracle source=bpa" in caplog.text
    assert not (tmp_path / "candidate").exists()
    assert not (tmp_path / "reports").exists()


@pytest.mark.parametrize(("overrides", "message"), [
    (("--from-competencia", "2026-1"), "competencia_invalid value=2026-1"),
    (("--to-competencia", "2026-13"), "competencia_invalid value=2026-13"),
    (("--from-competencia", "2026-12", "--to-competencia", "2026-01"),
     "competencia_range_invalid from=2026-12 to=2026-01"),
])
def test_rejeita_competencia_malformada_ou_intervalo_invertido(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], overrides: tuple[str, ...], message: str
) -> None:
    with pytest.raises(SystemExit) as raised:
        main(_argv(tmp_path, ("bpa",), *overrides))

    assert raised.value.code == 2
    assert message in capsys.readouterr().err
    assert not (tmp_path / "candidate").exists()


def _tampered_legacy(
    work: Path, dataset: str, name: str, mutate: Callable[[Any], None]
) -> tuple[Path, Path]:
    oracle_dir = _contract()["datasets"][dataset]["oracle_dir"]
    legacy = work / "legacy"
    shutil.copytree(_ROOT / oracle_dir, legacy / oracle_dir)
    target = legacy / oracle_dir / name
    payload = json.loads(target.read_bytes())
    mutate(payload)
    target.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    return legacy, target


def _mutated_contract(work: Path, mutate: Callable[[dict[str, Any]], None]) -> Path:
    contract = _contract()
    mutate(contract)
    target = work / "contract-mutated.json"
    target.write_text(json.dumps(contract), encoding="utf-8")
    return target


def _repinned(work: Path, dataset: str, tampered: Path) -> Path:
    def repin(contract: dict[str, Any]) -> None:
        pins = contract["datasets"][dataset]["oracle_files"]
        pins[tampered.name] = sha256(tampered.read_bytes()).hexdigest()

    return _mutated_contract(work, repin)


def _tampered_argv(work: Path, name: str, mutate: Callable[[Any], None]) -> list[str]:
    legacy, tampered = _tampered_legacy(work, "bpa", name, mutate)
    contract = _repinned(work, "bpa", tampered)
    return _argv(work, ("bpa",), "--legacy-root", str(legacy), "--contract", str(contract))


def _bpa_linhas_treze(payload: dict[str, Any]) -> None:
    payload["overview"]["kpis"]["linhas"] = 13


def test_rejeita_oraculo_adulterado_antes_de_tocar_o_candidato(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    legacy, _ = _tampered_legacy(tmp_path, "bpa", "expected_serving.json", _bpa_linhas_treze)

    assert main(_argv(tmp_path, ("bpa",), "--legacy-root", str(legacy))) == 1

    assert "oracle_digest_mismatch dataset=bpa" in caplog.text
    assert "expected_serving.json" in caplog.text
    assert not (tmp_path / "candidate").exists()
    assert not (tmp_path / "reports").exists()


def test_fixture_negativa_retorna_um_e_nomeia_a_metrica_divergente(tmp_path: Path) -> None:
    argv = _tampered_argv(tmp_path, "expected_serving.json", _bpa_linhas_treze)

    assert main(argv) == 1

    report = _report(tmp_path, "bpa")
    mismatches = [c for c in report["comparisons"] if c["status"] == "MISMATCH"]
    assert report["accepted"] is False
    assert [(c["metric"], c["legacy_value"], c["candidate_value"]) for c in mismatches] == [
        ("bpa-serving-overview::kpis.linhas", 13, 12),
    ]
    assert _aggregate(tmp_path)["accepted"] is False


def test_documento_vazio_no_oraculo_exige_candidato_vazio(tmp_path: Path) -> None:
    argv = _tampered_argv(
        tmp_path, "expected_normalized.json", lambda p: p["BPA_C"].update(quality_issues=[])
    )

    assert main(argv) == 1

    report = _report(tmp_path, "bpa")
    emptied = [c for c in report["comparisons"] if c["metric"].endswith("quality-bpa-c::")]
    assert report["accepted"] is False
    fields = [(c["legacy_value"], c["candidate_value"], c["absent"], c["status"]) for c in emptied]
    assert fields == [({"empty": "list"}, None, "candidate", "MISMATCH")]
    assert _aggregate(tmp_path)["failures"] == []


def _sem_nacional(contract: dict[str, Any]) -> None:
    spec = contract["datasets"]["cnes"]
    spec["raw_inputs"] = [
        raw for raw in spec["raw_inputs"] if raw["manifest"]["source_type"] == "CNES_LOCAL"
    ]


def _bpa_overview(contract: dict[str, Any]) -> dict[str, Any]:
    documents = contract["datasets"]["bpa"]["documents"]
    return next(item for item in documents if item["doc_id"] == "bpa-serving-overview")


def _folha_inexistente(contract: dict[str, Any]) -> None:
    _bpa_overview(contract)["candidate"]["leaf"] = "inexistente.json"


def _caminho_inexistente(contract: dict[str, Any]) -> None:
    _bpa_overview(contract)["oracle"]["path"] = ["inexistente"]


def _manifest_invalido(contract: dict[str, Any]) -> None:
    contract["datasets"]["bpa"]["raw_inputs"][0]["manifest"]["manifest_version"] = 2


@pytest.mark.parametrize(("mutate", "error"), [
    (_manifest_invalido, "unexpected_error type=ValidationError"),
    (_folha_inexistente, "candidate_leaf_missing layer=serving leaf=inexistente.json matches=0"),
    (_caminho_inexistente, "oracle_path_missing file=expected_serving.json path=inexistente"),
])
def test_falha_do_job_e_registrada_sem_expor_a_mensagem_original(
    tmp_path: Path, mutate: Callable[[dict[str, Any]], None], error: str
) -> None:
    contract = _mutated_contract(tmp_path, mutate)

    assert main(_argv(tmp_path, ("bpa",), "--contract", str(contract))) == 1

    _assert_falha(tmp_path, "bpa", error)


def test_run_degradado_retorna_um_sem_relatorio_do_dataset(tmp_path: Path) -> None:
    contract = _mutated_contract(tmp_path, _sem_nacional)

    assert main(_argv(tmp_path, ("cnes",), "--contract", str(contract))) == 1

    _assert_falha(
        tmp_path, "cnes", "run_not_published run_id=mig010-cnes-2026-01 state=PUBLISHED_DEGRADED"
    )
    assert not (tmp_path / "reports" / _TENANT / "cnes").exists()


def test_oraculo_com_float_falha_no_achatamento(tmp_path: Path) -> None:
    argv = _tampered_argv(
        tmp_path, "expected_serving.json", lambda p: p["overview"]["kpis"].update(linhas=1.5)
    )

    assert main(argv) == 1

    _assert_falha(
        tmp_path, "bpa",
        "flatten_failed doc_id=bpa-serving-overview "
        "error=float_not_allowed metric=bpa-serving-overview::kpis.linhas",
    )


def test_onda_que_nao_termina_no_prazo_retorna_um(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(LocalWorkerPool, "status", lambda *_: ExecutionStatus.RUNNING)
    monkeypatch.setattr("scripts.run_historical_shadow._WAVE_DEADLINE_SECONDS", 0.0)

    assert main(_argv(tmp_path, ("bpa",))) == 1

    _assert_falha(tmp_path, "bpa", "wave_timeout run_id=mig010-bpa-2026-08")


def test_publicacao_de_outro_run_retorna_um(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def other_run(tenant: str, dataset: str, competencia: str, run_id: str) -> Expected:
        return Expected(tenant, dataset, competencia, "outro-run")

    monkeypatch.setattr("scripts.run_historical_shadow.Expected", other_run)

    assert main(_argv(tmp_path, ("sihd",))) == 1

    _assert_falha(tmp_path, "sihd", "publication_mismatch dataset=sihd run_id=outro-run")


def test_agregado_criado_por_outro_processo_retorna_um(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    aggregate = _aggregate_path(tmp_path)

    def concurrent_bytes(payload: dict[str, object]) -> bytes:
        aggregate.write_bytes(b"outro processo")
        return aggregate_bytes(payload)

    monkeypatch.setattr("scripts.run_historical_shadow.aggregate_bytes", concurrent_bytes)

    assert main(_argv(tmp_path, ("sihd",))) == 1

    assert aggregate.read_bytes() == b"outro processo"
    assert "shadow_aggregate_failed error=report_exists report=aggregate.json" in caplog.text


def test_contrato_com_campo_de_tolerancia_retorna_um(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    contract = _mutated_contract(tmp_path, lambda data: data.update(tolerance=0.1))

    assert main(_argv(tmp_path, ("sihd",), "--contract", str(contract))) == 1

    assert "forbidden_field key=tolerance" in caplog.text
    assert not (tmp_path / "candidate").exists()


@pytest.mark.parametrize("relative", ["sihd/2026-01.json", "aggregate.json"])
def test_recusa_sobrescrever_relatorio_existente(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, relative: str
) -> None:
    existing = tmp_path / "reports" / _TENANT / relative
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"sentinela")

    assert main(_argv(tmp_path, ("sihd",))) == 1

    assert existing.read_bytes() == b"sentinela"
    assert "report_exists" in caplog.text
    assert not (tmp_path / "candidate").exists()


def test_recusa_candidate_root_nao_vazio(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    marker = tmp_path / "candidate" / "marcador.txt"
    marker.parent.mkdir()
    marker.write_text("x", encoding="utf-8")

    assert main(_argv(tmp_path, ("sihd",))) == 1

    assert "candidate_root_not_empty" in caplog.text
    assert [path.name for path in (tmp_path / "candidate").iterdir()] == ["marcador.txt"]
    assert not (tmp_path / "reports").exists()
