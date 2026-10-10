"""Testes da CLI de equivalencia historica (MIG-010)."""

from __future__ import annotations

import json
import re
import shutil
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from cnes_contracts.manifests.outputs import OutputManifest, RunManifest
from cnes_domain.ports.object_store import ObjectStat
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.object_store import FilesystemObjectStore
from scripts.run_historical_shadow import (
    ShadowRunError,
    main,
    read_verified_outputs,
    write_report,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager
    from typing import BinaryIO

_ROOT = Path(__file__).resolve().parents[1]
_CONTRACT = _ROOT / "docs/fixtures/migration/equivalence-contract-v1.json"
_TENANT = "354130"
_NOW = datetime(2026, 10, 10, 12, tzinfo=UTC)
_COMPETENCIA = {"cnes": "2026-01", "sihd": "2026-01", "bpa": "2026-08", "sia": "2026-01"}
_DATASETS = tuple(_COMPETENCIA)
_WAVES = [["NORMALIZE"], ["RECONCILE"], ["MATERIALIZE"]]
_APPROVED_RULES = frozenset({
    "MIG010-RUN-ID", "MIG010-GENERATED-AT", "MIG010-NORMALIZED-AT",
    "MIG010-CNES-NORMALIZED-IDS", "MIG010-CNES-DIVERGENCE-TEXT",
    "MIG010-SIA-RAW-MANIFEST-SHA256",
})
_RUN_ID = "r1"
_KEY = f"serving/{_TENANT}/{_RUN_ID}/overview.json"
_BODY = b'{"valor": 1}'
_BODY_SHA = sha256(_BODY).hexdigest()


def _argv(work: Path, sources: tuple[str, ...], *overrides: str) -> list[str]:
    base = [
        "--tenant", _TENANT, "--from-competencia", "2026-01", "--to-competencia", "2026-12",
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


@dataclass(frozen=True)
class _Executed:
    work: Path
    exit_code: int


@pytest.fixture(scope="module")
def executed(tmp_path_factory: pytest.TempPathFactory) -> _Executed:
    work = tmp_path_factory.mktemp("shadow")
    return _Executed(work, main(_argv(work, _DATASETS)))


def test_execucao_completa_cobre_as_quatro_fontes_sem_falhas(executed: _Executed) -> None:
    aggregate = _aggregate(executed.work)

    assert executed.exit_code == 0
    assert aggregate["accepted"] is True
    assert aggregate["failures"] == []
    assert [(item["dataset"], item["competencia"]) for item in aggregate["covered"]] == [
        ("bpa", "2026-08"), ("cnes", "2026-01"), ("sia", "2026-01"), ("sihd", "2026-01"),
    ]


def test_agregado_lista_o_hash_de_cada_relatorio_imutavel(executed: _Executed) -> None:
    for item in _aggregate(executed.work)["covered"]:
        path = executed.work / "reports" / _TENANT / item["report"]

        assert sha256(path.read_bytes()).hexdigest() == item["report_sha256"]
        assert path.stat().st_mode & 0o222 == 0


@pytest.mark.parametrize("dataset", _DATASETS)
def test_fonte_retida_sem_mismatch(executed: _Executed, dataset: str) -> None:
    report = _report(executed.work, dataset)
    explained = {c["rule_id"] for c in report["comparisons"] if c["status"] == "EXPLAINED"}

    assert report["accepted"] is True
    assert report["summary"]["MISMATCH"] == 0
    assert report["summary"]["MATCH"] > 0
    assert explained <= _APPROVED_RULES


@pytest.mark.parametrize("dataset", _DATASETS)
def test_drena_tres_ondas_pelo_ponteiro(executed: _Executed, dataset: str) -> None:
    report = _report(executed.work, dataset)
    run_id = f"mig010-{dataset}-{_COMPETENCIA[dataset]}"
    job = executed.work / "candidate" / f"{dataset}-{_COMPETENCIA[dataset]}"
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
    outputs = _report(executed.work, dataset)["outputs"]
    asserted = {
        (output["layer"], output["object_key"].rsplit("/", 1)[-1])
        for output in outputs
        if output["asserted"]
    }

    assert asserted == declared


@pytest.mark.parametrize(("dataset", "layer"), [
    ("sihd", "reconciliation"), ("bpa", "reconciliation"), ("sia", "reconciliation"),
    ("cnes", "normalized"),
])
def test_camada_nao_afirmada_aparece_sem_marca(
    executed: _Executed, dataset: str, layer: str
) -> None:
    outputs = [o for o in _report(executed.work, dataset)["outputs"] if o["layer"] == layer]

    assert outputs
    assert not any(output["asserted"] for output in outputs)


@pytest.mark.parametrize(("dataset", "kind"), [
    ("cnes", "independent_frozen"), ("sihd", "reproduction"), ("bpa", "reproduction"),
    ("sia", "reproduction"),
])
def test_relatorio_registra_a_proveniencia_do_oraculo(
    executed: _Executed, dataset: str, kind: str
) -> None:
    provenance = _report(executed.work, dataset)["provenance"]

    assert (provenance["kind"], provenance["data_nature"]) == (kind, "synthetic")
    assert provenance["frozen_in"]


def test_agregado_e_reproduzivel_e_sem_caminhos_locais(tmp_path: Path) -> None:
    first, second = tmp_path / "a", tmp_path / "b"

    assert main(_argv(first, ("sihd",))) == 0
    assert main(_argv(second, ("sihd",))) == 0

    assert _aggregate_path(first).read_bytes() == _aggregate_path(second).read_bytes()
    assert _report(first, "sihd") == _report(second, "sihd")
    assert re.fullmatch(r"[0-9a-f]{40}", _aggregate(first)["git_commit"])
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


def _usage_error(work: Path, capsys: pytest.CaptureFixture[str], *overrides: str) -> str:
    with pytest.raises(SystemExit) as raised:
        main(_argv(work, ("bpa",), *overrides))
    assert raised.value.code == 2
    assert not (work / "candidate").exists()
    return capsys.readouterr().err


@pytest.mark.parametrize("override", [
    ("--from-competencia", "2026-1"), ("--to-competencia", "2026-13"),
])
def test_rejeita_competencia_malformada(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], override: tuple[str, str]
) -> None:
    assert f"competencia_invalid value={override[1]}" in _usage_error(tmp_path, capsys, *override)


def test_rejeita_intervalo_invertido(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    overrides = ("--from-competencia", "2026-12", "--to-competencia", "2026-01")

    assert "competencia_range_invalid from=2026-12 to=2026-01" in _usage_error(
        tmp_path, capsys, *overrides
    )


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


def test_documento_sem_metricas_do_oraculo_retorna_um(tmp_path: Path) -> None:
    argv = _tampered_argv(
        tmp_path, "expected_normalized.json", lambda p: p["BPA_C"].update(quality_issues=[])
    )

    assert main(argv) == 1

    _assert_falha(tmp_path, "bpa", "oracle_document_empty doc_id=bpa-normalized-quality-bpa-c")


def _sem_nacional(contract: dict[str, Any]) -> None:
    spec = contract["datasets"]["cnes"]
    spec["raw_inputs"] = [
        raw for raw in spec["raw_inputs"] if raw["manifest"]["source_type"] == "CNES_LOCAL"
    ]


def _folha_inexistente(contract: dict[str, Any]) -> None:
    for item in contract["datasets"]["bpa"]["documents"]:
        if item["doc_id"] == "bpa-serving-overview":
            item["candidate"]["leaf"] = "inexistente.json"


def _manifest_invalido(contract: dict[str, Any]) -> None:
    contract["datasets"]["bpa"]["raw_inputs"][0]["manifest"]["manifest_version"] = 2


def test_erro_inesperado_e_registrado_sem_expor_a_mensagem_original(tmp_path: Path) -> None:
    contract = _mutated_contract(tmp_path, _manifest_invalido)

    assert main(_argv(tmp_path, ("bpa",), "--contract", str(contract))) == 1

    _assert_falha(tmp_path, "bpa", "unexpected_error type=ValidationError")


def test_run_degradado_retorna_um_sem_relatorio_do_dataset(tmp_path: Path) -> None:
    contract = _mutated_contract(tmp_path, _sem_nacional)

    assert main(_argv(tmp_path, ("cnes",), "--contract", str(contract))) == 1

    _assert_falha(
        tmp_path, "cnes", "run_not_published run_id=mig010-cnes-2026-01 state=PUBLISHED_DEGRADED"
    )
    assert not (tmp_path / "reports" / _TENANT / "cnes").exists()


def test_folha_do_candidato_ausente_no_run_manifest_retorna_um(tmp_path: Path) -> None:
    contract = _mutated_contract(tmp_path, _folha_inexistente)

    assert main(_argv(tmp_path, ("bpa",), "--contract", str(contract))) == 1

    _assert_falha(
        tmp_path, "bpa", "candidate_leaf_missing layer=serving leaf=inexistente.json matches=0"
    )


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


def test_write_report_cria_somente_leitura_e_recusa_sobrescrever(tmp_path: Path) -> None:
    target = tmp_path / "nivel" / "relatorio.json"

    write_report(target, b"{}\n")

    assert target.read_bytes() == b"{}\n"
    assert target.stat().st_mode & 0o222 == 0
    with pytest.raises(ShadowRunError, match=r"report_exists report=relatorio\.json"):
        write_report(target, b"outro")
    assert target.read_bytes() == b"{}\n"


@dataclass
class _FakeStore:
    objects: dict[str, bytes]
    stat_sha: dict[str, str] = field(default_factory=dict[str, str])

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        raise AssertionError(f"put_unexpected key={key}")

    def open(self, key: str) -> AbstractContextManager[BinaryIO]:
        return nullcontext(BytesIO(self.objects[key]))

    def stat(self, key: str) -> ObjectStat | None:
        data = self.objects.get(key)
        if data is None:
            return None
        return ObjectStat(key, len(data), self.stat_sha.get(key, sha256(data).hexdigest()))

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        raise AssertionError(f"promote_unexpected key={source_key}")

    def delete(self, key: str) -> None:
        raise AssertionError(f"delete_unexpected key={key}")


def _run_manifest(object_sha256: str = _BODY_SHA) -> tuple[RunManifest, bytes]:
    output = OutputManifest(
        manifest_version=1, manifest_id="serving-overview", tenant_id=_TENANT,
        layer="serving", source_type=None, competencia="2026-01", run_id=_RUN_ID,
        unit_id="unit-1", attempt=1, schema_version="demo-v1", object_key=_KEY,
        object_sha256=object_sha256, row_count=1, created_at=_NOW,
    )
    manifest = RunManifest(
        manifest_version=1, tenant_id=_TENANT, dataset_name="demo", run_id=_RUN_ID,
        competencia="2026-01", outputs=(output,), missing_sources=(), published_at=_NOW,
    )
    return manifest, manifest.model_dump_json(exclude_none=False, by_alias=False).encode()


def test_le_as_saidas_quando_hash_e_bytes_conferem() -> None:
    manifest, stored = _run_manifest()

    outputs = read_verified_outputs(_FakeStore({_KEY: _BODY}), manifest, stored, ("overview",))

    assert outputs == {_KEY: _BODY}


@pytest.mark.parametrize("adulterado", ["manifest", "objeto"])
def test_rejeita_hash_de_manifest_adulterado(adulterado: str) -> None:
    manifest, stored = _run_manifest("0" * 64 if adulterado == "manifest" else _BODY_SHA)
    body = b"adulterado" if adulterado == "objeto" else _BODY

    with pytest.raises(ShadowRunError, match=r"output_sha256_mismatch key=serving/354130/r1/"):
        read_verified_outputs(_FakeStore({_KEY: body}), manifest, stored, ("overview",))


def test_rejeita_hash_do_stat_divergente_do_manifest() -> None:
    manifest, stored = _run_manifest()
    store = _FakeStore({_KEY: _BODY}, {_KEY: "f" * 64})

    with pytest.raises(ShadowRunError, match=r"output_stat_mismatch key=serving/354130/r1/"):
        read_verified_outputs(store, manifest, stored, ("overview",))


def test_rejeita_run_manifest_nao_canonico() -> None:
    manifest, stored = _run_manifest()
    pretty = json.dumps(json.loads(stored), indent=2).encode()

    with pytest.raises(ShadowRunError, match=r"manifest_not_canonical run_id=r1"):
        read_verified_outputs(_FakeStore({_KEY: _BODY}), manifest, pretty, ("overview",))


def test_rejeita_chaves_de_serving_fora_do_catalogo() -> None:
    manifest, stored = _run_manifest()
    documents = ("overview", "by-establishment")

    with pytest.raises(ShadowRunError, match=r"serving_keys_mismatch run_id=r1"):
        read_verified_outputs(_FakeStore({_KEY: _BODY}), manifest, stored, documents)
