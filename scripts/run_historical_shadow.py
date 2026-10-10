"""Reproduz o oraculo congelado pela orquestracao Parquet real e grava a evidencia MIG-010."""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import polars as pl

from central_api.services.run_planning import RunPlanningDependencies, RunPlanningService
from cnes_contracts.manifests.outputs import OutputManifest, RunManifest
from cnes_contracts.manifests.raw import RawManifest
from cnes_contracts.manifests.validation import manifest_sha256
from cnes_domain.control_plane.entities import RawManifestRecord, Run
from cnes_domain.control_plane.enums import RunStage, RunState
from cnes_domain.orchestration.source_catalog import build_source_catalog
from cnes_domain.ports.processing import ExecutionPolicyConfig, ExecutionStatus
from cnes_domain.profiles import ProfileSettings
from cnes_infra.billing import (
    LOCAL_BILLING_SETTINGS,
    BillingGateResources,
    build_execution_callbacks,
)
from data_processor.composition import build_local_processor_runtime
from data_processor.migration.equivalence import (
    ContractInvalid,
    DatasetSpec,
    DocumentSpec,
    EquivalenceContract,
    RawInput,
    Scalar,
    SourceEquivalenceReport,
    aggregate_bytes,
    compare_shadow_run,
    flatten_payload,
    load_contract,
    report_bytes,
    sha256_hex,
)
from data_processor.orchestration.coordinator import noop_execution_started

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_CONTRACT = _ROOT / "docs/fixtures/migration/equivalence-contract-v1.json"
_SOURCES = ("cnes", "sihd", "bpa", "sia")
_AGGREGATE = "aggregate.json"
_CONCURRENCY, _LEASE_SECONDS = 4, 300
_MAX_ROUNDS, _WAVE_DEADLINE_SECONDS, _POLL_SECONDS = 12, 120.0, 0.002
_ACTIVE = frozenset({RunState.PROCESSING, RunState.PUBLISHING})
_DTYPES = {"String": pl.String, "Int64": pl.Int64, "Float64": pl.Float64}


class ShadowRunError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class _Settings:
    contract: EquivalenceContract
    contract_sha256: str
    tenant: str
    legacy_root: Path
    candidate_root: Path
    report_root: Path


@dataclass(frozen=True, slots=True)
class _Job:
    dataset: str
    competencia: str
    spec: DatasetSpec
    legacy_sha256: str
    settings: _Settings


@dataclass(frozen=True, slots=True)
class _Published:
    manifest_key: str
    manifest_sha256: str
    manifest: RunManifest
    outputs: dict[str, bytes]


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--source", action="append", choices=_SOURCES, required=True)
    parser.add_argument("--from-competencia", required=True)
    parser.add_argument("--to-competencia", required=True)
    for name in ("legacy-root", "candidate-root", "report-root"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--contract", type=Path, default=_DEFAULT_CONTRACT)
    return parser.parse_args(argv)


def _verify_oracle(root: Path, dataset: str, spec: DatasetSpec) -> str:
    pairs: list[str] = []
    for name, expected in sorted(spec.oracle_files.items()):
        relative = f"{spec.oracle_dir}/{name}"
        data = (root / relative).read_bytes()
        actual = sha256_hex(data.replace(b"\r\n", b"\n") if name.endswith(".json") else data)
        if actual != expected:
            raise ShadowRunError(
                f"oracle_digest_mismatch dataset={dataset} file={relative} "
                f"expected={expected} actual={actual}"
            )
        pairs.append(f"{relative} {expected}")
    return sha256_hex("\n".join(pairs).encode())


def _plan_jobs(args: argparse.Namespace, settings: _Settings) -> list[_Job]:
    jobs: list[_Job] = []
    for dataset in sorted(set(cast("list[str]", args.source))):
        spec = settings.contract.datasets.get(dataset)
        inside = [
            item for item in (spec.competencias if spec else ())
            if args.from_competencia <= item <= args.to_competencia
        ]
        if spec is None or spec.tenant_id != args.tenant or not inside:
            raise ShadowRunError(
                f"missing_oracle source={dataset} tenant={args.tenant} "
                f"from={args.from_competencia} to={args.to_competencia}"
            )
        digest = _verify_oracle(settings.legacy_root, dataset, spec)
        jobs.extend(_Job(dataset, item, spec, digest, settings) for item in inside)
    return jobs


def _preflight(settings: _Settings, jobs: list[_Job]) -> None:
    candidate = settings.candidate_root
    if candidate.exists() and (not candidate.is_dir() or any(candidate.iterdir())):
        raise ShadowRunError(f"candidate_root_not_empty path={candidate}")
    for name in [*(f"{job.dataset}/{job.competencia}.json" for job in jobs), _AGGREGATE]:
        if (settings.report_root / settings.tenant / name).exists():
            raise ShadowRunError(f"report_exists report={name}")


def _prepare(args: argparse.Namespace) -> tuple[_Settings, list[_Job]]:
    contract = load_contract(args.contract)
    settings = _Settings(
        contract, sha256_hex(args.contract.read_bytes()), args.tenant, args.legacy_root,
        args.candidate_root, args.report_root,
    )
    jobs = _plan_jobs(args, settings)
    _preflight(settings, jobs)
    return settings, jobs


def _dig(node: object, path: tuple[str, ...], label: str) -> object:
    for segment in path:
        if not isinstance(node, dict) or segment not in node:
            raise ShadowRunError(f"oracle_path_missing file={label} path={'/'.join(path)}")
        node = cast("dict[str, object]", node)[segment]
    return node


def _decode(name: str, data: bytes) -> object:
    if name.endswith(".parquet"):
        return pl.read_parquet(BytesIO(data)).to_dicts()
    return json.loads(data)


def _raw_body(job: _Job, raw: RawInput) -> tuple[bytes, int]:
    base = job.settings.legacy_root / job.spec.oracle_dir
    if raw.rows is None:
        body = (base / cast("str", raw.parquet_file)).read_bytes()
        return body, pl.read_parquet(BytesIO(body)).height
    rows = _dig(_decode(raw.rows.file, (base / raw.rows.file).read_bytes()), raw.rows.path, "rows")
    frame = pl.DataFrame(rows, schema_overrides={k: _DTYPES[v] for k, v in raw.dtypes.items()})
    buffer = BytesIO()
    frame.write_parquet(buffer, compression="zstd", compression_level=3)
    return buffer.getvalue(), frame.height


class _Driver:
    def __init__(self, job: _Job) -> None:
        settings = job.settings
        self.job, self.tenant, self.moment = job, settings.tenant, settings.contract.clock
        self.run_id = f"mig010-{job.dataset}-{job.competencia}"
        data_dir = settings.candidate_root / f"{job.dataset}-{job.competencia}"
        self.runtime = build_local_processor_runtime(
            ProfileSettings(tenant_id=self.tenant, data_dir=data_dir), self.clock
        )
        self.cp = cast("SQLiteControlPlane", self.runtime.control_plane)
        self.catalog = build_source_catalog()
        callbacks = build_execution_callbacks(
            LOCAL_BILLING_SETTINGS, self.cp, BillingGateResources(self.clock, _CONCURRENCY),
            noop_execution_started,
        )
        self.service = RunPlanningService(
            RunPlanningDependencies(
                self.cp, self.runtime.object_store, self.runtime.executor, self.catalog
            ),
            ExecutionPolicyConfig(_CONCURRENCY, _LEASE_SECONDS, callbacks), self.clock,
            dispatch_enabled=False,
        )

    def clock(self) -> datetime:
        return self.moment

    def seed(self, raw: RawInput) -> tuple[str, str]:
        body, rows = _raw_body(self.job, raw)
        digest = sha256_hex(body)
        manifest = RawManifest.model_validate_json(json.dumps({
            **raw.manifest, "object_sha256": digest, "size_bytes": len(body), "row_count": rows,
        }))
        store = self.runtime.object_store
        store.put(manifest.object_key, BytesIO(body), digest)
        sidecar = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
        key = (f"raw/{self.tenant}/{manifest.source_type.value}/{manifest.competencia}/"
               f"{manifest.snapshot_id}/manifest.json")
        store.put(key, BytesIO(sidecar), sha256_hex(sidecar))
        record = RawManifestRecord(
            tenant_id=self.tenant, manifest_id=manifest.manifest_id, manifest_key=key,
            agent_id=manifest.agent_id, source_type=manifest.source_type.value,
            file_subtype=manifest.file_subtype, competencia=manifest.competencia,
            snapshot_mode=cast("Literal['FULL', 'DELTA']", manifest.snapshot_mode.value),
            snapshot_id=manifest.snapshot_id, base_snapshot_id=manifest.base_snapshot_id,
            sequence=manifest.sequence, previous_manifest_sha256=manifest.previous_manifest_sha256,
            manifest_sha256=manifest_sha256(manifest), created_at=self.moment,
        )
        with self.cp.write_transaction() as connection:
            self.cp.put_manifest_record(connection, record)
        return manifest.file_subtype, record.manifest_sha256

    def launch(self) -> None:
        job = self.job
        dependencies = self.catalog.for_pipeline(job.dataset).dependencies
        self.cp.put_run(Run(
            tenant_id=self.tenant, run_id=self.run_id, competencia=job.competencia,
            dataset_name=job.dataset, state=RunState.PLANNED, dependencies=dependencies,
            missing_sources=(), created_at=self.moment,
        ))
        self.service.launch(self.tenant, self.run_id)

    def state(self) -> RunState:
        return cast("Run", self.cp.get_run(self.tenant, self.run_id)).state

    def wait(self, refs: set[str]) -> None:
        deadline = time.monotonic() + _WAVE_DEADLINE_SECONDS
        while any(self.runtime.executor.status(ref) is ExecutionStatus.RUNNING for ref in refs):
            if time.monotonic() > deadline:
                raise ShadowRunError(f"wave_timeout run_id={self.run_id}")
            time.sleep(_POLL_SECONDS)

    def drain(self) -> None:
        refs: set[str] = set()
        for _ in range(_MAX_ROUNDS):
            results = self.runtime.coordinator.recover()
            refs.update(item.execution_ref for item in results if item.execution_ref)
            self.wait(refs)
            if self.state() not in _ACTIVE:
                break
        if self.state() is not RunState.PUBLISHED:
            raise ShadowRunError(f"run_not_published run_id={self.run_id} state={self.state()}")

    def waves(self) -> tuple[tuple[str, ...], ...]:
        order = list(RunStage)
        grouped: dict[str | None, set[RunStage]] = {}
        for unit in self.cp.list_run_units(self.tenant, self.run_id):
            grouped.setdefault(unit.dispatch_id, set()).add(unit.stage)
        waves = [sorted(group, key=order.index) for group in grouped.values()]
        waves.sort(key=lambda wave: order.index(wave[0]))
        return tuple(tuple(stage.value for stage in wave) for wave in waves)

    def read_published(self) -> _Published:
        dataset = self.job.dataset
        pointer = self.cp.get_dataset_pointer(self.tenant, dataset)
        version = pointer and self.cp.get_dataset_version(self.tenant, dataset, pointer.version_id)
        if not (pointer and version) or {pointer.version_id, version.run_id} != {self.run_id}:
            raise ShadowRunError(f"publication_mismatch dataset={dataset} run_id={self.run_id}")
        store = self.runtime.object_store
        with store.open(version.run_manifest_key) as stream:
            stored = stream.read()
        manifest = RunManifest.model_validate_json(stored)
        serving = self.catalog.for_pipeline(dataset).layout.serving_documents
        outputs = read_verified_outputs(store, manifest, stored, serving)
        return _Published(version.run_manifest_key, sha256_hex(stored), manifest, outputs)


def _read_checked(store: ObjectStorePort, output: OutputManifest) -> bytes:
    with store.open(output.object_key) as stream:
        data = stream.read()
    if sha256_hex(data) != output.object_sha256:
        raise ShadowRunError(f"output_sha256_mismatch key={output.object_key}")
    stat = store.stat(output.object_key)
    if stat is None or stat.sha256 != output.object_sha256:
        raise ShadowRunError(f"output_stat_mismatch key={output.object_key}")
    return data


def read_verified_outputs(
    store: ObjectStorePort, manifest: RunManifest, stored: bytes, serving: tuple[str, ...]
) -> dict[str, bytes]:
    """Valida o RunManifest publicado e le cada saida conferindo o hash.

    Args: store: object store. manifest: interpretado. stored: bytes gravados. serving: catalogo.
    Returns: bytes de cada saida por chave de objeto.
    Raises: ShadowRunError: manifest nao canonico, serving fora do catalogo ou hash divergente.
    """
    if manifest.model_dump_json(exclude_none=False, by_alias=False).encode() != stored:
        raise ShadowRunError(f"manifest_not_canonical run_id={manifest.run_id}")
    prefix = f"serving/{manifest.tenant_id}/{manifest.run_id}"
    expected = {f"{prefix}/{name}.json" for name in serving}
    actual = {item.object_key for item in manifest.outputs if item.layer == "serving"}
    if actual != expected:
        raise ShadowRunError(
            f"serving_keys_mismatch run_id={manifest.run_id} "
            f"missing={sorted(expected - actual)} extra={sorted(actual - expected)}"
        )
    return {item.object_key: _read_checked(store, item) for item in manifest.outputs}


def _metrics(doc: DocumentSpec, value: object) -> dict[str, Scalar]:
    try:
        return flatten_payload(doc.doc_id, value, doc.key_paths)
    except ValueError as error:
        raise ShadowRunError(f"flatten_failed doc_id={doc.doc_id} error={error}") from error


def _collect(job: _Job, published: _Published) -> tuple[dict[str, Scalar], dict[str, Scalar]]:
    legacy: dict[str, Scalar] = {}
    candidate: dict[str, Scalar] = {}
    for doc in job.spec.documents:
        path = job.settings.legacy_root / job.spec.oracle_dir / doc.oracle.file
        document = _dig(_decode(path.name, path.read_bytes()), doc.oracle.path, doc.oracle.file)
        expected = _metrics(doc, document)
        if not expected:
            raise ShadowRunError(f"oracle_document_empty doc_id={doc.doc_id}")
        found = [o for o in published.manifest.outputs if o.layer == doc.candidate.layer
                 and o.object_key.endswith(f"/{doc.candidate.leaf}")]
        if len(found) != 1:
            raise ShadowRunError(
                f"candidate_leaf_missing layer={doc.candidate.layer} "
                f"leaf={doc.candidate.leaf} matches={len(found)}"
            )
        key = found[0].object_key
        legacy.update(expected)
        candidate.update(_metrics(doc, _decode(key, published.outputs[key])))
    return legacy, candidate


def _context(job: _Job, published: _Published, raw_sha256: dict[str, str]) -> dict[str, str]:
    context = {
        "version_id": published.manifest.run_id,
        "clock_z": job.settings.contract.clock_z,
        "clock_offset": job.settings.contract.clock_offset,
    }
    ids: dict[str, list[str]] = {}
    for output in published.manifest.outputs:
        if output.layer == "normalized" and output.source_type is not None:
            ids.setdefault(output.source_type.value, []).append(output.manifest_id)
    context.update({f"normalized_id/{name}": found[0] for name, found in ids.items()
                    if len(found) == 1})
    context.update({
        f"raw_manifest_sha256/{doc.doc_id}": raw_sha256[doc.raw_subtype]
        for doc in job.spec.documents
        if doc.raw_subtype is not None and doc.raw_subtype in raw_sha256
    })
    return context


def _evidence(
    job: _Job, published: _Published, waves: tuple[tuple[str, ...], ...]
) -> dict[str, object]:
    declared = {(doc.candidate.layer, doc.candidate.leaf) for doc in job.spec.documents}
    outputs = [
        {
            "asserted": (item.layer, item.object_key.rpartition("/")[2]) in declared,
            "layer": item.layer, "object_key": item.object_key,
            "object_sha256": item.object_sha256, "row_count": item.row_count,
        }
        for item in published.manifest.outputs
    ]
    return {
        "contract_sha256": job.settings.contract_sha256, "outputs": outputs,
        "provenance": job.spec.provenance.model_dump(), "run_manifest_key": published.manifest_key,
        "run_manifest_sha256": published.manifest_sha256, "run_state": RunState.PUBLISHED.value,
        "waves": [list(wave) for wave in waves],
    }


def write_report(path: Path, data: bytes) -> None:
    """Grava o arquivo de forma imutavel: criacao exclusiva e somente leitura.

    Args: path: destino. data: bytes canonicos.
    Raises: ShadowRunError: o destino ja existe.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(data)
    except FileExistsError as error:
        raise ShadowRunError(f"report_exists report={path.name}") from error
    path.chmod(0o444)


def _attempt(job: _Job) -> dict[str, object]:
    driver = _Driver(job)
    try:
        raw_sha256 = dict(driver.seed(raw) for raw in job.spec.raw_inputs)
        driver.launch()
        driver.drain()
        waves = driver.waves()
        published = driver.read_published()
    finally:
        driver.runtime.executor.close()
    legacy, candidate = _collect(job, published)
    report = SourceEquivalenceReport(
        tenant_id=driver.tenant, dataset=job.dataset,
        source_types=driver.catalog.for_pipeline(job.dataset).source_types,
        competencia=job.competencia, legacy_sha256=job.legacy_sha256,
        candidate_version_id=driver.run_id,
        comparisons=compare_shadow_run(
            contract=job.settings.contract, legacy=legacy, candidate=candidate,
            context=_context(job, published, raw_sha256),
        ),
    )
    data = report_bytes(report, _evidence(job, published, waves))
    name = f"{job.dataset}/{job.competencia}.json"
    write_report(job.settings.report_root / driver.tenant / name, data)
    logger.info(
        "shadow_job dataset=%s competencia=%s accepted=%s metrics=%d",
        job.dataset, job.competencia, report.accepted, len(report.comparisons),
    )
    return {"accepted": report.accepted, "report": name, "report_sha256": sha256_hex(data)}


def _execute_job(job: _Job) -> dict[str, object]:
    identity: dict[str, object] = {"competencia": job.competencia, "dataset": job.dataset}
    try:
        return {**identity, **_attempt(job)}
    except ShadowRunError as error:
        message = str(error)
    except Exception as error:
        logger.exception("shadow_job_unexpected dataset=%s", job.dataset)
        message = f"unexpected_error type={type(error).__name__}"
    logger.error("shadow_job_failed dataset=%s error=%s", job.dataset, message)
    return {**identity, "error": message}


def _git_commit() -> str:
    git = shutil.which("git")
    if git is None:
        return "unknown"
    args = [git, "rev-parse", "HEAD"]
    completed = subprocess.run(args, capture_output=True, text=True, check=False, cwd=_ROOT)
    return completed.stdout.strip() or "unknown"


def _write_aggregate(settings: _Settings, entries: list[dict[str, object]]) -> bool:
    covered = [item for item in entries if "error" not in item]
    failures = [item for item in entries if "error" in item]
    accepted = bool(covered) and not failures and all(item["accepted"] for item in covered)
    payload: dict[str, object] = {
        "accepted": accepted, "contract_sha256": settings.contract_sha256,
        "contract_version": settings.contract.contract_version, "covered": covered,
        "failures": failures, "git_commit": _git_commit(), "tenant_id": settings.tenant,
    }
    write_report(settings.report_root / settings.tenant / _AGGREGATE, aggregate_bytes(payload))
    return accepted


def main(argv: list[str] | None = None) -> int:
    """Roda a orquestracao real por dataset; 0 somente se todos os relatorios forem aceitos."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        settings, jobs = _prepare(_parse_args(argv))
    except (ContractInvalid, ShadowRunError, OSError) as error:
        logger.error("shadow_precondition_failed error=%s", error)
        return 1
    entries = [_execute_job(job) for job in jobs]
    try:
        accepted = _write_aggregate(settings, entries)
    except ShadowRunError as error:
        logger.error("shadow_aggregate_failed error=%s", error)
        return 1
    logger.info("shadow_finished accepted=%s jobs=%d", accepted, len(jobs))
    return 0 if accepted else 1


if __name__ == "__main__":
    sys.exit(main())
