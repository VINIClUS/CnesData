"""Reproduz o oraculo congelado pela orquestracao Parquet real e grava a evidencia MIG-010."""

from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import polars as pl

from central_api.services.run_planning import RunPlanningDependencies, RunPlanningService
from cnes_contracts.manifests.raw import RawManifest
from cnes_contracts.manifests.validation import COMPETENCIA_PATTERN, manifest_sha256
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
    compare_shadow_run,
    parse_contract,
)
from data_processor.migration.flatten import Leaf, flatten_payload
from data_processor.migration.publication import (
    Expected,
    PublishedRun,
    ShadowRunError,
    read_published,
)
from data_processor.migration.report import (
    Covered,
    Failure,
    Outcome,
    OutputEvidence,
    Request,
    SourceEquivalenceReport,
    Stamp,
    aggregate_accepted,
    aggregate_bytes,
    build_aggregate,
    output_evidence,
    report_bytes,
    sha256_hex,
)
from data_processor.orchestration.coordinator import noop_execution_started

if TYPE_CHECKING:
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
type _Waves = tuple[tuple[str, ...], ...]


@dataclass(frozen=True, slots=True)
class _Settings:
    contract: EquivalenceContract
    contract_sha256: str
    source_commit: str
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
    oracle: dict[str, bytes]
    settings: _Settings


def _competencia(value: str) -> str:
    if not re.fullmatch(COMPETENCIA_PATTERN, value):
        raise argparse.ArgumentTypeError(f"competencia_invalid value={value}")
    return value


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--source", action="append", choices=_SOURCES, required=True)
    parser.add_argument("--from-competencia", type=_competencia, required=True)
    parser.add_argument("--to-competencia", type=_competencia, required=True)
    for name in ("legacy-root", "candidate-root", "report-root"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--contract", type=Path, default=_DEFAULT_CONTRACT)
    args = parser.parse_args(argv)
    first, last = args.from_competencia, args.to_competencia
    if first > last:
        parser.error(f"competencia_range_invalid from={first} to={last}")
    return args


def _verify_oracle(root: Path, dataset: str, spec: DatasetSpec) -> tuple[str, dict[str, bytes]]:
    pairs: list[str] = []
    verified: dict[str, bytes] = {}
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
        verified[name] = data
    return sha256_hex("\n".join(pairs).encode()), verified


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
        digest, verified = _verify_oracle(settings.legacy_root, dataset, spec)
        jobs.extend(_Job(dataset, item, spec, digest, verified, settings) for item in inside)
    return jobs


def _preflight(settings: _Settings, jobs: list[_Job]) -> None:
    candidate = settings.candidate_root
    if candidate.exists() and (not candidate.is_dir() or any(candidate.iterdir())):
        raise ShadowRunError(f"candidate_root_not_empty path={candidate}")
    for name in [*(f"{job.dataset}/{job.competencia}.json" for job in jobs), _AGGREGATE]:
        if (settings.report_root / settings.tenant / name).exists():
            raise ShadowRunError(f"report_exists report={name}")


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
    if raw.rows is None:
        body = job.oracle[cast("str", raw.parquet_file)]
        return body, pl.read_parquet(BytesIO(body)).height
    rows = _dig(_decode(raw.rows.file, job.oracle[raw.rows.file]), raw.rows.path, "rows")
    frame = pl.DataFrame(rows, schema_overrides={k: _DTYPES[v] for k, v in raw.dtypes.items()})
    buffer = BytesIO()
    frame.write_parquet(buffer, compression="zstd", compression_level=3)
    return buffer.getvalue(), frame.height


class _Driver:
    def __init__(self, job: _Job) -> None:
        settings = job.settings
        self.job, self.tenant, self.moment = job, settings.tenant, settings.contract.clock
        self.clock = lambda: self.moment
        self.run_id = f"mig010-{job.dataset}-{job.competencia}"
        data_dir = settings.candidate_root / f"{job.dataset}-{job.competencia}"
        profile = ProfileSettings(tenant_id=self.tenant, data_dir=data_dir)
        self.runtime = build_local_processor_runtime(profile, self.clock)
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
        dependencies = self.catalog.for_pipeline(self.job.dataset).dependencies
        self.cp.put_run(Run(
            tenant_id=self.tenant, run_id=self.run_id, competencia=self.job.competencia,
            dataset_name=self.job.dataset, state=RunState.PLANNED, dependencies=dependencies,
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

    def waves(self) -> _Waves:
        order = list(RunStage)
        grouped: dict[str | None, set[RunStage]] = {}
        for unit in self.cp.list_run_units(self.tenant, self.run_id):
            grouped.setdefault(unit.dispatch_id, set()).add(unit.stage)
        waves = [sorted(group, key=order.index) for group in grouped.values()]
        waves.sort(key=lambda wave: order.index(wave[0]))
        return tuple(tuple(stage.value for stage in wave) for wave in waves)

    def published(self) -> PublishedRun:
        job = self.job
        expected = Expected(self.tenant, job.dataset, job.competencia, self.run_id)
        serving = self.catalog.for_pipeline(job.dataset).layout.serving_documents
        return read_published(self.cp, self.runtime.object_store, expected, serving)


def _metrics(doc: DocumentSpec, value: object) -> dict[str, Leaf]:
    try:
        return flatten_payload(doc.doc_id, value, doc.key_paths)
    except ValueError as error:
        raise ShadowRunError(f"flatten_failed doc_id={doc.doc_id} error={error}") from error


def _collect(job: _Job, published: PublishedRun) -> tuple[dict[str, Leaf], dict[str, Leaf]]:
    legacy: dict[str, Leaf] = {}
    candidate: dict[str, Leaf] = {}
    for doc in job.spec.documents:
        name = doc.oracle.file
        document = _dig(_decode(name, job.oracle[name]), doc.oracle.path, name)
        expected = _metrics(doc, document)
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


def _context(job: _Job, published: PublishedRun, raw_sha256: dict[str, str]) -> dict[str, str]:
    context = {
        "version_id": published.version_id,
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
    job: _Job, published: PublishedRun, outputs: tuple[OutputEvidence, ...], waves: _Waves
) -> dict[str, object]:
    return {
        "contract_sha256": job.settings.contract_sha256, "outputs": [asdict(o) for o in outputs],
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


def _attempt(job: _Job) -> Covered:
    driver = _Driver(job)
    try:
        raw_sha256 = dict(driver.seed(raw) for raw in job.spec.raw_inputs)
        driver.launch()
        driver.drain()
        waves = driver.waves()
        published = driver.published()
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
    outputs = output_evidence(published.manifest.outputs, job.spec.documents)
    data = report_bytes(report, _evidence(job, published, outputs, waves))
    name = f"{job.dataset}/{job.competencia}.json"
    write_report(job.settings.report_root / driver.tenant / name, data)
    logger.info(
        "shadow_job dataset=%s competencia=%s accepted=%s metrics=%d",
        job.dataset, job.competencia, report.accepted, len(report.comparisons),
    )
    return Covered(
        job.dataset, job.competencia, report.accepted, name, sha256_hex(data), outputs
    )


def _execute_job(job: _Job) -> Outcome:
    try:
        return _attempt(job)
    except ShadowRunError as error:
        message = str(error)
    except Exception as error:
        logger.exception("shadow_job_unexpected dataset=%s", job.dataset)
        message = f"unexpected_error type={type(error).__name__}"
    logger.error("shadow_job_failed dataset=%s error=%s", job.dataset, message)
    return Failure(job.dataset, job.competencia, message)


def _git(*args: str) -> str:
    git = shutil.which("git")
    if git is None:
        raise ShadowRunError("source_unidentified reason=git_missing")
    command = [git, "--no-optional-locks", *args]
    completed = subprocess.run(command, capture_output=True, text=True, check=False, cwd=_ROOT)
    if completed.returncode != 0:
        raise ShadowRunError(f"source_unidentified reason=git_failed command={args[0]}")
    return completed.stdout


def _source_commit() -> str:
    commit = _git("rev-parse", "--verify", "HEAD").strip()
    changes = _git("status", "--porcelain", "--untracked-files=normal").splitlines()
    if changes:
        raise ShadowRunError(f"source_tree_dirty entries={len(changes)}")
    return commit


def _write_aggregate(settings: _Settings, request: Request, outcomes: list[Outcome]) -> bool:
    stamp = Stamp(settings.tenant, settings.contract_sha256, settings.source_commit)
    payload = build_aggregate(stamp, settings.contract, request, outcomes)
    write_report(settings.report_root / settings.tenant / _AGGREGATE, aggregate_bytes(payload))
    return aggregate_accepted(outcomes)


def main(argv: list[str] | None = None) -> int:
    """Roda a orquestracao real por dataset; 0 somente se todos os relatorios forem aceitos."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv)
    try:
        commit = _source_commit()
        data = cast("Path", args.contract).read_bytes()
        settings = _Settings(
            parse_contract(data), sha256_hex(data), commit, args.tenant, args.legacy_root,
            args.candidate_root, args.report_root,
        )
        jobs = _plan_jobs(args, settings)
        _preflight(settings, jobs)
    except (ContractInvalid, ShadowRunError, OSError) as error:
        logger.error("shadow_precondition_failed error=%s", error)
        return 1
    outcomes = [_execute_job(job) for job in jobs]
    request = Request(tuple(sorted(set(args.source))), args.from_competencia, args.to_competencia)
    try:
        accepted = _write_aggregate(settings, request, outcomes)
    except ShadowRunError as error:
        logger.error("shadow_aggregate_failed error=%s", error)
        return 1
    logger.info("shadow_finished accepted=%s jobs=%d", accepted, len(jobs))
    return 0 if accepted else 1


if __name__ == "__main__":
    sys.exit(main())
