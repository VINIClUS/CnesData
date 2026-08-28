"""Executa e registra a matriz de baseline do monorepo."""

import argparse
import json
import shlex
import shutil
import subprocess
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from re import compile as compile_pattern

APPROVED_SHA = "8a2d3a4cbce85e87e31dc4769aab2a7a2b4bf7b2"
WAIVED_SUITES = frozenset({"python-fast", "python-integration-docker"})
WAIVER = {
    "failure": (
        "ModuleNotFoundError: No module named "
        "'cnes_infra.storage.repositories.estabelecimento_repo' during collection of "
        "tests/perf/{macro,micro,soak,spike,stress}"
    ),
    "expected_exit_code": 2,
    "approved_by": "VINIClUS",
    "approved_at": "2026-08-26",
}
WAIVER_FINGERPRINT = "b4b22c61c271c1cf5c4e129644b21a774960901fc3fee3be2bd837f502ae623f"
COLLECTION_HEADER = compile_pattern(
    r"(?m)^_*[ \t]*ERROR collecting "
    r"(?P<path>tests/[\w./-]*test_[\w-]+\.py)(?:[ \t]+_+)?[ \t]*$"
)
MISSING_MODULE_LINE = compile_pattern(
    r"(?m)^E[ \t]+(?P<exception>[A-Za-z_][\w]*): "
    r"No module named ['\"](?P<module>[^'\"]+)['\"]"
)

Command = tuple[str, ...]
Suite = tuple[str, Command]

PYTHON_FAST_MARKERS = (
    "not integration and not postgres and not bigquery and not e2e and not stress and "
    "not soak and not spike and not windows_only and not chaos_infra"
)
PYTHON_PACKAGE_MARKERS = "not bigquery and not e2e and not stress and not soak and not spike"
PYTHON_APP_MARKERS = (
    "not integration and not bigquery and not e2e and not stress and not soak and not spike "
    "and not windows_only"
)
GO_SCRIPT = """cd apps/dump_agent_go &&
go test -race -count=1 -coverprofile=coverage.out ./... &&
grep -v -E 'internal/apiclient/generated\\.go|cmd/|internal/service/|_windows\\.go:' \
coverage.out > coverage.filtered.out &&
total=$(go tool cover -func=coverage.filtered.out | tail -1 | awk '{print $3}' | tr -d '%') &&
go tool cover -func=coverage.filtered.out | tail -1 &&
awk -v total="$total" 'BEGIN { exit !(total >= 65) }'"""
DASHBOARD_SCRIPT = """cd apps/web_dashboard &&
bun run codegen &&
bun run lint &&
bun run typecheck &&
bun run test --coverage &&
bun run build"""
INTEGRATION_SCRIPT = (
    "docker compose -p cnesdata up -d --wait postgres && "
    'uv run pytest packages/ -m "postgres and not e2e" -q && '
    'uv run pytest apps/ -m "postgres and not e2e" -q'
)

SUITES: tuple[Suite, ...] = (
    ("python-fast", ("uv", "run", "pytest", "-m", PYTHON_FAST_MARKERS, "-q")),
    (
        "python-packages-coverage",
        (
            "uv",
            "run",
            "pytest",
            "packages/cnes_domain",
            "packages/cnes_infra",
            "-m",
            PYTHON_PACKAGE_MARKERS,
            "--cov",
            "--cov-config=pyproject.toml",
            "--cov-report=term-missing",
        ),
    ),
    (
        "python-apps-coverage",
        (
            "uv",
            "run",
            "pytest",
            "apps/",
            "-m",
            PYTHON_APP_MARKERS,
            "--cov",
            "--cov-config=.coveragerc",
            "--cov-report=term-missing",
        ),
    ),
    ("go-race-coverage", ("sh", "-c", GO_SCRIPT)),
    ("dashboard", ("sh", "-c", DASHBOARD_SCRIPT)),
    ("python-integration-docker", ("sh", "-c", INTEGRATION_SCRIPT)),
)


@dataclass(frozen=True)
class SuiteResult:
    name: str
    command: str
    exit_code: int
    duration_seconds: float
    failure_fingerprint: str | None = field(default=None, kw_only=True)


def _collection_failures(output: str) -> list[tuple[str, str, str]] | None:
    headers = list(COLLECTION_HEADER.finditer(output))
    if not headers:
        return None
    failures = []
    for index, header in enumerate(headers):
        block_end = headers[index + 1].start() if index + 1 < len(headers) else len(output)
        signatures = list(MISSING_MODULE_LINE.finditer(output, header.end(), block_end))
        if len(signatures) != 1:
            return None
        signature = signatures[0]
        failures.append(
            (
                header.group("path"),
                signature.group("exception"),
                signature.group("module"),
            )
        )
    return failures


def _canonical_failure_document(output: str) -> bytes | None:
    failures = _collection_failures(output)
    if failures is None:
        return None
    signatures = {(exception, module) for _, exception, module in failures}
    if len(signatures) != 1:
        return None
    exception, module = signatures.pop()
    document = {
        "exception": exception,
        "module": module,
        "affected_paths": sorted({path for path, _, _ in failures}),
    }
    return json.dumps(document, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _failure_fingerprint(output: str) -> str | None:
    document = _canonical_failure_document(output)
    return sha256(document).hexdigest() if document is not None else None


def run_suite(name: str, command: Sequence[str]) -> SuiteResult:
    formatted_command = shlex.join(command)
    started_at = time.perf_counter()
    print(f"suite_start name={name} command={formatted_command}", flush=True)
    try:
        completed = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output = completed.stdout
        exit_code = completed.returncode
    except OSError as error:
        output = f"{type(error).__name__}: {error}"
        exit_code = 127
    duration = time.perf_counter() - started_at
    if output:
        print(output, end="" if output.endswith("\n") else "\n", flush=True)
    print(f"suite_end name={name} exit_code={exit_code} duration_seconds={duration:.3f}")
    return SuiteResult(
        name,
        formatted_command,
        exit_code,
        round(duration, 3),
        failure_fingerprint=_failure_fingerprint(output),
    )


def run_suites(suites: Sequence[Suite]) -> list[SuiteResult]:
    return [run_suite(name, command) for name, command in suites]


def _waiver_for(result: SuiteResult, commit_sha: str) -> dict[str, object] | None:
    if (
        commit_sha == APPROVED_SHA
        and result.name in WAIVED_SUITES
        and result.exit_code == WAIVER["expected_exit_code"]
        and result.failure_fingerprint == WAIVER_FINGERPRINT
    ):
        return dict(WAIVER)
    return None


def write_report(path: Path, results: Sequence[SuiteResult], commit_sha: str) -> None:
    suites = []
    for result in results:
        suite = asdict(result)
        if result.failure_fingerprint is None:
            del suite["failure_fingerprint"]
        waiver = _waiver_for(result, commit_sha)
        if waiver is not None:
            suite["waiver"] = waiver
        suites.append(suite)
    payload = {
        "commit_sha": commit_sha,
        "recorded_at": datetime.now(UTC).isoformat(),
        "suites": suites,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def matrix_exit_code(results: Sequence[SuiteResult], commit_sha: str) -> int:
    accepted = all(result.exit_code == 0 or _waiver_for(result, commit_sha) for result in results)
    return 0 if accepted else 1


def _commit_sha() -> str:
    git = shutil.which("git")
    if git is None:
        raise FileNotFoundError("executable=git")
    completed = subprocess.run(
        (git, "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    commit_sha = _commit_sha()
    results = run_suites(SUITES)
    write_report(args.output, results, commit_sha)
    return matrix_exit_code(results, commit_sha)


if __name__ == "__main__":
    raise SystemExit(main())
