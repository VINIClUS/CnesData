"""Baselines toolchain, policy e cost de S para os gates de entrega (P-B)."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import subprocess
from pathlib import Path
from shutil import which
from typing import TYPE_CHECKING, Any

from release.verify_candidate import require
from scripts.promotion._canonical import DOCUMENT_LIMIT, canonical_bytes, decode_json

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["DOCUMENT_LIMIT", "canonical_bytes", "decode_json"]

TOFU_VERSION = "1.13.2"
CONFTEST_VERSION = "0.71.1"
LOCK_PATH = "infra/opentofu/env/prod/.terraform.lock.hcl"
POLICY_PATH = "policies/cnesdata.rego"
COST_PATH = "infra/opentofu/modules/observability/cost-manifest.json"
TARGET_ROOT = "infra/opentofu"
ARTIFACTS = {
    "delivery-baseline-toolchain": ("toolchain.json",),
    "delivery-baseline-policy": ("policy.json",),
    "delivery-baseline-cost": ("cost-manifest-source.json", "cost-context.json"),
}
COMMIT = re.compile(r"[0-9a-f]{40}")
PROVIDER = re.compile(r'^provider "([^"]+)" \{\n(.*?)^\}$', re.DOTALL | re.MULTILINE)
VERSION = re.compile(r'^\s*version\s*=\s*"([^"]+)"', re.MULTILINE)
ZH = re.compile(r'"zh:([0-9a-f]{64})"')
CONFTEST_BANNER = re.compile(r"^Conftest:\s*(\S+)", re.MULTILINE)
PRIVATE_KEYS = frozenset({"password", "secret", "token", "access_key_id", "secret_access_key",
                          "state_json", "terraform_state"})
PRIVATE_TEXT = re.compile(
    r"arn:aws(?:-[a-z]+)?:|://|BEGIN (?:RSA |EC )?PRIVATE KEY|(?:AKIA|ASIA)[A-Z0-9]{16}"
    r"|(?:vpc|subnet|sg|eni|i)-[0-9a-f]{8,17}"
)
MODES = frozenset({"100644", "100755"})
FAILURES = (ValueError, OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired)

logger = logging.getLogger(__name__)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _git(checkout: Path, *args: str, stdin: bytes | None = None) -> bytes:
    return subprocess.run(
        [which("git") or "git", *args],
        cwd=checkout,
        input=stdin,
        capture_output=True,
        check=True,
        timeout=120,
    ).stdout


def _at(checkout: Path, source_sha: str, path: str) -> bytes:
    return _git(checkout, "cat-file", "blob", f"{source_sha}:{path}")


def _public_safe(value: Any) -> None:
    if type(value) is dict:
        for key, child in value.items():
            require(key.lower() not in PRIVATE_KEYS, "baselines=public_safe")
            _public_safe(child)
    elif type(value) is list:
        for child in value:
            _public_safe(child)
    elif type(value) is str:
        require(len(value) <= 4096 and not PRIVATE_TEXT.search(value), "baselines=public_safe")


def documento(value: Any) -> bytes:
    """Args: valor JSON do baseline.

    Returns: bytes canônicos, públicos e de no máximo 256 KiB.
    Raises: ValueError para material privado ou acima do limite.
    """
    _public_safe(value)
    return canonical_bytes(value, DOCUMENT_LIMIT)


def parse_lock(lock: bytes) -> dict[str, tuple[str, frozenset[str]]]:
    """Returns: por fonte do provedor, a versão do lock e o conjunto de hashes zh."""
    providers = {}
    for source, body in PROVIDER.findall(lock.decode("utf-8")):
        version = VERSION.search(body)
        require(version is not None, f"toolchain=lock_sem_versao source={source}")
        providers[source] = (version.group(1) if version else "", frozenset(ZH.findall(body)))
    return providers


def _version(binary: Path, args: list[str]) -> str:
    result = subprocess.run(
        [str(binary), *args], capture_output=True, text=True, check=True, timeout=30,
    )
    return result.stdout


def _tofu_version(binary: Path) -> str | None:
    try:
        return json.loads(_version(binary, ["version", "-json"])).get("terraform_version")
    except (ValueError, AttributeError):
        return None


def _packages(mirror: Path, providers: dict[str, tuple[str, frozenset[str]]]) -> dict[str, bytes]:
    expected = {
        f"{source}/terraform-provider-{source.rsplit('/', 1)[1]}_{version}_linux_amd64.zip": source
        for source, (version, _) in providers.items()
    }
    found = {path.relative_to(mirror).as_posix() for path in mirror.rglob("*.zip")}
    extra = sorted(found - set(expected))
    require(not extra, f"toolchain=zip_extra path={extra[0] if extra else ''}")
    packages = {}
    for name, source in expected.items():
        require(name in found, f"toolchain=zip_ausente source={source}")
        packages[source] = (mirror / name).read_bytes()
    return packages


def build_toolchain(lock: bytes, mirror: Path, tofu: Path) -> bytes:
    """Args: bytes do lock, espelho linux_amd64 de `tofu providers mirror` e binário do tofu.

    Returns: toolchain.json canônico `{opentofu, providers, lock_sha256}`.
    Raises: ValueError se o zip não estiver no conjunto zh do lock ou a versão divergir.
    """
    providers = parse_lock(lock)
    require(bool(providers), "toolchain=lock_sem_provedor")
    found = _tofu_version(tofu)
    require(found == TOFU_VERSION, f"toolchain=tofu_version found={found}")
    packages = _packages(mirror, providers)
    selections = []
    for source in sorted(providers):
        version, zh = providers[source]
        package = _sha(packages[source])
        require(package in zh, f"toolchain=zh_ausente source={source}")
        selections.append({"source": source, "version": version, "package_sha256": package})
    return documento({
        "opentofu": {"version": TOFU_VERSION, "binary_sha256": _sha(tofu.read_bytes())},
        "providers": selections,
        "lock_sha256": _sha(lock),
    })


def build_policy(policy: bytes, conftest: Path) -> bytes:
    """Args: bytes de policies/cnesdata.rego e binário do conftest.

    Returns: policy.json canônico `{policy_sha256, conftest_version, conftest_sha256}`.
    Raises: ValueError se o conftest não for a versão fixada.
    """
    banner = CONFTEST_BANNER.search(_version(conftest, ["--version"]))
    found = banner.group(1) if banner else None
    require(found == CONFTEST_VERSION, f"policy=conftest_version found={found}")
    return documento({
        "policy_sha256": _sha(policy),
        "conftest_version": CONFTEST_VERSION,
        "conftest_sha256": _sha(conftest.read_bytes()),
    })


def _target_files(checkout: Path, source_sha: str) -> dict[str, str]:
    tree = _git(checkout, "ls-tree", "-r", "-z", source_sha, TARGET_ROOT)
    records = [item.split(b"\t", 1) for item in tree.split(b"\0") if item]
    names, oids = [], []
    for metadata, encoded in records:
        mode, kind, oid = metadata.decode("ascii").split(" ")
        name = encoded.decode("utf-8")
        require(mode in MODES and kind == "blob", f"baselines=entry path={name} mode={mode}")
        names.append(name)
        oids.append(oid)
    stream = _git(checkout, "cat-file", "--batch",
                  stdin="".join(f"{oid}\n" for oid in oids).encode("ascii"))
    files, offset = {}, 0
    for name in names:
        end = stream.index(b"\n", offset)
        size = int(stream[offset:end].split(b" ")[2])
        files[name] = _sha(stream[end + 1:end + 1 + size])
        offset = end + size + 2
    return files


def build_cost_context(checkout: Path, source_sha: str, cost_source: bytes) -> bytes:
    """Args: checkout git em S, o SHA completo S e os bytes do cost-manifest do CD.

    Returns: cost-context.json canônico no estágio pre-activation, sem CloudFront.
    Raises: ValueError se S não for a cabeça do checkout.
    """
    head = _git(checkout, "rev-parse", "--verify", "HEAD^{commit}").decode("ascii").strip()
    require(bool(COMMIT.fullmatch(source_sha)) and head == source_sha, "baselines=checkout_head")
    target = documento({"files": _target_files(checkout, source_sha)})
    return documento({
        "stage": "pre-activation",
        "source_sha": source_sha,
        "checkpoint_sha256": _sha(cost_source),
        "target_sha256": _sha(target),
        "unbound_required": True,
        "cloudfront": None,
    })


def _cost_source(checkout: Path, source_sha: str) -> bytes:
    raw = _at(checkout, source_sha, COST_PATH)
    require(len(raw) <= DOCUMENT_LIMIT, "baselines=cost_source_size")
    try:
        require(type(json.loads(raw)) is dict, "baselines=cost_source_json")
    except json.JSONDecodeError as error:
        raise ValueError("baselines=cost_source_json") from error
    return raw


def build_all(
    checkout: Path, source_sha: str, mirror: Path, binaries: tuple[Path, Path],
) -> dict[str, dict[str, bytes]]:
    """Args: checkout git, S, espelho de provedores e (binário do tofu, binário do conftest).

    Returns: por nome de artefato, os membros exatos como bytes.
    Raises: ValueError em qualquer recusa; nada é gravado.
    """
    tofu, conftest = binaries
    cost = _cost_source(checkout, source_sha)
    context = build_cost_context(checkout, source_sha, cost)
    return {
        "delivery-baseline-toolchain": {
            "toolchain.json": build_toolchain(_at(checkout, source_sha, LOCK_PATH), mirror, tofu),
        },
        "delivery-baseline-policy": {
            "policy.json": build_policy(_at(checkout, source_sha, POLICY_PATH), conftest),
        },
        "delivery-baseline-cost": {"cost-manifest-source.json": cost, "cost-context.json": context},
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gera os baselines de entrega em S.")
    parser.add_argument("--checkout", type=Path, default=Path.cwd())
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--mirror-dir", required=True, type=Path)
    parser.add_argument("--tofu-binary", required=True, type=Path)
    parser.add_argument("--conftest-binary", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser


def _write(output: Path, artifacts: dict[str, dict[str, bytes]]) -> None:
    require(not output.exists(), "baselines=output_exists")
    for name, members in artifacts.items():
        (output / name).mkdir(parents=True)
        for member, data in members.items():
            (output / name / member).write_bytes(data)


def main(argv: Sequence[str] | None = None) -> int:
    """Returns: 0 com os três artefatos gravados; 2 em qualquer recusa, sem gravar a saída."""
    arguments = _parser().parse_args(argv)
    try:
        artifacts = build_all(arguments.checkout, arguments.source_sha, arguments.mirror_dir,
                              (arguments.tofu_binary, arguments.conftest_binary))
        _write(arguments.output_dir, artifacts)
    except FAILURES as error:
        logger.error("delivery_baselines=failed reason=%s", error)
        return 2
    for name, members in artifacts.items():
        logger.info("delivery_baselines=written artifact=%s members=%d bytes=%d", name,
                    len(members), sum(len(data) for data in members.values()))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(main())
