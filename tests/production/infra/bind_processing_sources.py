"""Gera o vínculo PROCESSING_SOURCE_BINDINGS consumido pelos testes de processamento."""

import json
import sys
from hashlib import sha256
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOTS = (
    ("apps/data_processor/src", "data_processor"),
    ("packages/cnes_contracts/src", "cnes_contracts"),
    ("packages/cnes_domain/src", "cnes_domain"),
    ("packages/cnes_infra/src", "cnes_infra"),
)


def build_bindings(repo_root: Path) -> dict[str, object]:
    """Vincula raízes na ordem dos namespaces e o sha256 de cada fonte Python.

    Args:
        repo_root: raiz do repositório cujas fontes serão vinculadas.
    Returns:
        ``{"roots": [...], "files": {caminho absoluto: sha256}}``.
    """
    roots = [(repo_root / relative).resolve() for relative, _ in SOURCE_ROOTS]
    files: dict[str, str] = {}
    for root, (_, name) in zip(roots, SOURCE_ROOTS, strict=True):
        package = root / name
        if not package.is_dir():
            raise SystemExit(f"processing_source_root=missing path={package}")
        files |= {
            str(path.resolve()): sha256(path.read_bytes()).hexdigest()
            for path in sorted(package.rglob("*.py"))
        }
    return {"roots": [str(root) for root in roots], "files": files}


def main(argv: list[str]) -> None:
    if len(argv) != 1:
        raise SystemExit("usage=bind_processing_sources.py <output.json>")
    binding = build_bindings(REPO_ROOT)
    Path(argv[0]).write_text(json.dumps(binding), encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv[1:])
