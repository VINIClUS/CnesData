"""Consolida multiplos docker-compose em um so via profiles.

Uso:
    python scripts/consolidate_compose.py \\
        --input docker-compose.yml docker-compose.perf.yml \\
        --profile docker-compose.yml=dev \\
        --profile docker-compose.perf.yml=perf \\
        --output docker-compose.yml
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml


def consolidate(
    inputs: list[Path], output: Path, *, profiles: dict[str, str],
) -> None:
    merged: dict = {"services": {}, "volumes": {}, "networks": {}}
    for path in inputs:
        data = yaml.safe_load(path.read_text()) or {}
        profile = profiles.get(path.name)
        for name, svc in (data.get("services") or {}).items():
            if profile:
                svc = dict(svc)
                svc["profiles"] = [profile]
            merged["services"][name] = svc
        for name, vol in (data.get("volumes") or {}).items():
            merged["volumes"][name] = vol
        for name, net in (data.get("networks") or {}).items():
            merged["networks"][name] = net

    if not merged["volumes"]:
        del merged["volumes"]
    if not merged["networks"]:
        del merged["networks"]

    output.write_text(yaml.safe_dump(merged, sort_keys=False))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", nargs="+", type=Path, required=True)
    parser.add_argument("--profile", action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    profiles = dict(p.split("=", 1) for p in args.profile)
    consolidate(args.input, args.output, profiles=profiles)
    return 0


if __name__ == "__main__":
    sys.exit(main())
