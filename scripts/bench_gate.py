"""Parse benchstat output + fail se regressão p50 > threshold.

Uso:
    benchstat main.bench pr.bench | python scripts/bench_gate.py --max-pct 20
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass


@dataclass
class BenchResult:
    name: str
    delta_pct: float
    p_value: float


_LINE = re.compile(
    r"^(?P<name>\S+)\s+\S+\s+±\s*\S+\s+\S+\s+±\s*\S+\s+"
    r"(?P<delta>[+-]?\d+\.\d+)%\s+\(p=(?P<p>\d+\.\d+)\s+n=\d+\)",
)


def parse_benchstat(text: str) -> list[BenchResult]:
    out: list[BenchResult] = []
    for line in text.splitlines():
        m = _LINE.match(line)
        if not m:
            continue
        out.append(BenchResult(
            name=m.group("name"),
            delta_pct=float(m.group("delta")),
            p_value=float(m.group("p")),
        ))
    return out


def check_regression(
    results: list[BenchResult], *, max_pct: float,
) -> list[BenchResult]:
    return [
        r for r in results
        if r.delta_pct > max_pct and r.p_value <= 0.05
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-pct", type=float, default=20.0)
    args = parser.parse_args()

    text = sys.stdin.read()
    results = parse_benchstat(text)
    failures = check_regression(results, max_pct=args.max_pct)

    for r in results:
        print(f"{r.name}: {r.delta_pct:+.2f}% (p={r.p_value:.3f})")

    if failures:
        print(f"\nREGRESSIONS (>{args.max_pct}%):", file=sys.stderr)
        for r in failures:
            print(f"  - {r.name}: {r.delta_pct:+.2f}%", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
