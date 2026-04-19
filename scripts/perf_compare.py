"""Compara pytest-benchmark output com baseline versionado."""
import argparse
import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_TOLERANCIA = 0.20


def _indexar(benchmarks: list[dict]) -> dict[str, float]:
    return {b["fullname"]: b["stats"]["mean"] for b in benchmarks}


def _comparar(atual: dict[str, float], baseline: dict[str, float]) -> int:
    regressoes = 0
    for nome, mean_atual in atual.items():
        mean_base = baseline.get(nome)
        if mean_base is None:
            logger.info("new_benchmark name=%s mean=%.6fs", nome, mean_atual)
            continue
        piora = (mean_atual - mean_base) / mean_base
        if piora > _TOLERANCIA:
            logger.error(
                "perf_regression name=%s mean=%.6fs baseline=%.6fs piora=%.1f%%",
                nome, mean_atual, mean_base, piora * 100,
            )
            regressoes += 1
        else:
            logger.info(
                "perf_ok name=%s mean=%.6fs baseline=%.6fs delta=%.1f%%",
                nome, mean_atual, mean_base, piora * 100,
            )
    return regressoes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", required=True)
    parser.add_argument("--baseline", required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    baseline_path = Path(args.baseline)
    if not baseline_path.exists():
        logger.warning("baseline_missing path=%s bootstrap_ok", baseline_path)
        return 0

    try:
        current = json.loads(Path(args.current).read_text(encoding="utf-8"))
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        logger.error(
            "baseline_invalid_json err=%s regenerar_com=--benchmark-save", e,
        )
        return 2

    cur = _indexar(current.get("benchmarks", []))
    base = _indexar(baseline.get("benchmarks", []))

    regressoes = _comparar(cur, base)
    if regressoes > 0:
        logger.error("perf_fail count=%d", regressoes)
        return 1
    logger.info("perf_pass count=%d", len(cur))
    return 0


if __name__ == "__main__":
    sys.exit(main())
