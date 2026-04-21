"""Testes do bench_gate."""
from scripts.bench_gate import BenchResult, check_regression, parse_benchstat


def test_parse_benchstat_dois_cols() -> None:
    sample = """name         old time/op    new time/op    delta
ExtractCnes    120ns ± 2%     110ns ± 1%    -8.33%  (p=0.001 n=10)
ParquetGzip    500µs ± 5%     600µs ± 4%   +20.00%  (p=0.001 n=10)
"""
    results = parse_benchstat(sample)
    assert len(results) == 2
    assert results[0].name == "ExtractCnes"
    assert results[0].delta_pct == -8.33
    assert results[1].name == "ParquetGzip"
    assert results[1].delta_pct == 20.00


def test_check_regression_fail_acima_20() -> None:
    r = [BenchResult("X", -5.0, 0.01), BenchResult("Y", 20.5, 0.01)]
    failures = check_regression(r, max_pct=20.0)
    assert len(failures) == 1
    assert failures[0].name == "Y"


def test_check_regression_pass_se_nao_significativo() -> None:
    r = [BenchResult("X", 25.0, 0.20)]
    failures = check_regression(r, max_pct=20.0)
    assert len(failures) == 0
