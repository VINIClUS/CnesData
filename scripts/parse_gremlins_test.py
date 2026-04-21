"""Teste do parse_gremlins."""
from scripts.parse_gremlins import summarize


def test_summarize_formata_markdown() -> None:
    report = {
        "mutations": 100,
        "killed": 85,
        "lived": 10,
        "timed_out": 5,
        "by_package": {
            "extractor": {"total": 40, "killed": 35},
            "writer": {"total": 30, "killed": 25},
        },
    }
    md = summarize(report)
    assert "Mutation Score: 85/100 (85.00%)" in md
    assert "extractor" in md
    assert "87.50%" in md  # 35/40
