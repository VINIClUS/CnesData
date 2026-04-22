"""Tests for scripts/flag_quality_violation.py."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@patch("subprocess.run")
def test_aplica_label_em_pr_context(mock_run, monkeypatch):
    from scripts.flag_quality_violation import flag

    mock_run.return_value = MagicMock(returncode=0)
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("PR_NUMBER", "42")

    with pytest.raises(SystemExit) as exc:
        flag(kind="n+1", details="count=20 limit=15")

    assert exc.value.code == 1
    call_args = mock_run.call_args_list
    assert any("--add-label" in str(c) and "needs-quality-review" in str(c) for c in call_args)


@patch("subprocess.run")
def test_memleak_soft_retorna_zero(mock_run, monkeypatch):
    from scripts.flag_quality_violation import flag

    mock_run.return_value = MagicMock(returncode=0)
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("PR_NUMBER", "42")

    with pytest.raises(SystemExit) as exc:
        flag(kind="memleak-soft", details="info only")
    assert exc.value.code == 0


@patch("subprocess.run")
def test_escreve_step_summary(mock_run, monkeypatch, tmp_path):
    from scripts.flag_quality_violation import flag

    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("PR_NUMBER", "42")
    mock_run.return_value = MagicMock(returncode=0)

    with pytest.raises(SystemExit):
        flag(kind="chaos", details="test_xyz failed")

    content = summary.read_text()
    assert "chaos" in content
    assert "test_xyz failed" in content


def test_no_gh_token_still_exits_on_violation(monkeypatch):
    from scripts.flag_quality_violation import flag

    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("PR_NUMBER", raising=False)

    with pytest.raises(SystemExit) as exc:
        flag(kind="race", details="double_claim")
    assert exc.value.code == 1


def test_kind_mapping_negative_aplica_security_review(monkeypatch):
    from scripts.flag_quality_violation import LABEL_MAP

    assert LABEL_MAP["negative"] == "needs-security-review"
    assert LABEL_MAP["chaos"] == "needs-chaos-review"
    assert LABEL_MAP["n+1"] == "needs-quality-review"
