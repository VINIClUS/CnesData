"""Tests for processor._op delta routing."""
from __future__ import annotations

from unittest.mock import MagicMock

import polars as pl
import pytest

from data_processor.cdc_merger import FatalError
from data_processor.processor import maybe_route_delta


def test_maybe_route_delta_no_op_returns_none(monkeypatch):
    monkeypatch.setenv("DELTA_MODE", "true")
    df = pl.DataFrame({"CNES": ["1"]})
    assert maybe_route_delta(df, MagicMock(), "cnes", "estabelecimentos") is None


def test_maybe_route_delta_op_present_delta_off_raises(monkeypatch):
    monkeypatch.setenv("DELTA_MODE", "false")
    df = pl.DataFrame({"CNES": ["1"], "_op": ["I"]})
    with pytest.raises(FatalError, match="delta_mode_required"):
        maybe_route_delta(df, MagicMock(), "cnes", "estabelecimentos")


def test_maybe_route_delta_op_present_delta_on_returns_counts(monkeypatch):
    monkeypatch.setenv("DELTA_MODE", "true")
    df = pl.DataFrame({"CNES": ["1"], "_op": ["I"]})
    result = maybe_route_delta(df, MagicMock(), "cnes", "estabelecimentos")
    assert result is not None
    assert result["inserts"] == 1
    assert result["applied"] == 0


def test_maybe_route_delta_passes_callback(monkeypatch):
    monkeypatch.setenv("DELTA_MODE", "true")
    df = pl.DataFrame({"CNES": ["1"], "_op": ["I"]})
    captured: list[pl.DataFrame] = []

    def cb(df_iu: pl.DataFrame) -> int:
        captured.append(df_iu)
        return len(df_iu)

    result = maybe_route_delta(
        df, MagicMock(), "cnes", "estabelecimentos", cb,
    )
    assert result is not None
    assert result["applied"] == 1
    assert len(captured) == 1
    assert "_op" not in captured[0].columns
