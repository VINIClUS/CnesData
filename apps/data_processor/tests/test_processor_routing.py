"""Tests for processor route_delta — delta is the only execution path."""
from __future__ import annotations

from unittest.mock import MagicMock

import polars as pl
import pytest

from data_processor.processor import route_delta


def test_route_delta_missing_op_raises():
    df = pl.DataFrame({"CNES": ["1"]})
    with pytest.raises(ValueError, match="missing_op_column"):
        route_delta(df, MagicMock(), "cnes", "estabelecimentos")


def test_route_delta_op_present_returns_counts():
    df = pl.DataFrame({"CNES": ["1"], "_op": ["I"]})
    result = route_delta(df, MagicMock(), "cnes", "estabelecimentos")
    assert result is not None
    assert result["inserts"] == 1
    assert result["applied"] == 0


def test_route_delta_passes_callback():
    df = pl.DataFrame({"CNES": ["1"], "_op": ["I"]})
    captured: list[pl.DataFrame] = []

    def cb(df_iu: pl.DataFrame) -> int:
        captured.append(df_iu)
        return len(df_iu)

    result = route_delta(
        df, MagicMock(), "cnes", "estabelecimentos", cb,
    )
    assert result is not None
    assert result["applied"] == 1
    assert len(captured) == 1
    assert "_op" not in captured[0].columns
