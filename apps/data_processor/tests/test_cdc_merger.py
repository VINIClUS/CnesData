"""Tests for cdc_merger module — _op routing + DELETE inline."""
from __future__ import annotations

from unittest.mock import MagicMock

import polars as pl
import pytest

from data_processor.cdc_merger import (
    FatalError,
    has_op_column,
    is_delta_mode_enabled,
    merge_delta,
)


def test_has_op_column_true():
    df = pl.DataFrame({"CNES": ["1"], "_op": ["I"]})
    assert has_op_column(df) is True


def test_has_op_column_false():
    df = pl.DataFrame({"CNES": ["1"]})
    assert has_op_column(df) is False


def test_is_delta_mode_enabled_default(monkeypatch):
    monkeypatch.delenv("DELTA_MODE", raising=False)
    assert is_delta_mode_enabled() is True


def test_is_delta_mode_enabled_true(monkeypatch):
    monkeypatch.setenv("DELTA_MODE", "true")
    assert is_delta_mode_enabled() is True


def test_is_delta_mode_enabled_false(monkeypatch):
    monkeypatch.setenv("DELTA_MODE", "false")
    assert is_delta_mode_enabled() is False


def test_merge_delta_buckets_iud():
    df = pl.DataFrame({
        "CNES": ["1", "2", "3"],
        "NOME_FANTA": ["A", "B", None],
        "_op": ["I", "U", "D"],
    })
    conn = MagicMock()
    conn.execute.return_value = MagicMock(rowcount=1)
    counts = merge_delta(df, conn, "cnes", "estabelecimentos")
    assert counts == {
        "inserts": 1, "updates": 1, "deletes": 1, "applied": 0,
    }
    conn.execute.assert_called_once()


def test_merge_delta_with_apply_iu_fn_calls_callback():
    df = pl.DataFrame({
        "CNES": ["1", "2"],
        "NOME_FANTA": ["A", "B"],
        "_op": ["I", "U"],
    })
    conn = MagicMock()
    captured: list[pl.DataFrame] = []

    def apply_iu(df_iu: pl.DataFrame) -> int:
        captured.append(df_iu)
        return len(df_iu)

    counts = merge_delta(df, conn, "cnes", "estabelecimentos", apply_iu)
    assert counts == {
        "inserts": 1, "updates": 1, "deletes": 0, "applied": 2,
    }
    assert len(captured) == 1
    assert "_op" not in captured[0].columns
    assert len(captured[0]) == 2


def test_merge_delta_no_callback_skips_apply():
    df = pl.DataFrame({"CNES": ["1"], "_op": ["I"]})
    conn = MagicMock()
    counts = merge_delta(df, conn, "cnes", "estabelecimentos")
    assert counts["applied"] == 0
    assert counts["inserts"] == 1


def test_merge_delta_callback_only_deletes_applied_zero():
    df = pl.DataFrame({"CNES": ["1"], "_op": ["D"]})
    conn = MagicMock()
    conn.execute.return_value = MagicMock(rowcount=1)
    captured: list[pl.DataFrame] = []

    def apply_iu(df_iu: pl.DataFrame) -> int:
        captured.append(df_iu)
        return len(df_iu)

    counts = merge_delta(df, conn, "cnes", "estabelecimentos", apply_iu)
    assert counts["applied"] == 0
    assert counts["deletes"] == 1
    assert captured == []


def test_merge_delta_unknown_op_raises():
    df = pl.DataFrame({"CNES": ["1"], "_op": ["X"]})
    conn = MagicMock()
    with pytest.raises(FatalError, match="unknown_op"):
        merge_delta(df, conn, "cnes", "estabelecimentos")


def test_merge_delta_unknown_source_intent_raises():
    df = pl.DataFrame({"CNES": ["1"], "_op": ["D"]})
    conn = MagicMock()
    with pytest.raises(FatalError, match="unknown_source_intent"):
        merge_delta(df, conn, "xyz", "abc")


def test_merge_delta_delete_no_op_logs(caplog):
    df = pl.DataFrame({"CNES": ["404"], "_op": ["D"]})
    conn = MagicMock()
    conn.execute.return_value = MagicMock(rowcount=0)
    with caplog.at_level("INFO"):
        counts = merge_delta(df, conn, "cnes", "estabelecimentos")
    assert counts["deletes"] == 0
    assert any("delete_no_op" in r.message for r in caplog.records)
