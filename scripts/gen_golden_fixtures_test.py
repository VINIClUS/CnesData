"""Teste do gerador de fixtures golden."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl

from scripts.gen_golden_fixtures import capture_intent, write_fixture


def test_write_fixture_cria_parquet_e_meta(tmp_path: Path) -> None:
    df = pl.DataFrame({"cnes": ["0001"], "nome": ["UBS"]})
    write_fixture(df, tmp_path, "cnes_estabelecimentos", query="SELECT *", params=("354130",))

    parquet_path = tmp_path / "cnes_estabelecimentos.parquet"
    meta_path = tmp_path / "cnes_estabelecimentos.meta.json"

    assert parquet_path.exists()
    assert meta_path.exists()

    roundtrip = pl.read_parquet(parquet_path)
    assert roundtrip.equals(df)


def test_capture_intent_chama_extractor_mockado() -> None:
    fake_extractor = MagicMock(return_value=pl.DataFrame({"x": [1]}))

    with patch("scripts.gen_golden_fixtures._resolve_extractor", return_value=fake_extractor):
        df = capture_intent("cnes_estabelecimentos", cod_municipio="354130")

    assert fake_extractor.called
    assert df.height == 1
