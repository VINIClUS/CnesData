"""Test SIA DBF fixture generator."""
from __future__ import annotations

from typing import TYPE_CHECKING

from scripts.gen_sia_dbf_fixtures import generate_all

if TYPE_CHECKING:
    from pathlib import Path


class TestGenSiaDbf:
    def test_gera_cinco_dbfs(self, tmp_path: Path) -> None:
        generate_all(tmp_path, seed=42)
        names = {p.name for p in tmp_path.glob("*.DBF")}
        assert names == {"S_APA.DBF", "S_BPI.DBF", "S_BPIHST.DBF",
                         "S_CDN.DBF", "CADMUN.DBF"}

    def test_s_apa_tem_registros(self, tmp_path: Path) -> None:
        from dbfread import DBF
        generate_all(tmp_path, seed=42)
        d = DBF(str(tmp_path / "S_APA.DBF"), encoding="cp1252")
        records = list(d)
        assert len(records) >= 3

    def test_determinismo_mesmo_seed(self, tmp_path: Path) -> None:
        from dbfread import DBF
        generate_all(tmp_path / "a", seed=42)
        generate_all(tmp_path / "b", seed=42)
        a = list(DBF(str(tmp_path / "a" / "S_BPI.DBF"), encoding="cp1252"))
        b = list(DBF(str(tmp_path / "b" / "S_BPI.DBF"), encoding="cp1252"))
        assert a == b
