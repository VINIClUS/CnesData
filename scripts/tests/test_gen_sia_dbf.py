"""Test SIA DBF fixture generator."""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from dbfread import DBF

from scripts.gen_sia_dbf_fixtures import generate_all

_DICTIONARY = Path(__file__).resolve().parents[2] / "docs" / "data-dictionary-sia.md"
_ROW = re.compile(r"^\| (\w+) \| (\w)\(\d+(?:,\d+)?\) \| (\d+) \| (\d+) \|$")
_ARQUIVOS = {"S_APA.DBF", "S_PRD.DBF", "S_BPI.DBF", "S_BPIHST.DBF", "S_PA.DBF", "CADMUN.DBF"}


def _layout_do_dicionario(name: str) -> list[tuple[str, str, int, int]]:
    lines = _DICTIONARY.read_text(encoding="utf-8").splitlines()
    start = lines.index(f"### `{name}`")
    fields = []
    for line in lines[start + 1:]:
        if line.startswith("### "):
            break
        match = _ROW.match(line)
        if match:
            fields.append((match[1], match[2], int(match[3]), int(match[4])))
    return fields


class TestGenSiaDbf:
    def test_gera_os_dbfs_dos_subtipos_sia(self, tmp_path: Path) -> None:
        generate_all(tmp_path, seed=42)
        assert {p.name for p in tmp_path.glob("*.DBF")} == _ARQUIVOS

    @pytest.mark.parametrize("name", sorted(_ARQUIVOS))
    def test_layout_igual_ao_dbf_real(self, tmp_path: Path, name: str) -> None:
        generate_all(tmp_path, seed=42)
        dbf = DBF(str(tmp_path / name), encoding="cp1252")
        got = [(f.name, f.type, f.length, f.decimal_count) for f in dbf.fields]
        assert got == _layout_do_dicionario(name)

    def test_s_prd_tem_linhas_apac_e_bpa(self, tmp_path: Path) -> None:
        generate_all(tmp_path, seed=42)
        rows = list(DBF(str(tmp_path / "S_PRD.DBF"), encoding="cp1252"))
        assert any(r["PRD_APANUM"].strip() for r in rows)
        assert any(not r["PRD_APANUM"].strip() for r in rows)

    def test_determinismo_mesmo_seed(self, tmp_path: Path) -> None:
        generate_all(tmp_path / "a", seed=42)
        generate_all(tmp_path / "b", seed=42)
        for name in _ARQUIVOS:
            assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()
