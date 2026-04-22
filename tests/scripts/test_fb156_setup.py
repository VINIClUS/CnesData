"""Tests for scripts/fb156_setup.py."""
from __future__ import annotations

import hashlib
import io
import zipfile
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path


def _build_fake_zip(tmp_path: Path, include_embed: bool = True) -> Path:
    """Build a fake FB zip (>1MB to pass size gate) with optional fbembed.dll."""
    zip_path = tmp_path / "fake_fb.zip"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        if include_embed:
            zf.writestr("fbembed.dll", b"FAKE_DLL_CONTENT" * 200_000)
        zf.writestr("firebird.msg", b"")
        zf.writestr("IPLicense.txt", b"fake license text")
    zip_path.write_bytes(buf.getvalue())
    return zip_path


def _sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_extrai_para_cache_limpo(tmp_path, monkeypatch):
    """Cache vazio -> extrai zip + cria marker."""
    import scripts.fb156_setup as mod

    fake_zip = _build_fake_zip(tmp_path)
    cache = tmp_path / "cache"
    monkeypatch.setattr(mod, "FIXTURE_ZIP", fake_zip)
    monkeypatch.setattr(mod, "CACHE_DIR", cache)
    monkeypatch.setattr(
        "sys.argv",
        ["fb156_setup.py", "--expected-sha", _sha256_of(fake_zip)],
    )

    mod.main()

    assert (cache / "fbembed.dll").exists()
    assert (cache / "fbclient.dll").exists()
    assert (cache / ".sha256").exists()


def test_skip_se_cache_valido(tmp_path, monkeypatch):
    """Cache valido (SHA bate) -> noop na segunda chamada."""
    import scripts.fb156_setup as mod

    fake_zip = _build_fake_zip(tmp_path)
    cache = tmp_path / "cache"
    monkeypatch.setattr(mod, "FIXTURE_ZIP", fake_zip)
    monkeypatch.setattr(mod, "CACHE_DIR", cache)
    monkeypatch.setattr(
        "sys.argv",
        ["fb156_setup.py", "--expected-sha", _sha256_of(fake_zip)],
    )

    mod.main()
    first_mtime = (cache / "fbembed.dll").stat().st_mtime

    mod.main()
    second_mtime = (cache / "fbembed.dll").stat().st_mtime

    assert first_mtime == second_mtime, "cache should not be touched on idempotent re-run"


def test_reextrai_com_force(tmp_path, monkeypatch):
    """--force reextrai mesmo se cache valido."""
    import time

    import scripts.fb156_setup as mod

    fake_zip = _build_fake_zip(tmp_path)
    cache = tmp_path / "cache"
    expected = _sha256_of(fake_zip)
    monkeypatch.setattr(mod, "FIXTURE_ZIP", fake_zip)
    monkeypatch.setattr(mod, "CACHE_DIR", cache)

    monkeypatch.setattr(
        "sys.argv",
        ["fb156_setup.py", "--expected-sha", expected],
    )
    mod.main()
    first_mtime = (cache / "fbembed.dll").stat().st_mtime

    time.sleep(0.05)

    monkeypatch.setattr(
        "sys.argv",
        ["fb156_setup.py", "--force", "--expected-sha", expected],
    )
    mod.main()
    second_mtime = (cache / "fbembed.dll").stat().st_mtime

    assert second_mtime > first_mtime, "--force must re-extract"


def test_rejeita_sha_mismatch(tmp_path, monkeypatch):
    """--expected-sha errado -> exit 1."""
    import scripts.fb156_setup as mod

    fake_zip = _build_fake_zip(tmp_path)
    cache = tmp_path / "cache"
    monkeypatch.setattr(mod, "FIXTURE_ZIP", fake_zip)
    monkeypatch.setattr(mod, "CACHE_DIR", cache)
    monkeypatch.setattr(
        "sys.argv",
        ["fb156_setup.py", "--expected-sha", "deadbeef" * 8],
    )

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1


def test_cria_alias_fbclient(tmp_path, monkeypatch):
    """fbclient.dll aponta para (ou e copia de) fbembed.dll."""
    import scripts.fb156_setup as mod

    fake_zip = _build_fake_zip(tmp_path)
    cache = tmp_path / "cache"
    monkeypatch.setattr(mod, "FIXTURE_ZIP", fake_zip)
    monkeypatch.setattr(mod, "CACHE_DIR", cache)
    monkeypatch.setattr(
        "sys.argv",
        ["fb156_setup.py", "--expected-sha", _sha256_of(fake_zip)],
    )

    mod.main()

    fbembed = cache / "fbembed.dll"
    fbclient = cache / "fbclient.dll"
    assert fbembed.exists()
    assert fbclient.exists()
    assert fbclient.read_bytes() == fbembed.read_bytes()


def test_rejeita_lfs_pointer_nao_hidratado(tmp_path, monkeypatch):
    """Zip pequeno (<1MB) -> exit 1 com lfs_pull_required."""
    import scripts.fb156_setup as mod

    fake_pointer = tmp_path / "ptr.zip"
    fake_pointer.write_text(
        "version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 1000\n"
    )

    cache = tmp_path / "cache"
    monkeypatch.setattr(mod, "FIXTURE_ZIP", fake_pointer)
    monkeypatch.setattr(mod, "CACHE_DIR", cache)
    monkeypatch.setattr("sys.argv", ["fb156_setup.py"])

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
