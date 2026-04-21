"""Teste do seed_anonymize — hash determinístico + faker reproduzível."""
from pathlib import Path

from scripts.seed_anonymize import (
    anonymize_cpf,
    anonymize_nome,
    build_manifest,
)


def test_anonymize_cpf_hash_deterministico() -> None:
    cpf_a = anonymize_cpf("12345678901", salt="test_salt")
    cpf_b = anonymize_cpf("12345678901", salt="test_salt")
    assert cpf_a == cpf_b
    assert len(cpf_a) == 11
    assert cpf_a != "12345678901"


def test_anonymize_cpf_salt_muda_output() -> None:
    a = anonymize_cpf("12345678901", salt="salt_a")
    b = anonymize_cpf("12345678901", salt="salt_b")
    assert a != b


def test_anonymize_nome_faker_seed_deterministico() -> None:
    nome_a = anonymize_nome("João Silva", seed=42)
    nome_b = anonymize_nome("João Silva", seed=42)
    assert nome_a == nome_b
    assert nome_a != "João Silva"
    assert isinstance(nome_a, str)
    assert len(nome_a) > 0


def test_build_manifest_calcula_sha256(tmp_path: Path) -> None:
    f1 = tmp_path / "a.fbk"
    f1.write_bytes(b"test_data")
    manifest = build_manifest([f1], salt="x", seed=1)
    assert "a.fbk" in manifest["files"]
    assert manifest["files"]["a.fbk"]["sha256"] == (
        "e7d87b738825c33824cf3fd32b7314161fc8c425129163ff5e7260fc7288da36"
    )
    assert manifest["salt_hash"] != ""
