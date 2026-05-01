from __future__ import annotations

import pytest
from pydantic import ValidationError

from cnes_contracts.landing import FileManifest


class TestFileManifest:
    def test_aceita_manifest_valido_bpa_c(self) -> None:
        m = FileManifest(
            minio_key="bpa/2026-01/bpa_c.parquet.gz",
            fato_subtype="BPA_C",
            size_bytes=1024,
            sha256="a" * 64,
        )
        assert m.fato_subtype == "BPA_C"

    def test_rejeita_minio_key_sem_extensao(self) -> None:
        with pytest.raises(ValidationError):
            FileManifest(
                minio_key="bpa/2026-01/bpa_c.csv",
                fato_subtype="BPA_C", size_bytes=1024, sha256="a" * 64,
            )

    def test_rejeita_sha256_curto(self) -> None:
        with pytest.raises(ValidationError):
            FileManifest(
                minio_key="x.parquet.gz", fato_subtype="BPA_C",
                size_bytes=1024, sha256="a" * 10,
            )

    def test_rejeita_size_zero(self) -> None:
        with pytest.raises(ValidationError):
            FileManifest(
                minio_key="x.parquet.gz", fato_subtype="BPA_C",
                size_bytes=0, sha256="a" * 64,
            )

    def test_rejeita_fato_subtype_desconhecido(self) -> None:
        with pytest.raises(ValidationError):
            FileManifest(
                minio_key="x.parquet.gz", fato_subtype="UNKNOWN",
                size_bytes=1024, sha256="a" * 64,
            )

    def test_frozen_rejeita_mutacao(self) -> None:
        m = FileManifest(
            minio_key="x.parquet.gz", fato_subtype="BPA_C",
            size_bytes=1024, sha256="a" * 64,
        )
        with pytest.raises(ValidationError):
            m.size_bytes = 2048
