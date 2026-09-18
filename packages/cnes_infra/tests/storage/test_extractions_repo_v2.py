"""Tests for extractions_repo N-file manifest impl (Task 6)."""
from __future__ import annotations

from datetime import date
from uuid import UUID

import pytest
from sqlalchemy import text

from cnes_infra.storage import extractions_repo

pytestmark = pytest.mark.postgres


_TENANT = "354130"


@pytest.fixture
def _cleanup_extractions(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE landing.extractions CASCADE"))
    yield
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE landing.extractions CASCADE"))


@pytest.mark.usefixtures("_cleanup_extractions")
class TestExtractionsRepoV2:
    def test_enqueue_com_dois_files_persiste_jsonb(
        self, pg_engine,
    ) -> None:
        files = [
            {
                "minio_key": "bpa/2026-01/bpa_c.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 1024,
                "sha256": "a" * 64,
            },
            {
                "minio_key": "bpa/2026-01/bpa_i.parquet.gz",
                "fato_subtype": "BPA_I",
                "size_bytes": 2048,
                "sha256": "b" * 64,
            },
        ]
        job_id = extractions_repo.enqueue(
            pg_engine,
            tenant_id=_TENANT,
            source_type="BPA_MAG",
            competencia=date(2026, 1, 1),
            files=files,
        )
        assert isinstance(job_id, UUID)
        with pg_engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT files, source_type FROM landing.extractions "
                    "WHERE job_id = :j",
                ),
                {"j": job_id},
            ).one()
        assert len(row.files) == 2
        assert row.source_type == "BPA_MAG"

    def test_enqueue_com_depends_on_persiste_array(
        self, pg_engine,
    ) -> None:
        dep = extractions_repo.enqueue(
            pg_engine,
            tenant_id=_TENANT,
            source_type="SIA_LOCAL",
            competencia=date(2026, 1, 1),
            files=[{
                "minio_key": "x/dim_sigtap.parquet.gz",
                "fato_subtype": "DIM_SIGTAP",
                "size_bytes": 100,
                "sha256": "c" * 64,
            }],
        )
        job_id = extractions_repo.enqueue(
            pg_engine,
            tenant_id=_TENANT,
            source_type="SIA_LOCAL",
            competencia=date(2026, 1, 1),
            files=[{
                "minio_key": "x/sia_apa.parquet.gz",
                "fato_subtype": "SIA_APA",
                "size_bytes": 100,
                "sha256": "d" * 64,
            }],
            depends_on=[dep],
        )
        with pg_engine.begin() as conn:
            got = conn.execute(
                text(
                    "SELECT depends_on FROM landing.extractions "
                    "WHERE job_id = :j",
                ),
                {"j": job_id},
            ).scalar_one()
        assert got == [dep]

    def test_claim_next_retorna_none_quando_sem_pending(
        self, pg_engine,
    ) -> None:
        claimed = extractions_repo.claim_next(pg_engine)
        assert claimed is None

    def test_mark_completed_muda_status(self, pg_engine) -> None:
        job_id = extractions_repo.enqueue(
            pg_engine,
            tenant_id=_TENANT,
            source_type="BPA_MAG",
            competencia=date(2026, 2, 1),
            files=[{
                "minio_key": "bpa/ok.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 100,
                "sha256": "1" * 64,
            }],
        )
        extractions_repo.mark_completed(pg_engine, job_id=job_id)
        with pg_engine.begin() as conn:
            status = conn.execute(
                text(
                    "SELECT status FROM landing.extractions "
                    "WHERE job_id = :j",
                ),
                {"j": job_id},
            ).scalar_one()
        assert status == "COMPLETED"

    def test_mark_failed_muda_status(self, pg_engine) -> None:
        job_id = extractions_repo.enqueue(
            pg_engine,
            tenant_id=_TENANT,
            source_type="BPA_MAG",
            competencia=date(2026, 3, 1),
            files=[{
                "minio_key": "bpa/err.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 100,
                "sha256": "2" * 64,
            }],
        )
        extractions_repo.mark_failed(
            pg_engine, job_id=job_id, reason="test boom",
        )
        with pg_engine.begin() as conn:
            status = conn.execute(
                text(
                    "SELECT status FROM landing.extractions "
                    "WHERE job_id = :j",
                ),
                {"j": job_id},
            ).scalar_one()
        assert status == "FAILED"

    def test_claim_pula_job_com_depends_on_pendente(
        self, pg_engine,
    ) -> None:
        dep = extractions_repo.enqueue(
            pg_engine,
            tenant_id=_TENANT,
            source_type="SIA_LOCAL",
            competencia=date(2026, 1, 2),
            files=[{
                "minio_key": "y/dim.parquet.gz",
                "fato_subtype": "DIM_SIGTAP",
                "size_bytes": 100,
                "sha256": "e" * 64,
            }],
        )
        blocked = extractions_repo.enqueue(
            pg_engine,
            tenant_id=_TENANT,
            source_type="SIA_LOCAL",
            competencia=date(2026, 1, 2),
            files=[{
                "minio_key": "y/apa.parquet.gz",
                "fato_subtype": "SIA_APA",
                "size_bytes": 100,
                "sha256": "f" * 64,
            }],
            depends_on=[dep],
        )
        claimed = extractions_repo.claim_next(pg_engine)
        assert claimed is not None
        assert claimed.job_id == dep
        assert claimed.job_id != blocked

    def test_register_atualiza_status_e_files(
        self, pg_engine,
    ) -> None:
        files = [
            {
                "minio_key": "bpa/2026-01/bpa_c.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 1024,
                "sha256": "a" * 64,
            },
        ]
        job_id = extractions_repo.enqueue(
            pg_engine,
            tenant_id=_TENANT,
            source_type="BPA_MAG",
            competencia=date(2026, 1, 1),
            files=files,
        )

        manifest = [
            {"minio_key": "bpa/2026-01/bpa_c.parquet.gz", "size_bytes": 1024,
             "sha256": "a" * 64, "fato_subtype": "BPA_C"},
        ]
        registered = extractions_repo.register(
            pg_engine, job_id=job_id, files=manifest,
        )
        assert registered == job_id

        with pg_engine.connect() as conn:
            row = conn.execute(
                text("SELECT status, files FROM landing.extractions WHERE job_id = :j"),
                {"j": str(job_id)},
            ).one()
            assert row.status == "REGISTERED"
            assert len(row.files) == 1

    def test_register_retorna_none_para_job_inexistente(
        self, pg_engine,
    ) -> None:
        fake_id = UUID("00000000-0000-0000-0000-000000000999")
        result = extractions_repo.register(
            pg_engine, job_id=fake_id, files=[],
        )
        assert result is None

    def test_register_persiste_agent_version_e_machine_id(
        self, pg_engine,
    ) -> None:
        files = [{
            "minio_key": "bpa/2026-01/bpa_c.parquet.gz",
            "fato_subtype": "BPA_C",
            "size_bytes": 1024,
            "sha256": "a" * 64,
        }]
        job_id = extractions_repo.enqueue(
            pg_engine, tenant_id=_TENANT, source_type="BPA_MAG",
            competencia=date(2026, 1, 1), files=files,
        )
        extractions_repo.register(
            pg_engine, job_id=job_id, files=files,
            agent_version="1.2.3", machine_id="edge-01",
        )
        with pg_engine.connect() as conn:
            row = conn.execute(
                text("SELECT agent_version, machine_id "
                     "FROM landing.extractions WHERE job_id = :j"),
                {"j": str(job_id)},
            ).one()
        assert row.agent_version == "1.2.3"
        assert row.machine_id == "edge-01"

    def test_register_sem_metadata_mantem_columns_null(
        self, pg_engine,
    ) -> None:
        files = [{
            "minio_key": "bpa/2026-01/bpa_c.parquet.gz",
            "fato_subtype": "BPA_C",
            "size_bytes": 1024,
            "sha256": "a" * 64,
        }]
        job_id = extractions_repo.enqueue(
            pg_engine, tenant_id=_TENANT, source_type="BPA_MAG",
            competencia=date(2026, 1, 1), files=files,
        )
        extractions_repo.register(pg_engine, job_id=job_id, files=files)
        with pg_engine.connect() as conn:
            row = conn.execute(
                text("SELECT agent_version, machine_id "
                     "FROM landing.extractions WHERE job_id = :j"),
                {"j": str(job_id)},
            ).one()
        assert row.agent_version is None
        assert row.machine_id is None

    def test_register_com_coalesce_preserva_quando_explicit_null(
        self, pg_engine,
    ) -> None:
        files = [{
            "minio_key": "bpa/2026-01/bpa_c.parquet.gz",
            "fato_subtype": "BPA_C",
            "size_bytes": 1024,
            "sha256": "a" * 64,
        }]
        job_id = extractions_repo.enqueue(
            pg_engine, tenant_id=_TENANT, source_type="BPA_MAG",
            competencia=date(2026, 1, 1), files=files,
        )
        with pg_engine.begin() as conn:
            conn.execute(
                text("UPDATE landing.extractions "
                     "SET agent_version = '0.9.0', machine_id = 'old-edge' "
                     "WHERE job_id = :j"),
                {"j": str(job_id)},
            )
        extractions_repo.register(pg_engine, job_id=job_id, files=files)
        with pg_engine.connect() as conn:
            row = conn.execute(
                text("SELECT agent_version, machine_id "
                     "FROM landing.extractions WHERE job_id = :j"),
                {"j": str(job_id)},
            ).one()
        assert row.agent_version == "0.9.0"
        assert row.machine_id == "old-edge"

    def test_claim_next_atravessa_tenants(self, pg_engine) -> None:
        from datetime import date as _date
        # Two tenants, one PENDING row each, distinct created_at
        t1 = "354130"
        t2 = "550017"
        j1 = extractions_repo.enqueue(
            pg_engine, tenant_id=t1, source_type="BPA_MAG",
            competencia=_date(2026, 4, 1),
            files=[{
                "minio_key": "t1/bpa_c.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 100, "sha256": "1" * 64,
            }],
        )
        j2 = extractions_repo.enqueue(
            pg_engine, tenant_id=t2, source_type="BPA_MAG",
            competencia=_date(2026, 4, 1),
            files=[{
                "minio_key": "t2/bpa_c.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 100, "sha256": "2" * 64,
            }],
        )
        # First claim: oldest by created_at (j1 enqueued first)
        first = extractions_repo.claim_next(pg_engine)
        assert first is not None
        assert first.job_id == j1
        assert first.tenant_id == t1
        # Second claim: the other tenant's row
        second = extractions_repo.claim_next(pg_engine)
        assert second is not None
        assert second.job_id == j2
        assert second.tenant_id == t2
        # Third claim: nothing PENDING anywhere
        assert extractions_repo.claim_next(pg_engine) is None

    def test_reap_expired_zero_em_tabela_vazia(self, pg_engine) -> None:
        assert extractions_repo.reap_expired(pg_engine) == 0

    def test_reap_expired_libera_claimed_com_lease_vencido(
        self, pg_engine,
    ) -> None:
        from datetime import date as _date
        job_id = extractions_repo.enqueue(
            pg_engine, tenant_id=_TENANT, source_type="BPA_MAG",
            competencia=_date(2026, 5, 1),
            files=[{
                "minio_key": "r/exp.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 100, "sha256": "9" * 64,
            }],
        )
        # Claim it (becomes CLAIMED with lease_until = NOW() + 300s)
        claimed = extractions_repo.claim_next(pg_engine)
        assert claimed is not None
        assert claimed.job_id == job_id
        # Force lease_until into the past
        with pg_engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE landing.extractions "
                    "SET lease_until = NOW() - INTERVAL '1 minute' "
                    "WHERE job_id = :j",
                ),
                {"j": str(job_id)},
            )
        reaped = extractions_repo.reap_expired(pg_engine)
        assert reaped == 1
        with pg_engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT status, lease_until FROM landing.extractions "
                    "WHERE job_id = :j",
                ),
                {"j": str(job_id)},
            ).one()
        assert row.status == "PENDING"
        assert row.lease_until is None

    def test_reap_expired_ignora_claimed_com_lease_valido(
        self, pg_engine,
    ) -> None:
        from datetime import date as _date
        extractions_repo.enqueue(
            pg_engine, tenant_id=_TENANT, source_type="BPA_MAG",
            competencia=_date(2026, 5, 2),
            files=[{
                "minio_key": "r/fresh.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 100, "sha256": "8" * 64,
            }],
        )
        claimed = extractions_repo.claim_next(pg_engine)
        assert claimed is not None
        assert extractions_repo.reap_expired(pg_engine) == 0
        with pg_engine.begin() as conn:
            status = conn.execute(
                text(
                    "SELECT status FROM landing.extractions "
                    "WHERE job_id = :j",
                ),
                {"j": str(claimed.job_id)},
            ).scalar_one()
        assert status == "CLAIMED"

    def test_reap_expired_ignora_status_nao_claimed(
        self, pg_engine,
    ) -> None:
        from datetime import date as _date
        # PENDING row (untouched)
        extractions_repo.enqueue(
            pg_engine, tenant_id=_TENANT, source_type="BPA_MAG",
            competencia=_date(2026, 5, 3),
            files=[{
                "minio_key": "r/p.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 100, "sha256": "7" * 64,
            }],
        )
        # COMPLETED row
        c = extractions_repo.enqueue(
            pg_engine, tenant_id=_TENANT, source_type="BPA_MAG",
            competencia=_date(2026, 5, 3),
            files=[{
                "minio_key": "r/c.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 100, "sha256": "6" * 64,
            }],
        )
        extractions_repo.mark_completed(pg_engine, job_id=c)
        # FAILED row
        f = extractions_repo.enqueue(
            pg_engine, tenant_id=_TENANT, source_type="BPA_MAG",
            competencia=_date(2026, 5, 3),
            files=[{
                "minio_key": "r/f.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 100, "sha256": "5" * 64,
            }],
        )
        extractions_repo.mark_failed(pg_engine, job_id=f, reason="x")
        assert extractions_repo.reap_expired(pg_engine) == 0

    def test_reap_expired_atravessa_tenants(self, pg_engine) -> None:
        from datetime import date as _date
        t1 = "354130"
        t2 = "550017"
        j1 = extractions_repo.enqueue(
            pg_engine, tenant_id=t1, source_type="BPA_MAG",
            competencia=_date(2026, 5, 4),
            files=[{
                "minio_key": "rt/t1.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 100, "sha256": "4" * 64,
            }],
        )
        j2 = extractions_repo.enqueue(
            pg_engine, tenant_id=t2, source_type="BPA_MAG",
            competencia=_date(2026, 5, 4),
            files=[{
                "minio_key": "rt/t2.parquet.gz",
                "fato_subtype": "BPA_C",
                "size_bytes": 100, "sha256": "3" * 64,
            }],
        )
        # Claim both (cross-tenant), then expire both leases
        assert extractions_repo.claim_next(pg_engine) is not None
        assert extractions_repo.claim_next(pg_engine) is not None
        with pg_engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE landing.extractions "
                    "SET lease_until = NOW() - INTERVAL '1 minute' "
                    "WHERE job_id IN (:a, :b)",
                ),
                {"a": str(j1), "b": str(j2)},
            )
        assert extractions_repo.reap_expired(pg_engine) == 2
