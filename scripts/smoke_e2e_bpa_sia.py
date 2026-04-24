"""Smoke E2E: enqueue BPA+SIA jobs, verify Gold rows structure."""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

import httpx
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def run_smoke(api_url: str, admin_token: str, db_url: str,
              tenant: str = "354130") -> int:
    engine = create_engine(db_url)
    try:
        _check_dims_seeded(engine)

        r1 = httpx.post(
            f"{api_url}/api/v1/extractions/enqueue",
            json={"source_type": "BPA_MAG", "tenant_id": tenant,
                  "competencia": str(date(2026, 1, 1))},
            headers={"X-Admin-Token": admin_token,
                     "X-Tenant-Id": tenant},
        )
        r1.raise_for_status()
        logger.info("enqueue_bpa_mag jobs=%d", len(r1.json()["job_ids"]))

        r2 = httpx.post(
            f"{api_url}/api/v1/extractions/enqueue",
            json={"source_type": "SIA_LOCAL", "tenant_id": tenant,
                  "competencia": str(date(2026, 1, 1))},
            headers={"X-Admin-Token": admin_token,
                     "X-Tenant-Id": tenant},
        )
        r2.raise_for_status()
        logger.info("enqueue_sia_local jobs=%d", len(r2.json()["job_ids"]))

        with engine.begin() as conn:
            count = conn.execute(text("""
                SELECT COUNT(*) FROM landing.extractions
                WHERE tenant_id = :t
                  AND source_type IN ('BPA_MAG', 'SIA_LOCAL')
                  AND competencia = :c
            """), {"t": tenant, "c": date(2026, 1, 1)}).scalar_one()
        logger.info("smoke_ok tenant=%s jobs_enqueued=%d", tenant, count)
        return 0
    finally:
        engine.dispose()


def _check_dims_seeded(engine) -> None:
    with engine.begin() as conn:
        proc_n = conn.execute(text(
            "SELECT COUNT(*) FROM gold.dim_procedimento_sus"
        )).scalar_one()
        cid_n = conn.execute(text(
            "SELECT COUNT(*) FROM gold.dim_cid10"
        )).scalar_one()
    if proc_n < 1 or cid_n < 1:
        raise RuntimeError(
            f"dims_not_seeded proc={proc_n} cid={cid_n}")


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                       format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--admin-token", required=True)
    parser.add_argument("--db-url", required=True)
    parser.add_argument("--tenant", default="354130")
    args = parser.parse_args()

    return run_smoke(args.api_url, args.admin_token, args.db_url,
                     tenant=args.tenant)


if __name__ == "__main__":
    sys.exit(main())
