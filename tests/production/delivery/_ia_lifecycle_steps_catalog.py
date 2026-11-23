"""Catálogo lifecycle_steps espelhado do infra-ansible main 097da073 (nomes de job do P27)."""

from __future__ import annotations

IA_CATALOG_PIN = "097da073"
APPLICATION_JOB = "application"
HOST_JOB = "host-gate"
KEEPER_STEP = "application-inventory"
CATALOG = {
    KEEPER_STEP: {"role": "application", "purpose": "application-prepare",
                  "job_name": f"{APPLICATION_JOB} / execute", "input_jobs": {}},
    "application-prepare-operational": {
        "role": "deploy", "purpose": "application-prepare",
        "job_name": "application-prepare-operational / execute", "input_jobs": {}},
    "phase-b-deploy": {"role": "deploy", "purpose": "phase-b", "job_name": "phase-b / execute",
                       "input_jobs": {}},
}
SLICE_A_STEPS = frozenset({KEEPER_STEP})
