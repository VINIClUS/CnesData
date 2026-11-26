"""Espelho das constantes de sonda do modelo de REST do stage_wait do infra-ansible, fixado por commit."""

from __future__ import annotations

# infra-ansible fcdb61d6 (P35 mergeado): tools/cnesdata_delivery/stage_wait.py.
IA_PIN = "fcdb61d68af9d6c42db20300c5b15fdf150dfa3a"
IA_MIRRORED_SOURCES = {
    "tools/cnesdata_delivery/stage_wait.py": (
        "5ad39b179f8929f95d56b9265111af20c10aa6e116e0cf9039d52a959641f454"
    ),
}
PROBE_POLL_SECONDS = 5
PROBE_ARTIFACT_READS_PER_POLL = 1
PROBE_JOBS_READS_PER_MINUTE = 1
PROBE_END_STAGES = {"closed-ready": "api"}
ASSUMED_RATE_LIMIT = 5000
