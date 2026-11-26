"""O espelho das constantes de sonda do stage_wait do infra-ansible é fixado por commit e sha256."""

from __future__ import annotations

import ast
import hashlib
import os
import subprocess

import pytest

from tests.production.delivery import _ia_stage_wait_consumer as ia


def test_pin_do_espelho_e_o_merge_do_p35_com_o_sha256_do_stage_wait() -> None:
    assert ia.IA_PIN == "fcdb61d68af9d6c42db20300c5b15fdf150dfa3a"
    assert ia.IA_MIRRORED_SOURCES == {"tools/cnesdata_delivery/stage_wait.py": "5ad39b179f8929f95d56b9265111af20c10aa6e116e0cf9039d52a959641f454"}


def test_constantes_espelhadas_da_sonda_sao_as_do_modelo_de_rest() -> None:
    assert (ia.PROBE_POLL_SECONDS, ia.PROBE_ARTIFACT_READS_PER_POLL,
            ia.PROBE_JOBS_READS_PER_MINUTE, ia.ASSUMED_RATE_LIMIT) == (5, 1, 1, 5000)
    assert ia.PROBE_END_STAGES == {"closed-ready": "api"}


@pytest.mark.ia_tree
def test_constantes_espelhadas_conferem_com_o_stage_wait_do_infra_ansible_no_pin() -> None:
    repository = os.environ.get("INFRA_ANSIBLE_REPO")
    if not repository:
        pytest.skip("INFRA_ANSIBLE_REPO ausente")
    path = "tools/cnesdata_delivery/stage_wait.py"
    data = subprocess.run(  # noqa: S603 -- git fixo sobre o checkout do pin.
        ["git", "-C", repository, "show", f"{ia.IA_PIN}:{path}"],  # noqa: S607 -- git do dev.
        capture_output=True, check=True).stdout
    assert hashlib.sha256(data).hexdigest() == ia.IA_MIRRORED_SOURCES[path]
    wanted = ("PROBE_POLL_SECONDS", "PROBE_ARTIFACT_READS_PER_POLL", "PROBE_JOBS_READS_PER_MINUTE",
              "PROBE_END_STAGES", "ASSUMED_RATE_LIMIT")
    found = {node.targets[0].id: ast.literal_eval(node.value) for node in ast.parse(data).body
             if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
             and node.targets[0].id in wanted}
    assert set(found) == set(wanted)
    for name in wanted:
        assert found[name] == getattr(ia, name), name
