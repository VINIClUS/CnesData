"""O pin do espelho do consumidor do infra-ansible é explícito e bem formado."""

from __future__ import annotations

import hashlib
import os
import re
import subprocess

import pytest

from tests.production.delivery import _ia_lifecycle_consumer as ia


def test_pin_do_espelho_e_um_commit_de_40_hex() -> None:
    assert re.fullmatch(r"[0-9a-f]{40}", ia.IA_PIN)


def test_espelho_lista_os_tres_fontes_do_consumidor_com_sha256_de_64_hex() -> None:
    assert set(ia.IA_MIRRORED_SOURCES) == {
        "tools/cnesdata_delivery/contract.py",
        "tools/cnesdata_delivery/lifecycle_contract.py",
        "tools/cnesdata_delivery/lifecycle_runtime.py",
    }
    assert all(re.fullmatch(r"[0-9a-f]{64}", sha) for sha in ia.IA_MIRRORED_SOURCES.values())


def test_shas_dos_fontes_espelhados_sao_distintos() -> None:
    assert len(set(ia.IA_MIRRORED_SOURCES.values())) == len(ia.IA_MIRRORED_SOURCES)


@pytest.mark.ia_tree
def test_fontes_espelhados_conferem_com_a_arvore_do_infra_ansible_no_pin() -> None:
    repository = os.environ.get("INFRA_ANSIBLE_REPO")
    if not repository:
        pytest.skip("INFRA_ANSIBLE_REPO ausente")
    for path, expected in ia.IA_MIRRORED_SOURCES.items():
        data = subprocess.run(  # noqa: S603 -- git fixo sobre o checkout do pin.
            ["git", "-C", repository, "show", f"{ia.IA_PIN}:{path}"],  # noqa: S607 -- git do dev.
            capture_output=True, check=True).stdout
        assert hashlib.sha256(data).hexdigest() == expected, path


def test_pin_do_espelho_e_o_merge_do_p40_com_os_tres_sha256() -> None:
    assert ia.IA_PIN == "797e7fb3b0fc3e521344609f633c81b07151eb18"
    assert ia.IA_MIRRORED_SOURCES == {
        "tools/cnesdata_delivery/contract.py": (
            "1f881e85cd21ab45ea5be22d12e81d3515c7ea7849b5c90aa6c54cb1e8b13d3b"
        ),
        "tools/cnesdata_delivery/lifecycle_contract.py": (
            "2f689fcd2e0c41525ba7e002e45289071af01e1c5c56b9333f23d6fb8424e63f"
        ),
        "tools/cnesdata_delivery/lifecycle_runtime.py": (
            "cf0a18a95fa64a300fef4463059759b451f22ce776ed48cd96d8e98af64a731b"
        ),
    }
