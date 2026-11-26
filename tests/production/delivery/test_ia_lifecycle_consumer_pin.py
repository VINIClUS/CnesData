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
    assert ia.IA_PIN == "249c9115557f142bfdd80b0cfe185b9cac6e1efb"
    assert ia.IA_MIRRORED_SOURCES == {
        "tools/cnesdata_delivery/contract.py": (
            "1f881e85cd21ab45ea5be22d12e81d3515c7ea7849b5c90aa6c54cb1e8b13d3b"
        ),
        "tools/cnesdata_delivery/lifecycle_contract.py": (
            "fdd7d8ccf92a987e5018a9f6d4e6d1a75524a7b788b1c37cd05f7c8d3d3d3511"
        ),
        "tools/cnesdata_delivery/lifecycle_runtime.py": (
            "0f86b67ad7d7075eb62a6c8a0335a3fc93de5c67922e277e029c052ed076ca1a"
        ),
    }
