"""O pin do espelho do consumidor do infra-ansible é explícito e bem formado."""

from __future__ import annotations

import re

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
