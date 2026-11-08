"""Contratos mínimos dos adaptadores de autenticação local."""

import pytest

from cnes_identity.local_auth import _MembershipReader, _TenantProfile


@pytest.mark.parametrize("port", [_MembershipReader, _TenantProfile])
def test_rejeita_adapter_nominal_sem_operacao_de_autenticacao(port: type[object]) -> None:
    incomplete = type("IncompleteAuthAdapter", (port,), {})

    with pytest.raises(TypeError, match="abstract"):
        incomplete()
