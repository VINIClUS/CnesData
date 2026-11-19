"""A aceitação de certificado emitido independe do instante de coleta do teste."""

from collections.abc import Callable
from datetime import timedelta

import pytest
from cryptography import x509

from cnes_infra.auth.ca import CertAuthority
from cnes_infra.auth.errors import OAuthError
from cnes_infra.auth.peer_cert import verify_peer_cert
from packages.cnes_infra.tests.auth import test_peer_cert as peer_examples


@pytest.fixture
def certificate_case(
    test_root_ca: tuple[bytes, bytes],
    test_csr_pem: bytes,
    monkeypatch: pytest.MonkeyPatch,
    record_property: Callable[[str, object], None],
) -> Callable[[int], None]:
    authority = CertAuthority(root_cert_pem=test_root_ca[0], root_key_pem=test_root_ca[1])
    pem = authority.issue_cert(csr_pem=test_csr_pem, agent_id="clock-agent", tenant_id="354130")
    leaf = x509.load_pem_x509_certificate(pem)
    issued_at = leaf.not_valid_before_utc + timedelta(minutes=1)
    verify_peer_cert(leaf, test_root_ca[0], issued_at)
    with pytest.raises(OAuthError):
        verify_peer_cert(leaf, test_root_ca[0], leaf.not_valid_before_utc - timedelta(seconds=1))
    with pytest.raises(OAuthError):
        verify_peer_cert(leaf, test_root_ca[0], leaf.not_valid_after_utc + timedelta(seconds=1))
    monkeypatch.setattr(peer_examples, "_issue", lambda root, csr: leaf)

    def invoke(delay_seconds: int) -> None:
        collected_at = issued_at - timedelta(seconds=delay_seconds)
        monkeypatch.setattr(peer_examples, "_NOW", collected_at)
        record_property("collection_time", collected_at.isoformat())
        record_property("not_valid_before", leaf.not_valid_before_utc.isoformat())
        record_property("not_valid_after", leaf.not_valid_after_utc.isoformat())
        record_property("delay_seconds", delay_seconds)
        error = None
        try:
            peer_examples.test_verify_peer_cert_aceita_cert_emitido_pela_ca(
                test_root_ca, test_csr_pem
            )
        except OAuthError as invalid:
            error = invalid
        record_property("native_error", repr(error))
        assert error is None

    return invoke


@pytest.mark.parametrize("delay_seconds", [0, 180], ids=["sem_atraso", "coleta_180s_antes"])
def test_certificado_emitido_independe_do_instante_de_coleta(
    certificate_case: Callable[[int], None], delay_seconds: int
) -> None:
    certificate_case(delay_seconds)
