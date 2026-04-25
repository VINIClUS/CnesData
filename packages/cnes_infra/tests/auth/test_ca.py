"""CertAuthority tests: load CA + issue 90-day leaf cert."""
import pytest
from cryptography import x509
from cryptography.x509.oid import NameOID

from cnes_infra.auth.ca import CertAuthority


def test_emite_certificado_com_validade_90_dias(test_root_ca, test_csr_pem):
    cert_pem, key_pem = test_root_ca
    ca = CertAuthority(root_cert_pem=cert_pem, root_key_pem=key_pem)

    leaf_pem = ca.issue_cert(
        csr_pem=test_csr_pem,
        agent_id="agent-001",
        tenant_id="presidente-epitacio",
        ttl_days=90,
    )

    leaf = x509.load_pem_x509_certificate(leaf_pem)
    delta = leaf.not_valid_after_utc - leaf.not_valid_before_utc
    assert 89 <= delta.days <= 91


def test_emite_certificado_com_subject_cn_do_csr(test_root_ca, test_csr_pem):
    cert_pem, key_pem = test_root_ca
    ca = CertAuthority(root_cert_pem=cert_pem, root_key_pem=key_pem)

    leaf_pem = ca.issue_cert(
        csr_pem=test_csr_pem,
        agent_id="agent-001",
        tenant_id="presidente-epitacio",
    )

    leaf = x509.load_pem_x509_certificate(leaf_pem)
    cn = leaf.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
    assert cn == "agent-machine-001"


def test_rejeita_csr_invalido(test_root_ca):
    cert_pem, key_pem = test_root_ca
    ca = CertAuthority(root_cert_pem=cert_pem, root_key_pem=key_pem)

    with pytest.raises(ValueError, match="csr_invalid"):
        ca.issue_cert(
            csr_pem=b"not a real CSR",
            agent_id="x",
            tenant_id="y",
        )


def test_certificado_assinado_pelo_root_ca(test_root_ca, test_csr_pem):
    cert_pem, key_pem = test_root_ca
    ca = CertAuthority(root_cert_pem=cert_pem, root_key_pem=key_pem)

    leaf_pem = ca.issue_cert(
        csr_pem=test_csr_pem,
        agent_id="agent-001",
        tenant_id="presidente-epitacio",
    )

    leaf = x509.load_pem_x509_certificate(leaf_pem)
    root = x509.load_pem_x509_certificate(cert_pem)
    assert leaf.issuer == root.subject
