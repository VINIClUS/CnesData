"""CertAuthority tests: load CA + issue 90-day leaf cert."""
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization
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


def test_rejeita_root_key_de_tipo_nao_suportado():
    """Ed25519 root key should be rejected (not EC/RSA)."""
    from cryptography.hazmat.primitives.asymmetric import ed25519

    key = ed25519.Ed25519PrivateKey.generate()
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    cert_pem, _ = _build_self_signed_cert(key)
    with pytest.raises(ValueError, match="root_key_unsupported_type"):
        CertAuthority(root_cert_pem=cert_pem, root_key_pem=key_pem)


def test_root_cert_pem_property_retorna_bytes(test_root_ca):
    cert_pem, key_pem = test_root_ca
    ca = CertAuthority(root_cert_pem=cert_pem, root_key_pem=key_pem)
    assert ca.root_cert_pem == cert_pem


def _build_self_signed_cert(key):
    """Helper for ed25519 CA cert (used in unsupported-key-type test)."""
    import datetime as _dt

    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ed25519-ca")])
    now = _dt.datetime.now(_dt.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - _dt.timedelta(minutes=1))
        .not_valid_after(now + _dt.timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, None)
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return cert_pem, key_pem
