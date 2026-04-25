"""CertAuthority: load root CA + issue 90-day leaf certs."""
from __future__ import annotations

import datetime as dt
import logging

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID

logger = logging.getLogger(__name__)

_TENANT_OID = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1.1")
_AGENT_ID_OID = x509.ObjectIdentifier("1.3.6.1.4.1.99999.1.2")


class CertAuthority:
    """Wraps a root CA cert + private key. Issues leaf certs for agents."""

    def __init__(self, *, root_cert_pem: bytes, root_key_pem: bytes) -> None:
        self._root_cert = x509.load_pem_x509_certificate(root_cert_pem)
        key = serialization.load_pem_private_key(root_key_pem, password=None)
        if not isinstance(key, ec.EllipticCurvePrivateKey | rsa.RSAPrivateKey):
            raise ValueError("root_key_unsupported_type")
        self._root_key: ec.EllipticCurvePrivateKey | rsa.RSAPrivateKey = key

    def issue_cert(
        self,
        *,
        csr_pem: bytes,
        agent_id: str,
        tenant_id: str,
        ttl_days: int = 90,
    ) -> bytes:
        try:
            csr = x509.load_pem_x509_csr(csr_pem)
        except ValueError as exc:
            raise ValueError("csr_invalid") from exc

        if not csr.is_signature_valid:
            raise ValueError("csr_signature_invalid")

        now = dt.datetime.now(dt.UTC)
        builder = (
            x509.CertificateBuilder()
            .subject_name(csr.subject)
            .issuer_name(self._root_cert.subject)
            .public_key(csr.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(minutes=1))
            .not_valid_after(now + dt.timedelta(days=ttl_days))
            .add_extension(
                x509.BasicConstraints(ca=False, path_length=None), critical=True,
            )
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True, content_commitment=False,
                    key_encipherment=True, data_encipherment=False,
                    key_agreement=False, key_cert_sign=False, crl_sign=False,
                    encipher_only=False, decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
                critical=False,
            )
            .add_extension(
                x509.UnrecognizedExtension(_TENANT_OID, tenant_id.encode()),
                critical=False,
            )
            .add_extension(
                x509.UnrecognizedExtension(_AGENT_ID_OID, agent_id.encode()),
                critical=False,
            )
        )
        leaf = builder.sign(self._root_key, hashes.SHA256())
        logger.info(
            "ca_issued_cert agent_id=%s tenant_id=%s ttl_days=%d serial=%s",
            agent_id, tenant_id, ttl_days, leaf.serial_number,
        )
        return leaf.public_bytes(serialization.Encoding.PEM)

    @property
    def root_cert_pem(self) -> bytes:
        return self._root_cert.public_bytes(serialization.Encoding.PEM)
