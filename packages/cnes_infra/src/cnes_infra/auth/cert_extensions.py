"""OIDs e valor DER das extensões X.509 do agente (PEN IANA 67386)."""
from __future__ import annotations

from cryptography import x509

TENANT_ID_OID = x509.ObjectIdentifier("1.3.6.1.4.1.67386.1.1")
AGENT_ID_OID = x509.ObjectIdentifier("1.3.6.1.4.1.67386.1.2")

_UTF8_STRING_TAG = 0x0C
_SHORT_FORM_MAX = 0x7F
_LONG_FORM_BIT = 0x80
_MAX_LENGTH_OCTETS = 4


def encode_extension_value(value: str) -> bytes:
    """Returns: valor da extensão como DER UTF8String. Raises: ValueError se vazio."""
    if not value:
        raise ValueError("extension_value_empty")
    payload = value.encode()
    return bytes([_UTF8_STRING_TAG]) + _encode_length(len(payload)) + payload


def decode_extension_value(der: bytes) -> str:
    """Returns: texto do DER UTF8String. Raises: ValueError se não for DER canônico."""
    if len(der) < 2 or der[0] != _UTF8_STRING_TAG:
        raise ValueError("extension_value_tag_invalid")
    length, offset = _decode_length(der)
    if len(der) != offset + length:
        raise ValueError("extension_value_length_mismatch")
    if length == 0:
        raise ValueError("extension_value_empty")
    try:
        return der[offset:].decode()
    except UnicodeDecodeError as exc:
        raise ValueError("extension_value_utf8_invalid") from exc


def _encode_length(length: int) -> bytes:
    if length <= _SHORT_FORM_MAX:
        return bytes([length])
    octets = (length.bit_length() + 7) // 8
    return bytes([_LONG_FORM_BIT | octets]) + length.to_bytes(octets, "big")


def _decode_length(der: bytes) -> tuple[int, int]:
    first = der[1]
    if first <= _SHORT_FORM_MAX:
        return first, 2
    octets = first & _SHORT_FORM_MAX
    end = 2 + octets
    if not 0 < octets <= _MAX_LENGTH_OCTETS or len(der) < end or der[2] == 0:
        raise ValueError("extension_value_length_invalid")
    length = int.from_bytes(der[2:end], "big")
    if length <= _SHORT_FORM_MAX:
        raise ValueError("extension_value_length_invalid")
    return length, end
