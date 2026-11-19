"""cert_extensions: OIDs do PEN IANA 67386 + valor DER UTF8String estrito."""
import pytest

from cnes_infra.auth.cert_extensions import (
    AGENT_ID_OID,
    TENANT_ID_OID,
    decode_extension_value,
    encode_extension_value,
)


def test_oids_ficam_no_arco_cnesdata_do_pen_67386():
    assert TENANT_ID_OID.dotted_string == "1.3.6.1.4.1.67386.1.1"
    assert AGENT_ID_OID.dotted_string == "1.3.6.1.4.1.67386.1.2"


def test_codifica_utf8string_der_forma_curta():
    assert encode_extension_value("354130") == b"\x0c\x06354130"


def test_codifica_utf8string_der_forma_longa():
    valor = "a" * 200
    assert encode_extension_value(valor) == b"\x0c\x81\xc8" + valor.encode()


def test_codifica_utf8string_der_com_comprimento_de_dois_bytes():
    valor = "b" * 300
    assert encode_extension_value(valor) == b"\x0c\x82\x01\x2c" + valor.encode()


def test_codifica_comprimento_em_bytes_e_nao_em_caracteres():
    assert encode_extension_value("ação") == b"\x0c\x06" + "ação".encode()


@pytest.mark.parametrize(
    "valor", ["354130", "agent-0f3c", "ação", "x" * 127, "y" * 128, "z" * 300],
)
def test_decodifica_ida_e_volta(valor):
    assert decode_extension_value(encode_extension_value(valor)) == valor


@pytest.mark.parametrize(
    ("der", "erro"),
    [
        (b"", "extension_value_tag_invalid"),
        (b"\x0c", "extension_value_tag_invalid"),
        (b"\x13\x06354130", "extension_value_tag_invalid"),
        (b"354130", "extension_value_tag_invalid"),
        (b"\x0c\x80354130\x00\x00", "extension_value_length_invalid"),
        (b"\x0c\x85\x00\x00\x00\x00\x06", "extension_value_length_invalid"),
        (b"\x0c\x82\x00", "extension_value_length_invalid"),
        (b"\x0c\x81\x06354130", "extension_value_length_invalid"),
        (b"\x0c\x82\x00\xc8" + b"a" * 200, "extension_value_length_invalid"),
        (b"\x0c\x07354130", "extension_value_length_mismatch"),
        (b"\x0c\x05354130", "extension_value_length_mismatch"),
        (b"\x0c\x03\xff\xfe\xfd", "extension_value_utf8_invalid"),
        (b"\x0c\x00", "extension_value_empty"),
    ],
)
def test_rejeita_valor_que_nao_e_utf8string_der_canonico(der, erro):
    with pytest.raises(ValueError, match=erro):
        decode_extension_value(der)


def test_recusa_codificar_valor_vazio():
    with pytest.raises(ValueError, match="extension_value_empty"):
        encode_extension_value("")
