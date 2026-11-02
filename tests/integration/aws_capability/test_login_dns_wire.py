"""Codec DNS contra bytes reais capturados e respostas sintéticas malformadas."""

import struct

import pytest

from tests.integration.aws_capability._login_dns_captures_testing import CAPTURES
from tests.integration.aws_capability._login_dns_wire import (
    RCODE_NOERROR,
    RCODE_NXDOMAIN,
    TYPE_A,
    TYPE_NS,
    Record,
    decode_reply,
    encode_query,
)

ZONE = "auth.us-east-2.amazoncognito.com"
ROUTE53 = {
    "ns-664.awsdns-19.net",
    "ns-1253.awsdns-28.org",
    "ns-1617.awsdns-10.co.uk",
    "ns-287.awsdns-35.com",
}
APEX_A = {"13.184.221.81", "18.116.191.109", "18.190.204.199"}
EXAMPLE_A = {
    "23.192.228.80",
    "23.192.228.84",
    "23.215.0.136",
    "23.215.0.138",
    "23.220.75.232",
    "23.220.75.245",
}


def capture(key: str) -> tuple[bytes, int, str, int]:
    item = CAPTURES[key]
    reply = bytes.fromhex("".join(item["reply_hex"]))
    return reply, int(item["txid"]), str(item["name"]), int(item["qtype"])


def qname(name: str) -> bytes:
    return b"".join(bytes([len(p)]) + p.encode() for p in name.split(".")) + b"\x00"


def synthetic(flags: int = 0x8400, answers: bytes = b"", count: int = 0) -> bytes:
    header = struct.pack(">HHHHHH", 7, flags, 1, count, 0, 0)
    return header + qname("x.example.com") + struct.pack(">HH", TYPE_A, 1) + answers


def a_record(rdata: bytes, rtype: int = TYPE_A) -> bytes:
    return b"\xc0\x0c" + struct.pack(">HHIH", rtype, 1, 60, len(rdata)) + rdata


def test_consulta_codifica_cabecalho_pergunta_e_bit_de_recursao() -> None:
    expected = (
        bytes.fromhex("123401000001000000000000") + qname("example.com") + b"\x00\x01\x00\x01"
    )
    assert encode_query(0x1234, "Example.COM.", TYPE_A, True) == expected
    assert encode_query(0x1234, "example.com", TYPE_A, False)[2:4] == b"\x00\x00"
    assert encode_query(0xBEEF, ZONE, TYPE_NS, True)[-4:] == b"\x00\x02\x00\x01"


@pytest.mark.parametrize("key", list(CAPTURES))
def test_consulta_e_identica_a_enviada_na_captura_real(key: str) -> None:
    item = CAPTURES[key]
    packet = encode_query(
        int(item["txid"]), str(item["name"]), int(item["qtype"]), bool(item["recursion"])
    )
    assert packet.hex() == "".join(item["query_hex"])


@pytest.mark.parametrize(
    "name", ["", "a..b", "a" * 64 + ".com", ".".join(["a" * 63] * 4) + ".com", "é.com", "a_b.com"]
)
def test_rejeita_nome_invalido_na_consulta(name: str) -> None:
    with pytest.raises(ValueError, match="dns=invalid_name"):
        encode_query(1, name, TYPE_A, False)


def test_le_ns_reais_da_zona_com_nomes_comprimidos() -> None:
    reply, txid, name, qtype = capture("zone_ns_via_resolver")
    answer = decode_reply(reply, txid, name, qtype)
    assert answer is not None
    assert (answer.rcode, answer.authoritative) == (RCODE_NOERROR, False)
    assert {record.name for record in answer.records} == {ZONE}
    assert {record.rtype for record in answer.records} == {TYPE_NS}
    assert {record.data for record in answer.records} == ROUTE53


def test_le_nxdomain_autoritativo_real_sem_registros() -> None:
    reply, txid, name, qtype = capture("nxdomain_authoritative")
    answer = decode_reply(reply, txid, name, qtype)
    assert answer is not None
    assert (answer.rcode, answer.authoritative, answer.records) == (RCODE_NXDOMAIN, True, ())


@pytest.mark.parametrize(
    ("key", "owner", "addresses"),
    [
        ("apex_a_authoritative", ZONE, APEX_A),
        ("positive_a_authoritative", "example.com", EXAMPLE_A),
    ],
)
def test_le_registros_a_autoritativos_reais(key: str, owner: str, addresses: set[str]) -> None:
    reply, txid, name, qtype = capture(key)
    answer = decode_reply(reply, txid, name, qtype)
    assert answer is not None
    assert (answer.rcode, answer.authoritative) == (RCODE_NOERROR, True)
    assert set(answer.records) == {Record(owner, TYPE_A, address) for address in addresses}


def test_descarta_resposta_de_outro_txid_nome_ou_tipo() -> None:
    reply, txid, name, qtype = capture("apex_a_authoritative")
    assert decode_reply(reply, txid ^ 1, name, qtype) is None
    assert decode_reply(reply, txid, "other." + name, qtype) is None
    assert decode_reply(reply, txid, name, TYPE_NS) is None


def test_compara_o_nome_da_pergunta_sem_caixa_nem_ponto_final() -> None:
    reply, txid, name, qtype = capture("apex_a_authoritative")
    assert decode_reply(reply, txid, name.upper() + ".", qtype) is not None


@pytest.mark.parametrize("cut", [0, 11, 12, 40, -1, -5])
def test_descarta_resposta_curta_ou_cortada(cut: int) -> None:
    reply, txid, name, qtype = capture("apex_a_authoritative")
    assert decode_reply(reply[:cut], txid, name, qtype) is None


@pytest.mark.parametrize("flags", [0x0400, 0x8600])
def test_descarta_pacote_que_nao_e_resposta_ou_veio_truncado(flags: int) -> None:
    assert decode_reply(synthetic(flags=flags), 7, "x.example.com", TYPE_A) is None


def test_descarta_resposta_com_mais_de_uma_pergunta() -> None:
    packet = bytearray(synthetic())
    packet[5] = 2
    assert decode_reply(bytes(packet), 7, "x.example.com", TYPE_A) is None


def test_descarta_bytes_sobrando_depois_dos_registros() -> None:
    reply, txid, name, qtype = capture("apex_a_authoritative")
    assert decode_reply(reply + b"\x00", txid, name, qtype) is None


def test_descarta_ns_com_bytes_sobrando_dentro_do_rdata() -> None:
    rdata = qname("ns-1.example.net") + b"\x00"
    packet = synthetic(answers=a_record(rdata, rtype=TYPE_NS), count=1)
    assert decode_reply(packet, 7, "x.example.com", TYPE_A) is None


def test_nomes_da_resposta_viram_minusculos() -> None:
    header = struct.pack(">HHHHHH", 7, 0x8400, 1, 1, 0, 0)
    question = qname("X.Example.COM") + struct.pack(">HH", TYPE_A, 1)
    packet = header + question + a_record(b"\x01\x02\x03\x04")
    answer = decode_reply(packet, 7, "x.example.com", TYPE_A)
    assert answer is not None
    assert answer.records == (Record("x.example.com", TYPE_A, "1.2.3.4"),)


def test_descarta_registro_a_com_tamanho_diferente_de_quatro() -> None:
    packet = synthetic(answers=a_record(b"\x01\x02\x03"), count=1)
    assert decode_reply(packet, 7, "x.example.com", TYPE_A) is None


def test_descarta_rotulo_seguido_de_ponteiro_de_volta_para_ele() -> None:
    base = synthetic(count=1)
    start = len(base)
    looping = base + b"\x01a\xc0" + bytes([start])
    packet = looping + struct.pack(">HHIH", TYPE_A, 1, 60, 4) + b"\x01\x02\x03\x04"
    assert decode_reply(packet, 7, "x.example.com", TYPE_A) is None


def bad_names() -> dict[str, bytes]:
    base = synthetic(count=1)
    start = len(base)
    fixed = struct.pack(">HHIH", TYPE_A, 1, 60, 4) + b"\x01\x02\x03\x04"
    ns_rdata = start + 2 + 10
    return {
        "dono_aponta_para_si": base + b"\xc0" + bytes([start]) + fixed,
        "ponteiro_alem_do_fim": base + b"\xff\xff" + fixed,
        "rdata_ns_em_laco": base
        + b"\xc0\x0c"
        + struct.pack(">HHIH", TYPE_NS, 1, 60, 2)
        + b"\xc0"
        + bytes([ns_rdata]),
        "rotulo_reservado": base + b"\x41" + b"a" * 65 + b"\x00" + fixed,
    }


@pytest.mark.parametrize("case", list(bad_names()))
def test_descarta_ponteiro_em_laco_fora_do_pacote_ou_rotulo_reservado(case: str) -> None:
    assert decode_reply(bad_names()[case], 7, "x.example.com", TYPE_A) is None


def test_registro_de_outro_tipo_fica_com_dado_vazio() -> None:
    packet = synthetic(answers=a_record(qname("y.example.com"), rtype=5), count=1)
    answer = decode_reply(packet, 7, "x.example.com", TYPE_A)
    assert answer is not None
    assert answer.records == (Record("x.example.com", 5, ""),)


def test_codigo_de_resposta_e_bit_aa_vem_do_cabecalho() -> None:
    answer = decode_reply(synthetic(flags=0x8003), 7, "x.example.com", TYPE_A)
    assert answer is not None
    assert (answer.rcode, answer.authoritative, answer.records) == (RCODE_NXDOMAIN, False, ())
