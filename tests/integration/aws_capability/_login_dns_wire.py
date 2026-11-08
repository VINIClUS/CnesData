"""Codec DNS mínimo para consultas A e NS, sem dependências fora da stdlib."""

import re
import struct
from dataclasses import dataclass

TYPE_A = 1
TYPE_NS = 2
CLASS_IN = 1
RCODE_NOERROR = 0
RCODE_NXDOMAIN = 3
_HEADER = struct.Struct(">HHHHHH")
_QUESTION = struct.Struct(">HH")
_FIXED = struct.Struct(">HHIH")
_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_QR = 0x8000
_AA = 0x0400
_TC = 0x0200
_RD = 0x0100
_POINTER = 0xC0
_MAX_NAME = 255


class _Malformed(Exception):
    pass


@dataclass(frozen=True)
class Record:
    name: str
    rtype: int
    data: str


@dataclass(frozen=True)
class Answer:
    rcode: int
    authoritative: bool
    records: tuple[Record, ...]


def normalize(name: str) -> str:
    return (name[:-1] if name.endswith(".") else name).lower()


def encode_query(txid: int, name: str, qtype: int, recursion: bool) -> bytes:
    """Returns: pacote de consulta IN; Raises: ValueError para nome fora do padrão DNS."""
    normalized = normalize(name)
    labels = normalized.split(".")
    if len(normalized) > 253 or not all(_LABEL.fullmatch(label) for label in labels):
        raise ValueError("dns=invalid_name")
    header = _HEADER.pack(txid & 0xFFFF, _RD if recursion else 0, 1, 0, 0, 0)
    encoded = b"".join(bytes([len(label)]) + label.encode("ascii") for label in labels)
    return header + encoded + b"\x00" + _QUESTION.pack(qtype, CLASS_IN)


def decode_reply(reply: bytes, txid: int, name: str, qtype: int) -> Answer | None:
    """Returns: resposta à pergunta exata ou None para pacote alheio, truncado ou malformado."""
    try:
        return _decode(reply, txid, normalize(name), qtype)
    except (_Malformed, struct.error):
        return None


def _decode(reply: bytes, txid: int, name: str, qtype: int) -> Answer | None:
    ident, flags, questions, answers, authority, additional = _HEADER.unpack_from(reply, 0)
    if ident != txid or not flags & _QR or flags & _TC or questions != 1:
        return None
    asked, offset = _read_name(reply, _HEADER.size)
    if asked != name or _QUESTION.unpack_from(reply, offset) != (qtype, CLASS_IN):
        return None
    offset += _QUESTION.size
    records: list[Record] = []
    for index in range(answers + authority + additional):
        record, offset = _read_record(reply, offset)
        if index < answers:
            records.append(record)
    if offset != len(reply):
        raise _Malformed
    return Answer(flags & 0x000F, bool(flags & _AA), tuple(records))


def _read_record(reply: bytes, offset: int) -> tuple[Record, int]:
    owner, offset = _read_name(reply, offset)
    rtype, _, _, length = _FIXED.unpack_from(reply, offset)
    start = offset + _FIXED.size
    end = start + length
    return Record(owner, rtype, _rdata(reply, rtype, start, end)), end


def _rdata(reply: bytes, rtype: int, start: int, end: int) -> str:
    if rtype == TYPE_A:
        if end - start != 4:
            raise _Malformed
        return ".".join(str(octet) for octet in reply[start:end])
    if rtype == TYPE_NS:
        target, after = _read_name(reply, start)
        if after != end:
            raise _Malformed
        return target
    return ""


def _read_name(reply: bytes, offset: int) -> tuple[str, int]:
    labels: list[str] = []
    position, resume, size = offset, None, 1
    while (length := _byte(reply, position)) != 0:
        if length & _POINTER == _POINTER:
            target = (length & 0x3F) << 8 | _byte(reply, position + 1)
            if target >= position:
                raise _Malformed
            resume = position + 2 if resume is None else resume
            position = target
            continue
        labels.append(_label(reply, position, length))
        size += 1 + length
        if size > _MAX_NAME:
            raise _Malformed
        position += 1 + length
    return ".".join(labels), position + 1 if resume is None else resume


def _label(reply: bytes, position: int, length: int) -> str:
    if length & _POINTER:
        raise _Malformed
    raw = reply[position + 1 : position + 1 + length]
    if len(raw) != length:
        raise _Malformed
    try:
        return raw.decode("ascii").lower()
    except UnicodeDecodeError:
        raise _Malformed from None


def _byte(reply: bytes, position: int) -> int:
    if position >= len(reply):
        raise _Malformed
    return reply[position]
