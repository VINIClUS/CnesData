"""Codec A3 UTF-8/LF e Decimal exato com limites antes de alocação."""

from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal
from typing import TYPE_CHECKING, Any, NoReturn, cast

from release.verify_candidate import require

if TYPE_CHECKING:
    from collections.abc import Iterator

type Node = dict[str, Any]
STATE_LIMIT = 128 * 1024**2
DOCUMENT_LIMIT = 262144


def _invalid_constant(value: str) -> NoReturn:
    raise ValueError(f"json=nonfinite value={value}")


def _pairs(items: list[tuple[str, Any]]) -> Node:
    result = {}
    for key, value in items:
        require(key not in result, "json=duplicate_key")
        result[key] = value
    return result


def _decimal_text(value: Decimal) -> str:
    sign, coefficients, exponent = value.as_tuple()
    require(type(exponent) is int, "json=nonfinite")
    exponent = cast("int", exponent)
    digits = "".join(str(item) for item in coefficients)
    if not any(coefficients):
        return "0"
    while digits.endswith("0"):
        digits, exponent = digits[:-1], exponent + 1
    point = len(digits) + exponent
    length = (point if exponent >= 0 else len(digits) + 1) if point > 0 else 2 - exponent
    require(length + sign <= 4096, "json=number_expansion")
    if exponent >= 0:
        text = digits + "0" * exponent
    elif point > 0:
        text = digits[:point] + "." + digits[point:]
    else:
        text = "0." + "0" * -point + digits
    return ("-" if sign else "") + text


def _number(token: str) -> Decimal:
    require(len(token) <= 1024, "json=number_token")
    parts = re.split("[eE]", token)
    coefficient = parts[0].lstrip("-").replace(".", "")
    require(len(coefficient) <= 1024, "json=number_coefficient")
    if len(parts) == 2:
        exponent = parts[1].lstrip("+-").lstrip("0") or "0"
        require(len(exponent) <= 4 and int(exponent) <= 4096, "json=number_exponent")
    value = Decimal(token)
    _decimal_text(value)
    return value


def _integer(token: str) -> int:
    require(len(token) <= 1024, "json=number_token")
    return int(token)


def _no_float(token: str) -> NoReturn:
    raise ValueError("json=protocol_number_requires_integer_or_decimal_string")


def decode_json(data: bytes, state: bool = False, limit: int = DOCUMENT_LIMIT) -> Any:
    """Returns: JSON estrito, com decimais exatos somente no conteúdo de state/plan."""
    require(type(data) is bytes and 0 < len(data) <= limit, "json=size")
    require(not data.startswith(b"\xef\xbb\xbf"), "json=bom")
    try:
        return json.loads(
            data.decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_int=_number if state else _integer,
            parse_float=_number if state else _no_float,
            parse_constant=_invalid_constant,
        )
    except (UnicodeError, RecursionError) as error:
        raise ValueError("json=encoding_or_depth") from error


def _strings(value: str) -> Iterator[bytes]:
    yield b'"'
    for offset in range(0, len(value), 1024):
        chunk = json.dumps(value[offset : offset + 1024], ensure_ascii=False, allow_nan=False)
        yield chunk[1:-1].encode("utf-8")
    yield b'"'


def _containers(value: Any) -> Iterator[bytes]:
    mapping = type(value) is dict
    yield b"{" if mapping else b"["
    keys = sorted(value) if mapping else range(len(value))
    for index, key in enumerate(keys):
        if index:
            yield b","
        if mapping:
            require(type(key) is str, "json=key_type")
            yield from _strings(cast("str", key))
            yield b":"
        yield from _chunks(value[key])
    yield b"}" if mapping else b"]"


def _chunks(value: Any) -> Iterator[bytes]:
    if type(value) is str:
        yield from _strings(value)
    elif type(value) in (dict, list):
        yield from _containers(value)
    elif type(value) is Decimal:
        yield _decimal_text(value).encode("ascii")
    else:
        require(value is None or type(value) in (bool, int), "json=value_type")
        yield json.dumps(value, allow_nan=False).encode("ascii")


def canonical_bytes(value: Any, limit: int = DOCUMENT_LIMIT) -> bytes:
    """Returns: UTF-8 ordenado e compacto com LF único, respeitando orçamento em bytes."""
    result = bytearray()
    try:
        for chunk in _chunks(value):
            require(len(result) + len(chunk) + 1 <= limit, "json=output_size")
            result.extend(chunk)
    except (UnicodeError, RecursionError) as error:
        raise ValueError("json=encoding_or_depth") from error
    result.extend(b"\n")
    return bytes(result)


def canonical_state(data: bytes) -> bytes:
    """Returns: state JSON integral canônico sem roundtrip de float ou expansão ilimitada."""
    return canonical_bytes(decode_json(data, state=True, limit=STATE_LIMIT), STATE_LIMIT)


def parse_document(data: bytes) -> Node:
    """Returns: documento A3 somente se seus bytes já forem canônicos."""
    value = decode_json(data)
    require(type(value) is dict and canonical_bytes(value) == data, "json=noncanonical")
    return value


def object_hash(value: Any, limit: int = DOCUMENT_LIMIT) -> str:
    """Returns: SHA-256 da serialização canônica A3, incluindo um LF."""
    return hashlib.sha256(canonical_bytes(value, limit)).hexdigest()
