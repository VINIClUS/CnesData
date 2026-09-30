"""Invariant checks shared by immutable billing value objects."""

import math
import re
from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import datetime, timedelta

_LOWER_HEX_16 = re.compile(r"^[0-9a-f]{16}$")
_LOWER_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_COMPETENCIA = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_ATTRIBUTE_TYPES = (str, int, bool, type(None))


class FrozenMapping(Mapping[str, object]):
    __slots__ = ("_items",)

    def __init__(self, items: Mapping[str, object]) -> None:
        self._items = dict(items)

    def __getitem__(self, key: str) -> object:
        return self._items[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __hash__(self) -> int:
        return hash(frozenset(self._items.items()))

    def __repr__(self) -> str:
        return f"FrozenMapping({self._items!r})"


def require_id(value: str, name: str) -> None:
    """Args: value: Identificador opaco; name: Campo validado.
    Raises: ValueError: Valor vazio ou não textual.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"reason=blank_value field={name}")


def optional_id(value: str | None, name: str) -> None:
    """Args: value: Identificador opcional; name: Campo validado.
    Raises: ValueError: Valor presente e vazio.
    """
    if value is not None:
        require_id(value, name)


def require_utc(value: datetime, name: str) -> None:
    """Args: value: Instante; name: Campo validado.
    Raises: ValueError: Instante sem timezone UTC.
    """
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() != timedelta(0)
    ):
        raise ValueError(f"reason=datetime_not_utc field={name}")


def optional_utc(value: datetime | None, name: str) -> None:
    """Args: value: Instante opcional; name: Campo validado.
    Raises: ValueError: Instante presente sem timezone UTC.
    """
    if value is not None:
        require_utc(value, name)


def require_non_negative(value: int, name: str) -> None:
    """Args: value: Contador; name: Campo validado.
    Raises: ValueError: Valor negativo ou não inteiro.
    """
    if not _is_int(value) or value < 0:
        raise ValueError(f"reason=negative_value field={name}")


def optional_non_negative(value: int | None, name: str) -> None:
    """Args: value: Contador opcional; name: Campo validado.
    Raises: ValueError: Valor presente negativo ou não inteiro.
    """
    if value is not None:
        require_non_negative(value, name)


def require_positive(value: int, name: str) -> None:
    """Args: value: Inteiro; name: Campo validado.
    Raises: ValueError: Valor menor que 1 ou não inteiro.
    """
    if not _is_int(value) or value < 1:
        raise ValueError(f"reason=positive_value_required field={name}")


def require_hex16(value: str, name: str) -> None:
    """Args: value: Identificador de wave/dispatch; name: Campo validado.
    Raises: ValueError: Valor fora de hex minúsculo com 16 caracteres.
    """
    if not isinstance(value, str) or not _LOWER_HEX_16.fullmatch(value):
        raise ValueError(f"reason=invalid_hex16 field={name}")


def require_sha256(value: str, name: str) -> None:
    """Args: value: Digest; name: Campo validado.
    Raises: ValueError: Valor fora de hex minúsculo com 64 caracteres.
    """
    if not isinstance(value, str) or not _LOWER_HEX_64.fullmatch(value):
        raise ValueError(f"reason=invalid_sha256 field={name}")


def require_competencia(value: str, name: str) -> None:
    """Args: value: Competência; name: Campo validado.
    Raises: ValueError: Valor fora de YYYY-MM.
    """
    if not isinstance(value, str) or not _COMPETENCIA.fullmatch(value):
        raise ValueError(f"reason=invalid_competencia field={name}")


def require_unique_ids(values: Iterable[str], name: str) -> None:
    """Args: values: Identificadores; name: Campo validado.
    Raises: ValueError: Identificador vazio ou duplicado.
    """
    items = tuple(values)
    for item in items:
        require_id(item, name)
    if len(set(items)) != len(items):
        raise ValueError(f"reason=duplicate_value field={name}")


def require_not_before(later: datetime, earlier: datetime, reason: str) -> None:
    """Args: later: Instante final; earlier: Instante inicial; reason: Código do erro.
    Raises: ValueError: `later` anterior a `earlier`.
    """
    if later < earlier:
        raise ValueError(f"reason={reason}")


def freeze_attributes(
    values: Mapping[str, str | int | bool | None], name: str,
) -> Mapping[str, str | int | bool | None]:
    """Args: values: Atributos escalares; name: Campo validado.
    Returns: Cópia imutável dos atributos.
    Raises: ValueError: Chave vazia ou valor não escalar.
    """
    copied = dict(values)
    for key, value in copied.items():
        require_id(key, name)
        if not isinstance(value, _ATTRIBUTE_TYPES):
            raise ValueError(f"reason=invalid_attribute field={name}")
    return FrozenMapping(copied)


def freeze_dimensions(values: Mapping[str, str], name: str) -> Mapping[str, str]:
    """Args: values: Dimensões textuais; name: Campo validado.
    Returns: Cópia imutável das dimensões.
    Raises: ValueError: Chave ou valor vazio.
    """
    copied = dict(values)
    for key, value in copied.items():
        require_id(key, name)
        require_id(value, name)
    return FrozenMapping(copied)


def require_finite(value: float, name: str) -> None:
    """Args: value: Número; name: Campo validado.
    Raises: ValueError: Valor não numérico, infinito ou NaN.
    """
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        raise ValueError(f"reason=non_finite_value field={name}")


def require_fields(
    owner: object, check: Callable[[object, str], None], names: Iterable[str],
) -> None:
    """Args: owner: Objeto validado; check: Validador; names: Campos a validar.
    Raises: ValueError: Algum campo viola o validador.
    """
    for name in names:
        check(getattr(owner, name), name)


def require_bool(value: object, reason: str) -> None:
    """Args: value: Flag; reason: Código do erro.
    Raises: ValueError: Valor não booleano.
    """
    if not isinstance(value, bool):
        raise ValueError(f"reason={reason}")


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)
