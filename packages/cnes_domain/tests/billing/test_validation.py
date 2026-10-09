"""Testes das invariantes compartilhadas de billing."""

import copy
import json
import pickle
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import cast

import pytest

from cnes_domain.billing import validation

_NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
_NAIVE = _NOW.replace(tzinfo=None)


@pytest.mark.parametrize("value", ["", "   ", None, 7])
def test_rejeita_identificador_vazio(value: object) -> None:
    with pytest.raises(ValueError, match="blank_value field=campo"):
        validation.require_id(value, "campo")  # type: ignore[arg-type]


def test_identificador_opcional_aceita_none_e_valor() -> None:
    validation.optional_id(None, "campo")
    validation.optional_id("ba_01", "campo")
    with pytest.raises(ValueError, match="blank_value"):
        validation.optional_id(" ", "campo")


@pytest.mark.parametrize(
    "value",
    [_NAIVE, datetime(2026, 9, 1, tzinfo=timezone(timedelta(hours=-3))), "x"],
)
def test_rejeita_instante_nao_utc(value: object) -> None:
    with pytest.raises(ValueError, match="datetime_not_utc"):
        validation.require_utc(value, "at")  # type: ignore[arg-type]


def test_instante_opcional_aceita_none_e_utc() -> None:
    validation.optional_utc(None, "at")
    validation.optional_utc(_NOW, "at")
    with pytest.raises(ValueError, match="datetime_not_utc"):
        validation.optional_utc(_NAIVE, "at")


@pytest.mark.parametrize("value", [-1, True, 1.5])
def test_rejeita_contador_negativo_ou_nao_inteiro(value: object) -> None:
    with pytest.raises(ValueError, match="negative_value"):
        validation.require_non_negative(value, "n")  # type: ignore[arg-type]


def test_contador_opcional() -> None:
    validation.require_non_negative(0, "n")
    validation.optional_non_negative(None, "n")
    validation.optional_non_negative(3, "n")
    with pytest.raises(ValueError, match="negative_value"):
        validation.optional_non_negative(-1, "n")


@pytest.mark.parametrize("value", [0, -2, False])
def test_rejeita_inteiro_nao_positivo(value: object) -> None:
    with pytest.raises(ValueError, match="positive_value_required"):
        validation.require_positive(value, "n")  # type: ignore[arg-type]
    validation.require_positive(1, "n")


@pytest.mark.parametrize("value", ["A" * 16, "a" * 15, "g" * 16, 16])
def test_rejeita_hex16_invalido(value: object) -> None:
    with pytest.raises(ValueError, match="invalid_hex16"):
        validation.require_hex16(value, "wave_id")  # type: ignore[arg-type]
    validation.require_hex16("0123456789abcdef", "wave_id")


@pytest.mark.parametrize("value", ["A" * 64, "a" * 63, None])
def test_rejeita_sha256_invalido(value: object) -> None:
    with pytest.raises(ValueError, match="invalid_sha256"):
        validation.require_sha256(value, "hash")  # type: ignore[arg-type]
    validation.require_sha256("f" * 64, "hash")


@pytest.mark.parametrize("value", ["2026-13", "2026-1", "202601", None])
def test_rejeita_competencia_invalida(value: object) -> None:
    with pytest.raises(ValueError, match="invalid_competencia"):
        validation.require_competencia(value, "competencia")  # type: ignore[arg-type]
    validation.require_competencia("2026-01", "competencia")


def test_rejeita_ids_duplicados_ou_vazios() -> None:
    validation.require_unique_ids(("a", "b"), "unit_ids")
    with pytest.raises(ValueError, match="duplicate_value"):
        validation.require_unique_ids(("a", "a"), "unit_ids")
    with pytest.raises(ValueError, match="blank_value"):
        validation.require_unique_ids(("a", ""), "unit_ids")


def test_rejeita_instante_anterior() -> None:
    validation.require_not_before(_NOW, _NOW, "ordem")
    with pytest.raises(ValueError, match="reason=ordem"):
        validation.require_not_before(_NOW - timedelta(seconds=1), _NOW, "ordem")


def test_atributos_sao_copiados_imutaveis() -> None:
    source = {"a": "x", "b": 1, "c": True, "d": None}
    frozen = validation.freeze_attributes(source, "attributes")
    source["a"] = "mutated"
    assert frozen["a"] == "x"
    with pytest.raises(TypeError):
        frozen["a"] = "y"  # type: ignore[index]


@pytest.mark.parametrize("value", [1.5, {"card": "4242"}, ["x"]])
def test_atributos_rejeitam_valor_nao_escalar(value: object) -> None:
    with pytest.raises(ValueError, match="invalid_attribute"):
        validation.freeze_attributes({"a": value}, "attributes")  # type: ignore[dict-item]


def test_atributos_rejeitam_chave_vazia() -> None:
    with pytest.raises(ValueError, match="blank_value"):
        validation.freeze_attributes({"": "x"}, "attributes")


def test_dimensoes_sao_copiadas_imutaveis() -> None:
    frozen = validation.freeze_dimensions({"plan": "pro"}, "dimensions")
    assert dict(frozen) == {"plan": "pro"}
    with pytest.raises(ValueError, match="blank_value"):
        validation.freeze_dimensions({"plan": ""}, "dimensions")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True, "1"])
def test_rejeita_numero_nao_finito(value: object) -> None:
    with pytest.raises(ValueError, match="non_finite_value"):
        validation.require_finite(value, "value")  # type: ignore[arg-type]
    validation.require_finite(1, "value")
    validation.require_finite(0.5, "value")


def test_valida_cada_campo_nomeado() -> None:
    class _Obj:
        a = "x"
        b = ""

    validation.require_fields(_Obj(), validation.require_id, ("a",))
    with pytest.raises(ValueError, match="blank_value field=b"):
        validation.require_fields(_Obj(), validation.require_id, ("a", "b"))


@pytest.mark.parametrize("value", [1, None, "true"])
def test_rejeita_booleano_invalido(value: object) -> None:
    validation.require_bool(True, "flag_not_bool")
    with pytest.raises(ValueError, match="reason=flag_not_bool"):
        validation.require_bool(value, "flag_not_bool")


def test_mapa_congelado_e_hashable_copiavel_e_serializavel() -> None:
    frozen = validation.freeze_attributes({"b": 1, "a": "x"}, "attributes")
    assert hash(frozen) == hash(validation.freeze_attributes({"a": "x", "b": 1}, "attributes"))
    assert frozen == {"a": "x", "b": 1}
    assert copy.deepcopy(frozen) == frozen
    assert pickle.loads(pickle.dumps(frozen)) == frozen  # noqa: S301
    assert len(frozen) == 2
    assert sorted(frozen) == ["a", "b"]
    assert repr(frozen) == "FrozenMapping({'b': 1, 'a': 'x'})"


def test_mapa_congelado_sobrevive_a_asdict() -> None:
    @dataclass(frozen=True)
    class _Holder:
        values: validation.FrozenMapping

    holder = _Holder(
        cast(
            "validation.FrozenMapping",
            validation.freeze_dimensions({"plan": "pro"}, "dimensions"),
        )
    )
    assert asdict(holder) == {"values": {"plan": "pro"}}
    assert json.dumps(asdict(holder)) == '{"values": {"plan": "pro"}}'


@pytest.mark.parametrize(
    "mutate",
    [
        lambda m: m.__setitem__("a", 1),
        lambda m: m.__delitem__("a"),
        lambda m: m.clear(),
        lambda m: m.pop("a"),
        lambda m: m.popitem(),
        lambda m: m.setdefault("b", 1),
        lambda m: m.update(b=1),
        lambda m: m.__ior__({"b": 1}),
    ],
)
def test_mapa_congelado_rejeita_mutacao(
    mutate: Callable[[validation.FrozenMapping], object],
) -> None:
    frozen = cast(
        "validation.FrozenMapping", validation.freeze_attributes({"a": "x"}, "attributes")
    )
    with pytest.raises(TypeError, match="reason=immutable_mapping"):
        mutate(frozen)
    assert frozen == {"a": "x"}
