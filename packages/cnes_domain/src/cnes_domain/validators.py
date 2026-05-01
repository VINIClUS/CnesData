"""Domain validators for CPF/CNS/competencia."""

from __future__ import annotations


class InvalidCPFError(ValueError):
    pass


class InvalidCNSError(ValueError):
    pass


class InvalidCompetenciaError(ValueError):
    pass


_CPF_SEQUENTIAL_INVALID = frozenset(
    {
        "00000000000",
        "11111111111",
        "22222222222",
        "33333333333",
        "44444444444",
        "55555555555",
        "66666666666",
        "77777777777",
        "88888888888",
        "99999999999",
    }
)


def validate_cpf(cpf: str) -> None:
    if not isinstance(cpf, str) or len(cpf) != 11 or not cpf.isdigit():
        raise InvalidCPFError(f"cpf_invalid cpf={cpf!r}")
    if cpf in _CPF_SEQUENTIAL_INVALID:
        raise InvalidCPFError(f"cpf_sequential cpf={cpf!r}")


def validate_cns(cns: str) -> None:
    if not isinstance(cns, str) or len(cns) != 15 or not cns.isdigit():
        raise InvalidCNSError(f"cns_invalid cns={cns!r}")


def validate_competencia(c: int) -> None:
    if (
        not isinstance(c, int)
        or isinstance(c, bool)
        or c < 200001
        or c > 209912
        or (c % 100) not in range(1, 13)
    ):
        raise InvalidCompetenciaError(f"competencia_invalid value={c}")
