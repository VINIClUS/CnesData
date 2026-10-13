"""Quota mensal de tentativas de start: período UTC, comando validado e decisão pura."""

from collections.abc import Callable
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, cast

import pytest

from cnes_domain.control_plane.execution_quota import (
    MONTHLY_ATTEMPT_LIMIT,
    AttemptOutcome,
    AttemptStatus,
    ConsumeExecutionAttempt,
    decide_attempt,
    execution_period,
)

NOW = datetime(2026, 10, 13, 12, tzinfo=UTC)
NAIVE = datetime(2026, 10, 13, 12)  # noqa: DTZ001
OFFSET = NOW.astimezone(timezone(timedelta(hours=-3)))
PERIOD = "2026-10"
ENV = "prod"
DISPATCH = "0123456789abcdef"
LIMIT = 200


def _ids(count: int) -> frozenset[str]:
    return frozenset(f"{index:016x}" for index in range(count))


def _attempt(dispatch_id: str = DISPATCH, *, now: datetime = NOW) -> ConsumeExecutionAttempt:
    return ConsumeExecutionAttempt(ENV, execution_period(now), dispatch_id, now)


def test_limite_padrao_e_200_tentativas_por_mes() -> None:
    assert MONTHLY_ATTEMPT_LIMIT == LIMIT
    assert _attempt().limit == LIMIT


def test_tentativa_199_para_200_passa() -> None:
    outcome = decide_attempt(199, _ids(199), _attempt())

    assert outcome == AttemptOutcome(AttemptStatus.COUNTED, 200, LIMIT)


def test_tentativa_200_para_201_rejeita() -> None:
    outcome = decide_attempt(200, _ids(200), _attempt())

    assert outcome == AttemptOutcome(AttemptStatus.EXHAUSTED, 200, LIMIT)


def test_replay_do_mesmo_dispatch_nao_conta_de_novo() -> None:
    first = decide_attempt(0, frozenset(), _attempt())

    replay = decide_attempt(first.attempts, frozenset({DISPATCH}), _attempt())

    assert first == AttemptOutcome(AttemptStatus.COUNTED, 1, LIMIT)
    assert replay == AttemptOutcome(AttemptStatus.ALREADY_COUNTED, 1, LIMIT)


def test_replay_com_contador_no_limite_ja_esta_contado() -> None:
    outcome = decide_attempt(200, _ids(199) | {DISPATCH}, _attempt())

    assert outcome == AttemptOutcome(AttemptStatus.ALREADY_COUNTED, 200, LIMIT)


def test_virada_do_mes_conta_de_novo_no_mes_novo() -> None:
    # Conservative by design: the dispatch set is monthly, so a replay that crosses the turn of
    # the month counts once more in the new month. The counter may over-count, never under-count.
    october = datetime(2026, 10, 31, 23, 59, 59, 999999, tzinfo=UTC)
    november = october + timedelta(microseconds=1)
    ledgers = {"2026-10": frozenset({DISPATCH}), "2026-11": frozenset[str]()}

    late = _attempt(now=october)
    early = _attempt(now=november)

    assert (late.period, early.period) == ("2026-10", "2026-11")
    assert decide_attempt(1, ledgers[late.period], late).status is AttemptStatus.ALREADY_COUNTED
    assert decide_attempt(0, ledgers[early.period], early) == AttemptOutcome(
        AttemptStatus.COUNTED, 1, LIMIT,
    )


def test_limite_configurado_vale_na_fronteira() -> None:
    command = ConsumeExecutionAttempt(ENV, PERIOD, DISPATCH, NOW, 1)

    assert decide_attempt(0, frozenset(), command) == AttemptOutcome(AttemptStatus.COUNTED, 1, 1)
    assert decide_attempt(1, _ids(1), command) == AttemptOutcome(AttemptStatus.EXHAUSTED, 1, 1)


@pytest.mark.parametrize(
    ("instant", "period"),
    [
        (datetime(2026, 1, 1, tzinfo=UTC), "2026-01"),
        (datetime(2026, 10, 31, 23, 59, 59, 999999, tzinfo=UTC), "2026-10"),
        (datetime(2026, 11, 1, tzinfo=UTC), "2026-11"),
        (datetime(2026, 12, 31, 23, 59, tzinfo=timezone(timedelta(0))), "2026-12"),
        (datetime(999, 2, 1, tzinfo=UTC), "0999-02"),
    ],
)
def test_periodo_e_o_mes_utc_do_instante(instant: datetime, period: str) -> None:
    assert execution_period(instant) == period


@pytest.mark.parametrize("instant", [NAIVE, OFFSET, "2026-10-13T12:00:00+00:00", None])
def test_periodo_exige_instante_utc(instant: object) -> None:
    with pytest.raises(ValueError, match="reason=datetime_not_utc field=now"):
        execution_period(cast("datetime", instant))


@pytest.mark.parametrize(
    ("build", "reason"),
    [
        (lambda: ConsumeExecutionAttempt("Prod", PERIOD, DISPATCH, NOW), "invalid_environment"),
        (lambda: ConsumeExecutionAttempt(ENV, "2026-11", DISPATCH, NOW), "period_mismatch"),
        (lambda: ConsumeExecutionAttempt(ENV, "2026-10-13", DISPATCH, NOW), "period_mismatch"),
        (lambda: ConsumeExecutionAttempt(ENV, PERIOD, DISPATCH, NAIVE), "datetime_not_utc"),
        (lambda: ConsumeExecutionAttempt(ENV, PERIOD, DISPATCH, OFFSET), "datetime_not_utc"),
        (lambda: ConsumeExecutionAttempt(ENV, PERIOD, DISPATCH, NOW, 0), "out_of_range"),
        (lambda: ConsumeExecutionAttempt(ENV, PERIOD, DISPATCH, NOW, True), "out_of_range"),
        (lambda: ConsumeExecutionAttempt(ENV, PERIOD, DISPATCH, NOW, cast("int", "200")),
         "out_of_range"),
    ],
)
def test_comando_valida_ambiente_periodo_instante_dispatch_e_limite(
    build: Callable[[], object], reason: str,
) -> None:
    with pytest.raises(ValueError, match=f"reason={reason}"):
        build()


@pytest.mark.parametrize(
    ("build", "field"),
    [
        (lambda: ConsumeExecutionAttempt(ENV, "2026-09", DISPATCH, NOW), "period"),
        (lambda: ConsumeExecutionAttempt(ENV, PERIOD, " ", NOW), "dispatch_id"),
        (lambda: ConsumeExecutionAttempt(ENV, PERIOD, DISPATCH, NOW, -1), "limit"),
        (lambda: ConsumeExecutionAttempt(ENV, PERIOD, DISPATCH, NAIVE), "now"),
    ],
)
def test_erro_de_validacao_nomeia_o_campo(build: Callable[[], object], field: str) -> None:
    with pytest.raises(ValueError, match=f"field={field}$"):
        build()


@pytest.mark.parametrize(
    "dispatch_id",
    [
        " " * 16, "0123456789ABCDEF", "0123456789abcde", "0123456789abcdef0", "a" * 1024,
        "", " 123456789abcdef", "0123456789abcdef\n", "0123456789abcdeg", None,
    ],
    ids=[
        "espacos", "maiusculas", "15-chars", "17-chars", "1024-chars", "vazio", "espaco-inicial",
        "quebra-de-linha", "nao-hex", "nao-texto",
    ],
)
def test_rejeita_dispatch_id_fora_do_formato_hex16(dispatch_id: object) -> None:
    with pytest.raises(ValueError, match=r"reason=invalid_hex16 field=dispatch_id$"):
        ConsumeExecutionAttempt(ENV, PERIOD, cast("str", dispatch_id), NOW)


def test_comando_e_resultado_sao_imutaveis() -> None:
    outcome = decide_attempt(0, frozenset(), _attempt())

    with pytest.raises(FrozenInstanceError):
        cast("Any", _attempt()).limit = 201
    with pytest.raises(FrozenInstanceError):
        cast("Any", outcome).status = AttemptStatus.EXHAUSTED


def test_status_da_tentativa_tem_valores_estaveis() -> None:
    assert {status.value for status in AttemptStatus} == {
        "counted", "already_counted", "exhausted",
    }
