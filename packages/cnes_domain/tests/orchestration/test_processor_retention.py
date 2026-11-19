"""Exemplos fixos de admissão, proteção e recusa sem ações destrutivas."""

from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from typing import cast

import pytest

from cnes_domain.orchestration.processor_retention import (
    ProcessorImageReferences,
    ProcessorRetentionDecision,
    ProcessorRetentionSnapshot,
    RetainedProcessorDigest,
    RetentionOutcome,
    plan_processor_retention,
)

ATUAL = "sha256:" + "a" * 64
ANTERIOR = "sha256:" + "b" * 64
EM_VOO = "sha256:" + "c" * 64
REVISAO = "sha256:" + "d" * 64
LIVRE = "sha256:" + "e" * 64
NOVO = "sha256:" + "f" * 64
EXTRA = "sha256:" + "1" * 64
OUTRO = "sha256:" + "2" * 64
AGORA = datetime(2026, 10, 30, 12, tzinfo=UTC)


class _FoldDependentTimezone(tzinfo):
    def utcoffset(self, dt: datetime | None) -> timedelta:
        return timedelta(hours=0 if dt is None else dt.fold)

    def dst(self, dt: datetime | None) -> timedelta:
        return timedelta(0)


def _imagem(digest: str, instante: str = "2026-10-29T12:00:00+00:00") -> RetainedProcessorDigest:
    return RetainedProcessorDigest(digest, datetime.fromisoformat(instante))


def _referencias(
    current: frozenset[str] = frozenset({ATUAL}),
    prior: frozenset[str] = frozenset(),
    in_flight: frozenset[str] = frozenset(),
    retained_revisions: frozenset[str] = frozenset(),
) -> ProcessorImageReferences:
    return ProcessorImageReferences(current, prior, in_flight, retained_revisions)


def _snapshot(
    history: tuple[RetainedProcessorDigest, ...],
    references: ProcessorImageReferences | None = None,
    complete: bool = True,
) -> ProcessorRetentionSnapshot:
    return ProcessorRetentionSnapshot(history, references or _referencias(), complete)


def _planejado(
    decision: ProcessorRetentionDecision, retained: frozenset[str], prune: tuple[str, ...] = ()
) -> None:
    assert decision.outcome == RetentionOutcome.PLANNED
    assert decision.retained == retained
    assert decision.prune == prune


def _recusado(
    decision: ProcessorRetentionDecision,
    snapshot: ProcessorRetentionSnapshot,
    outcome: RetentionOutcome,
) -> None:
    assert decision.outcome == outcome
    assert decision.prune == ()
    assert decision.retained == frozenset(image.digest for image in snapshot.history)


def test_admite_quinto_digest_sem_podar() -> None:
    snapshot = _snapshot(tuple(_imagem(d) for d in (ATUAL, ANTERIOR, EM_VOO, REVISAO)))
    decision = plan_processor_retention(snapshot, _imagem(NOVO), AGORA)
    _planejado(decision, frozenset({ATUAL, ANTERIOR, EM_VOO, REVISAO, NOVO}))


def test_sexto_remove_somente_mais_antigo_seguro() -> None:
    snapshot = _snapshot(
        (
            _imagem(ATUAL),
            _imagem(ANTERIOR, "2026-10-05T12:00:00+00:00"),
            _imagem(EM_VOO),
            _imagem(REVISAO),
            _imagem(LIVRE),
        ),
    )
    decision = plan_processor_retention(snapshot, _imagem(NOVO), AGORA)
    _planejado(decision, frozenset({ATUAL, EM_VOO, REVISAO, LIVRE, NOVO}), (ANTERIOR,))


def test_conta_digest_uma_vez_entre_rotas_e_referencias() -> None:
    refs = _referencias(
        frozenset({ATUAL, ANTERIOR, EM_VOO}),
        frozenset({ATUAL, ANTERIOR, EM_VOO}),
        frozenset({ATUAL, ANTERIOR, EM_VOO}),
        frozenset({ATUAL, ANTERIOR, EM_VOO}),
    )
    snapshot = _snapshot(tuple(_imagem(d) for d in (ATUAL, ANTERIOR, EM_VOO, REVISAO)), refs)
    _planejado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        frozenset({ATUAL, ANTERIOR, EM_VOO, REVISAO, NOVO}),
    )


@pytest.mark.parametrize("protection", ["current", "prior", "in_flight", "retained_revisions"])
def test_preserva_cada_classe_antiga_e_poda_mais_jovem_seguro(protection: str) -> None:
    refs = replace(_referencias(), **{protection: frozenset({ATUAL, ANTERIOR})})
    snapshot = _snapshot(
        (
            _imagem(ATUAL),
            _imagem(ANTERIOR, "2026-07-02T12:00:00+00:00"),
            _imagem(EM_VOO, "2026-10-10T12:00:00+00:00"),
            _imagem(REVISAO),
            _imagem(LIVRE),
        ),
        refs,
    )
    _planejado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        frozenset({ATUAL, ANTERIOR, REVISAO, LIVRE, NOVO}),
        (EM_VOO,),
    )


def test_preserva_uniao_pre_transicao_de_todas_as_referencias_antigas() -> None:
    refs = _referencias(
        frozenset({ATUAL}), frozenset({ANTERIOR}), frozenset({EM_VOO}), frozenset({REVISAO})
    )
    snapshot = _snapshot(
        tuple(_imagem(d, "2026-06-01T12:00:00+00:00") for d in (ATUAL, ANTERIOR, EM_VOO, REVISAO))
        + (_imagem(LIVRE),),
        refs,
    )
    _planejado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        frozenset({ATUAL, ANTERIOR, EM_VOO, REVISAO, NOVO}),
        (LIVRE,),
    )


def test_trinta_dias_exatos_permanecem_um_microssegundo_alem_expira_sem_pressao() -> None:
    snapshot = _snapshot(
        (
            _imagem(ATUAL),
            _imagem(ANTERIOR, "2026-09-30T12:00:00+00:00"),
            _imagem(EM_VOO, "2026-09-30T11:59:59.999999+00:00"),
        ),
    )
    _planejado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        frozenset({ATUAL, ANTERIOR, NOVO}),
        (EM_VOO,),
    )


def test_poda_expirados_primeiro_depois_mais_antigo_seguro_ate_cinco() -> None:
    snapshot = _snapshot(
        (
            _imagem(ATUAL),
            _imagem(ANTERIOR, "2026-08-31T12:00:00+00:00"),
            _imagem(EM_VOO, "2026-09-20T12:00:00+00:00"),
            _imagem(REVISAO, "2026-10-05T12:00:00+00:00"),
            _imagem(LIVRE, "2026-10-10T12:00:00+00:00"),
            _imagem(EXTRA, "2026-10-20T12:00:00+00:00"),
            _imagem(OUTRO),
        ),
    )
    _planejado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        frozenset({ATUAL, LIVRE, EXTRA, OUTRO, NOVO}),
        (ANTERIOR, EM_VOO, REVISAO),
    )


def test_desempata_por_digest_sem_remover_empate_inteiro() -> None:
    snapshot = _snapshot(
        (
            _imagem(ATUAL),
            _imagem(EM_VOO, "2026-10-05T12:00:00+00:00"),
            _imagem(ANTERIOR, "2026-10-05T12:00:00+00:00"),
            _imagem(REVISAO),
            _imagem(LIVRE),
        ),
    )
    _planejado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        frozenset({ATUAL, EM_VOO, REVISAO, LIVRE, NOVO}),
        (ANTERIOR,),
    )


def test_ordena_todos_expirados_por_instante_utc_e_digest() -> None:
    snapshot = _snapshot(
        (
            _imagem(ATUAL),
            _imagem(EM_VOO, "2026-09-01T09:00:00-03:00"),
            _imagem(ANTERIOR, "2026-09-01T12:00:00+00:00"),
            _imagem(REVISAO, "2026-08-01T12:00:00+00:00"),
        ),
    )
    _planejado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        frozenset({ATUAL, NOVO}),
        (REVISAO, ANTERIOR, EM_VOO),
    )


def test_candidato_antigo_e_protegido_mesmo_sem_referencia_previa() -> None:
    snapshot = _snapshot((_imagem(ATUAL),))
    _planejado(
        plan_processor_retention(snapshot, _imagem(NOVO, "2026-06-01T12:00:00+00:00"), AGORA),
        frozenset({ATUAL, NOVO}),
    )


@pytest.mark.parametrize("extra_expired", [False, True], ids=["cinco_protegidos", "com_expirado"])
def test_rejeita_sem_capacidade_sem_poda_oportunista(extra_expired: bool) -> None:
    protected = frozenset({ATUAL, ANTERIOR, EM_VOO, REVISAO, LIVRE})
    history = tuple(_imagem(d) for d in (ATUAL, ANTERIOR, EM_VOO, REVISAO, LIVRE))
    if extra_expired:
        history += (_imagem(EXTRA, "2026-06-01T12:00:00+00:00"),)
    snapshot = _snapshot(history, _referencias(protected))
    _recusado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        snapshot,
        RetentionOutcome.REJECTED_CAPACITY,
    )


def test_rejeita_historico_protegido_acima_do_teto_mesmo_em_replay() -> None:
    digests = (ATUAL, ANTERIOR, EM_VOO, REVISAO, LIVRE, NOVO)
    snapshot = _snapshot(tuple(_imagem(d) for d in digests), _referencias(frozenset(digests)))
    _recusado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        snapshot,
        RetentionOutcome.REJECTED_CAPACITY,
    )


def test_replay_identico_nao_adiciona_slot_nem_renova_ancora() -> None:
    snapshot = _snapshot(
        (
            _imagem(ATUAL),
            _imagem(ANTERIOR, "2026-06-01T12:00:00+00:00"),
            _imagem(EM_VOO),
            _imagem(REVISAO),
            _imagem(LIVRE),
        ),
    )
    candidate = _imagem(ANTERIOR, "2026-06-01T12:00:00+00:00")
    _planejado(
        plan_processor_retention(snapshot, candidate, AGORA),
        frozenset({ATUAL, ANTERIOR, EM_VOO, REVISAO, LIVRE}),
    )
    assert snapshot.history[1].first_admitted_at == candidate.first_admitted_at


def test_replay_aceita_mesmo_instante_em_fuso_diferente() -> None:
    snapshot = _snapshot((_imagem(ATUAL), _imagem(ANTERIOR, "2026-10-01T12:00:00+00:00")))
    _planejado(
        plan_processor_retention(snapshot, _imagem(ANTERIOR, "2026-10-01T09:00:00-03:00"), AGORA),
        frozenset({ATUAL, ANTERIOR}),
    )


@pytest.mark.parametrize("anchor", ["2026-10-01T12:00:00+00:00", "2026-05-01T12:00:00+00:00"])
def test_rejeita_replay_com_ancora_renovada_ou_alterada(anchor: str) -> None:
    snapshot = _snapshot((_imagem(ATUAL), _imagem(ANTERIOR, "2026-06-01T12:00:00+00:00")))
    _recusado(
        plan_processor_retention(snapshot, _imagem(ANTERIOR, anchor), AGORA),
        snapshot,
        RetentionOutcome.REJECTED_INVALID,
    )


def test_normaliza_agora_e_limite_de_trinta_dias_em_fusos_aware() -> None:
    snapshot = _snapshot((_imagem(ATUAL), _imagem(ANTERIOR, "2026-09-30T09:00:00-03:00")))
    now = AGORA.astimezone(timezone(timedelta(hours=5, minutes=30)))
    _planejado(
        plan_processor_retention(snapshot, _imagem(NOVO), now),
        frozenset({ATUAL, ANTERIOR, NOVO}),
    )


@pytest.mark.parametrize("reference", ["current", "prior", "in_flight", "retained_revisions"])
def test_rejeita_referencia_valida_ausente_do_historico(reference: str) -> None:
    refs = replace(_referencias(), **{reference: frozenset({ATUAL, OUTRO})})
    snapshot = _snapshot((_imagem(ATUAL), _imagem(LIVRE, "2026-06-01T12:00:00+00:00")), refs)
    _recusado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        snapshot,
        RetentionOutcome.REJECTED_INCOMPLETE,
    )


def test_referencia_ao_candidato_ausente_nao_substitui_inscricao_no_historico() -> None:
    snapshot = _snapshot((_imagem(ATUAL),), _referencias(in_flight=frozenset({NOVO})))
    _recusado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        snapshot,
        RetentionOutcome.REJECTED_INCOMPLETE,
    )


def test_rejeita_censo_incompleto_sem_podar_expirado() -> None:
    snapshot = _snapshot(
        (_imagem(ATUAL), _imagem(LIVRE, "2026-06-01T12:00:00+00:00")),
        complete=False,
    )
    _recusado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        snapshot,
        RetentionOutcome.REJECTED_INCOMPLETE,
    )


@pytest.mark.parametrize("duplicate", ["2026-10-29T12:00:00+00:00", "2026-10-01T12:00:00+00:00"])
def test_rejeita_historico_duplicado_mesmo_com_instantes_iguais(duplicate: str) -> None:
    snapshot = _snapshot((_imagem(ATUAL), _imagem(ATUAL, duplicate)))
    _recusado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        snapshot,
        RetentionOutcome.REJECTED_INVALID,
    )


@pytest.mark.parametrize(
    "field", ["history", "candidate", "current", "prior", "in_flight", "retained_revisions"]
)
def test_rejeita_digest_malformado_sem_poda(field: str) -> None:
    snapshot = _snapshot((_imagem(ATUAL),))
    candidate = _imagem(NOVO)
    if field == "history":
        snapshot = replace(snapshot, history=(*snapshot.history, _imagem("sha256:xyz")))
    elif field == "candidate":
        candidate = _imagem("sha256:xyz")
    else:
        snapshot = replace(
            snapshot, references=replace(snapshot.references, **{field: frozenset({"sha256:xyz"})})
        )
    _recusado(
        plan_processor_retention(snapshot, candidate, AGORA),
        snapshot,
        RetentionOutcome.REJECTED_INVALID,
    )


@pytest.mark.parametrize("field", ["history", "candidate", "now"])
@pytest.mark.parametrize(
    "invalid_time",
    [
        None,
        AGORA.replace(tzinfo=None),
        datetime.min.replace(tzinfo=timezone(timedelta(hours=1))),
        datetime.max.replace(tzinfo=timezone(timedelta(hours=-1))),
        AGORA.replace(tzinfo=_FoldDependentTimezone()),
    ],
    ids=["ausente", "naive", "min_mais_01", "max_menos_01", "fold"],
)
def test_rejeita_instante_invalido_ou_normalizacao_ambigua(
    field: str, invalid_time: datetime | None
) -> None:
    timestamp = cast("datetime", invalid_time)
    snapshot = _snapshot((_imagem(ATUAL),))
    candidate = _imagem(NOVO)
    now = AGORA
    if field == "history":
        snapshot = replace(snapshot, history=(RetainedProcessorDigest(ATUAL, timestamp),))
    elif field == "candidate":
        candidate = RetainedProcessorDigest(NOVO, timestamp)
    else:
        now = timestamp
    before = repr((snapshot, candidate, now))
    decision = plan_processor_retention(snapshot, candidate, now)
    assert repr((snapshot, candidate, now)) == before
    _recusado(decision, snapshot, RetentionOutcome.REJECTED_INVALID)


@pytest.mark.parametrize("field", ["history", "candidate"])
def test_rejeita_ancora_futura_mesmo_protegida(field: str) -> None:
    snapshot = _snapshot((_imagem(ATUAL),))
    candidate = _imagem(NOVO)
    future = _imagem(ATUAL if field == "history" else NOVO, "2026-10-30T12:00:00.000001+00:00")
    if field == "history":
        snapshot = replace(snapshot, history=(future,))
    else:
        candidate = future
    _recusado(
        plan_processor_retention(snapshot, candidate, AGORA),
        snapshot,
        RetentionOutcome.REJECTED_INVALID,
    )


def test_bootstrap_vazio_admite_primeira_reserva() -> None:
    snapshot = _snapshot((), _referencias(current=frozenset()))
    _planejado(plan_processor_retention(snapshot, _imagem(NOVO), AGORA), frozenset({NOVO}))


def test_bootstrap_com_reservas_em_voo_admite_nova_reserva() -> None:
    snapshot = _snapshot(
        (_imagem(ATUAL), _imagem(ANTERIOR)),
        _referencias(current=frozenset(), in_flight=frozenset({ATUAL, ANTERIOR})),
    )
    _planejado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        frozenset({ATUAL, ANTERIOR, NOVO}),
    )


def test_bootstrap_com_reservas_em_voo_aceita_replay_antes_da_ativacao() -> None:
    snapshot = _snapshot(
        (_imagem(ATUAL), _imagem(ANTERIOR)),
        _referencias(current=frozenset(), in_flight=frozenset({ATUAL, ANTERIOR})),
    )
    _planejado(
        plan_processor_retention(snapshot, _imagem(ANTERIOR), AGORA),
        frozenset({ATUAL, ANTERIOR}),
    )


@pytest.mark.parametrize("invalid", ["sem_cobertura", "com_anterior", "com_revisao"])
def test_rejeita_bootstrap_nao_exclusivo_de_reservas(invalid: str) -> None:
    refs = _referencias(current=frozenset(), in_flight=frozenset({ATUAL}))
    if invalid == "com_anterior":
        refs = replace(refs, prior=frozenset({ATUAL}))
    elif invalid == "com_revisao":
        refs = replace(refs, retained_revisions=frozenset({ATUAL}))
    snapshot = _snapshot((_imagem(ATUAL), _imagem(ANTERIOR)), refs)
    if invalid != "sem_cobertura":
        snapshot = replace(snapshot, history=(_imagem(ATUAL),))
    _recusado(
        plan_processor_retention(snapshot, _imagem(NOVO), AGORA),
        snapshot,
        RetentionOutcome.REJECTED_INVALID,
    )


def test_permutacoes_nao_mudam_plano_nem_mutam_entradas() -> None:
    history = (
        _imagem(ATUAL),
        _imagem(EM_VOO, "2026-09-01T12:00:00+00:00"),
        _imagem(ANTERIOR, "2026-09-01T12:00:00+00:00"),
        _imagem(REVISAO),
        _imagem(LIVRE),
    )
    candidate = _imagem(NOVO)
    decisions = []
    for order in (history, tuple(reversed(history)), history[2:] + history[:2]):
        snapshot = _snapshot(order)
        before = (asdict(snapshot), asdict(candidate))
        decision = plan_processor_retention(snapshot, candidate, AGORA)
        assert (asdict(snapshot), asdict(candidate)) == before
        decisions.append(decision)
    assert decisions[0] == decisions[1] == decisions[2]
    _planejado(decisions[0], frozenset({ATUAL, REVISAO, LIVRE, NOVO}), (ANTERIOR, EM_VOO))


def test_recusa_preserva_valores_de_entrada_sem_poda() -> None:
    snapshot = _snapshot(
        (_imagem(ATUAL), _imagem(LIVRE, "2026-06-01T12:00:00+00:00")),
        complete=False,
    )
    candidate = _imagem(NOVO)
    before = (asdict(snapshot), asdict(candidate))
    decision = plan_processor_retention(snapshot, candidate, AGORA)
    assert (asdict(snapshot), asdict(candidate)) == before
    _recusado(decision, snapshot, RetentionOutcome.REJECTED_INCOMPLETE)


def test_recusa_invalida_preserva_vitima_expirada_e_valores_de_entrada() -> None:
    snapshot = _snapshot((_imagem(ATUAL), _imagem(LIVRE, "2026-06-01T12:00:00+00:00")))
    candidate = _imagem("sha256:xyz")
    before = (asdict(snapshot), asdict(candidate))
    decision = plan_processor_retention(snapshot, candidate, AGORA)
    assert (asdict(snapshot), asdict(candidate)) == before
    assert decision.outcome == RetentionOutcome.REJECTED_INVALID
    assert decision.retained == frozenset({ATUAL, LIVRE})
    assert decision.prune == ()


def test_capacidade_preserva_candidato_mais_antigo_que_vitima_segura() -> None:
    snapshot = _snapshot(
        (
            _imagem(ATUAL),
            _imagem(ANTERIOR, "2026-10-10T12:00:00+00:00"),
            _imagem(EM_VOO),
            _imagem(REVISAO),
            _imagem(LIVRE),
        ),
    )
    decision = plan_processor_retention(snapshot, _imagem(NOVO, "2026-10-01T12:00:00+00:00"), AGORA)
    _planejado(decision, frozenset({ATUAL, EM_VOO, REVISAO, LIVRE, NOVO}), (ANTERIOR,))
