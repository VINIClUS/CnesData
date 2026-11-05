"""Exemplos fixos de retenção, capacidade e inventário da release estática."""

from dataclasses import replace
from datetime import timedelta, timezone

import pytest

from cnes_domain.orchestration.static_release import (
    ReleaseObject,
    ReleaseOutcome,
    StaticBucketSnapshot,
    VersionRef,
    plan_static_release,
)
from packages.cnes_domain.tests.orchestration.test_static_release import (
    AGORA,
    ANTES,
    ASSET,
    FONTE,
    SEGREDO,
    TETO,
    VELHO,
    _candidato,
    _objeto,
    _recusado,
    _snapshot,
    _versao,
)


def test_retirada_exige_mais_de_sete_dias_desde_a_substituicao() -> None:
    no_limite = _versao("index.html", "limite", AGORA - timedelta(days=7))
    passou = _versao("index.html", "passou", AGORA - timedelta(days=7, seconds=1))
    decision = plan_static_release(_snapshot((no_limite, passou)), _candidato(), AGORA)
    assert decision.retire == (VersionRef("index.html", "passou"),)


def test_retirada_ordenada_por_chave_e_versao() -> None:
    versoes = (
        _versao("theme-init.js", "a", VELHO),
        _versao("index.html", "b", VELHO),
        _versao("index.html", "a", VELHO),
    )
    decision = plan_static_release(_snapshot(versoes), _candidato(), AGORA)
    assert decision.retire == (
        VersionRef("index.html", "a"),
        VersionRef("index.html", "b"),
        VersionRef("theme-init.js", "a"),
    )


def test_asset_de_release_antiga_vira_candidato_a_delete_marker() -> None:
    antigo = "assets/index-AAAAAAAA.js"
    decision = plan_static_release(_snapshot((_versao(antigo, "v1"),)), _candidato(), AGORA)
    assert decision.retire_latest == (VersionRef(antigo, "v1"),)


def test_delete_marker_exige_mais_de_sete_dias_desde_a_escrita() -> None:
    no_limite = replace(
        _versao("assets/index-AAAAAAAA.js", "v"), modified_at=AGORA - timedelta(days=7)
    )
    passou = replace(
        _versao("assets/index-BBBBBBBB.js", "v"), modified_at=AGORA - timedelta(days=7, seconds=1)
    )
    decision = plan_static_release(_snapshot((no_limite, passou)), _candidato(), AGORA)
    assert decision.retire_latest == (passou.ref,)


def test_referencias_e_candidato_nao_viram_delete_marker() -> None:
    index_atual = _versao("index.html", "atual")
    index_anterior = _versao("index.html", "anterior", VELHO)
    atual = _versao("assets/index-AAAAAAAA.js", "atual")
    anterior = _versao("assets/index-BBBBBBBB.js", "anterior")
    candidato_publicado = _versao(ASSET, "v1", sha256=_objeto(ASSET).sha256)
    snapshot = _snapshot(
        (index_atual, index_anterior, atual, anterior, candidato_publicado),
        atual=frozenset({index_atual.ref, atual.ref}),
        anterior=frozenset({index_anterior.ref, anterior.ref}),
    )
    assert plan_static_release(snapshot, _candidato(), AGORA).retire_latest == ()


def test_chave_referenciada_em_outra_versao_nao_vira_delete_marker() -> None:
    chave = "assets/index-AAAAAAAA.js"
    index = _versao("index.html", "v")
    referenciada = _versao(chave, "v1", VELHO)
    mais_nova = _versao(chave, "v2")
    snapshot = _snapshot(
        (index, referenciada, mais_nova), atual=frozenset({index.ref, referenciada.ref})
    )
    assert plan_static_release(snapshot, _candidato(), AGORA).retire_latest == ()


def test_entrypoint_fora_do_candidato_nao_vira_delete_marker() -> None:
    antigo = _versao("theme-init.js", "v1")
    candidato = (_objeto("index.html"), _objeto(ASSET))
    assert plan_static_release(_snapshot((antigo,)), candidato, AGORA).retire_latest == ()


def test_total_soma_versoes_incompletos_e_proxima_release() -> None:
    velha = _versao("index.html", "velha", VELHO, tamanho=100)
    atual = _versao("theme-init.js", "atual", tamanho=50)
    snapshot = _snapshot((velha, atual), incompletos=7)
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    novos = sum(objeto.size for objeto in _candidato())
    assert decision.projected_bytes == 100 + 50 + 7 + novos


def test_asset_ja_publicado_nao_conta_duas_vezes_no_total() -> None:
    objeto = _objeto(ASSET)
    publicado = _versao(ASSET, "v1", tamanho=objeto.size, sha256=objeto.sha256)
    decision = plan_static_release(_snapshot((publicado,)), _candidato(), AGORA)
    assert decision.projected_bytes == sum(item.size for item in _candidato())


def test_total_abaixo_do_teto_planeja() -> None:
    novos = sum(objeto.size for objeto in _candidato())
    snapshot = _snapshot((_versao("theme-init.js", "v", tamanho=TETO - novos - 1),))
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    assert decision.outcome == ReleaseOutcome.PLANNED
    assert decision.projected_bytes == TETO - 1


def test_total_igual_ao_teto_recusa() -> None:
    novos = sum(objeto.size for objeto in _candidato())
    snapshot = _snapshot((_versao("theme-init.js", "v", tamanho=TETO - novos),))
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_CAPACITY)
    assert decision.projected_bytes == TETO


def test_upload_incompleto_conta_para_o_teto() -> None:
    novos = sum(objeto.size for objeto in _candidato())
    decision = plan_static_release(_snapshot(incompletos=TETO - novos), _candidato(), AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_CAPACITY)


def test_capacidade_recusada_ainda_devolve_o_que_libera_espaco() -> None:
    index = _versao("index.html", "atual")
    orfa = _versao("index.html", "orfa", VELHO, tamanho=TETO)
    antigo = _versao("assets/index-AAAAAAAA.js", "v1")
    snapshot = _snapshot((index, orfa, antigo), atual=frozenset({index.ref}))
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    assert decision.outcome == ReleaseOutcome.REJECTED_CAPACITY
    assert decision.upload_order == ()
    assert decision.rollback_entrypoint is None
    assert decision.retire == (orfa.ref,)
    assert decision.retire_latest == (antigo.ref,)
    novos = sum(objeto.size for objeto in _candidato())
    assert decision.projected_bytes == TETO + 10 + 10 + novos


def test_inventario_truncado_recusa() -> None:
    decision = plan_static_release(_snapshot(complete=False), _candidato(), AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_INCOMPLETE)


@pytest.mark.parametrize("campo", ["atual", "anterior"])
def test_referencia_sem_versao_no_inventario_recusa(campo: str) -> None:
    index = _versao("index.html", "v1")
    fantasma = frozenset({VersionRef("index.html", "fantasma")})
    referencias = {"atual": frozenset({index.ref}), "anterior": frozenset()}
    referencias[campo] = fantasma
    snapshot = _snapshot((index,), atual=referencias["atual"], anterior=referencias["anterior"])
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_INCOMPLETE)


def test_anterior_sem_atual_recusa() -> None:
    index = _versao("index.html", "v1")
    snapshot = _snapshot((index,), anterior=frozenset({index.ref}))
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_INVALID)


def test_versao_duplicada_no_inventario_recusa() -> None:
    index = _versao("index.html", "v1")
    decision = plan_static_release(_snapshot((index, index)), _candidato(), AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_INVALID)


def test_duas_versoes_latest_da_mesma_chave_recusa() -> None:
    versoes = (_versao("index.html", "v1"), _versao("index.html", "v2"))
    decision = plan_static_release(_snapshot(versoes), _candidato(), AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_INVALID)


@pytest.mark.parametrize(
    "snapshot",
    [
        _snapshot(incompletos=-1),
        _snapshot((replace(_versao("index.html", "v"), size=-1),)),
        _snapshot((replace(_versao("index.html", "v"), modified_at=ANTES.replace(tzinfo=None)),)),
        _snapshot((_versao("index.html", "v", ANTES.replace(tzinfo=None)),)),
        _snapshot((replace(_versao("index.html", "v"), modified_at=AGORA + timedelta(seconds=1)),)),
        _snapshot((_versao("index.html", "v", AGORA + timedelta(seconds=1)),)),
        _snapshot((_versao("index.html", "v", ANTES - timedelta(seconds=1)),)),
    ],
    ids=[
        "incompleto_negativo",
        "tamanho_negativo",
        "modificado_ingenuo",
        "substituido_ingenuo",
        "modificado_no_futuro",
        "substituido_no_futuro",
        "substituido_antes_de_escrito",
    ],
)
def test_inventario_invalido_recusa(snapshot: StaticBucketSnapshot) -> None:
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_INVALID)


def test_relogio_ingenuo_recusa() -> None:
    decision = plan_static_release(_snapshot(), _candidato(), AGORA.replace(tzinfo=None))
    _recusado(decision, ReleaseOutcome.REJECTED_INVALID)


def test_janela_usa_instante_e_nao_fuso() -> None:
    fuso = timezone(timedelta(hours=-3))
    limite = (AGORA - timedelta(days=7)).astimezone(fuso)
    passou = (AGORA - timedelta(days=7, seconds=1)).astimezone(fuso)
    versoes = (_versao("index.html", "limite", limite), _versao("index.html", "passou", passou))
    decision = plan_static_release(_snapshot(versoes), _candidato(), AGORA)
    assert decision.retire == (VersionRef("index.html", "passou"),)


@pytest.mark.parametrize(
    ("snapshot", "candidato", "esperado"),
    [
        (_snapshot(incompletos=-1, complete=False), _candidato(), "REJECTED_INCOMPLETE"),
        (_snapshot(), (_objeto(ASSET, SEGREDO),), "REJECTED_INVALID"),
        (
            _snapshot((_versao(ASSET, "v1", sha256="b" * 64),)),
            (*_candidato()[:3], _objeto(FONTE, SEGREDO)),
            "REJECTED_SECRET",
        ),
        (
            _snapshot((_versao(ASSET, "v1", sha256="b" * 64, tamanho=TETO),)),
            _candidato(),
            "REJECTED_OVERWRITE",
        ),
    ],
    ids=[
        "incompleto_antes_de_invalido",
        "invalido_antes_de_segredo",
        "segredo_antes_de_overwrite",
        "overwrite_antes_de_capacidade",
    ],
)
def test_precedencia_entre_recusas(
    snapshot: StaticBucketSnapshot, candidato: tuple[ReleaseObject, ...], esperado: str
) -> None:
    decision = plan_static_release(snapshot, candidato, AGORA)
    _recusado(decision, ReleaseOutcome(esperado))


def test_asset_com_delete_marker_fica_sem_versao_recente_e_pode_voltar() -> None:
    """Delete marker não é versão: só define o superseded_at da versão anterior."""
    anterior = _versao(ASSET, "v1", AGORA - timedelta(days=3))
    decision = plan_static_release(_snapshot((anterior,)), _candidato(), AGORA)
    assert decision.outcome is ReleaseOutcome.PLANNED
    assert ASSET in decision.upload_order
    assert decision.retire_latest == ()


def test_asset_com_hash_desconhecido_na_versao_recente_recusa_sobrescrita() -> None:
    desconhecido = _versao(ASSET, "v1", sha256=None)
    decision = plan_static_release(_snapshot((desconhecido,)), _candidato(), AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_OVERWRITE)
