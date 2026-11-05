"""Exemplos fixos de publicação, retenção e recusa da release estática."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from cnes_domain.orchestration.static_release import (
    ENTRYPOINT_CACHE,
    IMMUTABLE_CACHE,
    ReleaseObject,
    ReleaseOutcome,
    StaticBucketSnapshot,
    StaticReleaseDecision,
    StoredVersion,
    VersionRef,
    describe_release_object,
    plan_static_release,
)

AGORA = datetime(2026, 11, 5, 12, tzinfo=UTC)
ANTES = AGORA - timedelta(days=30)
ASSET = "assets/index-BxvR3nbv.js"
FONTE = "assets/inter-latin-wght-normal-Dx4kXJAl.woff2"
INDEX_HTML = f'<script src="/{ASSET}"></script><link href="/{FONTE}">'.encode()
TETO = 5_000_000_000


def _objeto(key: str, content: bytes = b"conteudo") -> ReleaseObject:
    objeto = describe_release_object(key, content)
    assert objeto is not None
    return objeto


def _candidato() -> tuple[ReleaseObject, ...]:
    return (
        _objeto("index.html", INDEX_HTML),
        _objeto("theme-init.js"),
        _objeto(ASSET),
        _objeto(FONTE),
    )


def _versao(
    key: str,
    version: str,
    substituida_em: datetime | None = None,
    tamanho: int = 10,
    sha256: str | None = "a" * 64,
) -> StoredVersion:
    return StoredVersion(VersionRef(key, version), tamanho, sha256, ANTES, substituida_em)


def _snapshot(
    versoes: tuple[StoredVersion, ...] = (),
    atual: frozenset[VersionRef] = frozenset(),
    anterior: frozenset[VersionRef] = frozenset(),
    incompletos: int = 0,
    complete: bool = True,
) -> StaticBucketSnapshot:
    return StaticBucketSnapshot(versoes, incompletos, atual, anterior, complete)


def _recusado(decision: StaticReleaseDecision, outcome: ReleaseOutcome) -> None:
    assert decision.outcome == outcome
    assert decision.upload_order == ()
    assert decision.retire == ()
    assert decision.retire_latest == ()
    assert decision.rollback_entrypoint is None


def test_assets_imutaveis_e_index_sem_cache() -> None:
    assert _objeto(ASSET).cache_control == IMMUTABLE_CACHE
    assert _objeto(FONTE).cache_control == IMMUTABLE_CACHE
    assert _objeto("index.html").cache_control == ENTRYPOINT_CACHE
    assert _objeto("theme-init.js").cache_control == ENTRYPOINT_CACHE
    assert _objeto("manifest.webmanifest").cache_control == ENTRYPOINT_CACHE
    assert _objeto("sw.js").cache_control == ENTRYPOINT_CACHE


@pytest.mark.parametrize(
    ("key", "content_type"),
    [
        ("index.html", "text/html; charset=utf-8"),
        ("theme-init.js", "text/javascript; charset=utf-8"),
        (ASSET, "text/javascript; charset=utf-8"),
        ("assets/index-CSscBpPD.css", "text/css; charset=utf-8"),
        (FONTE, "font/woff2"),
        ("manifest.webmanifest", "application/manifest+json"),
    ],
)
def test_tipo_de_conteudo_exato_por_extensao(key: str, content_type: str) -> None:
    assert _objeto(key).content_type == content_type


@pytest.mark.parametrize(
    "key",
    [
        ".env",
        ".env.production",
        "index.js.map",
        "assets/index-BxvR3nbv.js.map",
        "assets/sem-hash.js",
        "assets/index-curto.js",
        "assets/../index.html",
        "outro/index-BxvR3nbv.js",
        "/index.html",
        "chave.pem",
        "",
    ],
)
def test_rejeita_chave_fora_do_contrato(key: str) -> None:
    assert describe_release_object(key, b"x") is None


def test_descreve_tamanho_e_sha256_do_conteudo() -> None:
    objeto = _objeto(ASSET, b"abc")
    assert objeto.size == 3
    assert objeto.sha256 == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


@pytest.mark.parametrize(
    ("conteudo", "marcador"),
    [
        (b"const k='AKIA" + b"A" * 16 + b"';", "aws_access_key"),
        (b"const k='ASIA" + b"B" * 16 + b"';", "aws_access_key"),
        (b"-----BEGIN RSA " + b"PRIVATE KEY-----", "private_key"),
        (b"-----BEGIN " + b"PRIVATE KEY-----", "private_key"),
        (b"k='sk_" + b"live_abc123'", "stripe_secret"),
        (b"k='rk_" + b"live_abc123'", "stripe_secret"),
        (b"k='whsec_" + b"abc123'", "stripe_secret"),
    ],
)
def test_marca_segredo_no_conteudo(conteudo: bytes, marcador: str) -> None:
    assert marcador in _objeto(ASSET, conteudo).secret_markers


def test_build_publico_nao_tem_marcador_de_segredo() -> None:
    publico = (
        b"VITE_API_BASE_URL='https://api.cnesdata.com.br/api/v1';"
        b"client_id='abc';AKIA;sk_test_abc;pk_live_abc"
    )
    assert _objeto(ASSET, publico).secret_markers == frozenset()


def test_segredo_no_candidato_recusa_a_release() -> None:
    candidato = (*_candidato()[:3], _objeto(FONTE, b"AKIA" + b"C" * 16))
    decision = plan_static_release(_snapshot(), candidato, AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_SECRET)


def test_index_envia_por_ultimo_e_assets_antes_dos_outros_entrypoints() -> None:
    decision = plan_static_release(_snapshot(), _candidato(), AGORA)
    assert decision.outcome == ReleaseOutcome.PLANNED
    assert decision.upload_order == (ASSET, FONTE, "theme-init.js", "index.html")
    assert decision.already_published == ()


def test_primeira_release_nao_tem_alvo_de_rollback() -> None:
    assert plan_static_release(_snapshot(), _candidato(), AGORA).rollback_entrypoint is None


def test_rollback_aponta_para_versao_atual_do_index() -> None:
    index = _versao("index.html", "v2")
    outro = _versao("theme-init.js", "v9")
    snapshot = _snapshot((index, outro), atual=frozenset({index.ref, outro.ref}))
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    assert decision.rollback_entrypoint == VersionRef("index.html", "v2")


def test_index_referenciando_asset_fora_da_release_recusa() -> None:
    candidato = (_objeto("index.html", INDEX_HTML), _objeto(ASSET))
    decision = plan_static_release(_snapshot(), candidato, AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_INVALID)


@pytest.mark.parametrize(
    "candidato",
    [
        (),
        (_objeto(ASSET),),
        (_objeto("index.html"), _objeto("index.html")),
        (_objeto("index.html"), replace(_objeto(ASSET), cache_control=ENTRYPOINT_CACHE)),
        (replace(_objeto("index.html"), cache_control=IMMUTABLE_CACHE),),
        (_objeto("index.html"), replace(_objeto(ASSET), key="assets/sem-hash.js")),
        (_objeto("index.html"), replace(_objeto(ASSET), size=-1)),
    ],
    ids=[
        "vazio",
        "sem_index",
        "chave_repetida",
        "asset_sem_cache_imutavel",
        "index_imutavel",
        "chave_invalida",
        "tamanho_negativo",
    ],
)
def test_candidato_malformado_recusa(candidato: tuple[ReleaseObject, ...]) -> None:
    decision = plan_static_release(_snapshot(), candidato, AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_INVALID)


def test_asset_identico_ja_publicado_nao_reenvia() -> None:
    objeto = _objeto(ASSET)
    publicado = _versao(ASSET, "v1", sha256=objeto.sha256)
    decision = plan_static_release(_snapshot((publicado,)), _candidato(), AGORA)
    assert decision.outcome == ReleaseOutcome.PLANNED
    assert decision.already_published == (ASSET,)
    assert ASSET not in decision.upload_order


@pytest.mark.parametrize("sha256", ["b" * 64, None], ids=["sha_diferente", "sem_checksum"])
def test_sobrescrever_asset_imutavel_recusa(sha256: str | None) -> None:
    publicado = _versao(ASSET, "v1", sha256=sha256)
    decision = plan_static_release(_snapshot((publicado,)), _candidato(), AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_OVERWRITE)


def test_entrypoint_pode_receber_nova_versao() -> None:
    index = _versao("index.html", "v1")
    decision = plan_static_release(_snapshot((index,)), _candidato(), AGORA)
    assert decision.outcome == ReleaseOutcome.PLANNED
    assert "index.html" in decision.upload_order


def test_lifecycle_retem_atual_e_anterior() -> None:
    velho = AGORA - timedelta(days=60)
    atual = _versao("index.html", "atual")
    anterior = _versao("index.html", "anterior", velho)
    orfa = _versao("index.html", "orfa", velho)
    snapshot = _snapshot(
        (atual, anterior, orfa), atual=frozenset({atual.ref}), anterior=frozenset({anterior.ref})
    )
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    assert decision.retire == (VersionRef("index.html", "orfa"),)


def test_retirada_exige_mais_de_sete_dias_desde_a_substituicao() -> None:
    no_limite = _versao("index.html", "limite", AGORA - timedelta(days=7))
    passou = _versao("index.html", "passou", AGORA - timedelta(days=7, seconds=1))
    decision = plan_static_release(_snapshot((no_limite, passou)), _candidato(), AGORA)
    assert decision.retire == (VersionRef("index.html", "passou"),)


def test_retirada_ordenada_por_chave_e_versao() -> None:
    velho = AGORA - timedelta(days=60)
    versoes = (
        _versao("theme-init.js", "a", velho),
        _versao("index.html", "b", velho),
        _versao("index.html", "a", velho),
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


def test_objeto_recente_sem_referencia_nao_vira_delete_marker() -> None:
    antigo = "assets/index-AAAAAAAA.js"
    recente = replace(_versao(antigo, "v1"), modified_at=AGORA - timedelta(days=7))
    decision = plan_static_release(_snapshot((recente,)), _candidato(), AGORA)
    assert decision.retire_latest == ()


def test_referencias_e_candidato_nao_viram_delete_marker() -> None:
    atual = _versao("assets/index-AAAAAAAA.js", "atual")
    anterior = _versao("assets/index-BBBBBBBB.js", "anterior")
    candidato_publicado = _versao(ASSET, "v1", sha256=_objeto(ASSET).sha256)
    snapshot = _snapshot(
        (atual, anterior, candidato_publicado),
        atual=frozenset({atual.ref}),
        anterior=frozenset({anterior.ref}),
    )
    assert plan_static_release(snapshot, _candidato(), AGORA).retire_latest == ()


def test_total_soma_versoes_incompletos_e_proxima_release() -> None:
    velha = _versao("index.html", "velha", AGORA - timedelta(days=60), tamanho=100)
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
    ],
    ids=[
        "incompleto_negativo",
        "tamanho_negativo",
        "modificado_ingenuo",
        "substituido_ingenuo",
        "modificado_no_futuro",
        "substituido_no_futuro",
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
    versao = _versao("index.html", "limite", limite)
    assert plan_static_release(_snapshot((versao,)), _candidato(), AGORA).retire == ()
