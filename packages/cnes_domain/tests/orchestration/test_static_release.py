"""Exemplos fixos de publicação, retenção e recusa da release estática."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from cnes_domain.orchestration.static_release import (
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
ANTES = AGORA - timedelta(days=90)
VELHO = AGORA - timedelta(days=60)
ASSET = "assets/index-BxvR3nbv.js"
FONTE = "assets/inter-latin-wght-normal-Dx4kXJAl.woff2"
INDEX_HTML = f'<script src="/{ASSET}"></script><link href="/{FONTE}">'.encode()
TETO = 5_000_000_000
CACHE_IMUTAVEL = "public,max-age=31536000,immutable"
CACHE_ENTRYPOINT = "no-cache"
SEGREDO = b"AKIA" + b"C" * 16
ASSETS_DO_BUILD = (
    "assets/index-BxvR3nbv.js",
    "assets/index-CSscBpPD.css",
    "assets/inter-cyrillic-ext-wght-normal-BOeWTOD4.woff2",
    "assets/inter-cyrillic-wght-normal-DqGufNeO.woff2",
    "assets/inter-greek-ext-wght-normal-DlzME5K_.woff2",
    "assets/inter-greek-wght-normal-CkhJZR-_.woff2",
    "assets/inter-latin-ext-wght-normal-DO1Apj_S.woff2",
    "assets/inter-latin-wght-normal-Dx4kXJAl.woff2",
    "assets/inter-vietnamese-wght-normal-CBcvBZtf.woff2",
    "assets/oidc-qdDnjKJv.js",
    "assets/react-DRuhoM8J.js",
    "assets/tanstack-C0n1W622.js",
)
INDEX_DO_BUILD = b"""<!doctype html>
<html lang="pt-BR">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <!-- Blocking, same-origin: applies the stored theme before first paint. -->
    <script src="/theme-init.js"></script>
    <script type="module" crossorigin src="/assets/index-BxvR3nbv.js"></script>
    <link rel="modulepreload" crossorigin href="/assets/react-DRuhoM8J.js">
    <link rel="modulepreload" crossorigin href="/assets/tanstack-C0n1W622.js">
    <link rel="modulepreload" crossorigin href="/assets/oidc-qdDnjKJv.js">
    <link rel="stylesheet" crossorigin href="/assets/index-CSscBpPD.css">
  </head>
  <body>
    <div id="root"></div>
  </body>
</html>
"""


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


def _build_real() -> tuple[ReleaseObject, ...]:
    return (
        _objeto("index.html", INDEX_DO_BUILD),
        _objeto("theme-init.js"),
        *(_objeto(key) for key in ASSETS_DO_BUILD),
    )


def _versao(key: str, version: str, substituida_em: datetime | None = None) -> StoredVersion:
    return StoredVersion(VersionRef(key, version), 10, "a" * 64, ANTES, substituida_em)


def _snapshot(
    versoes: tuple[StoredVersion, ...] = (),
    atual: frozenset[VersionRef] = frozenset(),
    anterior: frozenset[VersionRef] = frozenset(),
) -> StaticBucketSnapshot:
    return StaticBucketSnapshot(versoes, 0, atual, anterior, True)


def _recusado(decision: StaticReleaseDecision, outcome: ReleaseOutcome) -> None:
    assert decision.outcome == outcome
    assert decision.upload_order == ()
    assert decision.retire == ()
    assert decision.retire_latest == ()
    assert decision.rollback_entrypoint is None


INDEX_A = _versao("index.html", "a")
INDEX_B = _versao("index.html", "b", ANTES)
ASSET_V = _versao(ASSET, "v")


def test_assets_imutaveis_e_index_sem_cache() -> None:
    assert _objeto(ASSET).cache_control == CACHE_IMUTAVEL
    assert _objeto(FONTE).cache_control == CACHE_IMUTAVEL
    assert _objeto("index.html").cache_control == CACHE_ENTRYPOINT
    assert _objeto("theme-init.js").cache_control == CACHE_ENTRYPOINT


@pytest.mark.parametrize("key", ASSETS_DO_BUILD)
def test_chave_real_de_asset_fica_imutavel(key: str) -> None:
    assert _objeto(key).cache_control == CACHE_IMUTAVEL


@pytest.mark.parametrize(
    ("key", "content_type"),
    [
        ("index.html", "text/html; charset=utf-8"),
        ("theme-init.js", "text/javascript; charset=utf-8"),
        (ASSET, "text/javascript; charset=utf-8"),
        ("assets/index-CSscBpPD.css", "text/css; charset=utf-8"),
        (FONTE, "font/woff2"),
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
        "assets/sub/index-BxvR3nbv.js",
        "assets/index-BxvR3nbv.exe",
        "assets\\index-BxvR3nbv.js",
        "outro/index-BxvR3nbv.js",
        "/index.html",
        "INDEX.HTML",
        "index.html\n",
        "chave.pem",
        "credentials.json",
        "id_rsa",
        "backup.sql",
        "README.md",
        "manifest.webmanifest",
        "sw.js",
        "",
    ],
)
def test_rejeita_chave_fora_do_contrato(key: str) -> None:
    assert describe_release_object(key, b"x") is None


def test_descreve_tamanho_e_sha256_do_conteudo() -> None:
    objeto = _objeto(ASSET, b"abc")
    assert objeto.size == 3
    assert objeto.sha256 == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_referencias_do_index_real_sao_exatas() -> None:
    assert _objeto("index.html", INDEX_DO_BUILD).references == frozenset(
        {
            "theme-init.js",
            "assets/index-BxvR3nbv.js",
            "assets/react-DRuhoM8J.js",
            "assets/tanstack-C0n1W622.js",
            "assets/oidc-qdDnjKJv.js",
            "assets/index-CSscBpPD.css",
        }
    )


def test_so_o_index_declara_referencias() -> None:
    assert _objeto(ASSET, INDEX_DO_BUILD).references == frozenset()


@pytest.mark.parametrize(
    ("conteudo", "marcador"),
    [
        (b"const k='AKIA" + b"A" * 16 + b"';", "aws_access_key"),
        (b"const k='ASIA" + b"B" * 16 + b"';", "aws_access_key"),
        (b"-----BEGIN RSA " + b"PRIVATE KEY-----", "private_key"),
        (b"-----BEGIN " + b"PRIVATE KEY-----", "private_key"),
        (b"k='sk_" + b"live_abc123'", "stripe_secret"),
        (b"k='rk_" + b"live_abc123'", "stripe_secret"),
        (b"k='sk_" + b"test_abc123'", "stripe_secret"),
        (b"k='rk_" + b"test_abc123'", "stripe_secret"),
        (b"k='whsec_" + b"abc123'", "stripe_secret"),
    ],
)
def test_marca_segredo_no_conteudo(conteudo: bytes, marcador: str) -> None:
    objeto = _objeto(ASSET, conteudo)
    assert objeto.secret_markers == frozenset({marcador})
    assert conteudo.decode() not in repr(objeto)


def test_marca_cada_tipo_de_segredo_presente() -> None:
    conteudo = SEGREDO + b" whsec_" + b"abc123"
    assert _objeto(ASSET, conteudo).secret_markers == frozenset({"aws_access_key", "stripe_secret"})


def test_build_publico_nao_tem_marcador_de_segredo() -> None:
    publico = (
        b"VITE_API_BASE_URL='https://api.cnesdata.com.br/api/v1';"
        b"client_id='abc';AKIA;pk_live_abc;pk_test_abc;task_live_abc;desk_test_abc"
    )
    assert _objeto(ASSET, publico).secret_markers == frozenset()


@pytest.mark.parametrize("chave", ["index.html", FONTE])
def test_segredo_em_qualquer_objeto_recusa_a_release(chave: str) -> None:
    candidato = tuple(
        _objeto(item.key, INDEX_HTML + SEGREDO) if item.key == chave else item
        for item in _candidato()
    )
    decision = plan_static_release(_snapshot(), candidato, AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_SECRET)


def test_index_envia_por_ultimo_e_assets_antes_dos_outros_entrypoints() -> None:
    decision = plan_static_release(_snapshot(), _candidato(), AGORA)
    assert decision.outcome == ReleaseOutcome.PLANNED
    assert decision.upload_order == (ASSET, FONTE, "theme-init.js", "index.html")
    assert decision.already_published == ()


def test_build_real_planeja_com_fontes_fora_do_index() -> None:
    decision = plan_static_release(_snapshot(), _build_real(), AGORA)
    assert decision.outcome == ReleaseOutcome.PLANNED
    assert decision.upload_order == (*ASSETS_DO_BUILD, "theme-init.js", "index.html")


def test_primeira_release_nao_tem_alvo_de_rollback() -> None:
    assert plan_static_release(_snapshot(), _candidato(), AGORA).rollback_entrypoint is None


def test_rollback_aponta_para_versao_atual_do_index() -> None:
    index = _versao("index.html", "v2")
    outro = _versao("theme-init.js", "v9")
    snapshot = _snapshot((index, outro), atual=frozenset({index.ref, outro.ref}))
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    assert decision.rollback_entrypoint == VersionRef("index.html", "v2")


def test_rollback_escolhe_o_index_atual_e_nao_o_anterior() -> None:
    atual = _versao("index.html", "v2")
    anterior = _versao("index.html", "v1", VELHO)
    snapshot = _snapshot(
        (atual, anterior), atual=frozenset({atual.ref}), anterior=frozenset({anterior.ref})
    )
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    assert decision.rollback_entrypoint == VersionRef("index.html", "v2")


@pytest.mark.parametrize(
    ("atual", "anterior"),
    [
        ({ASSET_V.ref}, set()),
        ({INDEX_A.ref, INDEX_B.ref}, set()),
        ({INDEX_A.ref}, {ASSET_V.ref}),
        ({INDEX_A.ref}, {INDEX_A.ref, INDEX_B.ref}),
    ],
    ids=["atual_sem_index", "atual_com_dois", "anterior_sem_index", "anterior_com_dois"],
)
def test_referencia_sem_exatamente_um_index_recusa(
    atual: set[VersionRef], anterior: set[VersionRef]
) -> None:
    snapshot = _snapshot((INDEX_A, INDEX_B, ASSET_V), frozenset(atual), frozenset(anterior))
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_INVALID)


@pytest.mark.parametrize(
    "ausente",
    [
        "assets/index-BxvR3nbv.js",
        "assets/react-DRuhoM8J.js",
        "assets/index-CSscBpPD.css",
        "theme-init.js",
    ],
    ids=["script_modulo", "modulepreload", "stylesheet", "script_raiz"],
)
def test_index_referenciando_objeto_fora_da_release_recusa(ausente: str) -> None:
    candidato = tuple(item for item in _build_real() if item.key != ausente)
    decision = plan_static_release(_snapshot(), candidato, AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_INVALID)


@pytest.mark.parametrize(
    "candidato",
    [
        (),
        (_objeto(ASSET),),
        (_objeto("index.html"), _objeto("index.html")),
        (_objeto("index.html"), replace(_objeto(ASSET), cache_control=CACHE_ENTRYPOINT)),
        (replace(_objeto("index.html"), cache_control=CACHE_IMUTAVEL),),
        (_objeto("index.html"), replace(_objeto(ASSET), key="assets/sem-hash.js")),
        (_objeto("index.html"), replace(_objeto(ASSET), size=-1)),
        (_objeto("index.html"), replace(_objeto(ASSET), sha256="xyz")),
        (_objeto("index.html"), replace(_objeto(ASSET), content_type="text/html; charset=utf-8")),
    ],
    ids=[
        "vazio",
        "sem_index",
        "chave_repetida",
        "asset_sem_cache_imutavel",
        "index_imutavel",
        "chave_invalida",
        "tamanho_negativo",
        "sha256_invalido",
        "tipo_incoerente",
    ],
)
def test_candidato_malformado_recusa(candidato: tuple[ReleaseObject, ...]) -> None:
    decision = plan_static_release(_snapshot(), candidato, AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_INVALID)


def test_asset_identico_ja_publicado_nao_reenvia() -> None:
    objeto = _objeto(ASSET)
    publicado = replace(_versao(ASSET, "v1"), sha256=objeto.sha256)
    decision = plan_static_release(_snapshot((publicado,)), _candidato(), AGORA)
    assert decision.outcome == ReleaseOutcome.PLANNED
    assert decision.already_published == (ASSET,)
    assert ASSET not in decision.upload_order


@pytest.mark.parametrize("sha256", ["b" * 64, None], ids=["sha_diferente", "sem_checksum"])
def test_sobrescrever_asset_imutavel_recusa(sha256: str | None) -> None:
    publicado = replace(_versao(ASSET, "v1"), sha256=sha256)
    decision = plan_static_release(_snapshot((publicado,)), _candidato(), AGORA)
    _recusado(decision, ReleaseOutcome.REJECTED_OVERWRITE)


def test_asset_sob_delete_marker_pode_voltar_a_ser_publicado() -> None:
    retirado = replace(_versao(ASSET, "v1", VELHO), sha256="b" * 64)
    decision = plan_static_release(_snapshot((retirado,)), _candidato(), AGORA)
    assert decision.outcome == ReleaseOutcome.PLANNED
    assert ASSET in decision.upload_order


def test_entrypoint_pode_receber_nova_versao() -> None:
    index = _versao("index.html", "v1")
    decision = plan_static_release(_snapshot((index,)), _candidato(), AGORA)
    assert decision.outcome == ReleaseOutcome.PLANNED
    assert "index.html" in decision.upload_order
    assert decision.rollback_entrypoint is None
    assert decision.retire_latest == ()


def test_entrypoint_identico_nao_gera_nova_versao() -> None:
    sha256 = _objeto("index.html", INDEX_HTML).sha256
    publicado = replace(_versao("index.html", "v1"), sha256=sha256)
    decision = plan_static_release(_snapshot((publicado,)), _candidato(), AGORA)
    assert decision.outcome == ReleaseOutcome.PLANNED
    assert decision.already_published == ("index.html",)
    assert "index.html" not in decision.upload_order


def test_lifecycle_retem_atual_e_anterior() -> None:
    atual = _versao("index.html", "atual")
    anterior = _versao("index.html", "anterior", VELHO)
    orfa = _versao("index.html", "orfa", VELHO)
    snapshot = _snapshot(
        (atual, anterior, orfa), atual=frozenset({atual.ref}), anterior=frozenset({anterior.ref})
    )
    decision = plan_static_release(snapshot, _candidato(), AGORA)
    assert decision.retire == (VersionRef("index.html", "orfa"),)
