"""Produtor dos baselines toolchain, policy e cost: bytes canônicos e recusas (P23)."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from scripts.promotion import delivery_baselines as db
from tests.production.delivery._delivery_baselines_fixtures import (
    CONFTEST_BIN,
    COST,
    CUSTO,
    LOCK,
    POLICY,
    POLITICA,
    PROVEDORES,
    REGISTRY,
    TOFU_BIN,
    criar_binario,
    criar_espelho,
    criar_produto,
    lock_texto,
    sha,
)
from tests.production.delivery._product_source_fixtures import commit, escrever

ROOT = Path(__file__).resolve().parents[3]


def _toolchain(raiz: Path, tofu: bytes = TOFU_BIN) -> dict[str, object]:
    espelho = criar_espelho(raiz)
    binario = criar_binario(raiz, "tofu", tofu)
    return json.loads(db.build_toolchain(lock_texto(), espelho, binario))


def test_constantes_espelham_o_contrato_do_gate_e_da_spec() -> None:
    assert (db.TOFU_VERSION, db.CONFTEST_VERSION) == ("1.13.2", "0.71.1")
    assert db.DOCUMENT_LIMIT == 262144
    assert db.ARTIFACTS == {
        "delivery-baseline-toolchain": ("toolchain.json",),
        "delivery-baseline-policy": ("policy.json",),
        "delivery-baseline-cost": ("cost-manifest-source.json", "cost-context.json"),
    }


def test_toolchain_tem_forma_fechada_do_gate(tmp_path: Path) -> None:
    valor = _toolchain(tmp_path)

    assert set(valor) == {"opentofu", "providers", "lock_sha256"}
    assert valor["opentofu"] == {"version": "1.13.2", "binary_sha256": sha(TOFU_BIN)}
    assert valor["lock_sha256"] == sha(lock_texto())


def test_toolchain_package_sha256_e_o_hash_do_zip_linux_amd64_do_lock(tmp_path: Path) -> None:
    valor = _toolchain(tmp_path)

    assert valor["providers"] == [
        {"source": f"{REGISTRY}/{nome}", "version": versao, "package_sha256": sha(zip_)}
        for nome, (versao, zip_) in sorted(PROVEDORES.items())
    ]


def test_toolchain_e_canonico_com_lf_unico(tmp_path: Path) -> None:
    espelho = criar_espelho(tmp_path)
    binario = criar_binario(tmp_path, "tofu", TOFU_BIN)

    bruto = db.build_toolchain(lock_texto(), espelho, binario)

    assert bruto.endswith(b"}\n")
    assert bruto.count(b"\n") == 1
    assert b" " not in bruto
    assert bruto == db.canonical_bytes(json.loads(bruto))


def test_recusa_zip_que_nao_esta_no_conjunto_zh_do_lock(tmp_path: Path) -> None:
    espelho = criar_espelho(tmp_path)
    zip_ = espelho / REGISTRY / "aws" / "terraform-provider-aws_6.54.0_linux_amd64.zip"
    zip_.write_bytes(b"adulterado\n")
    binario = criar_binario(tmp_path, "tofu", TOFU_BIN)

    with pytest.raises(ValueError, match=r"toolchain=zh_ausente source=.*aws"):
        db.build_toolchain(lock_texto(), espelho, binario)


def test_recusa_provedor_do_lock_ausente_do_espelho(tmp_path: Path) -> None:
    espelho = criar_espelho(tmp_path)
    (espelho / REGISTRY / "aws" / "terraform-provider-aws_6.54.0_linux_amd64.zip").unlink()
    binario = criar_binario(tmp_path, "tofu", TOFU_BIN)

    with pytest.raises(ValueError, match=r"toolchain=zip_ausente source=.*aws"):
        db.build_toolchain(lock_texto(), espelho, binario)


def test_recusa_zip_extra_no_espelho(tmp_path: Path) -> None:
    espelho = criar_espelho(tmp_path)
    extra = espelho / REGISTRY / "aws" / "terraform-provider-aws_6.54.1_linux_amd64.zip"
    extra.write_bytes(b"outro\n")
    binario = criar_binario(tmp_path, "tofu", TOFU_BIN)

    with pytest.raises(ValueError, match="toolchain=zip_extra"):
        db.build_toolchain(lock_texto(), espelho, binario)


def test_recusa_espelho_com_provedor_fora_do_lock(tmp_path: Path) -> None:
    espelho = criar_espelho(tmp_path)
    pasta = espelho / "registry.opentofu.org/hashicorp/random"
    pasta.mkdir(parents=True)
    (pasta / "terraform-provider-random_3.0.0_linux_amd64.zip").write_bytes(b"x\n")
    binario = criar_binario(tmp_path, "tofu", TOFU_BIN)

    with pytest.raises(ValueError, match="toolchain=zip_extra"):
        db.build_toolchain(lock_texto(), espelho, binario)


def test_recusa_lock_sem_provedor(tmp_path: Path) -> None:
    espelho = criar_espelho(tmp_path)
    binario = criar_binario(tmp_path, "tofu", TOFU_BIN)

    with pytest.raises(ValueError, match="toolchain=lock_sem_provedor"):
        db.build_toolchain(b"# vazio\n", espelho, binario)


def test_recusa_binario_do_tofu_com_versao_diferente_da_fixada(tmp_path: Path) -> None:
    espelho = criar_espelho(tmp_path)
    outro = TOFU_BIN.replace(b"1.13.2", b"1.13.9")
    binario = criar_binario(tmp_path, "tofu", outro)

    with pytest.raises(ValueError, match=re.escape("toolchain=tofu_version found=1.13.9")):
        db.build_toolchain(lock_texto(), espelho, binario)


def test_policy_tem_forma_fechada_do_gate(tmp_path: Path) -> None:
    conftest = criar_binario(tmp_path, "conftest", CONFTEST_BIN)

    valor = json.loads(db.build_policy(POLITICA, conftest))

    assert valor == {"policy_sha256": sha(POLITICA), "conftest_version": "0.71.1",
                     "conftest_sha256": sha(CONFTEST_BIN)}


def test_recusa_conftest_com_versao_diferente_da_fixada(tmp_path: Path) -> None:
    conftest = criar_binario(tmp_path, "conftest", CONFTEST_BIN.replace(b"0.71.1", b"0.72.0"))

    with pytest.raises(ValueError, match=re.escape("policy=conftest_version found=0.72.0")):
        db.build_policy(POLITICA, conftest)


def test_recusa_conftest_que_nao_se_identifica(tmp_path: Path) -> None:
    conftest = criar_binario(tmp_path, "conftest", b"#!/bin/sh\necho nada\n")

    with pytest.raises(ValueError, match="policy=conftest_version found=None"):
        db.build_policy(POLITICA, conftest)


def test_cost_context_tem_forma_fechada_do_gate(tmp_path: Path) -> None:
    repo, source = criar_produto(tmp_path)

    valor = json.loads(db.build_cost_context(repo, source, CUSTO))

    assert set(valor) == {"stage", "checkpoint_sha256", "target_sha256", "source_sha",
                          "unbound_required", "cloudfront"}
    assert valor["stage"] == "pre-activation"
    assert valor["source_sha"] == source
    assert valor["unbound_required"] is True
    assert valor["cloudfront"] is None
    assert valor["checkpoint_sha256"] == sha(CUSTO)


def test_target_sha256_depende_so_da_arvore_rastreada_do_tofu(tmp_path: Path) -> None:
    repo, source = criar_produto(tmp_path)
    antes = json.loads(db.build_cost_context(repo, source, CUSTO))["target_sha256"]
    escrever(repo, "infra/opentofu/env/prod/solto.tf", b"# nao rastreado\n")
    escrever(repo, "README.md", b"fora do tofu\n")
    assert json.loads(db.build_cost_context(repo, source, CUSTO))["target_sha256"] == antes

    escrever(repo, "infra/opentofu/env/prod/main.tf", b"# alterado\n")
    novo = commit(repo, "tofu alterado")

    assert json.loads(db.build_cost_context(repo, novo, CUSTO))["target_sha256"] != antes


def test_cost_context_recusa_source_sha_que_nao_e_a_cabeca(tmp_path: Path) -> None:
    repo, _ = criar_produto(tmp_path)

    with pytest.raises(ValueError, match="baselines=checkout_head"):
        db.build_cost_context(repo, "a" * 40, CUSTO)


def test_documentos_sao_public_safe(tmp_path: Path) -> None:
    for valor in ({"x": "arn:aws:iam::111122223333:role/r"}, {"x": "https://exemplo"},
                  {"token": "x"}, {"x": "AK" + "IA" + "ABCDEFGHIJKLMNOP"}):
        with pytest.raises(ValueError, match="baselines=public_safe"):
            db.documento(valor)


def test_documento_acima_de_256_kib_e_recusado() -> None:
    with pytest.raises(ValueError, match="json=output_size"):
        db.documento({"x": ["a" * 100] * 3000})


def test_artefatos_tem_exatamente_os_membros_da_spec(tmp_path: Path) -> None:
    repo, source = criar_produto(tmp_path)
    espelho = criar_espelho(tmp_path)
    tofu = criar_binario(tmp_path, "tofu", TOFU_BIN)
    conftest = criar_binario(tmp_path, "conftest", CONFTEST_BIN)

    artefatos = db.build_all(repo, source, espelho, (tofu, conftest))

    assert {nome: tuple(membros) for nome, membros in artefatos.items()} == db.ARTIFACTS
    assert artefatos["delivery-baseline-cost"]["cost-manifest-source.json"] == CUSTO
    assert all(len(dado) <= db.DOCUMENT_LIMIT for membros in artefatos.values()
               for dado in membros.values())
    assert all(re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", n) for m in artefatos.values() for n in m)


def test_fonte_de_custo_acima_do_limite_e_recusada(tmp_path: Path) -> None:
    repo, _ = criar_produto(tmp_path)
    escrever(repo, COST, b'{"x": "' + b"a" * db.DOCUMENT_LIMIT + b'"}\n')
    source = commit(repo, "custo grande")
    espelho = criar_espelho(tmp_path)
    tofu = criar_binario(tmp_path, "tofu", TOFU_BIN)
    conftest = criar_binario(tmp_path, "conftest", CONFTEST_BIN)

    with pytest.raises(ValueError, match="baselines=cost_source_size"):
        db.build_all(repo, source, espelho, (tofu, conftest))


def test_fonte_de_custo_que_nao_e_json_objeto_e_recusada(tmp_path: Path) -> None:
    repo, _ = criar_produto(tmp_path)
    escrever(repo, COST, b"[1]\n")
    source = commit(repo, "custo invalido")
    espelho = criar_espelho(tmp_path)
    tofu = criar_binario(tmp_path, "tofu", TOFU_BIN)
    conftest = criar_binario(tmp_path, "conftest", CONFTEST_BIN)

    with pytest.raises(ValueError, match="baselines=cost_source_json"):
        db.build_all(repo, source, espelho, (tofu, conftest))


def _argv(raiz: Path, repo: Path, source: str, saida: Path) -> list[str]:
    return ["--checkout", str(repo), "--source-sha", source, "--mirror-dir",
            str(criar_espelho(raiz)), "--tofu-binary", str(criar_binario(raiz, "tofu", TOFU_BIN)),
            "--conftest-binary", str(criar_binario(raiz, "conftest", CONFTEST_BIN)),
            "--output-dir", str(saida)]


def test_main_grava_um_diretorio_por_artefato(tmp_path: Path) -> None:
    repo, source = criar_produto(tmp_path)
    saida = tmp_path / "saida"

    assert db.main(_argv(tmp_path, repo, source, saida)) == 0

    assert sorted(p.name for p in saida.iterdir()) == sorted(db.ARTIFACTS)
    for nome, membros in db.ARTIFACTS.items():
        assert sorted(p.name for p in (saida / nome).iterdir()) == sorted(membros)
    assert (saida / "delivery-baseline-cost/cost-manifest-source.json").read_bytes() == CUSTO


def test_main_recusa_saida_existente_sem_gravar_nada(tmp_path: Path) -> None:
    repo, source = criar_produto(tmp_path)
    saida = tmp_path / "saida"
    saida.mkdir()

    assert db.main(_argv(tmp_path, repo, source, saida)) == 2
    assert list(saida.iterdir()) == []


def test_main_recusa_e_nao_grava_quando_o_lock_diverge_do_espelho(tmp_path: Path) -> None:
    repo, _ = criar_produto(tmp_path)
    escrever(repo, LOCK, lock_texto({"aws": ("6.54.0", b"outro-zip\n")}))
    source = commit(repo, "lock divergente")
    saida = tmp_path / "saida"

    assert db.main(_argv(tmp_path, repo, source, saida)) == 2
    assert not saida.exists()


def test_lock_real_do_repositorio_resolve_zh_para_cada_provedor() -> None:
    bruto = (ROOT / LOCK).read_bytes()

    provedores = db.parse_lock(bruto)

    assert f"{REGISTRY}/aws" in provedores
    assert provedores[f"{REGISTRY}/aws"][0] == "6.54.0"
    assert all(zh for _, zh in provedores.values())
    assert all(re.fullmatch(r"[0-9a-f]{64}", item) for _, zh in provedores.values() for item in zh)


def test_politica_real_do_repositorio_existe() -> None:
    assert (ROOT / POLICY).is_file()
