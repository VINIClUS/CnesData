"""Fixtures nativas de capacidades de modo, sem AWS ou aplicação real."""

import hashlib
import json
import os
import shutil
from copy import deepcopy
from pathlib import Path

import pytest

from tests.production.infra._athena_root_plan import (
    CANARY_ID,
    CANARY_PRINCIPAL,
    REVISION,
    _canary_overrides,
    _canary_role_seeds,
    _phase_b_mock_root,
    after_of,
    fixture_canary_plan,  # noqa: F401
)
from tests.production.infra._processing_native import (
    _native_plan_events,
    _native_result,
    _save_plans,
    _suite,
)
from tests.production.infra._root_native_identities import merge_identity_records

ALIASES = ("mode_seed", "mode_dispatch", "mode_audit_launch", "mode_observe")
PREFIXO = "module.runtime_iam"
CONTA = "123456789012"
OPERACOES = {
    "promotion": {
        "unit-seed": ("us", "mode_seed", "unit"),
        "unit-start": ("ux", "mode_dispatch", "unit"),
        "unit-observe": ("uo", "mode_observe", "unit"),
        "recovery-seed": ("rs", "mode_seed", "recovery"),
        "recovery-start": ("rx", "mode_dispatch", "recovery"),
        "recovery-observe": ("ro", "mode_observe", "recovery"),
        "audit-start": ("ax", "mode_audit_launch", "audit"),
        "audit-observe": ("ao", "mode_observe", "audit"),
    },
    "readiness": {
        "drill-seed": ("ds", "mode_seed", "readiness"),
        "drill-unit-start": ("dx", "mode_dispatch", "readiness"),
        "drill-recovery-start": ("dr", "mode_dispatch", "readiness"),
        "drill-hang-first": ("h1", "mode_dispatch", "readiness"),
        "drill-hang-second": ("h2", "mode_dispatch", "readiness"),
        "drill-observe": ("do", "mode_observe", "readiness"),
    },
}


def _canonico(valor):
    return json.dumps(valor, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"


def _sha(texto):
    return hashlib.sha256(texto.encode()).hexdigest()


def _nome(alias):
    return "cnesdata-prod-" + alias.replace("_", "-")


def _escopo():
    tenant = "prod-canary-" + CANARY_ID
    return {
        "tenant_id": tenant,
        "run_ids": [
            f"{prefix}-4444444444444444" for prefix in ("recovery-a", "recovery-b", "unit")
        ],
        "raw_object_keys": [
            f"raw/{tenant}/cnes-nacional/2026-01/{'a' * 64}/data.parquet",
            f"raw/{tenant}/cnes-nacional/2026-01/{'a' * 64}/manifest.json",
        ],
        "output_object_keys": [f"outputs/{tenant}/unit-4444444444444444/manifest.json"],
        "audit_object_keys": [
            "audit/.event-id/" + "b" * 64 + ".json",
            "audit/2026/11/10/" + "b" * 64 + ".json",
        ],
    }


def _recursos(plano):
    def arn(endereco):
        return after_of(plano, endereco)["arn"]

    rede = [r for r in plano["resource_changes"] if r["type"] == "aws_subnet"]
    grupo = [r for r in plano["resource_changes"] if r["type"] == "aws_security_group"]
    tabela = arn("module.control_plane.aws_dynamodb_table.control_plane")
    base = f"arn:aws:scheduler:us-east-2:{CONTA}:schedule/cnesdata-prod-"
    return {
        "table_arn": tabela,
        "index_arn": tabela + "/index/gsi5",
        "data_bucket_arn": "arn:aws:s3:::cnesdata-prod-data",
        "audit_bucket_arn": "arn:aws:s3:::cnesdata-prod-audit",
        "cluster_arn": arn("module.processing.aws_ecs_cluster.processor"),
        "machine_arn": f"arn:aws:states:us-east-2:{CONTA}:stateMachine:cnesdata-prod",
        "revisions": {modo: REVISION % (modo, 17) for modo in ("unit", "recovery", "audit")},
        "task_role_arns": {
            modo: arn(f'{PREFIXO}.aws_iam_role.task["{modo}"]')
            for modo in ("unit", "recovery", "audit")
        },
        "execution_role_arn": arn(f"{PREFIXO}.aws_iam_role.execution"),
        "network": {
            "public_subnet_ids": sorted(r["change"]["after"]["id"] for r in rede),
            "security_group_id": grupo[0]["change"]["after"]["id"],
        },
        "log_group_arn": arn("aws_cloudwatch_log_group.processor").removesuffix(":*"),
        "schedule_arns": {
            modo: f"{base}{modo}/cnesdata-prod-{modo}" for modo in ("recovery", "audit")
        },
        "alarm_arns": {
            chave: f"arn:aws:cloudwatch:us-east-2:{CONTA}:alarm:"
            "cnesdata-prod-scheduler-" + chave.replace("_", "-")
            for chave in (
                "recovery_repeated_failure",
                "audit_repeated_failure",
                "audit_outbox_backlog",
            )
        },
        "scope": _escopo(),
    }


def _sessoes(manifesto, digest):
    dono = manifesto["owner"]
    campos = (
        "product_source_sha",
        "product_source_closure_sha256",
        "shared_source_sha",
        "shared_source_closure_sha256",
        "release_id",
        "activation_sha256",
        "context_kind",
    )
    base = {chave: manifesto[chave] for chave in campos} | dono
    base |= {"schema_version": 2, "context_manifest_sha256": digest}
    sessoes = {alias: [] for alias in ALIASES}
    for operacao, (codigo, alias, fase) in OPERACOES[manifesto["context_kind"]].items():
        binding = base | {"operation": operacao, "phase": fase}
        texto = json.dumps(binding, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
        sessoes[alias].append(f"mc-{codigo}-{_sha(texto)[:40]}")
    return {alias: sorted(nomes) for alias, nomes in sessoes.items()}


def _entrada(recursos, contexto="promotion"):
    dono = {
        "repository": "VINIClUS/CnesData",
        "run_id": 4990001,
        "run_attempt": 1,
        "generation": 7,
        "owner_sha256": "4" * 64,
    }
    manifesto = {
        "schema_version": 2,
        "context_kind": contexto,
        "owner": dono,
        "product_source_sha": "d58a7e4fd17c8a8128841e545428ab020ada1f88",
        "product_source_closure_sha256": "1" * 64,
        "shared_source_sha": "2e51aaa560372679a54002788d80d46ad4fee5b3",
        "shared_source_closure_sha256": "2" * 64,
        "release_id": "r-vector-499",
        "activation_sha256": "3" * 64,
        "resources_sha256": _sha(_canonico(recursos)),
        "fixture_manifest_sha256": "6" * 64,
        "scenario_plan_sha256": "7" * 64,
        "predecessor_terminal_sha256": "8" * 64 if contexto == "readiness" else None,
        "admitted_at": "2026-11-10T11:00:00Z",
        "deadline": "2026-11-10T11:15:00Z",
    }
    texto = _canonico(manifesto)
    return {
        "schema_version": 2,
        "context_kind": contexto,
        "application_principal_arn": f"arn:aws:iam::{CONTA}:role/cnesdata-prod-application",
        "owner": dono,
        "context_manifest": {"canonical_utf8": texto, "sha256": _sha(texto)},
        "delegation_sessions": _sessoes(manifesto, _sha(texto)),
        "resources": recursos,
        "enabled_until": "2026-11-10T11:50:00Z"
        if contexto == "promotion"
        else "2026-11-10T11:15:00Z",
    }


def _mutantes(entrada):
    mudancas = {
        "extra": (("extra",), True),
        "owner_extra": (("owner", "extra"), True),
        "digest": (("context_manifest", "sha256"), "0" * 64),
        "wrapper_extra": (("context_manifest", "extra"), True),
        "prazo": (("enabled_until",), "2026-11-10T10:59:59Z"),
        "conta": (("application_principal_arn",), "arn:aws:iam::999999999999:role/foreign"),
        "tabela": (
            ("resources", "table_arn"),
            "arn:aws:dynamodb:us-east-2:123456789012:table/limno",
        ),
        "indice": (("resources", "index_arn"), entrada["resources"]["table_arn"] + "/index/gsi6"),
        "sessao": (("delegation_sessions", "mode_seed"), ["mc-us-" + "0" * 40]),
        "tenant": (("resources", "scope", "tenant_id"), "outro-tenant"),
        "objeto": (("resources", "scope", "raw_object_keys"), ["raw/*"]),
    }
    casos = {}
    for nome, (caminho, valor) in mudancas.items():
        mutante = deepcopy(entrada)
        alvo = mutante
        for parte in caminho[:-1]:
            alvo = alvo[parte]
        alvo[caminho[-1]] = valor
        if caminho[0] == "resources":
            mutante = _entrada(mutante["resources"])
        casos["invalido_" + nome] = {"mode_canary": mutante}
    return (
        casos
        | _mutantes_recursos(entrada)
        | _mutantes_manifesto(entrada)
        | _mutantes_shape(entrada)
    )


def _mutantes_shape(entrada):
    casos = {}
    for campo in entrada:
        mutante = deepcopy(entrada)
        del mutante[campo]
        casos["invalido_sem_" + campo] = {"mode_canary": mutante}
    for nome, valor in (("nulo", None), ("string", "{}"), ("objeto", {}), ("lista", [])):
        casos["invalido_wrapper_" + nome] = {"mode_canary": entrada | {"context_manifest": valor}}
    for campo in ("run_id", "run_attempt", "generation"):
        for nome, valor in (("bool", True), ("string", "1"), ("float", 1.5), ("zero", 0)):
            dono = entrada["owner"] | {campo: valor}
            manifesto = json.loads(entrada["context_manifest"]["canonical_utf8"]) | {"owner": dono}
            casos[f"invalido_owner_{campo}_{nome}"] = {
                "mode_canary": _revincular(entrada | {"owner": dono}, manifesto)
            }
    return casos


def _revincular(entrada, manifesto=None, texto=None):
    novo = deepcopy(entrada)
    manifesto = manifesto or json.loads(novo["context_manifest"]["canonical_utf8"])
    texto = texto if texto is not None else _canonico(manifesto)
    novo["context_manifest"] = {"canonical_utf8": texto, "sha256": _sha(texto)}
    novo["delegation_sessions"] = _sessoes(manifesto, _sha(texto))
    return novo


def _mutantes_recursos(entrada):
    recursos = entrada["resources"]
    mudancas = dict.fromkeys(
        (
            "data_bucket_arn",
            "audit_bucket_arn",
            "cluster_arn",
            "machine_arn",
            "execution_role_arn",
            "log_group_arn",
        ),
        "arn:foreign",
    )
    mudancas |= {"extra": True}
    casos = {
        "invalido_recurso_" + k: {"mode_canary": _entrada(recursos | {k: v})}
        for k, v in mudancas.items()
    }
    for grupo in ("revisions", "task_role_arns", "network", "schedule_arns", "alarm_arns", "scope"):
        for campo in ("extra", next(iter(recursos[grupo]))):
            mutante = deepcopy(recursos)
            valor = mutante[grupo].get(campo)
            mutante[grupo][campo] = ["foreign"] if isinstance(valor, list) else "foreign"
            casos[f"invalido_{campo}_{grupo}"] = {"mode_canary": _entrada(mutante)}
    for campo in ("run_ids", "raw_object_keys", "output_object_keys", "audit_object_keys"):
        for tipo in ("duplicada", "desordenada", "vazia"):
            mutante = deepcopy(recursos)
            lista = mutante["scope"][campo]
            mutante["scope"][campo] = {
                "duplicada": lista * 2,
                "desordenada": sorted(lista, reverse=True),
                "vazia": [],
            }[tipo]
            if tipo != "desordenada" or len(lista) > 1:
                casos[f"invalido_{campo}_{tipo}"] = {"mode_canary": _entrada(mutante)}
    return casos


def _mutantes_manifesto(entrada):
    original = json.loads(entrada["context_manifest"]["canonical_utf8"])
    texto = _canonico(original)
    textos = {
        "espaco": texto.replace(":", ": ", 1),
        "lf": texto + "\n",
        "sem_lf": texto[:-1],
        "duplicata": '{"schema_version":2,' + texto[1:],
        "bom": "\ufeff" + texto,
    }
    casos = {
        "invalido_codec_" + k: {"mode_canary": _revincular(entrada, original, v)}
        for k, v in textos.items()
    }
    for campo in original:
        mutante = deepcopy(original)
        del mutante[campo]
        novo = _revincular(entrada, original, _canonico(mutante))
        casos["invalido_manifesto_sem_" + campo] = {"mode_canary": novo}
    for chave, valor in (
        ("extra", True),
        ("schema_version", 2.0),
        ("predecessor_terminal_sha256", int("8" * 64)),
        ("product_source_sha", "x" * 40),
        ("resources_sha256", "0" * 64),
        ("context_kind", "readiness"),
    ):
        casos["invalido_manifesto_" + chave] = {
            "mode_canary": _revincular(entrada, original | {chave: valor})
        }
    owner = deepcopy(original["owner"]) | {"generation": 8}
    casos["invalido_owner_divergente"] = {
        "mode_canary": _revincular(entrada, original | {"owner": owner})
    }
    pronto = _entrada(entrada["resources"], "readiness")
    manifesto = json.loads(pronto["context_manifest"]["canonical_utf8"])
    casos["invalido_readiness_sem_predecessor"] = {
        "mode_canary": _revincular(pronto, manifesto | {"predecessor_terminal_sha256": None})
    }
    casos["invalido_readiness_prazo"] = {
        "mode_canary": pronto | {"enabled_until": "2026-11-10T11:15:01Z"}
    }
    casos["invalido_principal_mesma_conta"] = {
        "mode_canary": entrada | {"application_principal_arn": f"arn:aws:iam::{CONTA}:role/foreign"}
    }
    return casos


def _identidades(artifacts, evidencia):
    originais = json.loads((evidencia / "processing-identity-overrides.json").read_text())
    declaradas = artifacts["configuration"]["root_module"]["variables"]
    novas = (
        []
        if "mode_canary" not in declaradas
        else [
            {
                "address": f"{PREFIXO}.aws_iam_role.{alias}[0]",
                "fields": {
                    "arn": {"value": f"arn:aws:iam::{CONTA}:role/{_nome(alias)}"},
                    "id": {"value": _nome(alias)},
                },
            }
            for alias in ALIASES
        ]
    )
    return merge_identity_records(
        originais, _canary_role_seeds(artifacts), _canary_overrides(), novas
    )


def _comuns():
    return {
        "image_digest": "sha256:" + "a" * 64,
        "canary_release_id": CANARY_ID,
        "canary_trusted_principal_arn": CANARY_PRINCIPAL,
        "canary_storage_enabled": True,
    }


def _selecionar(casos):
    nomes = os.environ.get("CNESDATA_MODE_TEST_CASES", "*").split(",")
    return {
        k: v
        for k, v in casos.items()
        if "*" in nomes or k in nomes or not k.startswith("invalido_")
    }


@pytest.fixture(name="preparacao", scope="module")
def fixture_preparacao(canary_plan, tofu_context, tofu_artifacts, prod_root):
    evidencia = Path(tofu_context[1]["TF_DATA_DIR"])
    recurso = _recursos(canary_plan)
    entrada = _entrada(recurso)
    comuns = _comuns()
    casos = {
        "desligado": {},
        "promocao": {"mode_canary": entrada},
        "readiness": {"mode_canary": _entrada(recurso, "readiness")},
    } | _mutantes(entrada)
    casos = _selecionar(casos)
    shared = json.loads((evidencia / "processing-shared-input.json").read_text())
    diretorio = prod_root / ".terraform/mode-canary-tests"
    diretorio.mkdir(parents=True, exist_ok=True)
    identidades = _identidades(tofu_artifacts, evidencia)
    for nome, valor in casos.items():
        suite = _suite(shared, {nome: comuns | valor}, identidades)
        (diretorio / f"{nome}.tftest.hcl").write_text(suite)
        (evidencia / f"mode-canary-{nome}.tftest.hcl").write_text(suite)
    try:
        resultado = _native_result(
            tofu_context,
            prod_root,
            (
                "test",
                "-json",
                "-no-color",
                "-verbose",
                "-test-directory=.terraform/mode-canary-tests",
            ),
            "mode-canary-red",
        )
        estados, planos, erros = _native_plan_events(resultado.stdout)
        assert planos, f"native_plans=missing stderr={resultado.stderr} errors={erros}"
        yield {
            "plans": planos,
            "statuses": estados,
            "errors": erros,
            "diagnostics": [
                e
                for linha in resultado.stdout.splitlines()
                if (e := json.loads(linha)).get("type") == "diagnostic"
            ],
            "input": entrada,
            "resources": recurso,
            "cases": casos,
        }
    finally:
        shutil.rmtree(diretorio)


@pytest.fixture(name="ciclo_modo", scope="module")
def fixture_ciclo_modo(canary_plan, tofu_context, tofu_artifacts, prod_root):
    evidencia = Path(tofu_context[1]["TF_DATA_DIR"])
    entrada = _entrada(_recursos(canary_plan))
    novo = _novo_owner(entrada)
    comuns = _comuns()
    casos = {
        "semente": comuns,
        "criar": comuns | {"mode_canary": entrada},
        "materializar": comuns | {"mode_canary": entrada},
        "replay": comuns | {"mode_canary": entrada},
        "novo_owner": comuns | {"mode_canary": novo},
    }
    shared = json.loads((evidencia / "processing-shared-input.json").read_text())
    suite = _suite(shared, casos, _identidades(tofu_artifacts, evidencia))
    for nome in ("semente", "materializar"):
        suite = suite.replace(
            f'run "{nome}" {{\n  command = plan', f'run "{nome}" {{\n  command = apply'
        )
    raiz, hashes = _phase_b_mock_root(prod_root, evidencia)
    diretorio = raiz / "env/prod/.terraform/processing-tests"
    diretorio.mkdir(parents=True)
    (diretorio / "processing.tftest.hcl").write_text(suite)
    tofu = (tofu_context[0], tofu_context[1] | {"TF_DATA_DIR": str(evidencia / "ciclo-modo-data")})
    Path(tofu[1]["TF_DATA_DIR"]).mkdir()
    init = _native_result(
        tofu,
        raiz / "env/prod",
        ("init", "-backend=false", "-input=false", "-lockfile=readonly"),
        "ciclo-init",
    )
    assert init.returncode == 0, init.stderr
    resultado = _native_result(
        tofu,
        raiz / "env/prod",
        ("test", "-json", "-verbose", "-no-color", "-test-directory=.terraform/processing-tests"),
        "ciclo-modo",
    )
    estados, planos, erros = _native_plan_events(resultado.stdout)
    assert resultado.returncode == 0, resultado.stdout + resultado.stderr
    assert not erros, erros
    assert estados == dict.fromkeys(casos, "pass")
    _save_plans(evidencia, "ciclo-modo", planos)
    return {"plans": planos, "input": entrada, "new_input": novo, "source_hashes": hashes}


def _pedido(acao, recurso, contexto=None):
    return {
        "action": acao,
        "resource": recurso,
        "context": {"aws:CurrentTime": "2026-11-10T11:10:00Z"} | (contexto or {}),
    }


def _chaves(particoes):
    return {"dynamodb:LeadingKeys": particoes}


def _novo_owner(entrada):
    novo = deepcopy(entrada)
    novo["owner"] |= {"owner_sha256": "5" * 64, "generation": 8}
    for campo in ("run_ids", "output_object_keys"):
        novo["resources"]["scope"][campo] = [
            r.replace("4444444444444444", "5555555555555555")
            for r in novo["resources"]["scope"][campo]
        ]
    manifesto = json.loads(novo["context_manifest"]["canonical_utf8"])
    manifesto |= {"owner": novo["owner"], "resources_sha256": _sha(_canonico(novo["resources"]))}
    novo = _revincular(novo, manifesto)
    return novo
