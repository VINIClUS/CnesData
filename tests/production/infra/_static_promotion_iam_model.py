"""Avaliação estreita de documentos IAM efetivos da publicação estática."""

import json
import re
from copy import deepcopy
from datetime import datetime


def _lista(valor):
    return valor if isinstance(valor, list) else [valor]


def _corresponde(valor, padrao):
    expressao = re.escape(padrao).replace(r"\*", ".*").replace(r"\?", ".")
    return re.fullmatch(expressao, valor) is not None


def _comparar(operador, atual, esperado):
    suportados = {"Null", "StringEquals", "ArnEquals", "StringLike", "ArnLike", "Bool"}
    if operador not in suportados | {"DateLessThan"}:
        raise ValueError(f"operador_nao_modelado={operador}")
    if operador == "Null":
        return (atual is None) == (str(esperado).lower() == "true")
    if atual is None:
        return False
    if operador in {"StringEquals", "ArnEquals"}:
        return str(atual) == str(esperado)
    if operador in {"StringLike", "ArnLike"}:
        return _corresponde(str(atual), str(esperado))
    if operador == "Bool":
        return str(atual).lower() == str(esperado).lower()
    if operador == "DateLessThan":
        return datetime.fromisoformat(atual) < datetime.fromisoformat(esperado)
    raise ValueError(f"operador_nao_modelado={operador}")


def _condicoes(condicoes, contexto):
    contexto = {chave.lower(): valor for chave, valor in contexto.items()}
    return all(
        any(_comparar(operador, contexto.get(chave.lower()), v) for v in _lista(valores))
        for operador, campos in condicoes.items()
        for chave, valores in campos.items()
    )


def _aplica(declaracao, pedido):
    campos = set(declaracao)
    assert campos <= {"Sid", "Effect", "Action", "Resource", "Principal", "Condition"}
    acoes = _lista(declaracao["Action"])
    recursos = _lista(declaracao.get("Resource", "*"))
    principal = declaracao.get("Principal", {}).get("AWS")
    return all([
        any(_corresponde(pedido["action"].lower(), a.lower()) for a in acoes),
        any(_corresponde(pedido["resource"], r) for r in recursos),
        principal is None or pedido.get("principal") in _lista(principal),
        _condicoes(declaracao.get("Condition", {}), pedido.get("context", {})),
    ])


def _efeitos(documentos, pedido):
    return {
        declaracao["Effect"]
        for documento in documentos
        for declaracao in _lista(documento["Statement"])
        if _aplica(declaracao, pedido)
    }


def autoriza(documento, pedido, sessao=None, bloqueios=()):
    """Args: Identity, contexto IAM, sessão opcional e denies externos.
    Returns: Decisão no subconjunto IAM modelado, com precedência de Deny.
    Raises: ValueError/AssertionError para gramática não suportada.
    """
    efeitos = _efeitos([documento], pedido)
    if "Deny" in efeitos | _efeitos(bloqueios, pedido) or "Allow" not in efeitos:
        return False
    return sessao is None or _efeitos([sessao], pedido) == {"Allow"}


def verificar_matriz(documentos, pedidos, sessao=None):
    """Args: Documentos observados, vetores independentes e sessão opcional.
    Returns: Nenhum valor.
    Raises: AssertionError quando a decisão diverge do vetor ratificado.
    """
    for pedido in pedidos:
        observado = autoriza(documentos[pedido["role"]], pedido, sessao)
        assert observado is pedido["allow"], f"vetor={pedido['id']} observado={observado}"


def documentos_do_plano(plano, prefixo="module.subject"):
    """Args: JSON nativo do plano e endereço do módulo produtor.
    Returns: Policies/trusts efetivos, sem interpretação de fonte HCL.
    Raises: KeyError para recurso/JSON ausente.
    """
    recursos = {
        item["address"]: item["change"]["after"]
        for item in plano["resource_changes"]
        if item["mode"] == "managed"
    }
    policies, confiancas = {}, {}
    for alias in ("observer", "normal", "recovery"):
        base = f"{prefixo}.aws_iam_role"
        policies[alias] = json.loads(recursos[f"{base}_policy.static_{alias}[0]"]["policy"])
        confiancas[alias] = json.loads(recursos[f"{base}.static_{alias}[0]"]["assume_role_policy"])
    return policies, confiancas


def suites_isoladas(casos, fixas, cabecalho):
    """Args: Casos nomeados, variáveis fixas e mock header.
    Returns: Um arquivo independente com um único run por caso.
    Raises: Nenhuma exceção própria.
    """
    suites = {}
    for nome, entrada in casos.items():
        variaveis = fixas | {"static_promotion": entrada}
        valores = "\n".join(f"{k} = {json.dumps(v)}" for k, v in variaveis.items())
        suites[nome] = (cabecalho + f'run "{nome}" {{\n command = plan\n'
                       f' variables {{\n{valores}\n }}\n}}\n')
    return suites


def verificar_cobertura_nativa(resultado, casos):
    """Args: Resultado observado e todos os casos emitidos.
    Returns: Nenhum valor.
    Raises: AssertionError para rc, ausência, skip ou status desconhecido.
    """
    esperado = 1 if any(n.startswith("invalido_") for n in casos) else 0
    assert resultado["returncode"] == esperado, f"native_rc={resultado['returncode']}"
    assert set(resultado["statuses"]) == set(casos), "native_run_ausente"
    assert set(resultado["statuses"].values()) <= {"pass", "fail", "error"}, "native_run_skip"
    erros = [e for e in resultado["diagnostics"] if e["diagnostic"]["severity"] == "error"]
    assert all(e.get("@testrun") in casos for e in erros), "native_erro_sem_run"


def verificar_rejeicao_nativa(resultado, nome):
    """Args: JSON nativo completo e nome do caso inválido isolado.
    Returns: Nenhum valor.
    Raises: AssertionError para sucesso, skip, diagnóstico ausente ou erro inesperado.
    """
    assert resultado["statuses"].get(nome) == "error", f"native_rejeicao_status={nome}"
    assert nome not in resultado["plans"], f"native_rejeicao_plano={nome}"
    erros = [e["diagnostic"] for e in resultado["diagnostics"]
             if e.get("@testrun") == nome and e["diagnostic"]["severity"] == "error"]
    assert erros, f"native_rejeicao_diagnostico={nome}"
    assert all(e["summary"] == "Invalid value for variable"
               and "static_promotion=invalid_contract" in e["detail"] for e in erros), erros


def _bootstrap(original):
    entrada = deepcopy(original)
    entrada |= {"mode": "observer-bootstrap", "inventory_sha256": None}
    entrada["binding"]["static_plan_sha256"] = None
    for chave in ("candidate_keys", "rollback_versions", "retire_versions", "multipart_keys"):
        entrada[chave] = []
    return entrada


def _mutantes(entrada):
    principal = entrada["application_promotion"]["application_role_arn"]
    mudancas = {
        "extra": (("extra",), True),
        "modo": (("mode",), "readiness"),
        "versao_fragmento": (("application_promotion", "fragment_version"), 3),
        "conta": (("application_promotion", "account_id"), "999999999999"),
        "principal": (("application_promotion", "application_role_arn"), principal + "*"),
        "root": (("application_promotion", "application_role_arn"),
                 "arn:aws:iam::123456789012:root"),
        "sts": (("application_promotion", "application_role_arn"),
                "arn:aws:sts::123456789012:assumed-role/cnesdata-prod-application/session"),
        "run": (("binding", "run_id"), True),
        "attempt": (("binding", "run_attempt"), 0),
        "generation": (("binding", "generation"), "7"),
        "source": (("binding", "shared_source_sha"), "0" * 40),
        "release": (("binding", "release_id"), "r*"),
        "plano": (("binding", "static_plan_sha256"), None),
        "inventario": (("inventory_sha256",), "nao-e-digest"),
        "candidata": (("candidate_keys",), ["assets/*"]),
        "chave_privada": (("candidate_keys",), ["tenants/private"]),
        "index_fora": (("candidate_keys",), ["indexXhtml"]),
        "theme_fora": (("candidate_keys",), ["theme-initXjs"]),
        "rollback_parcial": (("rollback_versions",), entrada["rollback_versions"][:1]),
        "retire_sem_vid": (("retire_versions",), [{"key": "assets/r497/app.js"}]),
        "prazo": (("enabled_until",), "2026-11-10T14:00:00Z"),
        "historico_nulo": (("application_promotion", "predecessor_reads"), None),
        "caller_antigo": (("application_promotion", "caller"), {}),
        "alias_extra": (("application_promotion", "delegations", "extra"), principal),
    }
    casos = {}
    for nome, (caminho, valor) in mudancas.items():
        mutante = deepcopy(entrada)
        destino = mutante
        for chave in caminho[:-1]:
            destino = destino[chave]
        destino[caminho[-1]] = valor
        casos["invalido_" + nome] = mutante
    casos["invalido_vazio"] = {}
    casos["invalido_quota_policy"] = entrada | {
        "candidate_keys": [f"assets/r498/{'a' * 80}{n:04d}.js" for n in range(150)],
    }
    casos["invalido_bootstrap_mutante"] = _bootstrap(entrada) | {"candidate_keys": ["index.html"]}
    return casos
