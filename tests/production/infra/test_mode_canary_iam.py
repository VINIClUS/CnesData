"""Decisões efetivas de IAM renderizado para promoção e readiness sintéticas."""

import json
from copy import deepcopy
from datetime import datetime
from fnmatch import fnmatchcase

import pytest

from tests.production.infra._athena_root_plan import after_of
from tests.production.infra._mode_canary_native import (
    ALIASES,
    PREFIXO,
    _chaves,
    _entrada,
    _nome,
    _pedido,
    fixture_canary_plan,  # noqa: F401
    fixture_preparacao,  # noqa: F401
)
from tests.production.infra.test_mode_canary_prepare import _pedidos_observacao


def _lista(valor):
    return valor if isinstance(valor, list) else [valor]


def _comparar(operador, atual, esperado):
    valores = _lista(esperado)
    if operador == "Null":
        return (atual in (None, [], "")) == (esperado == "true")
    if operador == "ForAllValues:StringEquals":
        return all(valor in valores for valor in atual or [])
    if atual is None:
        return False
    if operador in {"StringEquals", "ArnEquals", "Bool"}:
        return atual in valores
    if operador in {"DateLessThan", "DateGreaterThanEquals"}:
        antes = datetime.fromisoformat(atual) < datetime.fromisoformat(esperado)
        return antes if operador == "DateLessThan" else not antes
    if operador in {"StringLike", "ArnLike"}:
        return any(fnmatchcase(atual, valor) for valor in valores)
    raise AssertionError(f"unsupported_iam_condition={operador}")


def _condicoes(declaracao, pedido):
    return all(
        _comparar(operador, pedido.get("context", {}).get(chave), esperado)
        for operador, entradas in declaracao.get("Condition", {}).items()
        for chave, esperado in entradas.items()
    )


def _corresponde(declaracao, pedido):
    principals = declaracao.get("Principal", {"AWS": "*"})
    principals = principals if isinstance(principals, dict) else {"AWS": principals}
    return (
        any(fnmatchcase(pedido["action"], valor) for valor in _lista(declaracao["Action"]))
        and any(
            fnmatchcase(pedido["resource"], valor)
            for valor in _lista(declaracao.get("Resource", "*"))
        )
        and pedido.get("principal", "*") in _lista(principals["AWS"])
        and _condicoes(declaracao, pedido)
    )


def _permite(documento, pedido):
    efeitos = {item["Effect"] for item in documento["Statement"] if _corresponde(item, pedido)}
    return "Allow" in efeitos and "Deny" not in efeitos


def _policy(preparacao, alias, contexto="promocao"):
    plano = preparacao["plans"][contexto]
    endereco = f"{PREFIXO}.aws_iam_role_policy.{alias}[0]"
    return json.loads(after_of(plano, endereco)["policy"])


def _particoes(recursos):
    tenant = recursos["scope"]["tenant_id"]
    base = "TENANT#" + tenant.encode().hex()
    return [base] + [base + "#RUN#" + run.encode().hex() for run in recursos["scope"]["run_ids"]]


def _verificar_condicao(policy, pedido, chave, valor):
    assert _permite(policy, pedido), pedido
    invalido = pedido | {"context": pedido["context"] | {chave: valor}}
    assert not _permite(policy, invalido), invalido


def _verificar_trust_fechada(trust, principal):
    assert trust["Statement"]
    assert all(item["Effect"] == "Deny" for item in trust["Statement"])
    assert any(
        item["Action"] == "sts:AssumeRole"
        and item["Principal"] == {"AWS": principal}
        and not item.get("Condition")
        for item in trust["Statement"]
    )


def _verificar_expiracao(policy, pedidos, prazo):
    assert all(_permite(policy, pedido) for pedido in pedidos), "pedido_positivo_negado"
    for declaracao in policy["Statement"]:
        if declaracao["Effect"] == "Deny":
            continue
        positivos = [p for p in pedidos if _corresponde(declaracao, p)]
        assert positivos, f"statement_sem_pedido_positivo={declaracao}"
        for pedido in positivos:
            _verificar_condicao(policy, pedido, "aws:CurrentTime", prazo)


def _pedidos_positivos(recursos, alias, contexto="promotion"):
    tabela, partes = recursos["table_arn"], _particoes(recursos)
    cluster = {"ecs:cluster": recursos["cluster_arn"]}
    task = recursos["cluster_arn"].replace(":cluster/", ":task/") + "/" + "a" * 32
    maquina = recursos["machine_arn"]
    execucao = maquina.replace(":stateMachine:", ":execution:") + ":dispatch-original"
    pedidos = []
    if alias in ("mode_seed", "mode_dispatch", "mode_observe"):
        acoes = {
            "mode_seed": ("PutItem", "GetItem"),
            "mode_observe": ("GetItem", "Query"),
            "mode_dispatch": ("GetItem", "Query", "PutItem", "UpdateItem", "ConditionCheckItem"),
        }
        pedidos += [
            _pedido("dynamodb:" + a, tabela, _chaves([p])) for a in acoes[alias] for p in partes
        ]
    if alias == "mode_seed":
        pedidos += [
            _pedido(a, recursos["data_bucket_arn"] + "/" + k)
            for a in ("s3:PutObject", "s3:GetObject")
            for k in recursos["scope"]["raw_object_keys"]
        ]
    if alias == "mode_dispatch":
        pedidos += _pedidos_dispatch(tabela, maquina, execucao, (task, cluster))
    if alias in ("mode_dispatch", "mode_audit_launch"):
        modo = "recovery" if alias == "mode_dispatch" else "audit"
        pedidos += [_pedido("ecs:RunTask", recursos["revisions"][modo], cluster)]
        pedidos += [
            _pedido("iam:PassRole", arn, {"iam:PassedToService": "ecs-tasks.amazonaws.com"})
            for arn in (recursos["execution_role_arn"], recursos["task_role_arns"][modo])
        ]
    if alias != "mode_seed":
        pedidos += [_pedido("ecs:DescribeTasks", task, cluster)]
    if alias in ("mode_dispatch", "mode_observe"):
        pedidos += [
            _pedido("states:DescribeStateMachine", maquina),
            _pedido("states:DescribeExecution", execucao),
        ]
    if alias == "mode_observe":
        pedidos += _pedidos_observacao(recursos)
        if contexto == "readiness":
            pedidos += [
                _pedido("ecs:ListTasks", "*", cluster),
                _pedido("cloudwatch:GetMetricData", "*"),
            ]
    return pedidos


def _pedidos_dispatch(tabela, maquina, execucao, ecs):
    task, cluster = ecs
    pedidos = []
    pedidos += [
        _pedido("dynamodb:" + a, tabela, _chaves(["ENV#prod#" + k]))
        for a, k in (
            ("GetItem", "FENCE"),
            ("ConditionCheckItem", "FENCE"),
            ("GetItem", "SEMAPHORE"),
            ("UpdateItem", "SEMAPHORE"),
            ("UpdateItem", "ATTEMPTS"),
        )
    ]
    pedidos += [
        _pedido("states:StartExecution", maquina),
        _pedido("states:StopExecution", execucao),
        _pedido("ecs:StopTask", task, cluster),
    ]
    return pedidos


@pytest.mark.parametrize("alias", ALIASES)
@pytest.mark.parametrize("contexto", ["promocao", "readiness"])
def test_confianca_exige_principal_sessoes_derivadas_e_prazo(preparacao, alias, contexto):
    plano = preparacao["plans"][contexto]
    role = after_of(plano, f"{PREFIXO}.aws_iam_role.{alias}[0]")
    trust = json.loads(role["assume_role_policy"])
    entrada = _entrada(preparacao["resources"], "promotion" if contexto == "promocao" else contexto)
    assert role["name"] == _nome(alias)
    assert role["max_session_duration"] == 3600
    sessoes = entrada["delegation_sessions"][alias]
    for sessao in sessoes:
        pedido = _pedido("sts:AssumeRole", "*", {"sts:RoleSessionName": sessao})
        pedido["principal"] = entrada["application_principal_arn"]
        assert _permite(trust, pedido)
        assert not _permite(trust, pedido | {"principal": "arn:aws:iam::123456789012:role/foreign"})
        pedido["context"]["sts:RoleSessionName"] = sessao[:-1] + ("0" if sessao[-1] != "0" else "1")
        assert not _permite(trust, pedido)
    if contexto == "readiness" and alias == "mode_audit_launch":
        _verificar_trust_fechada(trust, entrada["application_principal_arn"])
    for item in trust["Statement"]:
        assert "StringLike" not in item.get("Condition", {})
    pedido = _pedido(
        "sts:AssumeRole",
        "*",
        {
            "sts:RoleSessionName": sessoes[0] if sessoes else "x",
            "aws:CurrentTime": entrada["enabled_until"],
        },
    )
    assert not _permite(trust, pedido | {"principal": entrada["application_principal_arn"]})


@pytest.mark.parametrize("acao", ["dynamodb:PutItem", "dynamodb:GetItem"])
def test_seed_acessa_particoes_sinteticas_sem_global_ou_fence(preparacao, acao):
    recursos = preparacao["resources"]
    policy = _policy(preparacao, "mode_seed")
    for particao in _particoes(recursos):
        assert _permite(policy, _pedido(acao, recursos["table_arn"], _chaves([particao])))
    for particao in ("TENANT#_GLOBAL", "OUTBOX#CURSOR", "ENV#prod#FENCE", "TENANT#616263"):
        assert not _permite(policy, _pedido(acao, recursos["table_arn"], _chaves([particao])))
    assert not _permite(policy, _pedido(acao, recursos["table_arn"]))
    assert not _permite(policy, _pedido(acao, recursos["table_arn"], _chaves([])))
    mistura = [_particoes(recursos)[0], "TENANT#_GLOBAL"]
    assert not _permite(policy, _pedido(acao, recursos["table_arn"], _chaves(mistura)))


def test_seed_put_get_somente_objetos_raw_exatos(preparacao):
    recursos = preparacao["resources"]
    policy = _policy(preparacao, "mode_seed")
    for chave in recursos["scope"]["raw_object_keys"]:
        arn = recursos["data_bucket_arn"] + "/" + chave
        for acao in ("s3:PutObject", "s3:GetObject"):
            assert _permite(policy, _pedido(acao, arn))
            assert not _permite(policy, _pedido(acao, arn + ".extra"))
    for arn in (recursos["audit_bucket_arn"] + "/audit/x", "arn:aws:s3:::shared-state/key"):
        assert not _permite(policy, _pedido("s3:PutObject", arn))


@pytest.mark.parametrize(
    "acao", ["GetItem", "Query", "PutItem", "UpdateItem", "ConditionCheckItem"]
)
def test_dispatch_usa_cp_sintetico_e_nega_particoes_alheias(preparacao, acao):
    recursos = preparacao["resources"]
    policy = _policy(preparacao, "mode_dispatch")
    for particao in _particoes(recursos):
        assert _permite(
            policy, _pedido("dynamodb:" + acao, recursos["table_arn"], _chaves([particao]))
        )
    for chave in ("TENANT#_GLOBAL", "OUTBOX#CURSOR", "RUN_RECOVERABLE", "TENANT#deadbeef"):
        assert not _permite(
            policy, _pedido("dynamodb:" + acao, recursos["table_arn"], _chaves([chave]))
        )


def test_dispatch_nao_muta_fence_e_preserva_quota_semaforo(preparacao):
    recursos = preparacao["resources"]
    policy = _policy(preparacao, "mode_dispatch")
    permitidos = [
        ("GetItem", "FENCE"),
        ("ConditionCheckItem", "FENCE"),
        ("GetItem", "SEMAPHORE"),
        ("UpdateItem", "SEMAPHORE"),
        ("UpdateItem", "ATTEMPTS"),
    ]
    for acao, sufixo in permitidos:
        pedido = _pedido("dynamodb:" + acao, recursos["table_arn"], _chaves(["ENV#prod#" + sufixo]))
        assert _permite(policy, pedido)
    for acao in ("PutItem", "UpdateItem", "DeleteItem"):
        pedido = _pedido("dynamodb:" + acao, recursos["table_arn"], _chaves(["ENV#prod#FENCE"]))
        assert not _permite(policy, pedido)


@pytest.mark.parametrize(
    ("alias", "modo"), [("mode_dispatch", "recovery"), ("mode_audit_launch", "audit")]
)
def test_runtask_passrole_exigem_revisao_cluster_e_roles_do_modo(preparacao, alias, modo):
    recursos = preparacao["resources"]
    policy = _policy(preparacao, alias)
    pedido = _pedido(
        "ecs:RunTask", recursos["revisions"][modo], {"ecs:cluster": recursos["cluster_arn"]}
    )
    assert _permite(policy, pedido)
    assert not _permite(policy, pedido | {"resource": recursos["revisions"]["unit"]})
    assert not _permite(policy, pedido | {"resource": pedido["resource"].replace(":17", ":18")})
    assert not _permite(
        policy, pedido | {"context": pedido["context"] | {"ecs:cluster": "foreign"}}
    )
    for arn in (recursos["task_role_arns"][modo], recursos["execution_role_arn"]):
        passar = _pedido("iam:PassRole", arn, {"iam:PassedToService": "ecs-tasks.amazonaws.com"})
        assert _permite(policy, passar)
        assert not _permite(
            policy,
            passar
            | {"context": passar["context"] | {"iam:PassedToService": "lambda.amazonaws.com"}},
        )
    assert not _permite(
        policy,
        _pedido(
            "iam:PassRole",
            recursos["task_role_arns"]["unit"],
            {"iam:PassedToService": "ecs-tasks.amazonaws.com"},
        ),
    )


def test_readiness_fecha_launcher_audit_e_preserva_observacao(preparacao):
    policy = _policy(preparacao, "mode_audit_launch", "readiness")
    assert policy["Statement"] == [{"Effect": "Deny", "Action": "*", "Resource": "*"}]
    recursos = preparacao["resources"]
    observar = _policy(preparacao, "mode_observe", "readiness")
    assert _permite(observar, _pedido("cloudwatch:GetMetricData", "*"))
    listar = _pedido("ecs:ListTasks", "*", {"ecs:cluster": recursos["cluster_arn"]})
    _verificar_condicao(
        observar, listar, "ecs:cluster", "arn:aws:ecs:us-east-2:123456789012:cluster/foreign"
    )
    _verificar_expiracao(
        observar,
        _pedidos_positivos(recursos, "mode_observe", "readiness"),
        _entrada(recursos, "readiness")["enabled_until"],
    )
    assert not _permite(
        _policy(preparacao, "mode_observe"), _pedido("cloudwatch:GetMetricData", "*")
    )


def test_observador_le_objetos_exatos_cursor_e_rotas_sem_mutacao(preparacao):
    recursos = preparacao["resources"]
    policy = _policy(preparacao, "mode_observe")
    pedido = _pedido("dynamodb:GetItem", recursos["table_arn"], _chaves(["OUTBOX#CURSOR"]))
    assert _permite(policy, pedido)
    assert not _permite(
        policy, pedido | {"context": pedido["context"] | _chaves(["TENANT#_GLOBAL"])}
    )
    for chave in recursos["scope"]["audit_object_keys"]:
        arn = recursos["audit_bucket_arn"] + "/" + chave
        for acao in ("s3:GetObject", "s3:GetObjectRetention"):
            assert _permite(policy, _pedido(acao, arn))
            assert not _permite(policy, _pedido(acao, arn + ".extra"))
    for arn in recursos["schedule_arns"].values():
        assert _permite(policy, _pedido("scheduler:GetSchedule", arn))
        assert not _permite(policy, _pedido("scheduler:UpdateSchedule", arn))


@pytest.mark.parametrize("alias", ALIASES)
def test_todas_capacidades_negam_scan_delete_iam_state_e_expiracao(preparacao, alias):
    recursos = preparacao["resources"]
    policy = _policy(preparacao, alias)
    negados = [
        ("dynamodb:Scan", recursos["table_arn"]),
        ("dynamodb:DeleteItem", recursos["table_arn"]),
        ("iam:PutRolePolicy", "*"),
        ("iam:AttachRolePolicy", "*"),
        ("s3:GetObject", "arn:aws:s3:::shared-opentofu-state-example/prod.tfstate"),
        ("s3:GetObject", "arn:aws:s3:::limnopulse-data/key"),
        ("cloudwatch:PutMetricData", "*"),
        ("cloudwatch:SetAlarmState", "*"),
    ]
    for acao, arn in negados:
        assert not _permite(policy, _pedido(acao, arn, _chaves(_particoes(recursos))))
    _verificar_expiracao(
        policy, _pedidos_positivos(recursos, alias), preparacao["input"]["enabled_until"]
    )


def test_iam_nao_finge_validar_container_override(preparacao):
    recursos = preparacao["resources"]
    pedido = _pedido(
        "ecs:RunTask", recursos["revisions"]["recovery"], {"ecs:cluster": recursos["cluster_arn"]}
    )
    policy = _policy(preparacao, "mode_dispatch")
    assert _permite(policy, pedido)
    assert _permite(policy, pedido | {"overrides": {"containerOverrides": [{"command": ["sh"]}]}})


def test_matriz_detecta_mutante_sem_condicao_de_particao(preparacao):
    policy = deepcopy(_policy(preparacao, "mode_seed"))
    recursos = preparacao["resources"]
    for item in policy["Statement"]:
        if "dynamodb:PutItem" in _lista(item["Action"]):
            item.pop("Condition", None)
    pedido = _pedido("dynamodb:PutItem", recursos["table_arn"], _chaves(["TENANT#_GLOBAL"]))
    assert not _permite(_policy(preparacao, "mode_seed"), pedido)
    assert _permite(policy, pedido)


@pytest.mark.parametrize("alias", ["mode_dispatch", "mode_observe"])
def test_stepfunctions_limita_maquina_execucoes_e_mutacoes_do_observador(preparacao, alias):
    recursos = preparacao["resources"]
    policy = _policy(preparacao, alias)
    maquina = recursos["machine_arn"]
    execucao = maquina.replace(":stateMachine:", ":execution:") + ":dispatch-original"
    assert _permite(policy, _pedido("states:DescribeStateMachine", maquina))
    assert _permite(policy, _pedido("states:DescribeExecution", execucao))
    for acao, arn in (("states:StartExecution", maquina), ("states:StopExecution", execucao)):
        assert _permite(policy, _pedido(acao, arn)) == (alias == "mode_dispatch")
        assert not _permite(policy, _pedido(acao, arn.replace("cnesdata-prod", "limnopulse")))


def test_observador_nega_stop_e_logs_alheios(preparacao):
    recursos = preparacao["resources"]
    policy = _policy(preparacao, "mode_observe")
    task = recursos["cluster_arn"].replace(":cluster/", ":task/") + "/" + "a" * 32
    log = (
        recursos["log_group_arn"].replace("cnesdata", "limnopulse") + ":log-stream:unit/processor/x"
    )
    assert not _permite(policy, _pedido("ecs:StopTask", task))
    assert not _permite(policy, _pedido("logs:GetLogEvents", log))


@pytest.mark.parametrize(
    ("acao", "chave", "correto", "errado"),
    [
        ("iam:PassRole", "iam:PassedToService", "ecs-tasks.amazonaws.com", "lambda.amazonaws.com"),
        (
            "ecs:ListTasks",
            "ecs:cluster",
            "arn:aws:ecs:us-east-2:123456789012:cluster/prod",
            "foreign",
        ),
    ],
)
def test_controle_negativo_detecta_perda_da_condicao_com_relogio(acao, chave, correto, errado):
    pedido = _pedido(acao, "*", {chave: correto})
    declaracao = {
        "Effect": "Allow",
        "Action": acao,
        "Resource": "*",
        "Condition": {
            "StringEquals": {chave: correto},
            "DateLessThan": {"aws:CurrentTime": "2026-11-10T11:15:00Z"},
        },
    }
    policy = {"Statement": [declaracao]}
    _verificar_condicao(policy, pedido, chave, errado)
    del declaracao["Condition"]["StringEquals"]
    with pytest.raises(AssertionError):
        _verificar_condicao(policy, pedido, chave, errado)


@pytest.mark.parametrize(
    "acao",
    [
        "ecs:RunTask",
        "iam:PassRole",
        "s3:GetObject",
        "s3:PutObject",
        "states:StartExecution",
        "logs:GetLogEvents",
        "ecs:ListTasks",
    ],
)
def test_controle_expiracao_exige_pedido_previamente_permitido(acao):
    prazo = "2026-11-10T11:15:00Z"
    declaracao = {
        "Effect": "Allow",
        "Action": acao,
        "Resource": "*",
        "Condition": {"DateLessThan": {"aws:CurrentTime": prazo}},
    }
    policy = {"Statement": [declaracao]}
    pedidos = [_pedido(acao, "*")]
    _verificar_expiracao(policy, pedidos, prazo)
    del declaracao["Condition"]
    with pytest.raises(AssertionError):
        _verificar_expiracao(policy, pedidos, prazo)


def test_controle_readiness_exige_deny_explicito_na_trust():
    principal = "arn:aws:iam::123456789012:role/cnesdata-prod-application"
    _verificar_trust_fechada(
        {
            "Statement": [
                {"Effect": "Deny", "Action": "sts:AssumeRole", "Principal": {"AWS": principal}}
            ]
        },
        principal,
    )
    with pytest.raises(AssertionError):
        _verificar_trust_fechada({"Statement": []}, principal)


@pytest.mark.parametrize("instante", ["2026-11-10T11:15:00.000Z", "2026-11-10T08:15:00-03:00"])
def test_controle_prazo_compara_instante_e_nao_ordem_lexicografica(instante):
    assert not _comparar("DateLessThan", instante, "2026-11-10T11:15:00Z")
