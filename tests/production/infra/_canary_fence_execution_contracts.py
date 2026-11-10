"""Matriz efetiva F2 e preservação das chaves do fence de promoção."""

from fnmatch import fnmatchcase

from tests.production.infra._step_functions_iam_contracts import Grants

EXECUCAO = "arn:aws:states:us-east-2:123456789012:execution:cnesdata-prod:execucao-1"
REQUISICOES = (
    ("states:DescribeExecution", EXECUCAO, True),
    ("states:DescribeExecution", EXECUCAO.replace("execucao-1", "execucao-2"), True),
    ("states:DescribeExecution", EXECUCAO.replace("cnesdata-prod:", "cnesdata-prod-outra:"), False),
    ("states:DescribeExecution", EXECUCAO.replace("123456789012", "999999999999"), False),
    ("states:DescribeExecution", EXECUCAO.replace("us-east-2", "us-east-1"), False),
    ("states:DescribeExecution", EXECUCAO.replace(":execution:", ":stateMachine:"), False),
    ("states:DescribeExecution", "*", False),
    ("states:StartExecution", EXECUCAO, False),
    ("states:StopExecution", EXECUCAO, False),
    ("states:ListExecutions", EXECUCAO, False),
    ("states:GetExecutionHistory", EXECUCAO, False),
    ("states:RedriveExecution", EXECUCAO, False),
)


def _permite_sem_contexto(grants: Grants, acao: str, recurso: str) -> bool:
    return any(
        fnmatchcase(acao.casefold(), permitida.casefold())
        and fnmatchcase(recurso, destino) and condicao == "{}"
        for permitida, destino, condicao in grants
    )


def verificar_leituras_execucoes(grants: Grants) -> None:
    assert all(c == "{}" for a, _, c in grants if a.lower().startswith("states:")), (
        "condicao_sfn_inesperada"
    )
    for acao, recurso, esperado in REQUISICOES:
        observado = _permite_sem_contexto(grants, acao, recurso)
        assert observado == esperado, f"acao={acao} recurso={recurso} permitido={observado}"


def verificar_grants_operador(observado: Grants, esperado: Grants, tabela: str) -> None:
    escritas = {(r, c) for a, r, c in observado if a == "dynamodb:UpdateItem"}
    assert escritas == {(tabela, next(iter(
        c for a, _, c in esperado if a == "dynamodb:UpdateItem")))}
    assert all("SEMAPHORE" not in c for a, _, c in observado if a == "dynamodb:UpdateItem")
    verificar_leituras_execucoes(observado)
    assert {a for a, _, _ in observado} == {
        "dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:ConditionCheckItem",
        "states:DescribeExecution",
    }
