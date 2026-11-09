"""Contract tests for the canonical Sonar workflow and its admin guard."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar, cast
from urllib.parse import parse_qs, urlsplit

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/sonar.yml"
TOKEN = "fixture-secret-must-not-leak"  # noqa: S105


def _workflow() -> dict[str, Any]:
    return cast("dict[str, Any]", yaml.safe_load(WORKFLOW.read_text(encoding="utf-8")))


def _triggers(workflow: dict[str, Any]) -> dict[str, Any]:
    if "on" in workflow:
        return cast("dict[str, Any]", workflow["on"])
    for key, value in workflow.items():
        if key is True:
            return cast("dict[str, Any]", value)
    raise AssertionError("workflow trigger block is missing")


def _admin_source() -> str:
    step = _workflow()["jobs"]["sonar-admin"]["steps"][0]
    run = step["run"]
    return run.split("python3 - <<'PY'\n", 1)[1].split("\nPY", 1)[0]


class _SonarDouble(BaseHTTPRequestHandler):
    branches: ClassVar[list[dict[str, Any]]] = []
    rename_result: ClassVar[list[dict[str, Any]] | None] = None
    post_status: ClassVar[int] = 200
    empty_mutation: ClassVar[bool] = False
    close_mutation: ClassVar[bool] = False
    requests: ClassVar[list[tuple[str, str, bytes, str | None]]] = []

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return

    def _write(self, status: int, payload: object) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_empty(self, status: int) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        self.__class__.requests.append(("GET", self.path, b"", self.headers.get("Authorization")))
        query = parse_qs(urlsplit(self.path).query)
        if urlsplit(self.path).path != "/api/project_branches/list" or query.get("project") != [
            "VINIClUS_CnesData"
        ]:
            self._write(404, {"errors": [{"msg": "not found"}]})
            return
        self._write(200, {"branches": self.__class__.branches})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.__class__.requests.append(("POST", self.path, body, self.headers.get("Authorization")))
        if self.__class__.close_mutation:
            self.connection.shutdown(2)
            self.connection.close()
            return
        if not 200 <= self.__class__.post_status < 300:
            self._write(self.__class__.post_status, {"errors": [{"msg": "denied"}]})
            return
        params = parse_qs(body.decode())
        if urlsplit(self.path).path == "/api/project_branches/delete":
            assert params == {"branch": ["develop"], "project": ["VINIClUS_CnesData"]}
            self.__class__.branches = [
                branch for branch in self.__class__.branches if branch["name"] != "develop"
            ]
        elif urlsplit(self.path).path == "/api/project_branches/rename":
            assert params == {"name": ["develop"], "project": ["VINIClUS_CnesData"]}
            self.__class__.branches = self.__class__.rename_result or [
                {"name": "develop", "isMain": True, "type": "LONG"}
            ]
        else:
            self._write(404, {"errors": [{"msg": "not found"}]})
            return
        if self.__class__.empty_mutation:
            self._write_empty(self.__class__.post_status)
            return
        self._write(200, {})


class _Server:
    def __init__(self, branches: list[dict[str, Any]], **kwargs: Any) -> None:
        _SonarDouble.branches = branches
        _SonarDouble.rename_result = kwargs.pop("rename_result", None)
        _SonarDouble.post_status = kwargs.pop("post_status", 200)
        _SonarDouble.empty_mutation = kwargs.pop("empty_mutation", False)
        _SonarDouble.close_mutation = kwargs.pop("close_mutation", False)
        _SonarDouble.requests = []
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _SonarDouble)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.httpd.server_port}"

    @property
    def requests(self) -> list[tuple[str, str, bytes, str | None]]:
        return _SonarDouble.requests

    def __enter__(self) -> _Server:
        self.thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)


def _run_admin(server: _Server | None, **overrides: str) -> subprocess.CompletedProcess[str]:
    source = _admin_source()
    if server is not None:
        source = source.replace("https://sonarcloud.io", server.url)
    env = os.environ.copy()
    env.update(
        {
            "GITHUB_EVENT_NAME": "workflow_dispatch",
            "GITHUB_REPOSITORY": "VINIClUS/CnesData",
            "GITHUB_REPOSITORY_ID": "1177879951",
            "GITHUB_REPOSITORY_OWNER_ID": "124298687",
            "GITHUB_ACTOR": "VINIClUS",
            "GITHUB_ACTOR_ID": "124298687",
            "GITHUB_REF": "refs/heads/main",
            "INPUT_ALIGN_PRIMARY_BRANCH": "true",
            "SONAR_TOKEN": TOKEN,
            **overrides,
        }
    )
    return subprocess.run(  # noqa: S603 - the source is the workflow under test
        [sys.executable, "-c", source],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=8,
    )


def test_workflow_direciona_analise_de_source_para_develop_e_mantem_prs() -> None:
    triggers = _triggers(_workflow())
    assert triggers["pull_request"]["branches"] == ["main", "develop"]
    assert triggers["push"]["branches"] == ["develop"]
    dispatch = triggers["workflow_dispatch"]["inputs"]["align_primary_branch"]
    assert dispatch == {
        "description": "Run the one-time Sonar primary-branch alignment",
        "required": False,
        "default": False,
        "type": "boolean",
    }


def test_concorrencia_e_global_e_nao_cancela_execucoes() -> None:
    concurrency = _workflow()["concurrency"]
    assert concurrency["group"] == "sonar-${{ github.repository }}"
    assert concurrency["cancel-in-progress"] is False


def test_admin_e_manual_opt_in_e_nunca_analisa_source() -> None:
    jobs = _workflow()["jobs"]
    admin = jobs["sonar-admin"]
    assert "workflow_dispatch" in admin["if"]
    assert "align_primary_branch" in admin["if"]
    assert len(admin["steps"]) == 1
    assert "actions/checkout" not in admin["steps"][0].get("uses", "")
    assert "sonarqube-scan-action" not in admin["steps"][0].get("run", "")
    assert "SONAR_TOKEN" in admin["steps"][0]["env"]


def test_actions_usam_commits_imutaveis_das_mesmas_tags_oficiais() -> None:
    steps = _workflow()["jobs"]["sonar"]["steps"]
    expected = {
        "actions/checkout": "11d5960a326750d5838078e36cf38b85af677262",
        "actions/setup-python": "a26af69be951a213d495a4c3e4e4022e16d87065",
        "astral-sh/setup-uv": "cec208311dfd045dd5311c1add060b2062131d57",
        "actions/setup-go": "40f1582b2485089dde7abd97c1529aa768e1baff",
        "oven-sh/setup-bun": "0c5077e51419868618aeaa5fe8019c62421857d6",
        "SonarSource/sonarqube-scan-action": "22918119ff8e1ca75a623e15c8296b6ea4fbe28f",
    }
    actual = {
        step["uses"].split("@")[0]: step["uses"].split("@")[1].split()[0]
        for step in steps
        if "uses" in step
    }
    assert actual == expected
    assert all(len(commit) == 40 for commit in actual.values())


def test_admin_alinha_main_apos_remover_apenas_develop_secundario_exato() -> None:
    branches = [
        {"name": "main", "isMain": True, "type": "LONG"},
        {"name": "develop", "isMain": False, "type": "SHORT"},
        {"name": "feature/keep", "isMain": False, "type": "SHORT"},
    ]
    with _Server(branches) as server:
        result = _run_admin(server)
        requests = server.requests
    assert result.returncode == 0, result.stderr
    assert [request[0] for request in requests] == [
        "GET",
        "POST",
        "GET",
        "POST",
        "GET",
    ]
    assert requests[1][1] == "/api/project_branches/delete"
    assert requests[3][1] == "/api/project_branches/rename"
    assert all(request[3] == f"Bearer {TOKEN}" for request in requests)
    assert TOKEN not in result.stdout + result.stderr


def test_admin_falha_fechado_para_estado_adulterado_e_nao_muta() -> None:
    with _Server([{"name": "develop", "isMain": False, "type": "SHORT"}]) as server:
        result = _run_admin(server)
    assert result.returncode != 0
    assert len(server.requests) == 1
    assert "unexpected" in result.stderr.lower()


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("GITHUB_REPOSITORY", "attacker/CnesData"),
        ("GITHUB_REPOSITORY_ID", "1"),
        ("GITHUB_REPOSITORY_OWNER_ID", "2"),
        ("GITHUB_ACTOR", "attacker"),
        ("GITHUB_ACTOR_ID", "3"),
        ("GITHUB_REF", "refs/heads/develop"),
        ("INPUT_ALIGN_PRIMARY_BRANCH", "false"),
    ],
)
def test_admin_recusa_contexto_de_dispatch_nao_confiavel(key: str, value: str) -> None:
    result = _run_admin(None, **{key: value})
    assert result.returncode != 0
    assert "SONAR_TOKEN" not in result.stdout + result.stderr


def test_admin_reporta_permissao_ausente_sem_repetir() -> None:
    with _Server([{"name": "main", "isMain": True, "type": "LONG"}], post_status=403) as server:
        result = _run_admin(server)
    assert result.returncode != 0
    assert [request[0] for request in server.requests] == ["GET", "POST"]
    assert "permission" in result.stderr.lower()
    assert TOKEN not in result.stdout + result.stderr


def test_admin_nao_repete_mutacao_incerta() -> None:
    with _Server([{"name": "main", "isMain": True, "type": "LONG"}], close_mutation=True) as server:
        result = _run_admin(server)
    assert result.returncode != 0
    assert sum(request[0] == "POST" for request in server.requests) == 1
    assert TOKEN not in result.stdout + result.stderr


@pytest.mark.parametrize("status", [200, 204])
def test_admin_aceita_mutacao_2xx_vazia_e_confirma_por_leitura(status: int) -> None:
    with _Server(
        [
            {"name": "main", "isMain": True, "type": "LONG"},
            {"name": "develop", "isMain": False, "type": "SHORT"},
        ],
        post_status=status,
        empty_mutation=True,
    ) as server:
        result = _run_admin(server)
    assert result.returncode == 0, result.stderr
    assert [request[0] for request in server.requests] == [
        "GET",
        "POST",
        "GET",
        "POST",
        "GET",
    ]


def test_admin_rejeita_dois_primarios_no_readback() -> None:
    with _Server(
        [{"name": "main", "isMain": True, "type": "LONG"}],
        rename_result=[
            {"name": "main", "isMain": True, "type": "LONG"},
            {"name": "develop", "isMain": True, "type": "LONG"},
        ],
    ) as server:
        result = _run_admin(server)
    assert result.returncode != 0
    assert [request[0] for request in server.requests] == ["GET", "POST", "GET"]
    assert "primary" in result.stderr.lower()


def test_admin_rejeita_nomes_de_branch_duplicados_antes_de_delete() -> None:
    with _Server(
        [
            {"name": "main", "isMain": True, "type": "LONG"},
            {"name": "develop", "isMain": False, "type": "SHORT"},
            {"name": "develop", "isMain": False, "type": "SHORT"},
        ]
    ) as server:
        result = _run_admin(server)
    assert result.returncode != 0
    assert len(server.requests) == 1
    assert "duplicate" in result.stderr.lower()


@pytest.mark.parametrize("branch_type", ["LONG", "UNKNOWN"])
def test_admin_rejeita_tipo_nao_aprovado_antes_de_delete(branch_type: str) -> None:
    with _Server(
        [
            {"name": "main", "isMain": True, "type": "LONG"},
            {"name": "develop", "isMain": False, "type": branch_type},
        ]
    ) as server:
        result = _run_admin(server)
    assert result.returncode != 0
    assert len(server.requests) == 1
    assert "unexpected" in result.stderr.lower()


def test_admin_rejeita_tipo_ausente_antes_de_delete() -> None:
    with _Server(
        [
            {"name": "main", "isMain": True, "type": "LONG"},
            {"name": "develop", "isMain": False},
        ]
    ) as server:
        result = _run_admin(server)
    assert result.returncode != 0
    assert len(server.requests) == 1


def test_admin_rejeita_payload_de_branch_malformado() -> None:
    with _Server([{"name": "main", "type": "LONG"}]) as server:
        result = _run_admin(server)
    assert result.returncode != 0
    assert len(server.requests) == 1


def test_admin_rejeita_readback_de_rename_adulterado_sem_segunda_escrita() -> None:
    with _Server(
        [{"name": "main", "isMain": True, "type": "LONG"}],
        rename_result=[{"name": "develop", "isMain": False, "type": "SHORT"}],
    ) as server:
        result = _run_admin(server)
    assert result.returncode != 0
    assert [request[0] for request in server.requests] == ["GET", "POST", "GET"]
    assert "readback" in result.stderr.lower()


def test_fonte_admin_nao_tem_redirect_repeticao_ou_dump_de_segredo() -> None:
    source = _admin_source()
    assert "HTTPRedirectHandler" in source
    assert "Authorization" in source
    assert "retry" not in source.lower()
    assert "print(token" not in source.lower()
    assert "SONAR_API_BASE" not in source
