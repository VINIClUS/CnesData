"""Probe cognito-login com o gate DNS: o domain não é consultado antes de todos os NS publicarem."""

import json
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._login_dns_fake_testing import (
    HOSTS,
    RESOLVER,
    ZONE,
    EarlyLookup,
)
from tests.integration.aws_capability._login_handoff import handoff_path
from tests.integration.aws_capability._login_lab_testing import (
    LoginWorld,
    create_root,
    make_login_world,
)
from tests.integration.aws_capability._login_oauth import READY_ATTEMPTS
from tests.integration.aws_capability._login_probe import probe_login
from tests.integration.aws_capability._probe_testing import janitor_ports
from tests.integration.aws_capability.contracts import Outcome, ServiceFailure
from tests.integration.aws_capability.services import AwsServices

CHILD_CREATIONS = {
    "create-resource-server",
    "create-user-pool-client",
    "admin-create-user",
    "create-user-pool-domain",
    "create-managed-login-branding",
}
STAGGERED = {"192.0.2.1": 0.0, "192.0.2.2": 15.0, "192.0.2.3": 40.0, "192.0.2.4": 25.0}


def run(tmp_path: Path, **options: object) -> tuple[LoginWorld, Outcome]:
    world = make_login_world(tmp_path, human_seconds=30.0, **options)
    outcome = lifecycle.run(world.lab.spec, janitor_ports(world.lab, supervised=True))
    return world, outcome


def probe_of(outcome: Outcome) -> dict[str, object]:
    value = outcome.evidence["probe"]
    assert isinstance(value, dict)
    return value


def test_guarda_do_mundo_falha_alto_se_o_domain_for_buscado_cedo(tmp_path: Path) -> None:
    world = make_login_world(tmp_path, dns_delay=60.0)
    world.dns.create(f"cnesdata-cap-x.{ZONE}")
    with pytest.raises(EarlyLookup):
        world.idp.client().get(f"https://cnesdata-cap-x.{ZONE}/oauth2/authorize")


def test_probe_so_busca_a_pagina_depois_de_todos_os_ns_publicarem(tmp_path: Path) -> None:
    _, outcome = run(tmp_path, dns_delay=STAGGERED)
    probe = probe_of(outcome)
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert probe["login"] == "ok"
    assert (probe["dns_gate"], probe["dns_nameservers"]) == ("ok", len(HOSTS))
    published = probe["domain_published"]
    assert isinstance(published, float)
    assert published >= 40.0
    polls, ready_polls = probe["domain_published_polls"], probe["domain_ready_polls"]
    assert isinstance(polls, int)
    assert isinstance(ready_polls, int)
    assert 7 <= polls <= ready_polls < READY_ATTEMPTS
    ready = probe["domain_ready"]
    assert isinstance(ready, float)
    assert ready >= published


def test_nome_do_domain_so_vai_aos_ns_autoritativos_sem_recursao(tmp_path: Path) -> None:
    world, _ = run(tmp_path, dns_delay=STAGGERED)
    target = world.dns.target
    queries = [q for q in world.dns.log if q.name == target]
    assert queries
    assert {q.server for q in queries} == set(HOSTS.values())
    assert not any(q.recursion for q in queries)
    to_resolver = [(q.name, q.qtype, q.recursion) for q in world.dns.log if q.server == RESOLVER]
    assert to_resolver == [(ZONE, 2, True)]


def test_descoberta_dos_ns_acontece_antes_de_criar_qualquer_filho(tmp_path: Path) -> None:
    world, _ = run(tmp_path)
    events = world.dns.events
    assert events.index("create") == 1 + len(HOSTS)
    assert set(events[: events.index("create")]) == {"query"}


@pytest.mark.parametrize(
    ("options", "code"),
    [
        ({"dns_resolvers": ()}, "no_resolver"),
        ({"dns_zone_answer": "drop"}, "no_nameservers"),
        ({"dns_probe": {"192.0.2.3": "plain"}}, "unverified"),
    ],
)
def test_falha_na_descoberta_para_antes_de_criar_filhos_e_limpa(
    tmp_path: Path, options: dict[str, object], code: str
) -> None:
    world, outcome = run(tmp_path, **options)
    probe = probe_of(outcome)
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert probe["dns_gate"] == code
    assert "domain_ready" not in probe
    assert "login" not in probe
    assert not CHILD_CREATIONS & set(world.lab.aws.operations())
    assert world.dns.target is None
    assert world.lab.aws.pools == {}
    assert not handoff_path(world.lab.spec).exists()


def test_ns_que_nunca_confirma_vira_timeout_sem_buscar_a_pagina(tmp_path: Path) -> None:
    world, outcome = run(tmp_path, dns_delay={"192.0.2.3": 10_000.0})
    probe = probe_of(outcome)
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert probe["dns_gate"] == "ok"
    assert (probe["domain_published"], probe["domain_ready"]) == ("timeout", "timeout")
    assert probe["domain_ready_polls"] == READY_ATTEMPTS
    assert "login" not in probe
    assert ("GET", "/oauth2/authorize") not in world.idp.requests
    assert world.listener.handoff_text is None
    assert world.lab.aws.pools == {}


def test_portas_sem_dns_falham_como_portas_indisponiveis(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    lab = world.lab
    root = create_root(lab)
    bare = AwsServices(lab.spec, replace(lab.services.ports, dns=None))
    with pytest.raises(ServiceFailure) as error:
        bare.probe([root], lab.spec, lab.context)
    assert error.value.args == ("probe=login_ports_unavailable",)
    assert not CHILD_CREATIONS & set(lab.aws.operations())


def test_probe_login_direto_sem_dns_falha_como_portas_indisponiveis(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    lab = world.lab
    root = create_root(lab)
    with pytest.raises(ServiceFailure) as error:
        probe_login([root], lab.spec, replace(lab.services.ports, dns=None), lab.context)
    assert error.value.args == ("probe=login_ports_unavailable",)
    assert world.listener.closed is False
    assert not CHILD_CREATIONS & set(lab.aws.operations())


def test_evidencia_do_gate_tem_so_codigos_contagens_e_segundos(tmp_path: Path) -> None:
    world, outcome = run(tmp_path, dns_delay=STAGGERED)
    text = json.dumps(outcome.evidence, sort_keys=True)
    assert "192.0.2" not in text
    assert "198.51.100" not in text
    assert ZONE not in text
    assert RESOLVER not in text
    assert world.dns.target is not None
    assert world.dns.target not in text
    probe = probe_of(outcome)
    for key in ("dns_nameservers", "domain_published_polls", "domain_ready_polls"):
        assert isinstance(probe[key], int)


def test_cliente_real_do_mundo_ainda_responde_ao_discovery_antes_do_dominio(
    tmp_path: Path,
) -> None:
    world = make_login_world(tmp_path, dns_delay=60.0)
    create_root(world.lab)
    reply = world.idp.client().get(f"{world.idp.issuer}/.well-known/openid-configuration")
    assert isinstance(reply, httpx.Response)
    assert reply.status_code == 200
