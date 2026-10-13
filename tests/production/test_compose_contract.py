"""Contrato do compose de produção do API: loopback, hardening e nenhuma chave estática."""

from __future__ import annotations

import copy
import re
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = REPO_ROOT / "deploy" / "compose.production.yaml"
API_SERVICE = "central-api"
LOOPBACK = "127.0.0.1"
API_BINDING = (LOOPBACK, "18080", "8000")
API_LOOPBACK_UPSTREAM = "http://127.0.0.1:18080"
RUNTIME_USER = "10001:10001"
TMPFS_TARGET = "/tmp"  # noqa: S108
TMPFS = f"{TMPFS_TARGET}:rw,noexec,nosuid,nodev,size=64m"
CREDENTIAL_MOUNTS = frozenset(
    {
        "/etc/cnesdata/aws/config",
        "/etc/cnesdata/aws/leaf-certificate.pem",
        "/etc/cnesdata/aws/leaf-private-key.pem",
    }
)
STATIC_KEY_NAMES = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
RESOURCE_LIMITS = ("cpus", "mem_limit", "memswap_limit", "pids_limit")
TOP_LEVEL_KEYS = frozenset({"name", "services"})
API_KEYS = frozenset(
    {
        "image", "build", "restart", "user", "read_only", "tmpfs", "cap_drop", "security_opt",
        "cpus", "mem_limit", "memswap_limit", "pids_limit", "ports", "volumes", "healthcheck",
        "logging", "env_file",
    }
)
RUNTIME_ENV_FILE = [{"path": "/run/cnesdata-production/api.env", "required": True}]
NO_NEW_PRIVILEGES = "no-new-privileges:true"
DATA_PROCESSOR = re.compile(r"data[-_ ]?processor", re.IGNORECASE)

Document = dict[str, Any]
Mutation = Callable[[Document], None]


@pytest.fixture
def compose() -> Document:
    loaded = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _services(document: Mapping[str, Any]) -> dict[str, Any]:
    return dict(document.get("services") or {})


def _api(document: Mapping[str, Any]) -> dict[str, Any]:
    return dict(_services(document).get(API_SERVICE) or {})


def _strings(value: object) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for key, item in value.items():
            yield str(key)
            yield from _strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _strings(item)


def _mentions(value: object, needle: str) -> bool:
    return any(needle.lower() in text.lower() for text in _strings(value))


def _port_binding(port: object) -> tuple[str | None, str | None, str]:
    if isinstance(port, Mapping):
        published = port.get("published")
        published_text = None if published is None else str(published)
        return port.get("host_ip"), published_text, str(port.get("target"))
    parts = str(port).split("/")[0].rsplit(":", 2)
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, None, parts[0]


def _volume_target(volume: object) -> str:
    if isinstance(volume, Mapping):
        return str(volume.get("target"))
    return str(volume).split(":")[1]


def _safe_mount(volume: object) -> bool:
    if not isinstance(volume, Mapping):
        return False
    bind = volume.get("bind") or {}
    return (
        volume.get("type") == "bind"
        and str(volume.get("source", "")).startswith("/")
        and volume.get("read_only") is True
        and bind.get("create_host_path") is False
    )


def _has_data_processor(document: Document) -> bool:
    return any(DATA_PROCESSOR.search(text) for text in _strings(_services(document)))


def _has_public_port(document: Document) -> bool:
    return any(
        _port_binding(port)[0] != LOOPBACK
        for service in _services(document).values()
        for port in service.get("ports") or []
    )


def _breaks_loopback_contract(document: Document) -> bool:
    ports = _api(document).get("ports") or []
    return [_port_binding(port) for port in ports] != [API_BINDING]


def _uses_host_network(document: Document) -> bool:
    return any(
        service.get("network_mode") == "host" for service in _services(document).values()
    )


def _uses_external_network(document: Document) -> bool:
    networks = document.get("networks") or {}
    return any(
        isinstance(network, Mapping) and network.get("external")
        for network in networks.values()
    )


def _has_static_key(document: Document) -> bool:
    return any(_mentions(document, name) for name in STATIC_KEY_NAMES)


def _keeps_capabilities(document: Document) -> bool:
    api = _api(document)
    return api.get("cap_drop") != ["ALL"] or bool(api.get("cap_add"))


def _missing_limits(document: Document) -> bool:
    api = _api(document)
    return any(not api.get(limit) for limit in RESOURCE_LIMITS)


def _missing_healthcheck(document: Document) -> bool:
    healthcheck = _api(document).get("healthcheck") or {}
    return healthcheck.get("disable") is True or not healthcheck.get("test")


def _unbounded_tmpfs(document: Document) -> bool:
    entries = _api(document).get("tmpfs") or []
    return entries != [TMPFS]


def _unexpected_mount(document: Document) -> bool:
    volumes = _api(document).get("volumes") or []
    targets = [_volume_target(volume) for volume in volumes]
    return sorted(targets) != sorted(CREDENTIAL_MOUNTS)


def _unsafe_mount(document: Document) -> bool:
    return not all(_safe_mount(volume) for volume in _api(document).get("volumes") or [])


VIOLATIONS: dict[str, Callable[[Document], bool]] = {
    "unexpected_top_level_key": lambda document: not set(document) <= TOP_LEVEL_KEYS,
    "unexpected_service_key": lambda document: not set(_api(document)) <= API_KEYS,
    "interpolation": lambda document: _mentions(document, "$"),
    "data_processor_service": _has_data_processor,
    "unexpected_service": lambda document: set(_services(document)) != {API_SERVICE},
    "public_port": _has_public_port,
    "loopback_contract_mismatch": _breaks_loopback_contract,
    "docker_socket": lambda document: _mentions(document, "docker.sock"),
    "host_network": _uses_host_network,
    "external_network": _uses_external_network,
    "limnopulse_reference": lambda document: _mentions(document, "limnopulse"),
    "static_aws_key": _has_static_key,
    "writable_root": lambda document: _api(document).get("read_only") is not True,
    "capabilities_kept": _keeps_capabilities,
    "new_privileges_allowed": lambda document: NO_NEW_PRIVILEGES
    not in (_api(document).get("security_opt") or []),
    "runtime_user_mismatch": lambda document: _api(document).get("user") != RUNTIME_USER,
    "privileged": lambda document: _api(document).get("privileged") is True,
    "missing_limits": _missing_limits,
    "missing_healthcheck": _missing_healthcheck,
    "unbounded_tmpfs": _unbounded_tmpfs,
    "unexpected_mount": _unexpected_mount,
    "unsafe_mount": _unsafe_mount,
    "runtime_env_file_mismatch": lambda document: _api(document).get("env_file")
    != RUNTIME_ENV_FILE,
}


def _violations(document: Document) -> set[str]:
    return {code for code, violated in VIOLATIONS.items() if violated(document)}


def _api_of(document: Document) -> dict[str, Any]:
    return document["services"][API_SERVICE]


def _add_data_processor(document: Document) -> None:
    document["services"]["data-processor"] = {
        "image": "ghcr.io/viniclus/cnesdata/data_processor:develop",
    }


def _publish_without_host_ip(document: Document) -> None:
    _api_of(document)["ports"] = ["18080:8000"]


def _publish_on_all_interfaces(document: Document) -> None:
    _api_of(document)["ports"] = ["0.0.0.0:18080:8000"]


def _publish_long_syntax(document: Document) -> None:
    _api_of(document)["ports"] = [{"target": 8000, "published": 18080}]


def _publish_other_loopback_port(document: Document) -> None:
    _api_of(document)["ports"] = ["127.0.0.1:18081:8000"]


def _publish_other_target(document: Document) -> None:
    _api_of(document)["ports"] = ["127.0.0.1:18080:8080"]


def _publish_second_loopback_port(document: Document) -> None:
    _api_of(document)["ports"].append("127.0.0.1:18081:8000")


def _publish_nothing(document: Document) -> None:
    del _api_of(document)["ports"]


def _mount_docker_socket(document: Document) -> None:
    _api_of(document)["volumes"].append("/var/run/docker.sock:/var/run/docker.sock")


def _mount_docker_socket_long(document: Document) -> None:
    _api_of(document)["volumes"].append(
        {"type": "bind", "source": "/run/docker.sock", "target": "/run/docker.sock"},
    )


def _use_host_network(document: Document) -> None:
    _api_of(document)["network_mode"] = "host"


def _join_external_network(document: Document) -> None:
    document["networks"] = {"edge": {"external": True, "name": "cnesdata_edge"}}
    _api_of(document)["networks"] = ["edge"]


def _join_limnopulse_network(document: Document) -> None:
    document["networks"] = {"limnopulse": {"name": "limnopulse_default"}}
    _api_of(document)["networks"] = ["limnopulse"]


def _mount_limnopulse_path(document: Document) -> None:
    _api_of(document)["volumes"].append("/opt/limnopulse/data:/srv/data:ro")


def _set_access_key_mapping(document: Document) -> None:
    _api_of(document)["environment"] = {"AWS_ACCESS_KEY_ID": "placeholder"}


def _set_secret_key_list(document: Document) -> None:
    _api_of(document)["environment"] = ["AWS_SECRET_ACCESS_KEY=placeholder"]


def _set_session_token(document: Document) -> None:
    _api_of(document)["environment"] = {"AWS_SESSION_TOKEN": "${AWS_SESSION_TOKEN}"}


def _writable_root(document: Document) -> None:
    _api_of(document)["read_only"] = False


def _keep_all_capabilities(document: Document) -> None:
    del _api_of(document)["cap_drop"]


def _add_capability(document: Document) -> None:
    _api_of(document)["cap_add"] = ["NET_ADMIN"]


def _allow_new_privileges(document: Document) -> None:
    _api_of(document)["security_opt"] = []


def _run_as_root(document: Document) -> None:
    _api_of(document)["user"] = "0:0"


def _run_privileged(document: Document) -> None:
    _api_of(document)["privileged"] = True


def _drop_memory_limit(document: Document) -> None:
    del _api_of(document)["mem_limit"]


def _drop_pids_limit(document: Document) -> None:
    del _api_of(document)["pids_limit"]


def _disable_healthcheck(document: Document) -> None:
    _api_of(document)["healthcheck"] = {"disable": True}


def _unbounded_tmp(document: Document) -> None:
    _api_of(document)["tmpfs"] = [TMPFS_TARGET]


def _extra_tmpfs(document: Document) -> None:
    _api_of(document)["tmpfs"].append("/var/lib/central-api:size=8m")


def _mount_extra_path(document: Document) -> None:
    _api_of(document)["volumes"].append(
        {
            "type": "bind",
            "source": "/srv/cnesdata/extra",
            "target": "/srv/extra",
            "read_only": True,
            "bind": {"create_host_path": False},
        },
    )


def _writable_private_key(document: Document) -> None:
    _api_of(document)["volumes"][-1]["read_only"] = False


def _auto_created_private_key(document: Document) -> None:
    _api_of(document)["volumes"][-1]["bind"] = {"create_host_path": True}


def _relative_config_source(document: Document) -> None:
    _api_of(document)["volumes"][0]["source"] = "./aws/config"


def _include_vps_compose(document: Document) -> None:
    document["include"] = ["prod/docker-compose.prod.yml"]


def _extend_vps_service(document: Document) -> None:
    _api_of(document)["extends"] = {"file": "prod/docker-compose.prod.yml", "service": API_SERVICE}


def _privileged_from_environment(document: Document) -> None:
    _api_of(document)["privileged"] = "${CNESDATA_PRIVILEGED:-true}"


def _share_host_pid(document: Document) -> None:
    _api_of(document)["pid"] = "host"


def _share_volumes_from(document: Document) -> None:
    _api_of(document)["volumes_from"] = ["container:portainer"]


MUTATIONS = [
    pytest.param(_add_data_processor, "data_processor_service", id="data-processor"),
    pytest.param(_add_data_processor, "unexpected_service", id="segundo-service"),
    pytest.param(_publish_without_host_ip, "public_port", id="porta-sem-ip"),
    pytest.param(_publish_on_all_interfaces, "public_port", id="porta-0.0.0.0"),
    pytest.param(_publish_long_syntax, "public_port", id="porta-sintaxe-longa"),
    pytest.param(
        _publish_other_loopback_port, "loopback_contract_mismatch", id="outra-porta-loopback",
    ),
    pytest.param(_publish_other_target, "loopback_contract_mismatch", id="outro-alvo"),
    pytest.param(
        _publish_second_loopback_port, "loopback_contract_mismatch", id="segunda-porta",
    ),
    pytest.param(_publish_nothing, "loopback_contract_mismatch", id="sem-porta"),
    pytest.param(_mount_docker_socket, "docker_socket", id="docker-sock"),
    pytest.param(_mount_docker_socket_long, "docker_socket", id="docker-sock-longo"),
    pytest.param(_use_host_network, "host_network", id="rede-host"),
    pytest.param(_join_external_network, "external_network", id="rede-externa"),
    pytest.param(_join_limnopulse_network, "limnopulse_reference", id="rede-limnopulse"),
    pytest.param(_mount_limnopulse_path, "limnopulse_reference", id="path-limnopulse"),
    pytest.param(_set_access_key_mapping, "static_aws_key", id="access-key"),
    pytest.param(_set_secret_key_list, "static_aws_key", id="secret-key"),
    pytest.param(_set_session_token, "static_aws_key", id="session-token"),
    pytest.param(_writable_root, "writable_root", id="rootfs-gravavel"),
    pytest.param(_keep_all_capabilities, "capabilities_kept", id="sem-cap-drop"),
    pytest.param(_add_capability, "capabilities_kept", id="cap-add"),
    pytest.param(_allow_new_privileges, "new_privileges_allowed", id="new-privileges"),
    pytest.param(_run_as_root, "runtime_user_mismatch", id="root"),
    pytest.param(_run_privileged, "privileged", id="privileged"),
    pytest.param(_drop_memory_limit, "missing_limits", id="sem-limite-memoria"),
    pytest.param(_drop_pids_limit, "missing_limits", id="sem-limite-pids"),
    pytest.param(_disable_healthcheck, "missing_healthcheck", id="sem-healthcheck"),
    pytest.param(_unbounded_tmp, "unbounded_tmpfs", id="tmpfs-sem-limite"),
    pytest.param(_extra_tmpfs, "unbounded_tmpfs", id="tmpfs-extra"),
    pytest.param(_mount_extra_path, "unexpected_mount", id="mount-extra"),
    pytest.param(_writable_private_key, "unsafe_mount", id="chave-gravavel"),
    pytest.param(_auto_created_private_key, "unsafe_mount", id="chave-criada-pelo-docker"),
    pytest.param(_relative_config_source, "unsafe_mount", id="config-relativa"),
    pytest.param(_include_vps_compose, "unexpected_top_level_key", id="include"),
    pytest.param(_extend_vps_service, "unexpected_service_key", id="extends"),
    pytest.param(_privileged_from_environment, "interpolation", id="privileged-interpolado"),
    pytest.param(_share_host_pid, "unexpected_service_key", id="pid-host"),
    pytest.param(_share_volumes_from, "unexpected_service_key", id="volumes-from"),
]


def test_compose_de_producao_nao_define_data_processor(compose: Document) -> None:
    assert set(_services(compose)) == {API_SERVICE}
    assert not _has_data_processor(compose)


def test_compose_rejeita_porta_publica_e_docker_sock(compose: Document) -> None:
    assert _api(compose)["ports"]
    assert not _has_public_port(compose)
    assert not _mentions(compose, "docker.sock")


def test_compose_publica_exatamente_o_upstream_de_loopback_da_infra(compose: Document) -> None:
    bindings = [_port_binding(port) for port in _api(compose)["ports"]]
    upstreams = [f"http://{host}:{published}" for host, published, _ in bindings]

    assert bindings == [("127.0.0.1", "18080", "8000")]
    assert upstreams == [API_LOOPBACK_UPSTREAM]


def test_compose_nao_tem_chave_estatica(compose: Document) -> None:
    api = _api(compose)

    assert not _has_static_key(compose)
    assert "environment" not in api


def test_compose_nao_usa_rede_do_host_nem_do_limnopulse(compose: Document) -> None:
    assert "network_mode" not in _api(compose)
    assert "networks" not in compose
    assert not _mentions(compose, "limnopulse")


def test_compose_endurece_o_container_da_api(compose: Document) -> None:
    api = _api(compose)

    assert api["read_only"] is True
    assert api["tmpfs"] == [TMPFS]
    assert api["cap_drop"] == ["ALL"]
    assert "cap_add" not in api
    assert api["security_opt"] == [NO_NEW_PRIVILEGES]
    assert api["user"] == RUNTIME_USER
    assert all(api[limit] for limit in RESOURCE_LIMITS)
    assert api["mem_limit"] == api["memswap_limit"]
    assert api["healthcheck"]["test"][0] == "CMD"


def test_compose_monta_so_config_e_material_da_folha_read_only(compose: Document) -> None:
    volumes = _api(compose)["volumes"]

    assert sorted(_volume_target(volume) for volume in volumes) == sorted(CREDENTIAL_MOUNTS)
    assert all(_safe_mount(volume) for volume in volumes)


def test_compose_usa_imagem_do_target_production(compose: Document) -> None:
    build = _api(compose)["build"]

    assert build["target"] == "production"
    assert build["dockerfile"] == "apps/central_api/Dockerfile"
    assert (COMPOSE_PATH.parent / build["context"]).resolve() == REPO_ROOT


def test_compose_real_nao_tem_violacoes(compose: Document) -> None:
    assert _violations(compose) == set()


@pytest.mark.parametrize(("mutate", "violation"), MUTATIONS)
def test_cada_rejeicao_do_compose_morde(
    compose: Document, mutate: Mutation, violation: str,
) -> None:
    mutated = copy.deepcopy(compose)

    mutate(mutated)

    assert violation in _violations(mutated)
