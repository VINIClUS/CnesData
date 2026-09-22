"""Regressão: docker_compose_command precisa selecionar um profile."""


def test_docker_compose_command_inclui_profile_dev(docker_compose_command):
    """Sem --profile, docker compose up não seleciona nenhum serviço —
    todo serviço em docker-compose.yml está atrás de uma chave profiles:."""
    assert "--profile dev" in docker_compose_command
