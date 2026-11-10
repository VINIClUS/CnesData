"""Metadados de ambiente não secretos para evidências de comandos nativos."""

from collections.abc import Mapping

_ENVIRONMENT_METADATA = (
    "CHECKPOINT_DISABLE",
    "CI",
    "PATH",
    "PROCESSING_SOURCE_BINDINGS",
    "TF_CLI_CONFIG_FILE",
    "TF_DATA_DIR",
    "TF_INPUT",
    "TF_IN_AUTOMATION",
    "TF_PLUGIN_CACHE_DIR",
)


def native_command_environment(environment: Mapping[str, str]) -> dict[str, str]:
    return {name: environment[name] for name in _ENVIRONMENT_METADATA if name in environment}
