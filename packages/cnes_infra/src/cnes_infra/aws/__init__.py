"""Settings e bundle de adapters do profile aws."""

from cnes_infra.aws.credentials import (
    CredentialAdoption,
    register_signing_observer,
    require_process_credentials,
    watch_credentials,
)
from cnes_infra.aws.runtime import (
    AwsClients,
    AwsRuntimeComponents,
    build_aws_runtime,
    create_aws_clients,
)
from cnes_infra.aws.settings import AwsRuntimeConfigurationError, AwsRuntimeSettings

__all__ = (
    "AwsClients",
    "AwsRuntimeComponents",
    "AwsRuntimeConfigurationError",
    "AwsRuntimeSettings",
    "CredentialAdoption",
    "build_aws_runtime",
    "create_aws_clients",
    "register_signing_observer",
    "require_process_credentials",
    "watch_credentials",
)
