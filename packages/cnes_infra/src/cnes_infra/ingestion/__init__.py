"""Adapters públicos de ingestão de fontes oficiais."""

from cnes_infra.ingestion.datasus_cnes_raw import DatasusCnesRawAdapter
from cnes_infra.ingestion.datasus_cnes_transport import (
    DatasusCnesError,
    DatasusCnesFtpTransport,
    DatasusCnesRequest,
    DatasusCnesTransportPort,
)

__all__ = (
    "DatasusCnesError",
    "DatasusCnesFtpTransport",
    "DatasusCnesRawAdapter",
    "DatasusCnesRequest",
    "DatasusCnesTransportPort",
)
