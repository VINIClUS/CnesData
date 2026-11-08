"""Ports e consultas tipadas do plano de controle."""

from cnes_control_plane.model.queries import (
    LatestSucceededJobQuery,
    RawIdentity,
    RawManifestChainQuery,
    WaitingRunsForDependencyQuery,
)
from cnes_control_plane.ports.control_plane import ControlPlanePort, TypedRawQueryPort

__all__ = [
    "ControlPlanePort", "LatestSucceededJobQuery", "RawIdentity", "RawManifestChainQuery",
    "TypedRawQueryPort", "WaitingRunsForDependencyQuery",
]
