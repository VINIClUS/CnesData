"""Fronteira pública do despacho dedicado de auditoria."""

from cnes_domain.outbox_dispatcher import dispatch_once
from data_processor.aws_composition import AwsAuditRuntime


def run_audit_dispatch_once(runtime: AwsAuditRuntime) -> int:
    """Executa uma única página canônica da outbox.

    Args: runtime: Portas e relógio do despacho.
    Returns: 0 em sucesso ou 1 quando algum evento falhou.
    """
    result = dispatch_once(
        runtime.control_plane, runtime.sink, runtime.clock(), limit=100,
    )
    return int(result.failed > 0)
