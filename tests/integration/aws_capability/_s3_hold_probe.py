"""Probe s3-hold: legal hold bloqueia delete até ser desligado."""

from tests.integration.aws_capability._probe_attempt import must, version_of
from tests.integration.aws_capability._s3_actor import Actor
from tests.integration.aws_capability.contracts import ServiceFailure

KEY = "cap-hold"


def probe_hold(actor: Actor) -> dict[str, object]:
    """Returns: códigos observados; Raises: ServiceFailure se o controle final falhar."""
    version = version_of(must(actor.put(KEY, None), "put"))
    must(actor.hold(KEY, version, True), "hold_on")
    blocked = actor.delete(KEY, version, False)
    blocked_bypass = actor.delete(KEY, version, True)
    must(actor.hold(KEY, version, False), "hold_off")
    released = actor.delete(KEY, version, False)
    if not released.ok:
        raise ServiceFailure("probe=control_failed step=hold_off_delete")
    return {
        "hold_delete": blocked.code,
        "hold_delete_bypass": blocked_bypass.code,
        "hold_off_delete": released.code,
    }
