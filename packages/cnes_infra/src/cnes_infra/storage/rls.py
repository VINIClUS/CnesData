"""RLS enforcement via SQLAlchemy event — SET LOCAL por transação."""

import logging

from sqlalchemy import Engine, event, text

from cnes_domain.tenant import tenant_id_ctx

logger = logging.getLogger(__name__)


def install_rls_listener(engine: Engine) -> None:
    @event.listens_for(engine, "begin")
    def _set_rls_on_begin(conn: object) -> None:
        try:
            tid = tenant_id_ctx.get()
        except LookupError:
            return
        conn.execute(
            text("SELECT set_config('rls.tenant_id', :tid, true)"),
            {"tid": tid},
        )
        logger.debug("rls_set tenant_id=%s", tid)
