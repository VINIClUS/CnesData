"""RLS enforcement via SQLAlchemy event — SET LOCAL por transação."""

import logging

from cnes_domain.tenant import tenant_id_ctx
from sqlalchemy import Engine, event, text

logger = logging.getLogger(__name__)


def install_rls_listener(engine: Engine) -> None:
    @event.listens_for(engine, "begin")
    def _set_rls_on_begin(conn: object) -> None:
        try:
            tid = tenant_id_ctx.get()
        except LookupError:
            return
        conn.execute(text("SET LOCAL rls.tenant_id = :tid"), {"tid": tid})
        logger.debug("rls_set tenant_id=%s", tid)
