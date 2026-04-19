"""Testes do RLS listener — SET LOCAL rls.tenant_id."""
from unittest.mock import MagicMock, patch

from cnes_domain.tenant import tenant_id_ctx


def _capturar_listener():
    from cnes_infra.storage.rls import install_rls_listener

    captured = {}

    def mock_listens_for(target, identifier):
        def decorator(fn):
            captured["fn"] = fn
            return fn
        return decorator

    engine = MagicMock()
    with patch("cnes_infra.storage.rls.event.listens_for", mock_listens_for):
        install_rls_listener(engine)

    return captured["fn"]


class TestRlsListener:

    def test_sem_tenant_nao_executa_set_local(self):
        listener = _capturar_listener()
        conn = MagicMock()
        with patch("cnes_infra.storage.rls.tenant_id_ctx") as mock_ctx:
            mock_ctx.get.side_effect = LookupError
            listener(conn)
        conn.execute.assert_not_called()

    def test_com_tenant_executa_set_local(self):
        listener = _capturar_listener()
        conn = MagicMock()
        token = tenant_id_ctx.set("355030")
        try:
            listener(conn)
        finally:
            tenant_id_ctx.reset(token)
        conn.execute.assert_called_once()
        args, _ = conn.execute.call_args
        assert args[1] == {"tid": "355030"}
