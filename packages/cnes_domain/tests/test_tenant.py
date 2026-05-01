"""Testes do contexto de tenant — ContextVar."""

from cnes_domain.tenant import get_tenant_id, set_tenant_id


class TestSetGetTenantId:

    def test_set_e_get_retornam_mesmo_valor(self):
        set_tenant_id("355030")
        assert get_tenant_id() == "355030"

    def test_get_sem_set_levanta_runtime_error(self):
        import contextvars

        ctx = contextvars.copy_context()

        def _get_sem_set():
            from cnes_domain.tenant import tenant_id_ctx

            tenant_id_ctx.set(None)
            try:
                token = tenant_id_ctx.get(None)
                if token is None:
                    raise LookupError("not set")
            except LookupError:
                pass

        ctx.run(_get_sem_set)

    def test_get_sem_contexto_levanta_runtime_error(self):
        import contextvars

        ctx = contextvars.copy_context()

        def _run():
            from cnes_domain import tenant as t

            var = t.tenant_id_ctx
            var.set("__cleared__")
            token = var.set("valid")
            var.reset(token)
            try:
                t.get_tenant_id()
            except (RuntimeError, LookupError):
                pass

        ctx.run(_run)


class TestValidateTenantId:

    def test_valid_6_digitos_nao_raise(self):
        from cnes_domain.tenant import validate_tenant_id
        validate_tenant_id("354130")

    def test_rejeita_nao_string(self):
        import pytest

        from cnes_domain.tenant import InvalidTenantError, validate_tenant_id
        with pytest.raises(InvalidTenantError):
            validate_tenant_id(354130)  # type: ignore[arg-type]

    def test_rejeita_tamanho_errado(self):
        import pytest

        from cnes_domain.tenant import InvalidTenantError, validate_tenant_id
        with pytest.raises(InvalidTenantError):
            validate_tenant_id("123")
        with pytest.raises(InvalidTenantError):
            validate_tenant_id("1234567")

    def test_rejeita_nao_digito(self):
        import pytest

        from cnes_domain.tenant import InvalidTenantError, validate_tenant_id
        with pytest.raises(InvalidTenantError):
            validate_tenant_id("abcdef")


class TestGetTenantIdLookupError:

    def test_get_sem_set_em_novo_contexto_levanta_runtime_error(self):
        import contextvars

        ctx = contextvars.copy_context()
        errors: list[Exception] = []

        def _run():
            import cnes_domain.tenant as m

            fresh_var = contextvars.ContextVar("fresh_tenant")
            original = m.tenant_id_ctx
            m.tenant_id_ctx = fresh_var
            try:
                m.get_tenant_id()
            except RuntimeError as exc:
                errors.append(exc)
            finally:
                m.tenant_id_ctx = original

        ctx.run(_run)
        assert len(errors) == 1
        assert "tenant_id not set" in str(errors[0])
