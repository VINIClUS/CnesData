"""AccessTokenStore: opaque single-use token store with TTL."""
import pytest

from cnes_infra.auth.access_tokens import AccessToken, AccessTokenStore


@pytest.mark.asyncio
async def test_emite_token_e_resolve_agent_id_e_tenant_id():
    store = AccessTokenStore()
    token = await store.issue(tenant_id="presidente-epitacio", ttl_seconds=300)
    assert len(token) == 43  # token_urlsafe(32) → 43 chars

    resolved = await store.consume(token)
    assert isinstance(resolved, AccessToken)
    assert resolved.tenant_id == "presidente-epitacio"
    assert len(resolved.agent_id) == 32  # token_hex(16) → 32 hex chars


@pytest.mark.asyncio
async def test_consume_segunda_vez_retorna_none():
    store = AccessTokenStore()
    token = await store.issue(tenant_id="t1", ttl_seconds=300)
    first = await store.consume(token)
    assert first is not None

    second = await store.consume(token)
    assert second is None


@pytest.mark.asyncio
async def test_token_inexistente_retorna_none():
    store = AccessTokenStore()
    resolved = await store.consume("token-fantasma")
    assert resolved is None


@pytest.mark.asyncio
async def test_token_expirado_retorna_none():
    store = AccessTokenStore(now=lambda: 0.0)
    token = await store.issue(tenant_id="t1", ttl_seconds=10)
    store._now = lambda: 11.0
    resolved = await store.consume(token)
    assert resolved is None


@pytest.mark.asyncio
async def test_dois_tokens_geram_agent_ids_distintos():
    store = AccessTokenStore()
    t1 = await store.issue(tenant_id="t")
    t2 = await store.issue(tenant_id="t")
    a1 = await store.consume(t1)
    a2 = await store.consume(t2)
    assert a1.agent_id != a2.agent_id
