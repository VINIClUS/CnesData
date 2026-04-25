"""DeviceCodeStore: TTL semantics + single-use redemption."""
import pytest

from cnes_infra.auth.device_codes import DeviceCodeStore


@pytest.mark.asyncio
async def test_emite_codigo_e_recupera_status_pendente():
    store = DeviceCodeStore()
    auth = await store.issue(client_id="agent", scope="agent.provision", ttl_seconds=600)
    assert auth.user_code != ""
    assert len(auth.user_code) >= 8

    status = await store.poll_device_code(auth.device_code)
    assert status.kind == "authorization_pending"


@pytest.mark.asyncio
async def test_redeem_user_code_marca_autorizado():
    store = DeviceCodeStore()
    auth = await store.issue(client_id="agent", scope="x", ttl_seconds=600)

    ok = await store.redeem_user_code(auth.user_code, tenant_id="presidente-epitacio")
    assert ok is True

    status = await store.poll_device_code(auth.device_code)
    assert status.kind == "authorized"
    assert status.tenant_id == "presidente-epitacio"


@pytest.mark.asyncio
async def test_redeem_segunda_vez_rejeita():
    store = DeviceCodeStore()
    auth = await store.issue(client_id="agent", scope="x", ttl_seconds=600)
    await store.redeem_user_code(auth.user_code, tenant_id="t1")

    ok2 = await store.redeem_user_code(auth.user_code, tenant_id="t2")
    assert ok2 is False


@pytest.mark.asyncio
async def test_codigo_expira_apos_ttl():
    clock = [0.0]
    store = DeviceCodeStore(now=lambda: clock[0])
    auth = await store.issue(client_id="agent", scope="x", ttl_seconds=10)

    clock[0] = 11.0
    status = await store.poll_device_code(auth.device_code)
    assert status.kind == "expired_token"


@pytest.mark.asyncio
async def test_user_code_inexistente_retorna_false():
    store = DeviceCodeStore()
    ok = await store.redeem_user_code("WRONG-CODE", tenant_id="t")
    assert ok is False


@pytest.mark.asyncio
async def test_device_code_apos_redeem_pode_ser_consumido_uma_vez():
    """Polling consumes the authorized code (single delivery)."""
    store = DeviceCodeStore()
    auth = await store.issue(client_id="agent", scope="x", ttl_seconds=600)
    await store.redeem_user_code(auth.user_code, tenant_id="t1")

    status1 = await store.poll_device_code(auth.device_code)
    assert status1.kind == "authorized"

    status2 = await store.poll_device_code(auth.device_code)
    assert status2.kind == "expired_token"
