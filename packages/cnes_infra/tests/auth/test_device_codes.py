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


@pytest.mark.asyncio
async def test_user_code_colisao_oito_vezes_levanta_runtime_error(monkeypatch):
    """If 8 consecutive user_codes collide, store raises RuntimeError."""
    store = DeviceCodeStore()
    auth = await store.issue(client_id="agent", scope="x", ttl_seconds=600)

    monkeypatch.setattr(
        "cnes_infra.auth.device_codes._generate_user_code",
        lambda: auth.user_code,
    )
    with pytest.raises(RuntimeError, match="user_code_collision"):
        await store.issue(client_id="agent", scope="x", ttl_seconds=600)


@pytest.mark.asyncio
async def test_redeem_apos_ttl_expirado_retorna_false():
    clock = [0.0]
    store = DeviceCodeStore(now=lambda: clock[0])
    auth = await store.issue(client_id="agent", scope="x", ttl_seconds=10)

    clock[0] = 11.0
    ok = await store.redeem_user_code(auth.user_code, tenant_id="t")
    assert ok is False


@pytest.mark.asyncio
async def test_poll_dentro_de_interval_aciona_slow_down():
    fake_time = [0.0]
    store = DeviceCodeStore(now=lambda: fake_time[0])
    auth = await store.issue(client_id="agent", scope="x", ttl_seconds=600)

    fake_time[0] = 0.0
    s1 = await store.poll_device_code(auth.device_code)
    assert s1.kind == "authorization_pending"

    fake_time[0] = 2.0
    s2 = await store.poll_device_code(auth.device_code)
    assert s2.kind == "slow_down"
    assert s2.interval == 10


@pytest.mark.asyncio
async def test_slow_down_dobra_interval_a_cada_violacao_com_cap_60():
    fake_time = [0.0]
    store = DeviceCodeStore(now=lambda: fake_time[0])
    auth = await store.issue(client_id="agent", scope="x", ttl_seconds=600)

    fake_time[0] = 0.0
    await store.poll_device_code(auth.device_code)

    expected_intervals = [10, 20, 40, 60, 60]
    for expected in expected_intervals:
        fake_time[0] += 1.0
        s = await store.poll_device_code(auth.device_code)
        assert s.kind == "slow_down"
        assert s.interval == expected


@pytest.mark.asyncio
async def test_poll_apos_interval_volta_a_comportamento_normal():
    fake_time = [0.0]
    store = DeviceCodeStore(now=lambda: fake_time[0])
    auth = await store.issue(client_id="agent", scope="x", ttl_seconds=600)

    fake_time[0] = 0.0
    await store.poll_device_code(auth.device_code)
    fake_time[0] = 6.0
    s = await store.poll_device_code(auth.device_code)
    assert s.kind == "authorization_pending"


@pytest.mark.asyncio
async def test_slow_down_state_isolado_entre_device_codes():
    fake_time = [0.0]
    store = DeviceCodeStore(now=lambda: fake_time[0])
    a1 = await store.issue(client_id="agent", scope="x", ttl_seconds=600)
    a2 = await store.issue(client_id="agent", scope="x", ttl_seconds=600)

    fake_time[0] = 0.0
    await store.poll_device_code(a1.device_code)
    fake_time[0] = 1.0
    s1 = await store.poll_device_code(a1.device_code)
    assert s1.kind == "slow_down"
    assert s1.interval == 10

    fake_time[0] = 1.0
    s2 = await store.poll_device_code(a2.device_code)
    assert s2.kind == "authorization_pending"
