"""Oráculos SDK de paginação limitada, consistência forte e papel de dispatch."""

import json
from collections.abc import Iterator
from copy import deepcopy
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.control_plane.errors import Conflict
from cnes_domain.outbox_dispatcher import DispatchResult, dispatch_once
from cnes_infra.control_plane.dynamodb_keys import item_key, outbox_key
from packages.cnes_infra.tests.contracts.clock import _NOW
from packages.cnes_infra.tests.control_plane.outbox_cursor_support import (
    OutboxHarness,
    evento,
)


@pytest.fixture
def dynamo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[OutboxHarness]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    with mock_aws():
        yield OutboxHarness("dynamodb", tmp_path)


@pytest.mark.parametrize("state", ["missing", "delivered", "position"])
def test_candidato_gsi_invalido_consume_pagina_bruta_e_avanca_ate_evento_bom(
    dynamo: OutboxHarness, state: str,
) -> None:
    dynamo.stale_candidate(state)
    dynamo.put(evento("good"))
    dynamo.sdk.calls.clear()
    sink = dynamo.sink()
    assert dispatch_once(dynamo.adapter, sink, _NOW, 1) == DispatchResult(0, 0)
    queries = [request for name, request in dynamo.sdk.calls if name == "query"]
    assert len(queries) == 1
    assert queries[0]["Limit"] == 1
    assert dynamo.stored("good").delivered_at is None
    assert dispatch_once(dynamo.adapter, sink, _NOW, 1) == DispatchResult(1, 0)
    assert [record.event_id for record in dynamo.local_records()] == ["good"]


def test_cada_append_tem_getitem_forte_da_identidade_correta(dynamo: OutboxHarness) -> None:
    dynamo.put(evento("first"))
    dynamo.put(evento("second", 1))
    sink = dynamo.sink()
    evidence: list[tuple[str, list[dict[str, Any]]]] = []
    previous = [0]
    dynamo.sdk.calls.clear()

    def observar(event: Any) -> None:
        calls = dynamo.sdk.calls[previous[0]:]
        reads = [request for name, request in calls if name == "get_item"]
        evidence.append((event.event_id, reads))
        previous[0] = len(dynamo.sdk.calls)

    sink.before = observar
    assert dispatch_once(dynamo.adapter, sink, _NOW, 2) == DispatchResult(2, 0)
    assert [event_id for event_id, _ in evidence] == ["first", "second"]
    for event_id, reads in evidence:
        assert reads
        assert reads[-1]["Key"] == item_key(*outbox_key(event_id))
        assert reads[-1]["ConsistentRead"] is True


def test_dispatch_inteiro_so_usa_query_getitem_updateitem(dynamo: OutboxHarness) -> None:
    dynamo.put(evento("first"))
    sink = dynamo.sink()
    dynamo.sdk.calls.clear()
    for _ in range(4):
        dispatch_once(dynamo.adapter, sink, _NOW, 1)
    assert dynamo.stored("first").delivered_at == _NOW
    operations = {name for name, _ in dynamo.sdk.calls}
    assert operations <= {"query", "get_item", "update_item"}
    assert {"query", "get_item", "update_item"} <= operations


def test_erro_de_armazenamento_no_cas_propaga_sem_perder_token(dynamo: OutboxHarness) -> None:
    dynamo.put(evento("first"))
    dynamo.put(evento("second", 1))
    page = dynamo.adapter.read_outbox_page(None, 1)

    def indisponivel(name: str, _: dict[str, Any]) -> None:
        if name == "update_item":
            raise ClientError({"Error": {"Code": "InternalServerError"}}, "UpdateItem")

    dynamo.sdk.before = indisponivel
    with pytest.raises(ClientError, match="InternalServerError"):
        dynamo.adapter.advance_outbox_cursor(page.expected, page.next_cursor)
    dynamo.sdk.before = None
    assert dynamo.restart().read_outbox_page(None, 1).expected == page.expected


def _cursor_valido(dynamo: OutboxHarness) -> tuple[dict[str, Any], dict[str, Any]]:
    dynamo.put(evento("first"))
    dynamo.put(evento("second", 1))
    page = dynamo.adapter.read_outbox_page(None, 1)
    assert dynamo.adapter.advance_outbox_cursor(page.expected, page.next_cursor)
    updates = [request for name, request in dynamo.sdk.calls if name == "update_item"]
    key = updates[-1]["Key"]
    assert key != item_key(*outbox_key("first"))
    assert key != item_key(*outbox_key("second"))
    item = dynamo.client.get_item(
        TableName="cnesdata-control-plane", Key=key, ConsistentRead=True
    )["Item"]
    assert "entity" in item
    assert dynamo.restart().read_outbox_page(None, 1).expected == page.next_cursor
    return key, item


def test_cursor_de_entidade_estranha_nao_e_sobrescrito(dynamo: OutboxHarness) -> None:
    key, valid = _cursor_valido(dynamo)
    foreign = deepcopy(valid)
    foreign["entity"] = {"S": "TENANT"}
    assert {name for name in valid if valid[name] != foreign[name]} == {"entity"}
    dynamo.client.put_item(TableName="cnesdata-control-plane", Item=foreign)
    with pytest.raises((ValueError, Conflict)):
        dispatch_once(dynamo.restart(), dynamo.sink(), _NOW, 1)
    stored = dynamo.client.get_item(
        TableName="cnesdata-control-plane", Key=key, ConsistentRead=True
    )["Item"]
    assert stored == foreign


def test_cursor_de_entidade_correta_com_conteudo_corrupto_nao_e_sobrescrito(
    dynamo: OutboxHarness,
) -> None:
    key, valid = _cursor_valido(dynamo)
    content_fields = set(valid) - set(key) - {"entity"}
    assert content_fields
    corrupt = valid | {name: {"NULL": True} for name in content_fields}
    assert corrupt["entity"] == valid["entity"]
    dynamo.client.put_item(TableName="cnesdata-control-plane", Item=corrupt)
    with pytest.raises((ValueError, Conflict)):
        dynamo.restart().read_outbox_page(None, 1)
    stored = dynamo.client.get_item(
        TableName="cnesdata-control-plane", Key=key, ConsistentRead=True
    )["Item"]
    assert stored == corrupt


def test_cursor_duravel_e_resolvido_com_getitem_forte(dynamo: OutboxHarness) -> None:
    key, _ = _cursor_valido(dynamo)
    dynamo.sdk.calls.clear()
    page = dynamo.restart().read_outbox_page(None, 1)
    assert page.events == (evento("second", 1),)
    reads = [
        request for name, request in dynamo.sdk.calls
        if name == "get_item" and request["Key"] == key
    ]
    assert reads
    assert all(request["ConsistentRead"] is True for request in reads)
    assert {name for name, _ in dynamo.sdk.calls} <= {"get_item", "query"}


def test_cas_estabelecido_perde_para_concorrente_antes_do_updateitem(
    dynamo: OutboxHarness,
) -> None:
    for index in range(4):
        dynamo.put(evento(f"event-{index}", index))
    initial = dynamo.adapter.read_outbox_page(None, 1)
    assert dynamo.adapter.advance_outbox_cursor(initial.expected, initial.next_cursor)
    key = [request for name, request in dynamo.sdk.calls if name == "update_item"][-1]["Key"]
    contender = dynamo.restart()
    loser_page = dynamo.adapter.read_outbox_page(None, 1)
    winner_page = contender.read_outbox_page(None, 2)
    assert loser_page.expected == winner_page.expected == initial.next_cursor
    assert loser_page.next_cursor.position != winner_page.next_cursor.position
    interleavings: list[bool] = []

    def vencer_antes_da_escrita(name: str, request: dict[str, Any]) -> None:
        if name == "update_item" and request["Key"] == key:
            dynamo.sdk.before = None
            interleavings.append(
                contender.advance_outbox_cursor(winner_page.expected, winner_page.next_cursor)
            )

    dynamo.sdk.before = vencer_antes_da_escrita
    assert not dynamo.adapter.advance_outbox_cursor(loser_page.expected, loser_page.next_cursor)
    assert interleavings == [True]
    assert dynamo.restart().read_outbox_page(None, 1).expected == winner_page.next_cursor


def test_sink_s3_real_persiste_corpo_canonico_e_retencao(dynamo: OutboxHarness) -> None:
    original = evento("safe-event")
    dynamo.put(original)
    assert dispatch_once(dynamo.adapter, dynamo.sink("s3"), _NOW) == DispatchResult(1, 0)
    s3 = boto3.client("s3", region_name="us-east-1")
    key = "audit/354130/2026/07/15/safe-event.json"
    stored = s3.get_object(Bucket="cursor-audit", Key=key)
    assert json.loads(stored["Body"].read()) == original.model_dump(mode="json")
    assert stored["ObjectLockMode"] == "COMPLIANCE"


def test_cursor_com_tipo_de_posicao_corrupto_preserva_item(dynamo: OutboxHarness) -> None:
    key, valid = _cursor_valido(dynamo)
    token = dynamo.adapter.read_outbox_page(None, 1).expected
    names = [name for name, value in valid.items() if value == {"S": token.position}]
    assert len(names) == 1
    corrupt = valid | {names[0]: {"BOOL": False}}
    dynamo.client.put_item(TableName="cnesdata-control-plane", Item=corrupt)
    with pytest.raises(ValueError):
        dynamo.restart().read_outbox_page(None, 1)
    stored = dynamo.client.get_item(
        TableName="cnesdata-control-plane", Key=key, ConsistentRead=True,
    )["Item"]
    assert stored == corrupt


def test_marcador_concorrente_identico_e_replay_idempotente(dynamo: OutboxHarness) -> None:
    dynamo.put(evento("event"))
    contender = dynamo.restart()
    raced: list[str] = []

    def marcar_antes_do_update(name: str, request: dict[str, Any]) -> None:
        if name == "update_item" and request["Key"] == item_key(*outbox_key("event")):
            dynamo.sdk.before = None
            contender.mark_outbox_delivered("event", _NOW)
            raced.append("winner")

    dynamo.sdk.before = marcar_antes_do_update
    dynamo.adapter.mark_outbox_delivered("event", _NOW)
    assert raced == ["winner"]
    assert dynamo.stored("event").delivered_at == _NOW


def test_marcador_concorrente_divergente_preserva_vencedor(dynamo: OutboxHarness) -> None:
    dynamo.put(evento("event"))
    contender = dynamo.restart()
    winner_time = _NOW.replace(year=_NOW.year + 1)
    raced: list[str] = []

    def marcar_antes_do_update(name: str, request: dict[str, Any]) -> None:
        if name == "update_item" and request["Key"] == item_key(*outbox_key("event")):
            dynamo.sdk.before = None
            contender.mark_outbox_delivered("event", winner_time)
            raced.append("winner")

    dynamo.sdk.before = marcar_antes_do_update
    with pytest.raises(Conflict, match="outbox_delivery_conflict"):
        dynamo.adapter.mark_outbox_delivered("event", _NOW)
    assert raced == ["winner"]
    assert dynamo.stored("event").delivered_at == winner_time


def test_erro_sdk_no_marcador_propaga_sem_marcar(dynamo: OutboxHarness) -> None:
    dynamo.put(evento("event"))

    def indisponivel(name: str, request: dict[str, Any]) -> None:
        if name == "update_item" and request["Key"] == item_key(*outbox_key("event")):
            raise ClientError({"Error": {"Code": "InternalServerError"}}, "UpdateItem")

    dynamo.sdk.before = indisponivel
    with pytest.raises(ClientError, match="InternalServerError"):
        dynamo.adapter.mark_outbox_delivered("event", _NOW)
    dynamo.sdk.before = None
    assert dynamo.stored("event") == evento("event")


def _item_da_segunda_outbox(dynamo: OutboxHarness) -> Any:
    return dynamo.client.get_item(
        TableName="cnesdata-control-plane", Key=item_key(*outbox_key("second")),
        ConsistentRead=True,
    ).get("Item")


def _alterar_so_metadados(dynamo: OutboxHarness, state: str) -> Any:
    before = _item_da_segunda_outbox(dynamo)
    request: dict[str, Any] = {
        "TableName": "cnesdata-control-plane", "Key": item_key(*outbox_key("second")),
    }
    if state == "unindexed":
        request["UpdateExpression"] = "REMOVE gsi6pk, gsi6sk"
        changed = {"gsi6pk", "gsi6sk"}
    else:
        field, value = {
            "foreign": ("entity", "TENANT"),
            "position": ("gsi6sk", "2099-01-01T00:00:00+00:00#second"),
            "partition": ("gsi6pk", "OUTBOX#OTHER"),
        }[state]
        request |= {
            "UpdateExpression": "SET #field = :value",
            "ExpressionAttributeNames": {"#field": field},
            "ExpressionAttributeValues": {":value": {"S": value}},
        }
        changed = {field}
    dynamo.client.update_item(**request)
    after = _item_da_segunda_outbox(dynamo)
    differences = {key for key in before.keys() | after.keys() if before.get(key) != after.get(key)}
    assert differences == changed
    assert tuple(after[key] for key in ("pk", "sk", "payload")) == tuple(
        before[key] for key in ("pk", "sk", "payload")
    )
    return after


def _observar_segunda_revalidacao(dynamo: OutboxHarness, sink: Any, result: Any) -> dict[str, Any]:
    markers = [
        request for name, request in dynamo.sdk.calls
        if name == "update_item" and request["Key"] == item_key(*outbox_key("second"))
    ]
    following = dynamo.restart().read_outbox_page(None, 1)
    return {
        "result": result,
        "attempts": [event.event_id for event in sink.attempts],
        "records": [event.event_id for event in dynamo.local_records()],
        "second_item": _item_da_segunda_outbox(dynamo),
        "second_markers": len(markers),
        "cursor_version": following.expected.version,
        "following": [event.event_id for event in following.events],
        "allowed_operations": {name for name, _ in dynamo.sdk.calls}
        <= {"query", "get_item", "update_item"},
        "strong_reads": all(
            request["ConsistentRead"] for name, request in dynamo.sdk.calls if name == "get_item"
        ),
    }


@pytest.mark.parametrize("state", ["foreign", "position", "partition", "unindexed"])
def test_get_final_rejeita_drift_so_de_metadados_sem_append_ou_marcador(
    dynamo: OutboxHarness, state: str,
) -> None:
    for index, event_id in enumerate(("first", "second", "third")):
        dynamo.put(evento(event_id, index))
    sink = dynamo.sink()
    changed: list[Any] = []
    dynamo.sdk.calls.clear()

    def alterar_apos_pagina(event: Any) -> None:
        if event.event_id == "first":
            reads = [
                request for name, request in dynamo.sdk.calls
                if name == "get_item" and request["Key"] == item_key(*outbox_key("second"))
            ]
            assert len(reads) == 1
            assert reads[0]["ConsistentRead"] is True
            changed.append(_alterar_so_metadados(dynamo, state))

    sink.before = alterar_apos_pagina
    result = dispatch_once(dynamo.adapter, sink, _NOW, 2)
    assert len(changed) == 1
    assert _observar_segunda_revalidacao(dynamo, sink, result) == {
        "result": DispatchResult(1, 0), "attempts": ["first"], "records": ["first"],
        "second_item": changed[0], "second_markers": 0, "cursor_version": 1,
        "following": ["third"], "allowed_operations": True, "strong_reads": True,
    }
    assert dynamo.stored("first").delivered_at == _NOW


def test_get_final_sem_drift_entrega_e_preserva_lookup_e_replay_entregue(
    dynamo: OutboxHarness,
) -> None:
    for index, event_id in enumerate(("first", "second", "third")):
        dynamo.put(evento(event_id, index))
    sink = dynamo.sink()
    dynamo.sdk.calls.clear()
    result = dispatch_once(dynamo.adapter, sink, _NOW, 2)
    delivered = evento("second", 1).model_copy(update={"delivered_at": _NOW})
    assert dynamo.adapter.get_outbox_event("second") == delivered
    dynamo.adapter.mark_outbox_delivered("second", _NOW)
    stored = _item_da_segunda_outbox(dynamo)
    assert "gsi6pk" not in stored
    assert "gsi6sk" not in stored
    assert _observar_segunda_revalidacao(dynamo, sink, result) == {
        "result": DispatchResult(2, 0), "attempts": ["first", "second"],
        "records": ["first", "second"], "second_item": stored, "second_markers": 1,
        "cursor_version": 1, "following": ["third"],
        "allowed_operations": True, "strong_reads": True,
    }


@pytest.mark.parametrize(("state", "markers"), [("delivered", 1), ("missing", 0)])
def test_get_final_preserva_entrega_ou_remocao_concorrente(
    dynamo: OutboxHarness, state: str, markers: int,
) -> None:
    for index, event_id in enumerate(("first", "second", "third")):
        dynamo.put(evento(event_id, index))
    sink = dynamo.sink()
    changed: list[Any] = []
    dynamo.sdk.calls.clear()

    def alterar_apos_pagina(event: Any) -> None:
        if event.event_id != "first":
            return
        if state == "delivered":
            dynamo.adapter.mark_outbox_delivered("second", _NOW)
        else:
            dynamo.client.delete_item(
                TableName="cnesdata-control-plane", Key=item_key(*outbox_key("second")),
            )
        changed.append(_item_da_segunda_outbox(dynamo))

    sink.before = alterar_apos_pagina
    result = dispatch_once(dynamo.adapter, sink, _NOW, 2)
    assert len(changed) == 1
    assert _observar_segunda_revalidacao(dynamo, sink, result) == {
        "result": DispatchResult(1, 0), "attempts": ["first"], "records": ["first"],
        "second_item": changed[0], "second_markers": markers,
        "cursor_version": 1, "following": ["third"],
        "allowed_operations": True, "strong_reads": True,
    }


def test_get_final_propaga_erro_sdk_e_dispatch_conta_falha_sem_append(
    dynamo: OutboxHarness,
) -> None:
    for index, event_id in enumerate(("first", "second", "third")):
        dynamo.put(evento(event_id, index))
    original = _item_da_segunda_outbox(dynamo)
    sink = dynamo.sink()
    dynamo.sdk.calls.clear()

    def indisponivel(name: str, request: dict[str, Any]) -> None:
        if name == "get_item" and request["Key"] == item_key(*outbox_key("second")):
            raise ClientError({"Error": {"Code": "InternalServerError"}}, "GetItem")

    def falhar_apos_pagina(event: Any) -> None:
        if event.event_id == "first":
            dynamo.sdk.before = indisponivel

    sink.before = falhar_apos_pagina
    result = dispatch_once(dynamo.adapter, sink, _NOW, 2)
    with pytest.raises(ClientError, match="InternalServerError"):
        dynamo.adapter.get_outbox_event("second")
    dynamo.sdk.before = None
    assert _observar_segunda_revalidacao(dynamo, sink, result) == {
        "result": DispatchResult(1, 1), "attempts": ["first"], "records": ["first"],
        "second_item": original, "second_markers": 0, "cursor_version": 1,
        "following": ["third"], "allowed_operations": True, "strong_reads": True,
    }
