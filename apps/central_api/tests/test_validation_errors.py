"""Testes do helper central_api.validation_errors.validation_error."""
from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, cast

import pytest
from fastapi import HTTPException

from central_api.validation_errors import validation_error

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


def test_detail_e_lista_com_loc_msg_type():
    exc = validation_error("algo_deu_errado", loc=["body", "campo"])
    assert exc.status_code == 422
    assert isinstance(exc.detail, list)
    assert exc.detail == [
        {"loc": ["body", "campo"], "msg": "algo_deu_errado", "type": "value_error"},
    ]


def test_loc_default_e_body():
    exc = validation_error("sem_loc_explicito")
    assert cast("list[dict[str, object]]", exc.detail)[0]["loc"] == ["body"]


def test_enqueue_rejeita_source_interno_desconhecido():
    from central_api.routes.extractions import EnqueueRequest, enqueue

    request = EnqueueRequest.model_construct(
        source_type="UNKNOWN", tenant_id="354130", competencia=date(2026, 1, 1)
    )

    with pytest.raises(HTTPException) as captured:
        enqueue(request, None, cast("Engine", None))

    assert captured.value.status_code == 422
    assert cast("list[dict[str, object]]", captured.value.detail)[0]["loc"] == [
        "body", "source_type",
    ]
