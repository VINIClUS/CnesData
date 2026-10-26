"""Envelopes fechados de client são validados pela facade antes de I/O."""

from pathlib import Path

import pytest

from tests.integration.aws_capability._service_cognito_testing import make_read_lab
from tests.integration.aws_capability._service_read_assertions import assert_private, assert_process
from tests.integration.aws_capability._service_read_testing import ReadLab
from tests.integration.aws_capability.contracts import Resource, ServiceFailure
from tests.integration.aws_capability.test_service_cognito_client_lookup import (
    CLIENT,
    DATA,
)


@pytest.fixture
def read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    return make_read_lab(monkeypatch, tmp_path)


@pytest.mark.parametrize("case", DATA["invalid_envelopes"])
@pytest.mark.parametrize("method", ["lookup", "inspect"])
def test_envelope_client_invalido_falha_antes_de_io(
    read_lab: ReadLab, case: str, method: str
) -> None:
    resource = Resource("cognito", DATA["invalid_envelopes"][case], "cnesdata-cap-a1b2c3d4")
    with pytest.raises(ServiceFailure) as error:
        getattr(read_lab.services, method)(resource, read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    assert_private(error.value, read_lab, "")
    assert_process(read_lab, [])


@pytest.mark.parametrize("name", ["", None, 7])
def test_nome_do_root_invalido_nao_consulta_client(read_lab: ReadLab, name: str) -> None:
    resource = Resource("cognito", CLIENT.resource_id, name)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource, read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    assert_private(error.value, read_lab, "")
    assert_process(read_lab, [])
