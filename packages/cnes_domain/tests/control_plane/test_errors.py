import pytest

from cnes_domain.control_plane.errors import (
    Conflict,
    ControlPlaneErrorCode,
    FenceRejected,
    InvalidTransition,
    LeaseLost,
    NotFound,
)
from packages.cnes_domain.tests.control_plane.legacy_error_codes import LEGACY_CODES

ERROR_TYPES = (Conflict, InvalidTransition, LeaseLost, FenceRejected, NotFound)


def test_catalogo_preserva_exatamente_os_codigos_legados() -> None:
    assert {code.value for code in ControlPlaneErrorCode} == set(LEGACY_CODES)
    assert len(ControlPlaneErrorCode.__members__) == len(LEGACY_CODES)


@pytest.mark.parametrize("code", LEGACY_CODES)
@pytest.mark.parametrize("error_type", ERROR_TYPES)
def test_normaliza_todo_codigo_legado_sem_alterar_texto(
    error_type: type[Exception], code: str
) -> None:
    error = error_type(code)

    assert error.code is ControlPlaneErrorCode(code)
    assert error.code.name == code.upper()
    assert str(error) == code
    assert error.args == (code,)


def test_normaliza_codigo_legado_preservando_mensagem_e_argumentos() -> None:
    error = Conflict("job_conflict")

    assert error.code is ControlPlaneErrorCode.JOB_CONFLICT
    assert str(error) == "job_conflict"
    assert error.args == ("job_conflict",)
    assert type(error.args[0]) is str


@pytest.mark.parametrize("error_type", ERROR_TYPES)
def test_aceita_enum_preservando_categoria_e_argumentos(error_type: type[Exception]) -> None:
    error = error_type(ControlPlaneErrorCode.JOB_CONFLICT)

    assert error.code is ControlPlaneErrorCode.JOB_CONFLICT
    assert str(error) == "job_conflict"
    assert error.args == ("job_conflict",)
    assert type(error.args[0]) is str
    assert isinstance(error, LookupError if error_type is NotFound else RuntimeError)
    assert isinstance(error, Conflict) is (error_type is not NotFound)


@pytest.mark.parametrize("error_type", ERROR_TYPES)
@pytest.mark.parametrize("message", ["external_new_code", "transition=QUEUED->SUCCEEDED", ""])
def test_preserva_codigos_desconhecidos_e_mensagens_dinamicas(
    error_type: type[Exception], message: str
) -> None:
    error = error_type(message)

    assert error.code == message
    assert type(error.code) is str
    assert str(error) == message
    assert error.args == (message,)


@pytest.mark.parametrize("error_type", ERROR_TYPES)
@pytest.mark.parametrize("args", [(), (None,), (42,), ("job_conflict", "detail")])
def test_preserva_construcao_legada_sem_codigo_ou_com_argumentos_adicionais(
    error_type: type[Exception], args: tuple[object, ...]
) -> None:
    error = error_type(*args)

    assert error.args == args
    assert str(error) == str(Exception(*args))
    expected = ControlPlaneErrorCode.JOB_CONFLICT if len(args) == 2 else None
    assert error.code is expected
