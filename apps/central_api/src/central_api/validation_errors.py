"""Emissão de erros de validação HTTP 422."""

from fastapi import HTTPException


def validation_error(msg: str, loc: list[str] | None = None) -> HTTPException:
    """Cria uma exceção HTTP 422 com detalhe estruturado.

    Args: msg e loc da validação.
    Returns: exceção pronta para ser lançada.
    """
    return HTTPException(
        status_code=422,
        detail=[{"loc": loc or ["body"], "msg": msg, "type": "value_error"}],
    )
