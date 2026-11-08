"""ARN de outro serviço falha antes da sessão do worker AWS."""

import pytest

from apps.data_processor.tests import test_aws_mode_settings_preflight as settings_tests

external_trace = settings_tests.external_trace


@pytest.mark.asyncio
async def test_recover_rejeita_arn_ecs_com_recurso_state_machine_antes_de_session(
    external_trace: list[str],
) -> None:
    result = await settings_tests._invoke("recover-once", {
        "AWS_STATE_MACHINE_ARN": "arn:aws:ecs:us-east-2:000000000000:stateMachine:processor",
    })

    assert (result, external_trace) == (2, [])
