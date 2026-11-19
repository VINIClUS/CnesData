"""Recibos sintéticos preservam o vínculo com os bytes do candidato autenticado."""

from copy import deepcopy

import pytest

from scripts.promotion.phase_a import finalize_phase_a, prepare_inputs
from scripts.promotion.promote_ecr import promote_candidate
from tests.production.delivery import test_candidate_manifest as candidate_fixtures
from tests.production.delivery._phase_a_fixtures import (
    ACCOUNT,
    ECR,
    NOW,
    REGION,
    FixtureClock,
    RegistryBoundary,
    _candidate_reference,
    _creator_pin,
)
from tests.production.delivery._phase_a_runtime_fixtures import (
    RuntimeBoundary,
    _finalization_fixture,
)

github = candidate_fixtures.github
DIGEST = "sha256:56f27ad97169bc7c46cfc6caf7349e40f15215b2744c354eed8c9dc7a0e3cdab"
OTHER_DIGEST = "sha256:" + "e" * 64
BASELINE = {
    "processor_active_unit_task_definition_arn": None,
    "processor_retained_unit_task_definition_arns": [],
}
DIVERGENCES = [
    pytest.param({"image_digest": OTHER_DIGEST}, id="digest"),
    pytest.param({"ecr_image": ECR + "@" + OTHER_DIGEST}, id="ecr-digest"),
    pytest.param(
        {"image_digest": OTHER_DIGEST, "ecr_image": ECR + "@" + OTHER_DIGEST},
        id="digest-e-ecr-coerentes",
    ),
    pytest.param({"ecr_image": ECR.replace(ACCOUNT, "999999999999") + "@" + DIGEST}, id="conta"),
    pytest.param({"ecr_image": ECR.replace(REGION, "us-east-1") + "@" + DIGEST}, id="regiao"),
    pytest.param({"ecr_image": ECR + "-outro@" + DIGEST}, id="repositorio"),
]


@pytest.fixture
def receipt(github):
    registry = RegistryBoundary()
    reference = _candidate_reference(github, registry)
    governed = {
        "account_id": ACCOUNT,
        "region": REGION,
        "now": NOW,
        "candidate_creator": _creator_pin(),
    }
    promotion = promote_candidate(reference, github, registry, governed)
    return promotion, registry


def _prepare_runtime(github, promotion, registry):
    prepared = _finalization_fixture(github, promotion, FixtureClock()) | {"registry": registry}
    governed = prepared["governed"]
    governed["canary_trusted_principal_arn"] = governed["phase_a_inputs"][
        "canary_trusted_principal_arn"
    ]
    return prepared


@pytest.mark.parametrize("overrides", DIVERGENCES)
def test_inputs_recusam_digest_e_destino_divergentes_do_candidato(github, receipt, overrides):
    promotion, registry = receipt
    prepared = _prepare_runtime(github, promotion, registry)
    candidate = deepcopy(promotion["candidate"])
    copied = list(registry.copied)
    promotion.update(overrides)
    with pytest.raises(ValueError, match="promotion=image_binding"):
        prepare_inputs(promotion, BASELINE, prepared["governed"])
    assert promotion["candidate"] == candidate
    assert registry.copied == copied


@pytest.mark.parametrize("overrides", DIVERGENCES)
def test_finalizacao_recusa_recibo_adulterado_mesmo_com_readbacks_coerentes(
    github, receipt, overrides
):
    promotion, registry = receipt
    candidate = deepcopy(promotion["candidate"])
    copied = list(registry.copied)
    promotion.update(overrides)
    prepared = _prepare_runtime(github, promotion, registry)
    api = RuntimeBoundary(github, prepared)
    with pytest.raises(ValueError, match="promotion=image_binding"):
        finalize_phase_a(promotion, prepared["bundle"]["references"], api, prepared["governed"])
    assert promotion["candidate"] == candidate
    assert api.deadlines == []
    assert api.writes == 0
    assert api.registrations == 0
    assert registry.copied == copied


@pytest.mark.parametrize("field", ["image_digest", "ecr_image"])
@pytest.mark.parametrize("value", [None, "", 42, [], "sha256:invalido", "ausente"])
def test_recibo_malformado_falha_antes_de_inputs_e_publicacao(github, receipt, field, value):
    promotion, registry = receipt
    prepared = _prepare_runtime(github, promotion, registry)
    api = RuntimeBoundary(github, prepared)
    copied = list(registry.copied)
    if value == "ausente":
        del promotion[field]
    else:
        promotion[field] = value
    with pytest.raises(ValueError, match="promotion=image_binding"):
        prepare_inputs(promotion, BASELINE, prepared["governed"])
    with pytest.raises(ValueError, match="promotion=image_binding"):
        finalize_phase_a(promotion, prepared["bundle"]["references"], api, prepared["governed"])
    assert api.deadlines == []
    assert api.writes == 0
    assert api.registrations == 0
    assert registry.copied == copied


def test_recibo_autenticado_preserva_digest_nos_inputs_e_evidencia_sem_nova_promocao(
    github, receipt
):
    promotion, registry = receipt
    prepared = _prepare_runtime(github, promotion, registry)
    api = RuntimeBoundary(github, prepared)
    copied = list(registry.copied)
    inputs = prepare_inputs(promotion, BASELINE, prepared["governed"])
    first = finalize_phase_a(promotion, prepared["bundle"]["references"], api, prepared["governed"])
    again = finalize_phase_a(promotion, prepared["bundle"]["references"], api, prepared["governed"])
    assert promotion["candidate"]["images"]["data_processor"]["image"].endswith("@" + DIGEST)
    assert inputs["image_digest"] == first["image_digest"] == DIGEST
    assert promotion["ecr_image"] == ECR + "@" + DIGEST
    assert first == again
    assert first["iam"]["proof_kind"] == "iam-readback-and-evaluation"
    assert api.writes == 1
    assert api.registrations == 0
    assert registry.copied == copied
