"""Tag imutável release_id no manifest promovido ao ECR, com readback e idempotência."""

from __future__ import annotations

import pytest

from scripts.promotion.promote_ecr import promote_image, verify_promoted_image
from tests.production.delivery._phase_a_fixtures import RegistryBoundary, _digest, _promotion

OTHER = "sha256:" + "e" * 64


class Readback(RegistryBoundary):
    def __init__(self, tags=None, holder=None):
        super().__init__()
        self.readback_tags, self.holder = tags, holder

    def describe_image(self, reference):
        observed = super().describe_image(reference)
        if observed is None:
            return None
        if reference != _digest(self.manifest):
            return observed if self.holder is None else {**observed, "digest": self.holder}
        return observed if self.readback_tags is None else {**observed, "tags": self.readback_tags}


def _manifests(registry):
    return [item for item in registry.copied if item[0] == _digest(registry.manifest)]


def test_put_do_manifest_leva_a_tag_release_id():
    registry = RegistryBoundary()
    context = _promotion(registry)
    result = promote_image(context, registry)
    digest = _digest(registry.manifest)
    assert result["image_digest"] == digest
    assert registry.tags == {context["release_id"]: digest}
    assert registry.describe_image(context["release_id"]) == registry.describe_image(digest)


@pytest.mark.parametrize(("registry", "reason"), [
    pytest.param(Readback(tags=[]), "ecr=tag_readback", id="tag_ausente"),
    pytest.param(Readback(tags=["candidate-outra"]), "ecr=tag_readback", id="tag_divergente"),
    pytest.param(Readback(holder=OTHER), "ecr=tag_readback", id="tag_em_outro_digest"),
])
def test_readback_recusa_digest_certo_com_tag_ausente_ou_divergente(registry, reason):
    context = _promotion(registry)
    with pytest.raises(ValueError, match=reason):
        promote_image(context, registry)
    with pytest.raises(ValueError, match=reason):
        verify_promoted_image(context, registry)


def test_repetir_a_mesma_promocao_e_idempotente():
    registry = RegistryBoundary()
    context = _promotion(registry)
    first = promote_image(context, registry)
    assert promote_image(context, registry) == first
    assert len(_manifests(registry)) == 1
    verify_promoted_image(context, registry)


def test_tag_ja_presente_em_outro_digest_e_recusada_antes_de_copiar():
    registry = RegistryBoundary()
    context = _promotion(registry)
    registry.tags[context["release_id"]] = OTHER
    with pytest.raises(ValueError, match="ecr=tag_conflict"):
        promote_image(context, registry)
    assert registry.copied == []
    assert registry.tags == {context["release_id"]: OTHER}


def test_readback_sem_promocao_recusa_digest_sem_tag():
    registry = RegistryBoundary()
    with pytest.raises(ValueError, match="ecr=tag_readback"):
        verify_promoted_image(_promotion(registry), registry)
