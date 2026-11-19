"""Wiring compartilhado de fontes e política de publicação do processor."""
from __future__ import annotations

from typing import TYPE_CHECKING

from cnes_contracts.manifests.raw import SourceType
from cnes_domain.billing.publication import BillingPublicationPolicy, PublicationPolicyDependencies
from cnes_domain.orchestration.source_catalog import build_source_catalog
from cnes_infra.billing.wiring import build_entitlement_gate
from data_processor.pipeline.materialize_cnes import materialize_cnes
from data_processor.pipeline.normalize_cnes_local import normalize_cnes_local
from data_processor.pipeline.normalize_cnes_nacional import normalize_cnes_nacional
from data_processor.pipeline.reconcile_cnes import reconcile_cnes
from data_processor.pipeline.source_registry import SourcePipeline, SourceRegistry
from data_processor.sources.bpa.normalize import normalize_bpa
from data_processor.sources.bpa.reconcile import reconcile_bpa
from data_processor.sources.bpa.serving import materialize_bpa
from data_processor.sources.sia.normalize import normalize_sia
from data_processor.sources.sia.reconcile import reconcile_sia
from data_processor.sources.sia.serving import materialize_sia
from data_processor.sources.sihd.normalize import normalize_sihd
from data_processor.sources.sihd.reconcile import reconcile_sihd
from data_processor.sources.sihd.serving import materialize_sihd

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import NormalizeRequest, NormalizeResult
    from cnes_domain.billing.ports import ClockPort
    from cnes_domain.orchestration.source_catalog import SourceCatalog
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_infra.billing import BillingSettings
    from cnes_infra.billing.wiring import BillingGateResources


class UnsupportedSourceType(ValueError):
    pass


def normalize_cnes(request: NormalizeRequest, store: ObjectStorePort) -> NormalizeResult:
    if request.source_type is SourceType.CNES_LOCAL:
        return normalize_cnes_local(request, store)
    if request.source_type is SourceType.CNES_NACIONAL:
        return normalize_cnes_nacional(request, store)
    raise UnsupportedSourceType(request.source_type)


def build_source_registry(catalog: SourceCatalog | None = None) -> SourceRegistry:
    resolved = catalog if catalog is not None else build_source_catalog()
    bundles = (
        SourcePipeline(
            definition=resolved.for_pipeline("cnes"), normalize=normalize_cnes,
            reconcile=reconcile_cnes, materialize=materialize_cnes,
        ),
        SourcePipeline(
            definition=resolved.for_pipeline("sihd"), normalize=normalize_sihd,
            reconcile=reconcile_sihd, materialize=materialize_sihd,
        ),
        SourcePipeline(
            definition=resolved.for_pipeline("bpa"), normalize=normalize_bpa,
            reconcile=reconcile_bpa, materialize=materialize_bpa,
        ),
        SourcePipeline(
            definition=resolved.for_pipeline("sia"), normalize=normalize_sia,
            reconcile=reconcile_sia, materialize=materialize_sia,
        ),
    )
    return SourceRegistry(resolved, bundles)


def build_publication_policy(
    billing: BillingSettings, control_plane: ControlPlanePort,
    resources: BillingGateResources, clock: ClockPort,
) -> BillingPublicationPolicy:
    return BillingPublicationPolicy(PublicationPolicyDependencies(
        control_plane, build_entitlement_gate(billing, resources), clock,
        billing.execution_mode,
    ))
