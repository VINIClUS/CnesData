"""Testes dos contratos de processamento."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from cnes_contracts.manifests.outputs import OutputManifest, ServingDocument
from cnes_contracts.manifests.processing import (
    MaterializeRequest,
    MaterializeResult,
    NormalizeRequest,
    NormalizeResult,
    ReconcileRequest,
    ReconcileResult,
)
from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType
from cnes_contracts.manifests.validation import manifest_sha256

HASH = "a" * 64
NOW = datetime(2026, 7, 2, tzinfo=UTC)


def raw_manifest(subtype: str, sequence: int = 1, source: SourceType = SourceType.BPA_MAG):
    mode = SnapshotMode.FULL if sequence == 1 else SnapshotMode.DELTA
    return RawManifest(
        manifest_version=1,
        manifest_id=f"{subtype}-{sequence}",
        tenant_id="354130",
        source_type=source,
        file_subtype=subtype,
        competencia="2026-07",
        agent_id="agent-1",
        agent_version="1",
        schema_version="raw-v1",
        snapshot_mode=mode,
        snapshot_id=f"{subtype.lower()}-{sequence}",
        base_snapshot_id=None if sequence == 1 else f"{subtype.lower()}-1",
        sequence=sequence,
        previous_manifest_sha256=None if sequence == 1 else HASH,
        object_sha256=HASH,
        row_count=1,
        size_bytes=1,
        object_key=(f"raw/354130/{source.value}/2026-07/{subtype.lower()}-{sequence}/data.parquet"),
        created_at=NOW,
    )


def raw_chain(subtype: str, length: int) -> tuple[RawManifest, ...]:
    chain = [raw_manifest(subtype)]
    for sequence in range(2, length + 1):
        chain.append(
            raw_manifest(subtype, sequence).model_copy(
                update={"previous_manifest_sha256": manifest_sha256(chain[-1])}
            )
        )
    return tuple(chain)


def output_manifest(
    layer: str = "normalized", name: str = "cnes", unit_id: str = "unit-1"
) -> OutputManifest:
    keys = {
        "normalized": f"normalized/354130/CNES_LOCAL/2026-07/run-1/{name}.parquet",
        "reconciliation": f"reconciliation/354130/2026-07/run-1/{name}.parquet",
        "serving": f"serving/354130/run-1/{name}.json",
    }
    return OutputManifest.model_validate(
        {
            "manifest_version": 1,
            "manifest_id": f"{layer}-{name}",
            "tenant_id": "354130",
            "layer": layer,
            "source_type": SourceType.CNES_LOCAL if layer == "normalized" else None,
            "competencia": "2026-07",
            "run_id": "run-1",
            "unit_id": unit_id,
            "attempt": 1,
            "schema_version": f"{layer}-v1",
            "object_key": keys[layer],
            "object_sha256": "b" * 64,
            "row_count": 1,
            "created_at": NOW,
        }
    )


def changed_raw(manifest: RawManifest, **changes: object) -> RawManifest:
    values = {**manifest.model_dump(), **changes}
    source = values["source_type"]
    values["object_key"] = (
        f"raw/{values['tenant_id']}/{source.value}/{values['competencia']}/"
        f"{values['snapshot_id']}/data.parquet"
    )
    return RawManifest.model_validate(values)


def changed_output(manifest: OutputManifest, **changes: object) -> OutputManifest:
    values = {**manifest.model_dump(), **changes}
    leaf = manifest.object_key.rsplit("/", 1)[-1]
    if values["layer"] == "normalized":
        source = values["source_type"]
        prefix = (
            f"normalized/{values['tenant_id']}/{source.value}/"
            f"{values['competencia']}/{values['run_id']}"
        )
    elif values["layer"] == "reconciliation":
        prefix = f"reconciliation/{values['tenant_id']}/{values['competencia']}/{values['run_id']}"
    else:
        prefix = f"serving/{values['tenant_id']}/{values['run_id']}"
    values["object_key"] = f"{prefix}/{leaf}"
    return OutputManifest.model_validate(values)


def document(name: str) -> ServingDocument:
    return ServingDocument(
        schema_version="serving-v1",
        document_name=name,
        tenant_id="354130",
        run_id="run-1",
        generated_at=NOW,
        payload={},
    )


def normalize_values() -> dict[str, object]:
    return {
        "tenant_id": "354130",
        "run_id": "run-1",
        "unit_id": "normalize-bpa",
        "attempt": 1,
        "source_type": SourceType.BPA_MAG,
        "raw_manifests": raw_chain("BPA_C", 2),
        "target_keys": ("normalized/354130/BPA_MAG/2026-07/run-1/bpa_c.parquet",),
        "normalized_at": NOW,
    }


def test_normalize_aceita_cadeias_independentes_e_ordena_por_subtipo():
    request = NormalizeRequest.model_validate(
        {
            **normalize_values(),
            "raw_manifests": (*raw_chain("BPA_I", 1), *raw_chain("BPA_C", 2)),
            "target_keys": (
                "normalized/354130/BPA_MAG/2026-07/run-1/bpa_i.parquet",
                "normalized/354130/BPA_MAG/2026-07/run-1/bpa_c.parquet",
            ),
        }
    )

    assert tuple(item.file_subtype for item in request.raw_manifests) == (
        "BPA_C",
        "BPA_C",
        "BPA_I",
    )
    assert request.target_keys == tuple(sorted(request.target_keys))


@pytest.mark.parametrize(
    ("manifests", "message"),
    [
        ((), "raw_manifests_required"),
        ((raw_manifest("BPA_C", 2),), "chain_full_required"),
        ((raw_manifest("BPA_C"), raw_manifest("BPA_C")), "chain_sequence"),
        ((raw_manifest("BPA_C"), raw_manifest("BPA_C", 3)), "chain_sequence"),
    ],
)
def test_normalize_rejeita_cadeia_invalida(manifests: tuple[RawManifest, ...], message: str):
    with pytest.raises(ValidationError, match=message):
        NormalizeRequest.model_validate({**normalize_values(), "raw_manifests": manifests})


def test_normalize_rejeita_hash_anterior_e_base_incorretos():
    full, delta = raw_chain("BPA_C", 2)
    for changed, message in (
        (delta.model_copy(update={"previous_manifest_sha256": "c" * 64}), "chain_hash"),
        (delta.model_copy(update={"base_snapshot_id": "outro"}), "chain_base"),
    ):
        with pytest.raises(ValidationError, match=message):
            NormalizeRequest.model_validate(
                {**normalize_values(), "raw_manifests": (full, changed)}
            )


@pytest.mark.parametrize(
    "changed",
    [
        {"tenant_id": "outro"},
        {"source_type": SourceType.SIA_LOCAL},
        {"competencia": "2026-08"},
    ],
)
def test_normalize_rejeita_identidade_raw_incorreta(changed: dict[str, object]):
    manifest = changed_raw(raw_manifest("BPA_C"), **changed)
    manifests = (manifest, raw_manifest("BPA_I")) if "competencia" in changed else (manifest,)
    with pytest.raises(ValidationError, match="raw_identity"):
        NormalizeRequest.model_validate({**normalize_values(), "raw_manifests": manifests})


@pytest.mark.parametrize(
    ("target_keys", "message"),
    [
        ((), "target_keys_required"),
        (("normalized/354130/BPA_MAG/2026-07/run-1/x.parquet",) * 2, "target_keys_unique"),
        (("reconciliation/354130/2026-07/run-1/x.parquet",), "target_key_layer"),
        (("normalized/outro/BPA_MAG/2026-07/run-1/x.parquet",), "target_key_identity"),
        (("normalized/354130/BPA_MAG/2026-07/run-2/x.parquet",), "target_key_identity"),
        (("normalized/354130/BPA_MAG/2026-07/run-1/.hidden",), "target_key_invalid"),
    ],
)
def test_normalize_rejeita_target_keys_invalidas(target_keys: tuple[str, ...], message: str):
    with pytest.raises(ValidationError, match=message):
        NormalizeRequest.model_validate({**normalize_values(), "target_keys": target_keys})


def test_normalize_request_e_estrito_frozen_e_utc():
    request = NormalizeRequest.model_validate(normalize_values())
    with pytest.raises(ValidationError):
        request.attempt = 2
    naive = datetime(2026, 1, 1, tzinfo=UTC).replace(tzinfo=None)
    for changes in ({"attempt": "1"}, {"extra": True}, {"normalized_at": naive}):
        with pytest.raises(ValidationError):
            NormalizeRequest.model_validate({**normalize_values(), **changes})


def test_normalize_result_exige_manifests_normalized_coerentes_e_unicos():
    first = output_manifest()
    second = output_manifest(name="cnes_extra")
    assert NormalizeResult(manifests=(first, second)).manifests == (first, second)
    for manifests, message in (
        ((), "manifests_required"),
        ((first, first), "manifests_unique"),
        ((first, first.model_copy(update={"manifest_id": "outro"})), "manifests_unique"),
        ((first, changed_output(second, run_id="run-2")), "manifest_identity"),
        ((output_manifest("reconciliation"),), "manifest_layer"),
    ):
        with pytest.raises(ValidationError, match=message):
            NormalizeResult(manifests=manifests)


def reconcile_values() -> dict[str, object]:
    return {
        "tenant_id": "354130",
        "competencia": "2026-07",
        "run_id": "run-1",
        "unit_id": "reconcile-cnes",
        "attempt": 1,
        "normalized_manifests": (output_manifest(),),
        "reconciliation_key": "reconciliation/354130/2026-07/run-1/cnes.parquet",
        "divergence_key": "reconciliation/354130/2026-07/run-1/divergence.parquet",
        "reconciled_at": NOW,
    }


def test_reconcile_request_valida_inputs_e_targets():
    assert ReconcileRequest.model_validate(reconcile_values()).run_id == "run-1"
    for changes, message in (
        ({"normalized_manifests": ()}, "normalized_manifests_required"),
        ({"normalized_manifests": (output_manifest("reconciliation"),)}, "manifest_layer"),
        (
            {"normalized_manifests": (changed_output(output_manifest(), tenant_id="x"),)},
            "manifest_identity",
        ),
        (
            {"normalized_manifests": (changed_output(output_manifest(), run_id="x"),)},
            "manifest_identity",
        ),
        (
            {"normalized_manifests": (changed_output(output_manifest(), competencia="2026-08"),)},
            "manifest_identity",
        ),
        ({"reconciliation_key": "serving/354130/run-1/x.json"}, "target_key_layer"),
        ({"divergence_key": "reconciliation/other/2026-07/run-1/x.parquet"}, "target_key_identity"),
        (
            {"divergence_key": "reconciliation/354130/2026-07/run-1/cnes.parquet"},
            "target_keys_unique",
        ),
    ):
        with pytest.raises(ValidationError, match=message):
            ReconcileRequest.model_validate({**reconcile_values(), **changes})


def test_reconcile_result_valida_artefatos():
    reconciliation = output_manifest("reconciliation", "cnes", "reconcile")
    divergence = output_manifest("reconciliation", "divergence", "reconcile")
    result = ReconcileResult(
        reconciliation_manifest=reconciliation,
        divergence_manifest=divergence,
        kpis={"total": 1},
    )
    assert result.kpis == {"total": 1}
    for changed, message in (
        (
            divergence.model_copy(update={"manifest_id": reconciliation.manifest_id}),
            "manifests_unique",
        ),
        (
            divergence.model_copy(update={"object_key": reconciliation.object_key}),
            "manifests_unique",
        ),
        (divergence.model_copy(update={"unit_id": "outro"}), "manifest_identity"),
        (output_manifest(), "manifest_layer"),
    ):
        with pytest.raises(ValidationError, match=message):
            ReconcileResult(
                reconciliation_manifest=reconciliation,
                divergence_manifest=changed,
                kpis={},
            )


def materialize_values() -> dict[str, object]:
    return {
        "tenant_id": "354130",
        "competencia": "2026-07",
        "run_id": "run-1",
        "unit_id": "materialize-cnes",
        "attempt": 1,
        "reconciliation_manifest": output_manifest("reconciliation", "cnes", "reconcile"),
        "divergence_manifest": output_manifest("reconciliation", "divergence", "reconcile"),
        "missing_sources": (),
        "target_keys": ("serving/354130/run-1/overview.json",),
        "generated_at": NOW,
    }


def test_materialize_request_valida_inputs_e_targets():
    assert MaterializeRequest.model_validate(materialize_values()).target_keys
    other_reconciliation = changed_output(output_manifest("reconciliation", "cnes"), run_id="run-2")
    other_divergence = changed_output(
        output_manifest("reconciliation", "divergence"), run_id="run-2"
    )
    for changes, message in (
        ({"reconciliation_manifest": output_manifest()}, "manifest_layer"),
        (
            {
                "divergence_manifest": changed_output(
                    output_manifest("reconciliation", "divergence"), run_id="x"
                )
            },
            "manifest_identity",
        ),
        (
            {
                "reconciliation_manifest": other_reconciliation,
                "divergence_manifest": other_divergence,
            },
            "manifest_identity",
        ),
        ({"missing_sources": ("SIHD", "SIHD")}, "missing_sources_unique"),
        ({"target_keys": ()}, "target_keys_required"),
        ({"target_keys": ("serving/354130/run-1/x.json",) * 2}, "target_keys_unique"),
        (
            {"target_keys": ("normalized/354130/CNES_LOCAL/2026-07/run-1/x.parquet",)},
            "target_key_layer",
        ),
        ({"target_keys": ("serving/outro/run-1/x.json",)}, "target_key_identity"),
        ({"target_keys": ("serving/354130/run-1/x.parquet",)}, "target_key_invalid"),
    ):
        with pytest.raises(ValidationError, match=message):
            MaterializeRequest.model_validate({**materialize_values(), **changes})


def test_materialize_result_associa_documentos_ordenados_a_manifests():
    overview = output_manifest("serving", "overview", "materialize")
    workforce = output_manifest("serving", "workforce", "materialize")
    result = MaterializeResult(
        manifests=(overview, workforce),
        documents=(document("overview"), document("workforce")),
    )
    assert tuple(item.document_name for item in result.documents) == ("overview", "workforce")

    cases = (
        ((), (), "manifests_required"),
        ((overview,), (), "result_cardinality"),
        ((overview, overview), (document("overview"), document("workforce")), "manifests_unique"),
        ((overview, workforce), (document("overview"), document("overview")), "documents_unique"),
        (
            (workforce, overview),
            (document("overview"), document("workforce")),
            "result_association",
        ),
        (
            (workforce, overview),
            (document("workforce"), document("overview")),
            "result_association",
        ),
        ((overview,), (document("workforce"),), "result_association"),
        ((output_manifest(),), (document("cnes"),), "manifest_layer"),
        ((changed_output(overview, tenant_id="x"),), (document("overview"),), "result_identity"),
        (
            (overview,),
            (document("overview").model_copy(update={"run_id": "x"}),),
            "result_identity",
        ),
    )
    for manifests, documents, message in cases:
        with pytest.raises(ValidationError, match=message):
            MaterializeResult(manifests=manifests, documents=documents)
