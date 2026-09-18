"""Testes dos manifestos de saída e serving."""

from datetime import UTC, datetime
from math import inf, nan

import pytest
from pydantic import ValidationError

from cnes_contracts.manifests.outputs import OutputManifest, RunManifest, ServingDocument
from cnes_contracts.manifests.raw import SourceType

HASH = "b" * 64


def output_values() -> dict[str, object]:
    return {
        "manifest_version": 1,
        "manifest_id": "output-1",
        "tenant_id": "354130",
        "layer": "normalized",
        "source_type": SourceType.CNES_LOCAL,
        "competencia": "2026-07",
        "run_id": "run-1",
        "unit_id": "normalize-cnes",
        "attempt": 1,
        "schema_version": "cnes-normalized-v1",
        "object_key": "normalized/354130/CNES_LOCAL/2026-07/run-1/cnes.parquet",
        "object_sha256": HASH,
        "row_count": 10,
        "created_at": datetime(2026, 7, 2, tzinfo=UTC),
    }


def serving_document(name: str = "overview") -> ServingDocument:
    return ServingDocument(
        schema_version="cnes-serving-v1",
        document_name=name,
        tenant_id="354130",
        run_id="run-1",
        generated_at=datetime(2026, 7, 2, tzinfo=UTC),
        payload={"total": 10, "items": [None, True, "ok", {"value": 1.5}]},
    )


def run_values(outputs: tuple[OutputManifest, ...]) -> dict[str, object]:
    return {
        "manifest_version": 1,
        "tenant_id": "354130",
        "dataset_name": "cnes",
        "run_id": "run-1",
        "competencia": "2026-07",
        "outputs": outputs,
        "missing_sources": (),
        "published_at": datetime(2026, 7, 2, tzinfo=UTC),
    }


def test_output_normalized_exige_source_type():
    with pytest.raises(ValidationError, match="source_type_required"):
        OutputManifest.model_validate({**output_values(), "source_type": None})


@pytest.mark.parametrize("layer", ["reconciliation", "serving"])
def test_output_nao_normalized_rejeita_source_type(layer: str):
    values = output_values()
    values.update(
        layer=layer,
        source_type=SourceType.CNES_LOCAL,
        object_key=(
            "reconciliation/354130/2026-07/run-1/cnes.parquet"
            if layer == "reconciliation"
            else "serving/354130/run-1/overview.json"
        ),
    )

    with pytest.raises(ValidationError, match="source_type_forbidden"):
        OutputManifest.model_validate(values)


def test_manifesto_de_divergencia_aceita_zero_linhas():
    values = output_values()
    values.update(
        layer="reconciliation",
        source_type=None,
        object_key="reconciliation/354130/2026-07/run-1/divergence.parquet",
        row_count=0,
    )

    assert OutputManifest.model_validate(values).row_count == 0


def test_output_rejeita_hash_timestamp_cardinalidade_e_extra():
    for changes in (
        {"object_sha256": "B" * 64},
        {"created_at": datetime(2026, 7, 2, tzinfo=UTC).replace(tzinfo=None)},
        {"row_count": -1},
        {"attempt": 0},
        {"extra": True},
    ):
        with pytest.raises(ValidationError):
            OutputManifest.model_validate({**output_values(), **changes})


def test_run_manifest_valida_identidade_e_duplicatas():
    output = OutputManifest.model_validate(output_values())
    assert RunManifest.model_validate(run_values((output,))).outputs == (output,)

    for outputs, message in (
        ((), "outputs_required"),
        ((output, output), "outputs_unique"),
        ((output, output.model_copy(update={"manifest_id": "output-2"})), "outputs_unique"),
    ):
        with pytest.raises(ValidationError, match=message):
            RunManifest.model_validate(run_values(outputs))


@pytest.mark.parametrize(
    ("changes", "object_key"),
    [
        ({"tenant_id": "outro"}, "normalized/outro/CNES_LOCAL/2026-07/run-1/cnes.parquet"),
        ({"run_id": "run-2"}, "normalized/354130/CNES_LOCAL/2026-07/run-2/cnes.parquet"),
        (
            {"competencia": "2026-08"},
            "normalized/354130/CNES_LOCAL/2026-08/run-1/cnes.parquet",
        ),
    ],
)
def test_run_manifest_rejeita_identidade_output(changes: dict[str, object], object_key: str):
    member = OutputManifest.model_validate({**output_values(), **changes, "object_key": object_key})

    with pytest.raises(ValidationError, match="output_identity"):
        RunManifest.model_validate(run_values((member,)))


def test_run_manifest_rejeita_missing_sources_repetidas():
    output = OutputManifest.model_validate(output_values())
    with pytest.raises(ValidationError, match="missing_sources_unique"):
        RunManifest(
            manifest_version=1,
            tenant_id="354130",
            dataset_name="cnes",
            run_id="run-1",
            competencia="2026-07",
            outputs=(output,),
            missing_sources=("SIHD", "SIHD"),
            published_at=datetime(2026, 7, 2, tzinfo=UTC),
        )


@pytest.mark.parametrize("schema", ["Cnes-v1", "cnes-v0", "cnes", "cnes-v01"])
def test_serving_document_rejeita_schema_invalido(schema: str):
    with pytest.raises(ValidationError):
        serving_document().model_copy(update={"schema_version": schema}).model_validate(
            {**serving_document().model_dump(), "schema_version": schema}
        )


@pytest.mark.parametrize("name", ["Overview", "../overview", "overview.json", "_overview"])
def test_serving_document_rejeita_nome_inseguro(name: str):
    with pytest.raises(ValidationError):
        ServingDocument.model_validate({**serving_document().model_dump(), "document_name": name})


@pytest.mark.parametrize("value", [nan, inf, -inf, (1, 2), datetime(2026, 1, 1, tzinfo=UTC)])
def test_serving_document_rejeita_payload_nao_json(value: object):
    with pytest.raises(ValidationError, match="payload_json_required"):
        ServingDocument.model_validate({**serving_document().model_dump(), "payload": {"x": value}})


def test_serving_document_e_estrito_frozen_e_utc():
    document = serving_document()
    with pytest.raises(ValidationError):
        document.run_id = "run-2"
    with pytest.raises(ValidationError):
        ServingDocument.model_validate({**document.model_dump(), "extra": True})
    with pytest.raises(ValidationError, match="datetime_utc_required"):
        ServingDocument.model_validate(
            {
                **document.model_dump(),
                "generated_at": datetime(2026, 7, 2, tzinfo=UTC).replace(tzinfo=None),
            }
        )
