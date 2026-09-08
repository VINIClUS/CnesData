from datetime import UTC, datetime, timedelta

import pytest

from central_api.services.delta_policy import DeltaPolicy, ResyncReason, _DeltaContext
from cnes_contracts import RawManifest, SnapshotMode, SourceType, manifest_sha256
from cnes_domain.control_plane.entities import RawResyncState

NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
HASH = "a" * 64


def manifest(
    sequence: int,
    created_at: datetime = NOW,
    schema_version: str = "v1",
    previous: str | None = None,
) -> RawManifest:
    mode = SnapshotMode.FULL if sequence == 1 else SnapshotMode.DELTA
    snapshot_id = "base" if sequence == 1 else f"delta-{sequence}"
    return RawManifest(
        manifest_version=1,
        manifest_id=f"manifest-{snapshot_id}",
        tenant_id="354130",
        source_type=SourceType.CNES_LOCAL,
        file_subtype="CNES_VINCULO",
        competencia="2026-07",
        agent_id="agent-1",
        agent_version="1.0",
        schema_version=schema_version,
        snapshot_mode=mode,
        snapshot_id=snapshot_id,
        base_snapshot_id=None if sequence == 1 else "base",
        sequence=sequence,
        previous_manifest_sha256=None if sequence == 1 else previous or HASH,
        object_sha256=HASH,
        row_count=1,
        size_bytes=10,
        object_key=f"raw/354130/CNES_LOCAL/2026-07/{snapshot_id}/data.parquet",
        created_at=created_at,
    )


def context(
    current: RawManifest,
    chain: tuple[RawManifest, ...],
    marker: RawResyncState | None = None,
    now: datetime = NOW,
) -> _DeltaContext:
    return _DeltaContext(current, chain, marker, now)


def linked_delta(chain: tuple[RawManifest, ...], **updates: object) -> RawManifest:
    values = {
        "sequence": chain[-1].sequence + 1,
        "previous": manifest_sha256(chain[-1]),
        "created_at": NOW,
        "schema_version": chain[-1].schema_version,
    } | updates
    return manifest(**values)


def old_base_context(_: RawManifest) -> _DeltaContext:
    old_base = manifest(1, NOW - timedelta(days=7, microseconds=1))
    return context(linked_delta((old_base,)), (old_base,))


@pytest.mark.parametrize(
    ("reason", "build"),
    [
        (
            ResyncReason.AGENT_RESYNC_REQUIRED,
            lambda base: context(
                manifest(9),
                (),
                RawResyncState(
                    tenant_id="354130",
                    agent_id="agent-1",
                    source_type="CNES_LOCAL",
                    file_subtype="CNES_VINCULO",
                    competencia="2026-07",
                    required_since=NOW,
                ),
            ),
        ),
        (ResyncReason.BASE_UNKNOWN, lambda base: context(manifest(2), ())),
        (ResyncReason.SEQUENCE_GAP, lambda base: context(manifest(3), (base,))),
        (
            ResyncReason.HASH_CHAIN_MISMATCH,
            lambda base: context(manifest(2, previous="b" * 64), (base,)),
        ),
        (
            ResyncReason.SCHEMA_INCOMPATIBLE,
            lambda base: context(linked_delta((base,), schema_version="v2"), (base,)),
        ),
        (
            ResyncReason.BASE_TOO_OLD,
            old_base_context,
        ),
        (
            ResyncReason.CHAIN_TOO_LONG,
            lambda base: context(
                linked_delta(tuple(manifest(index) for index in range(1, 32))),
                tuple(manifest(index) for index in range(1, 32)),
            ),
        ),
    ],
)
def test_delta_invalido_solicita_full(reason, build) -> None:
    base = manifest(1)
    assert DeltaPolicy().evaluate(build(base)) is reason


def test_aceita_limites_exatos_de_idade_e_cadeia() -> None:
    base = manifest(1, NOW - timedelta(days=7))
    chain = (base, *(manifest(index) for index in range(2, 31)))
    current = linked_delta(chain)

    assert DeltaPolicy().evaluate(context(current, chain)) is None


def test_rejeita_base_com_data_futura() -> None:
    base = manifest(1, NOW + timedelta(microseconds=1))

    assert DeltaPolicy().evaluate(context(linked_delta((base,)), (base,))) is (
        ResyncReason.BASE_TOO_OLD
    )


@pytest.mark.parametrize(
    "values",
    [
        {"max_base_age": timedelta(0)},
        {"max_base_age": timedelta(days=7, microseconds=1)},
        {"max_chain_length": 0},
        {"max_chain_length": 31},
    ],
)
def test_rejeita_limites_fora_dos_tetos(values: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="delta_policy_limit"):
        DeltaPolicy(**values)
