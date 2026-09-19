import json
from contextlib import contextmanager
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO

import pytest

from central_api.services.serving_access import LocalServingAccess, ServingUnavailable
from cnes_contracts.manifests.outputs import OutputManifest, RunManifest
from cnes_contracts.manifests.raw import SourceType
from cnes_domain.control_plane.entities import DatasetPointer, DatasetVersion, Membership
from cnes_domain.ports.object_store import ObjectStat
from cnes_domain.ports.serving import ServingAccessPort, ServingRequest

NOW = datetime(2026, 7, 2, tzinfo=UTC)
TENANT = "354130"
OTHER_TENANT = "999999"
USER = "user-1"
DATASET = "cnes"
RUN_ID = "run-1"
COMPETENCIA = "2026-07"
MANIFEST_KEY = f"reconciliation/{TENANT}/{COMPETENCIA}/{RUN_ID}/run-manifest.json"


def membership(**updates: object) -> Membership:
    values = {"tenant_id": TENANT, "user_id": USER, "role": "viewer", "created_at": NOW}
    return Membership(**(values | updates))


def pointer(**updates: object) -> DatasetPointer:
    values = {
        "tenant_id": TENANT,
        "dataset_name": DATASET,
        "pointer_name": "current",
        "version_id": RUN_ID,
        "updated_at": NOW,
    }
    return DatasetPointer(**(values | updates))


def version(**updates: object) -> DatasetVersion:
    values = {
        "tenant_id": TENANT,
        "dataset_name": DATASET,
        "version_id": RUN_ID,
        "run_id": RUN_ID,
        "run_manifest_key": MANIFEST_KEY,
        "created_at": NOW,
    }
    return DatasetVersion(**(values | updates))


def output(layer: str, name: str, *, run_id: str = RUN_ID, **updates: object) -> OutputManifest:
    keys = {
        "normalized": f"normalized/{TENANT}/CNES_LOCAL/{COMPETENCIA}/{run_id}/{name}.parquet",
        "reconciliation": f"reconciliation/{TENANT}/{COMPETENCIA}/{run_id}/{name}.parquet",
        "serving": f"serving/{TENANT}/{run_id}/{name}.json",
    }
    values = {
        "manifest_version": 1,
        "manifest_id": f"{layer}-{name}",
        "tenant_id": TENANT,
        "layer": layer,
        "source_type": SourceType.CNES_LOCAL if layer == "normalized" else None,
        "competencia": COMPETENCIA,
        "run_id": run_id,
        "unit_id": "unit-1",
        "attempt": 1,
        "schema_version": f"{layer}-v1",
        "object_key": keys[layer],
        "object_sha256": "b" * 64,
        "row_count": 1,
        "created_at": NOW,
    }
    return OutputManifest(**(values | updates))


def run_manifest(outputs: tuple[OutputManifest, ...], **updates: object) -> RunManifest:
    values = {
        "manifest_version": 1,
        "tenant_id": TENANT,
        "dataset_name": DATASET,
        "run_id": RUN_ID,
        "competencia": COMPETENCIA,
        "outputs": outputs,
        "missing_sources": (),
        "published_at": NOW,
    }
    return RunManifest(**(values | updates))


def raw_output(**overrides: object) -> dict:
    values = {
        "manifest_version": 1,
        "manifest_id": "serving-overview",
        "tenant_id": TENANT,
        "layer": "serving",
        "source_type": None,
        "competencia": COMPETENCIA,
        "run_id": RUN_ID,
        "unit_id": "unit-1",
        "attempt": 1,
        "schema_version": "cnes-serving-v1",
        "object_key": f"serving/{TENANT}/{RUN_ID}/overview.json",
        "object_sha256": "b" * 64,
        "row_count": 1,
        "created_at": NOW.isoformat(),
    }
    return values | overrides


def raw_run_manifest(outputs: list[dict], **overrides: object) -> dict:
    values = {
        "manifest_version": 1,
        "tenant_id": TENANT,
        "dataset_name": DATASET,
        "run_id": RUN_ID,
        "competencia": COMPETENCIA,
        "outputs": outputs,
        "missing_sources": [],
        "published_at": NOW.isoformat(),
    }
    return values | overrides


def request(**updates: object) -> ServingRequest:
    values = {"user_id": USER, "tenant_id": TENANT, "dataset_name": DATASET}
    return ServingRequest(**(values | updates))


class ControlPlane:
    def __init__(
        self,
        membership: Membership | None = None,
        pointer: DatasetPointer | None = None,
        version: DatasetVersion | None = None,
    ) -> None:
        self.membership = membership
        self.pointer = pointer
        self.version = version
        self.calls: list[str] = []

    def get_membership(self, tenant_id: str, user_id: str) -> Membership | None:
        del tenant_id, user_id
        self.calls.append("get_membership")
        return self.membership

    def get_dataset_pointer(self, tenant_id: str, dataset_name: str) -> DatasetPointer | None:
        del tenant_id, dataset_name
        self.calls.append("get_dataset_pointer")
        return self.pointer

    def get_dataset_version(
        self, tenant_id: str, dataset_name: str, version_id: str
    ) -> DatasetVersion | None:
        del tenant_id, dataset_name, version_id
        self.calls.append("get_dataset_version")
        return self.version


class ObjectStore:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})
        self.stat_calls: list[str] = []
        self.opened: list[str] = []
        self._stat_overrides: dict[str, ObjectStat | None] = {}

    def put_json(self, key: str, model: RunManifest | OutputManifest) -> str:
        body = model.model_dump_json(exclude_none=False, by_alias=False).encode()
        self.objects[key] = body
        return sha256(body).hexdigest()

    def override_stat(self, key: str, stat: ObjectStat | None) -> None:
        self._stat_overrides[key] = stat

    def stat(self, key: str) -> ObjectStat | None:
        self.stat_calls.append(key)
        if key in self._stat_overrides:
            return self._stat_overrides[key]
        body = self.objects.get(key)
        if body is None:
            return None
        return ObjectStat(key, len(body), sha256(body).hexdigest())

    @contextmanager
    def open(self, key: str):
        self.opened.append(key)
        body = self.objects.get(key)
        if body is None:
            raise FileNotFoundError(key)
        yield BytesIO(body)


def access(store: ObjectStore, control: ControlPlane) -> LocalServingAccess:
    return LocalServingAccess(control, store)


def store_manifest(store: ObjectStore, manifest: RunManifest) -> None:
    store.put_json(MANIFEST_KEY, manifest)


def test_local_serving_access_satisfaz_a_porta() -> None:
    instance = access(ObjectStore(), ControlPlane())

    assert isinstance(instance, ServingAccessPort)


def test_membership_ausente_rejeita_antes_de_consultar_ponteiro() -> None:
    control = ControlPlane(membership=None)
    store = ObjectStore()

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "membership_denied"
    assert control.calls == ["get_membership"]


def test_membership_de_outro_tenant_falha_fechado() -> None:
    control = ControlPlane(membership=membership(tenant_id=OTHER_TENANT))
    store = ObjectStore()

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "membership_denied"


def test_ponteiro_ausente_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=None)
    store = ObjectStore()

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "active_pointer_missing"


def test_ponteiro_nao_current_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(pointer_name="candidate"))
    store = ObjectStore()

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "active_pointer_missing"


def test_ponteiro_de_outro_dataset_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(dataset_name="other"))
    store = ObjectStore()

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "pointer_identity_mismatch"


def test_versao_ausente_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=None)
    store = ObjectStore()

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "active_version_missing"


def test_versao_de_outro_tenant_falha_fechado() -> None:
    other_key = f"reconciliation/{OTHER_TENANT}/{COMPETENCIA}/{RUN_ID}/run-manifest.json"
    control = ControlPlane(
        membership=membership(),
        pointer=pointer(),
        version=version(tenant_id=OTHER_TENANT, run_manifest_key=other_key),
    )
    store = ObjectStore()

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "version_identity_mismatch"


def test_versao_com_run_id_divergente_falha_fechado() -> None:
    hostile_version = DatasetVersion.model_construct(
        tenant_id=TENANT,
        dataset_name=DATASET,
        version_id=RUN_ID,
        run_id="other-run",
        run_manifest_key=MANIFEST_KEY,
        created_at=NOW,
    )
    control = ControlPlane(membership=membership(), pointer=pointer(), version=hostile_version)
    store = ObjectStore()

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "version_run_mismatch"


def test_serving_ausente_nao_cai_para_versao_antiga() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=version())
    store = ObjectStore({f"serving/{TENANT}/old-run/overview.json": b"{}"})

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "run_manifest_missing"
    assert store.stat_calls == [MANIFEST_KEY]
    assert store.opened == []


def test_manifesto_com_stat_de_chave_divergente_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=version())
    manifest = run_manifest((output("serving", "overview"),))
    store = ObjectStore()
    store_manifest(store, manifest)
    store.override_stat(MANIFEST_KEY, ObjectStat("wrong-key", 1, "a" * 64))

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "run_manifest_missing"
    assert store.opened == []


def test_manifesto_com_hash_divergente_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=version())
    manifest = run_manifest((output("serving", "overview"),))
    store = ObjectStore()
    store_manifest(store, manifest)
    store.override_stat(MANIFEST_KEY, ObjectStat(MANIFEST_KEY, 1, "a" * 64))

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "run_manifest_hash_mismatch"


def test_manifesto_com_json_invalido_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=version())
    store = ObjectStore()
    body = b"not-json"
    store.objects[MANIFEST_KEY] = body

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "run_manifest_invalid"


def test_manifesto_com_ids_duplicados_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=version())
    store = ObjectStore()
    payload = raw_run_manifest([raw_output(), raw_output()])
    store.objects[MANIFEST_KEY] = json.dumps(payload).encode()

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "run_manifest_invalid"


def test_manifesto_com_output_cross_tenant_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=version())
    store = ObjectStore()
    other = raw_output(
        manifest_id="serving-other",
        tenant_id=OTHER_TENANT,
        object_key=f"serving/{OTHER_TENANT}/{RUN_ID}/other.json",
    )
    payload = raw_run_manifest([raw_output(), other])
    store.objects[MANIFEST_KEY] = json.dumps(payload).encode()

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "run_manifest_invalid"


def test_manifesto_com_chave_serving_nao_canonica_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=version())
    store = ObjectStore()
    bad = raw_output(object_key=f"serving/{TENANT}/{RUN_ID}/../overview.json")
    payload = raw_run_manifest([bad])
    store.objects[MANIFEST_KEY] = json.dumps(payload).encode()

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "run_manifest_invalid"


def test_manifesto_de_outro_run_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=version())
    store = ObjectStore()
    other_run_manifest = run_manifest(
        (output("serving", "overview", run_id="other-run"),),
        run_id="other-run",
    )
    store_manifest(store, other_run_manifest)

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "manifest_identity_mismatch"


def test_objeto_de_serving_ausente_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=version())
    store = ObjectStore()
    manifest = run_manifest((output("serving", "overview"),))
    store_manifest(store, manifest)

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "serving_object_unavailable"


def test_objeto_de_serving_com_hash_divergente_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=version())
    store = ObjectStore()
    serving_key = f"serving/{TENANT}/{RUN_ID}/overview.json"
    manifest = run_manifest((output("serving", "overview"),))
    store_manifest(store, manifest)
    store.objects[serving_key] = b'{"ok": true}'
    store.override_stat(serving_key, ObjectStat(serving_key, 12, "c" * 64))

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "serving_object_unavailable"


def test_manifesto_so_com_normalized_e_reconciliation_falha_fechado() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=version())
    store = ObjectStore()
    manifest = run_manifest((output("normalized", "cnes"), output("reconciliation", "reconciled")))
    store_manifest(store, manifest)

    with pytest.raises(ServingUnavailable) as captured:
        access(store, control).authorize(request())

    assert captured.value.code == "serving_outputs_missing"


def test_authorize_carrega_manifesto_e_concede_so_serving() -> None:
    control = ControlPlane(membership=membership(), pointer=pointer(), version=version())
    store = ObjectStore()
    zzz_key = f"serving/{TENANT}/{RUN_ID}/zzz.json"
    aaa_key = f"serving/{TENANT}/{RUN_ID}/aaa.json"
    manifest = run_manifest((
        output("normalized", "cnes"),
        output("reconciliation", "reconciled"),
        output("serving", "zzz"),
        output("serving", "aaa"),
    ))
    store_manifest(store, manifest)
    store.objects[zzz_key] = b'{"a": 1}'
    store.objects[aaa_key] = b'{"b": 2}'
    store.override_stat(zzz_key, ObjectStat(zzz_key, 8, "b" * 64))
    store.override_stat(aaa_key, ObjectStat(aaa_key, 8, "b" * 64))

    grant = access(store, control).authorize(request())

    assert grant.tenant_id == TENANT
    assert grant.run_id == RUN_ID
    assert grant.version_id == RUN_ID
    assert grant.object_keys == (zzz_key, aaa_key)
    assert store.opened == [MANIFEST_KEY]
    assert store.stat_calls == [MANIFEST_KEY, zzz_key, aaa_key]
