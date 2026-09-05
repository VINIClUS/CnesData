from datetime import UTC, datetime, timedelta
from inspect import currentframe
from pathlib import Path
from warnings import catch_warnings, simplefilter

import pytest

from cnes_domain.control_plane.enums import JobState, RunState
from cnes_domain.control_plane.queries import (
    LatestSucceededJobQuery,
    RawIdentity,
    RawManifestChainQuery,
    WaitingRunsForDependencyQuery,
)
from cnes_domain.ports.control_plane import ControlPlanePort, TypedRawQueryPort
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.contracts.clock import MutableClock, _job, _raw_record, _run

_IDENTITY = RawIdentity("354130", "CNES", "ST", "2026-07")
_FIELDS = ("tenant_id", "source_type", "file_subtype", "competencia")
_VALUES = ("354130", "CNES", "ST", "2026-07")
_METHODS = (
    ("latest_succeeded_job", "query_latest_succeeded_job"),
    ("list_raw_manifest_chain", "query_raw_manifest_chain"),
    ("list_waiting_runs_for_dependency", "query_waiting_runs_for_dependency"),
)


@pytest.fixture
def adapter(tmp_path):
    clock = MutableClock(datetime(2026, 7, 15, 12, tzinfo=UTC))
    store = SQLiteControlPlane(tmp_path / "queries.sqlite3", clock.now)
    store.initialize()
    base = _raw_record("base-agent-a", "agent-a", 1, clock.now())
    with store.write_transaction() as connection:
        for job_id in ("job-a", "job-b"):
            store.put_job_record(
                connection, _job(job_id).model_copy(update={
                    "state": JobState.SUCCEEDED, "result_manifest_id": base.manifest_id,
                    "result_manifest_key": base.manifest_key}))
        store.put_manifest_record(connection, base)
    for run_id in ("waiting-b", "waiting-a"):
        store.put_run(_run(run_id, RunState.WAITING_INPUTS))
    return store


def _arguments(legacy, form):
    values = _VALUES
    fields = _FIELDS
    if legacy == "latest_succeeded_job":
        values = (_VALUES[0], "agent-a", *_VALUES[1:])
        fields = (_FIELDS[0], "agent_id", *_FIELDS[1:])
    split = {"posicional": len(values), "nomeado": 0, "misto": 2}[form]
    return values[:split], dict(zip(fields[split:], values[split:], strict=True))


def _query(legacy):
    if legacy == "latest_succeeded_job":
        return LatestSucceededJobQuery(_IDENTITY, "agent-a")
    if legacy == "list_raw_manifest_chain":
        return RawManifestChainQuery(_IDENTITY)
    return WaitingRunsForDependencyQuery(_IDENTITY)


def test_sqlite_cumpre_ambos_protocolos(adapter):
    assert isinstance(adapter, ControlPlanePort)
    assert isinstance(adapter, TypedRawQueryPort)


@pytest.mark.parametrize(("legacy", "typed"), _METHODS)
@pytest.mark.parametrize("form", ["posicional", "nomeado", "misto"])
def test_shim_preserva_resultado_e_avisa_no_chamador(adapter, legacy, typed, form):
    args, kwargs = _arguments(legacy, form)
    with catch_warnings(record=True) as warnings:
        simplefilter("always")
        line = currentframe().f_lineno + 1
        result = getattr(adapter, legacy)(*args, **kwargs)
    assert len(warnings) == 1
    assert warnings[0].category is DeprecationWarning
    assert str(warnings[0].message) == f"method={legacy} replacement={typed}"
    assert Path(warnings[0].filename) == Path(__file__)
    assert warnings[0].lineno == line
    with catch_warnings(record=True) as typed_warnings:
        simplefilter("always")
        assert result == getattr(adapter, typed)(_query(legacy))
    assert typed_warnings == []
    assert result is not None
    assert result != ()


@pytest.mark.parametrize(("legacy", "typed"), _METHODS)
@pytest.mark.parametrize(
    "invalid",
    [
        (("x",) * 6, {}, "too_many_arguments=6"),
        (("x",) * 6, {"extra": "x"}, "too_many_arguments=6"),
        ((), {"extra": "x"}, "unexpected_argument=extra"),
        (("354130",), {"tenant_id": "354130"}, "duplicate_argument=tenant_id"),
        (("354130",), {"extra": "x", "tenant_id": "354130"}, "unexpected_argument=extra"),
        (("354130",), {}, "missing_arguments="),
    ],
)
def test_shim_preserva_erros_e_emite_um_aviso(adapter, legacy, typed, invalid):
    args, kwargs, error = invalid
    if error == "missing_arguments=":
        fields = "source_type,file_subtype,competencia"
        error += f"agent_id,{fields}" if legacy == "latest_succeeded_job" else fields
    with catch_warnings(record=True) as warnings:
        simplefilter("always")
        with pytest.raises(TypeError) as raised:
            getattr(adapter, legacy)(*args, **kwargs)
    assert str(raised.value) == error
    assert len(warnings) == 1
    assert str(warnings[0].message) == f"method={legacy} replacement={typed}"


@pytest.mark.parametrize("limit", [0, -1, -100])
@pytest.mark.parametrize("methods", _METHODS[1:])
def test_limite_nao_positivo_retorna_tupla_vazia(adapter, limit, methods):
    legacy, typed = methods
    query = type(_query(legacy))(_IDENTITY, limit=limit)
    assert getattr(adapter, typed)(query) == ()
    if legacy == "list_waiting_runs_for_dependency" and limit < 0:
        return
    with pytest.warns(DeprecationWarning, match=f"method={legacy} replacement={typed}"):
        assert getattr(adapter, legacy)(*_VALUES, limit=limit) == ()


@pytest.mark.parametrize("limit", [-1, -100])
@pytest.mark.parametrize("form", ["posicional", "nomeado", "misto"])
def test_shim_sqlite_preserva_limite_negativo_ilimitado_da_versao_legada(adapter, limit, form):
    for index in range(101):
        adapter.put_run(_run(f"run-{index:03}", RunState.WAITING_INPUTS))
    args, kwargs = _arguments("list_waiting_runs_for_dependency", form)
    if form == "posicional":
        args = (*args, limit)
    else:
        kwargs["limit"] = limit
    with pytest.warns(DeprecationWarning, match="method=list_waiting_runs_for_dependency"):
        legacy = adapter.list_waiting_runs_for_dependency(*args, **kwargs)
    expected = (*[f"run-{index:03}" for index in range(101)], "waiting-a", "waiting-b")
    assert tuple(run.run_id for run in legacy) == expected
    assert adapter.query_waiting_runs_for_dependency(
        WaitingRunsForDependencyQuery(_IDENTITY, limit)) == ()


def test_shim_sqlite_com_limite_negativo_avisa_apenas_no_chamador(adapter):
    with catch_warnings(record=True) as warnings:
        simplefilter("always")
        line = currentframe().f_lineno + 1
        adapter.list_waiting_runs_for_dependency(*_VALUES, limit=-1)
        adapter.query_waiting_runs_for_dependency(WaitingRunsForDependencyQuery(_IDENTITY, -1))
    assert len(warnings) == 1
    assert warnings[0].category is DeprecationWarning
    assert str(warnings[0].message) == (
        "method=list_waiting_runs_for_dependency replacement=query_waiting_runs_for_dependency"
    )
    assert Path(warnings[0].filename) == Path(__file__)
    assert warnings[0].lineno == line


@pytest.mark.parametrize("field", _FIELDS)
def test_shim_sqlite_valida_identidade_antes_do_limite_negativo(adapter, field):
    values = dict(zip(_FIELDS, _VALUES, strict=True))
    values[field] = ""
    with pytest.warns(DeprecationWarning, match="method=list_waiting_runs_for_dependency"):
        with pytest.raises(ValueError):
            adapter.list_waiting_runs_for_dependency(**values, limit=-1)


def test_consultas_preservam_ordem_isolamento_e_persistencia(adapter):
    reopened = SQLiteControlPlane(adapter._database_path, adapter.now)
    latest = reopened.query_latest_succeeded_job(LatestSucceededJobQuery(_IDENTITY, "agent-a"))
    assert latest.job_id == "job-b"
    waiting = reopened.query_waiting_runs_for_dependency(WaitingRunsForDependencyQuery(_IDENTITY))
    assert tuple(run.run_id for run in waiting) == ("waiting-a", "waiting-b")
    for legacy, typed in _METHODS:
        query = _query(legacy)
        foreign = RawIdentity("other", "CNES", "ST", "2026-07")
        if legacy == "latest_succeeded_job":
            query = type(query)(foreign, "agent-a")
        else:
            query = type(query)(foreign)
        result = getattr(reopened, typed)(query)
        assert result is None if legacy == "latest_succeeded_job" else result == ()


def test_limites_padrao_e_explicitos_preservados(adapter):
    for index in range(101):
        adapter.put_run(_run(f"run-{index:03}", RunState.WAITING_INPUTS))
    with adapter.write_transaction() as connection:
        for sequence in range(2, 33):
            record = _raw_record(
                f"delta-{sequence}", "agent-a", sequence,
                adapter.now() + timedelta(seconds=sequence))
            adapter.put_manifest_record(connection, record)
    with pytest.warns(DeprecationWarning, match="method=list_raw_manifest_chain"):
        assert adapter.list_raw_manifest_chain(*_VALUES) == ()
    with pytest.warns(DeprecationWarning, match="method=list_raw_manifest_chain"):
        assert len(adapter.list_raw_manifest_chain(*_VALUES, 32)) == 32
    with pytest.warns(DeprecationWarning, match="method=list_waiting_runs_for_dependency"):
        assert len(adapter.list_waiting_runs_for_dependency(*_VALUES)) == 100
    with pytest.warns(DeprecationWarning, match="method=list_waiting_runs_for_dependency"):
        assert len(adapter.list_waiting_runs_for_dependency(*_VALUES, limit=1)) == 1
