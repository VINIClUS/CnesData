from typing import Any

from cnes_domain.control_plane.enums import RunState
from cnes_domain.control_plane.queries import (
    LatestSucceededJobQuery,
    RawManifestChainQuery,
    WaitingRunsForDependencyQuery,
)
from cnes_infra.control_plane.raw_query_compat import DeprecatedRawQueryMixin


class HarnessRawQueries(DeprecatedRawQueryMixin):
    def query_latest_succeeded_job(self, query: LatestSucceededJobQuery) -> Any | None:
        identity = query.identity
        matches = [
            job
            for job in self.jobs.values()
            if (job.tenant_id, job.agent_id, job.source_type, job.file_subtype, job.competencia)
            == (identity.tenant_id, query.agent_id, identity.source_type,
                identity.file_subtype, identity.competencia)
            and job.state.value == "SUCCEEDED"
        ]
        return max(matches, key=lambda job: (job.created_at, job.job_id), default=None)

    def query_raw_manifest_chain(self, query: RawManifestChainQuery) -> tuple[Any, ...]:
        if query.limit <= 0 or self.mutation == "legacy_shims":
            return ()
        raw = query.identity
        identity = (raw.tenant_id, raw.source_type, raw.file_subtype, raw.competencia)
        records = [
            item
            for item in self.raw_records
            if (item.tenant_id, item.source_type, item.file_subtype, item.competencia) == identity
        ]
        if not records:
            return ()
        if self.mutation == "raw_chains":
            records = self._mutate_raw_chains(records)
        selected = self._select_raw_chain(records)
        bounded = selected if len(selected) <= query.limit else []
        return tuple(self._manifest_ref(item) for item in bounded)

    @staticmethod
    def _mutate_raw_chains(records: list[Any]) -> list[Any]:
        return [
            item.model_copy(update={"base_snapshot_id": f"base-{item.agent_id}"})
            if item.sequence > 1 else item for item in records
        ]

    @staticmethod
    def _manifest_ref(record: Any) -> Any:
        from cnes_domain.control_plane.entities import ManifestRef

        return ManifestRef(manifest_id=record.manifest_id, manifest_key=record.manifest_key)

    def query_waiting_runs_for_dependency(
        self, query: WaitingRunsForDependencyQuery
    ) -> tuple[Any, ...]:
        if query.limit <= 0:
            return ()
        identity = query.identity
        values = [
            run
            for run in self.runs.values()
            if run.tenant_id == identity.tenant_id
            and run.competencia == identity.competencia
            and run.state is RunState.WAITING_INPUTS
            and any(
                (dep.source_type, dep.file_subtype) == (identity.source_type, identity.file_subtype)
                for dep in run.dependencies
            )
        ]
        if self.mutation == "run_discovery":
            values = list(self.runs.values())
        return tuple(sorted(values, key=lambda item: (item.created_at, item.run_id))[:query.limit])
