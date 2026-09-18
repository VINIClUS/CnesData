"""Consultas tipadas RAW e descoberta de candidatos no DynamoDB."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from cnes_domain.control_plane.entities import (
    Job,
    ManifestRef,
    RawManifestRecord,
    RawResyncState,
    Run,
)
from cnes_domain.control_plane.enums import RunState
from cnes_domain.control_plane.queries import RawManifestByIdQuery
from cnes_infra.control_plane.dynamodb_codec import (
    CandidateQuery,
    _ancestry_prefix,
    bounded_candidates,
    raw_head_chain,
    unique_partition_item,
)
from cnes_infra.control_plane.dynamodb_keys import (
    key_component,
    raw_manifest_lookup_key,
    raw_partition,
    raw_resync_key,
)

if TYPE_CHECKING:
    from pydantic import BaseModel

    from cnes_domain.control_plane.queries import (
        AgentRawManifestChainQuery,
        LatestSucceededJobQuery,
        RawManifestChainQuery,
        RawResyncStateQuery,
        WaitingRunsForDependencyQuery,
    )


class DynamoDBQueries:
    """Consulta identidades RAW e relê candidatos dos índices."""

    def _query[T: BaseModel](
        self, index_name: str, partition: str, query: CandidateQuery[T]
    ) -> tuple[T, ...]:
        request = {
            "TableName": self._table_name,
            "IndexName": index_name,
            "KeyConditionExpression": f"{index_name}pk = :partition",
            "ExpressionAttributeValues": {":partition": {"S": partition}},
        }
        return bounded_candidates(self._client, request, query)

    def query_latest_succeeded_job(self, query: LatestSucceededJobQuery) -> Job | None:
        """Returns: Job concluído mais recente da identidade e do agente."""
        identity = query.identity
        partition = raw_partition(
            identity.tenant_id, identity.source_type, identity.file_subtype, identity.competencia
        )
        return self._get_model((partition, f"LATEST_JOB#{key_component(query.agent_id)}"), Job)

    def query_raw_manifest_by_id(
        self, query: RawManifestByIdQuery
    ) -> RawManifestRecord | None:
        """Returns: Manifesto carregado fortemente pela identidade imutável."""
        return self._get_model(
            raw_manifest_lookup_key(query.tenant_id, query.manifest_id), RawManifestRecord
        )

    def query_agent_raw_manifest_chain(
        self, query: AgentRawManifestChainQuery
    ) -> tuple[ManifestRef, ...]:
        """Returns: Cadeia materializada da cabeça forte do agente."""
        if query.limit <= 0:
            return ()
        identity = query.identity
        partition = raw_partition(
            identity.tenant_id, identity.source_type, identity.file_subtype, identity.competencia
        )
        job = self._get_model((partition, f"LATEST_JOB#{key_component(query.agent_id)}"), Job)
        if job is None or job.result_manifest_id is None:
            return ()
        record = self.query_raw_manifest_by_id(
            RawManifestByIdQuery(identity.tenant_id, job.result_manifest_id)
        )
        if record is None or record.agent_id != query.agent_id:
            return ()
        prefix = _ancestry_prefix(record, record.sequence, record.manifest_sha256)
        ancestry = unique_partition_item(self._client, self._table_name, partition, prefix)
        if ancestry is None or "chain" not in ancestry:
            return ()
        chain = json.loads(ancestry["chain"]["S"])
        if len(chain) > query.limit:
            return ()
        return tuple(ManifestRef.model_validate(item) for item in chain)

    def query_raw_resync_state(self, query: RawResyncStateQuery) -> RawResyncState | None:
        """Returns: Marcador forte de resync da identidade e do agente."""
        identity = query.identity
        partition = raw_partition(
            identity.tenant_id, identity.source_type, identity.file_subtype, identity.competencia
        )
        return self._get_model(raw_resync_key(partition, query.agent_id), RawResyncState)

    def query_raw_manifest_chain(self, query: RawManifestChainQuery) -> tuple[ManifestRef, ...]:
        """Returns: Cadeia válida de manifestos RAW ordenados."""
        identity = query.identity
        partition = raw_partition(
            identity.tenant_id, identity.source_type, identity.file_subtype, identity.competencia
        )
        return raw_head_chain(self._client, self._table_name, partition, query.limit)

    def query_waiting_runs_for_dependency(
        self, query: WaitingRunsForDependencyQuery
    ) -> tuple[Run, ...]:
        """Returns: Runs elegíveis ordenados por criação e identificador."""
        if query.limit <= 0:
            return ()
        identity = query.identity
        values = (
            identity.tenant_id, identity.source_type, identity.file_subtype, identity.competencia
        )
        partition = "RUN_DEP#" + "#".join(key_component(value) for value in values)

        def valid(run: Run) -> bool:
            return (
                run.state is RunState.WAITING_INPUTS
                and run.tenant_id == identity.tenant_id
                and run.competencia == identity.competencia
                and any(
                    (dep.source_type, dep.file_subtype)
                    == (identity.source_type, identity.file_subtype)
                    for dep in run.dependencies
                )
            )

        runs = self._query("gsi3", partition, CandidateQuery(Run, valid, query.limit))
        return tuple(sorted(runs, key=lambda run: (run.created_at, run.run_id))[:query.limit])
