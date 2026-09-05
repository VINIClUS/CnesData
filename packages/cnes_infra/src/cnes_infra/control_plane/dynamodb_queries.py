"""Consultas tipadas RAW e descoberta de candidatos no DynamoDB."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cnes_domain.control_plane.entities import Job, Run
from cnes_domain.control_plane.enums import RunState
from cnes_infra.control_plane.dynamodb_codec import (
    CandidateQuery,
    bounded_candidates,
    raw_head_chain,
)
from cnes_infra.control_plane.dynamodb_keys import key_component, raw_partition

if TYPE_CHECKING:
    from pydantic import BaseModel

    from cnes_domain.control_plane.entities import ManifestRef
    from cnes_domain.control_plane.queries import (
        LatestSucceededJobQuery,
        RawManifestChainQuery,
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
