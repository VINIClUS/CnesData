-- Schema anterior ao cursor: BASE 440a8948939c909cf92d93756193f8cf86c2ffa8.
CREATE TABLE access_request_decision_writes (
    tenant_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, request_id),
    FOREIGN KEY (tenant_id, request_id)
        REFERENCES access_requests (tenant_id, request_id) ON DELETE CASCADE
);
CREATE TABLE access_requests (
    tenant_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    data TEXT NOT NULL,
    creation_request_data TEXT NOT NULL,
    creation_event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, request_id)
);
CREATE TABLE agents (
    tenant_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    state TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, agent_id)
);
CREATE TABLE dataset_pointers (
    tenant_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    pointer_name TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, dataset_name, pointer_name)
);
CREATE TABLE dataset_publications (
    tenant_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    version_id TEXT NOT NULL,
    data TEXT NOT NULL,
    response_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, dataset_name, version_id),
    FOREIGN KEY (tenant_id, dataset_name, version_id)
        REFERENCES dataset_versions (tenant_id, dataset_name, version_id) ON DELETE CASCADE
);
CREATE TABLE dataset_versions (
    tenant_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    version_id TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, dataset_name, version_id)
);
CREATE TABLE idempotency_records (
    tenant_id TEXT NOT NULL,
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, scope, key)
);
CREATE TABLE job_cancellation_writes (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, job_id) ON DELETE CASCADE
);
CREATE TABLE job_creation_writes (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    job_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, job_id) ON DELETE CASCADE
);
CREATE TABLE job_terminal_writes (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, job_id) ON DELETE CASCADE
);
CREATE TABLE jobs (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    file_subtype TEXT NOT NULL,
    competencia TEXT NOT NULL,
    state TEXT NOT NULL,
    lease_until TEXT,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id)
);
CREATE TABLE memberships (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, user_id)
);
CREATE TABLE outbox_events (
    event_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    data TEXT NOT NULL
);
CREATE TABLE raw_agent_heads (
    tenant_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    file_subtype TEXT NOT NULL,
    competencia TEXT NOT NULL,
    job_id TEXT NOT NULL,
    manifest_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, agent_id, source_type, file_subtype, competencia),
    FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, job_id) ON DELETE CASCADE
);
CREATE TABLE raw_manifests (
    tenant_id TEXT NOT NULL,
    manifest_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    file_subtype TEXT NOT NULL,
    competencia TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    base_snapshot_id TEXT,
    sequence INTEGER NOT NULL,
    previous_manifest_sha256 TEXT,
    manifest_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, manifest_id)
);
CREATE TABLE raw_resync_states (
    tenant_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    file_subtype TEXT NOT NULL,
    competencia TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, agent_id, source_type, file_subtype, competencia)
);
CREATE TABLE run_billing_states (tenant_id TEXT NOT NULL, run_id TEXT NOT NULL, data TEXT NOT NULL,
PRIMARY KEY (tenant_id, run_id));
CREATE TABLE run_cancellation_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE run_dependencies (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    file_subtype TEXT NOT NULL,
    required INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, run_id, source_type, file_subtype),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE run_dispatch_bind_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    dispatch_id TEXT NOT NULL,
    command_data TEXT NOT NULL,
    response_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, dispatch_id),
    FOREIGN KEY (tenant_id, run_id)
        REFERENCES run_dispatches (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE run_dispatch_terminal_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    dispatch_id TEXT NOT NULL,
    command_data TEXT NOT NULL,
    response_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, dispatch_id)
);
CREATE TABLE run_dispatch_wave_identities (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    wave_id TEXT NOT NULL,
    unit_ids TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, wave_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE run_dispatches (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    dispatch_id TEXT NOT NULL,
    wave_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state TEXT NOT NULL,
    lease_until TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id),
    UNIQUE (tenant_id, dispatch_id)
);
CREATE TABLE run_transition_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    expected_state TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, expected_state),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE run_unit_terminal_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, unit_id),
    FOREIGN KEY (tenant_id, run_id, unit_id)
        REFERENCES run_units (tenant_id, run_id, unit_id) ON DELETE CASCADE
);
CREATE TABLE run_units (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    state TEXT NOT NULL,
    lease_until TEXT,
    dispatch_id TEXT,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, unit_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE runs (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    competencia TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL,
    unit_registry_data TEXT,
    PRIMARY KEY (tenant_id, run_id)
);
CREATE TABLE tenants (
    tenant_id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);
CREATE INDEX ix_jobs_claimable_v2
ON jobs (tenant_id, agent_id, state, lease_until, created_at, job_id);
CREATE INDEX ix_outbox_pending
ON outbox_events (created_at, event_id) WHERE delivered_at IS NULL;
CREATE INDEX ix_raw_manifest_ancestry
ON raw_manifests (
    tenant_id, source_type, file_subtype, competencia, agent_id,
    sequence, manifest_sha256, base_snapshot_id, snapshot_id
);
CREATE INDEX ix_raw_manifest_heads
ON raw_manifests (
    tenant_id, source_type, file_subtype, competencia,
    created_at DESC, agent_id DESC, snapshot_id DESC, manifest_id DESC
);
