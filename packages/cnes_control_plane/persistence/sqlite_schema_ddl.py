"""DDL do plano de controle SQLite."""

SCHEMA = """
CREATE TABLE IF NOT EXISTS provisioned_certificates (
    tenant_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    serial TEXT NOT NULL,
    certificate_fingerprint_sha256 TEXT NOT NULL,
    subject_cn TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    revoked_at TEXT,
    PRIMARY KEY (tenant_id, serial)
);
CREATE INDEX IF NOT EXISTS ix_provisioned_certificates_agent
ON provisioned_certificates (tenant_id, agent_id);
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS memberships (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, user_id)
);
CREATE TABLE IF NOT EXISTS agents (
    tenant_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    state TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, agent_id)
);
CREATE TABLE IF NOT EXISTS jobs (
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
CREATE INDEX IF NOT EXISTS ix_jobs_claimable_v2
ON jobs (tenant_id, agent_id, state, lease_until, created_at, job_id);
CREATE TABLE IF NOT EXISTS job_creation_writes (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    job_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, job_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS job_terminal_writes (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, job_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS job_cancellation_writes (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, job_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS raw_manifests (
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
CREATE INDEX IF NOT EXISTS ix_raw_manifest_heads
ON raw_manifests (
    tenant_id, source_type, file_subtype, competencia,
    created_at DESC, agent_id DESC, snapshot_id DESC, manifest_id DESC
);
CREATE INDEX IF NOT EXISTS ix_raw_manifest_ancestry
ON raw_manifests (
    tenant_id, source_type, file_subtype, competencia, agent_id,
    sequence, manifest_sha256, base_snapshot_id, snapshot_id
);
CREATE TABLE IF NOT EXISTS raw_agent_heads (
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
CREATE TABLE IF NOT EXISTS raw_resync_states (
    tenant_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    file_subtype TEXT NOT NULL,
    competencia TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, agent_id, source_type, file_subtype, competencia)
);
CREATE TABLE IF NOT EXISTS runs (
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
CREATE TABLE IF NOT EXISTS run_transition_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    expected_state TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, expected_state),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS run_dependencies (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    file_subtype TEXT NOT NULL,
    required INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, run_id, source_type, file_subtype),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS run_units (
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
CREATE TABLE IF NOT EXISTS run_unit_terminal_writes (
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
CREATE TABLE IF NOT EXISTS run_dispatches (
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
CREATE TABLE IF NOT EXISTS run_dispatch_wave_identities (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    wave_id TEXT NOT NULL,
    unit_ids TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, wave_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS run_dispatch_bind_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    dispatch_id TEXT NOT NULL,
    command_data TEXT NOT NULL,
    response_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, dispatch_id),
    FOREIGN KEY (tenant_id, run_id)
        REFERENCES run_dispatches (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS run_dispatch_terminal_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    dispatch_id TEXT NOT NULL,
    command_data TEXT NOT NULL,
    response_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, dispatch_id)
);
CREATE TABLE IF NOT EXISTS run_cancellation_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS idempotency_records (
    tenant_id TEXT NOT NULL,
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, scope, key)
);
CREATE TABLE IF NOT EXISTS dataset_versions (
    tenant_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    version_id TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, dataset_name, version_id)
);
CREATE TABLE IF NOT EXISTS dataset_publications (
    tenant_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    version_id TEXT NOT NULL,
    data TEXT NOT NULL,
    response_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, dataset_name, version_id),
    FOREIGN KEY (tenant_id, dataset_name, version_id)
        REFERENCES dataset_versions (tenant_id, dataset_name, version_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS dataset_pointers (
    tenant_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    pointer_name TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, dataset_name, pointer_name)
);
CREATE TABLE IF NOT EXISTS access_requests (
    tenant_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    data TEXT NOT NULL,
    creation_request_data TEXT NOT NULL,
    creation_event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, request_id)
);
CREATE TABLE IF NOT EXISTS access_request_decision_writes (
    tenant_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, request_id),
    FOREIGN KEY (tenant_id, request_id)
        REFERENCES access_requests (tenant_id, request_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS outbox_cursor (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL CHECK (version >= 0),
    position TEXT
);
CREATE TABLE IF NOT EXISTS outbox_events (
    event_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_outbox_pending
ON outbox_events (created_at, event_id) WHERE delivered_at IS NULL;
"""
