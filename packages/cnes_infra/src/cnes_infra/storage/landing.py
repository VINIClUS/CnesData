"""SQLAlchemy Core — tabelas do schema landing (camada bronze)."""

from sqlalchemy import Column, DateTime, MetaData, String, Table, text
from sqlalchemy.dialects.postgresql import JSONB, UUID

landing_metadata = MetaData(schema="landing")

raw_payload = Table(
    "raw_payload",
    landing_metadata,
    Column(
        "id", UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    ),
    Column("tenant_id", String(6), nullable=False),
    Column("source_system", String(30), nullable=False),
    Column("competencia", String(7), nullable=False),
    Column("payload", JSONB),
    Column("object_key", String(512)),
    Column(
        "received_at", DateTime(timezone=True),
        server_default=text("NOW()"),
    ),
)
