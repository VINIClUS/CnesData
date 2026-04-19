"""Configuração do data_processor."""
import os

POLL_INTERVAL: float = float(os.getenv("PROCESSOR_POLL_INTERVAL", "5.0"))
IDLE_POLL_INTERVAL: float = float(
    os.getenv("PROCESSOR_IDLE_POLL_INTERVAL", "60.0"),
)
PROCESSOR_ID: str = os.getenv("PROCESSOR_ID", "processor-01")
MINIO_BUCKET: str = os.getenv("MINIO_BUCKET", "cnesdata-landing")

MINIO_CA_CERT: str | None = os.getenv("MINIO_CA_CERT")
MINIO_CLIENT_CERT: str | None = os.getenv("MINIO_CLIENT_CERT")
MINIO_CLIENT_KEY: str | None = os.getenv("MINIO_CLIENT_KEY")
