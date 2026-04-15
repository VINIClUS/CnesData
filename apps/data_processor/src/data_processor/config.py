"""Configuração do data_processor."""
import os

POLL_INTERVAL: float = float(os.getenv("PROCESSOR_POLL_INTERVAL", "5.0"))
PROCESSOR_ID: str = os.getenv("PROCESSOR_ID", "processor-01")
MINIO_BUCKET: str = os.getenv("MINIO_BUCKET", "cnesdata-landing")
