"""Configuração do data_processor."""
import os

POLL_INTERVAL: float = float(os.getenv("PROCESSOR_POLL_INTERVAL", "5.0"))
