"""Tests for worker.streaming_executor tempdir lifecycle."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch


@patch("dump_agent.worker.streaming_executor.REGISTRY")
@patch("dump_agent.worker.streaming_executor.pre_flight_check")
@patch("dump_agent.worker.streaming_executor._upload_payload")
@patch("dump_agent.worker.streaming_executor.unregister_temp_dir")
@patch("dump_agent.worker.streaming_executor.register_temp_dir")
def test_stream_to_storage_registra_e_desregistra_tempdir(
    mock_reg, mock_unreg, mock_upload, mock_preflight, mock_registry,
    tmp_path,
):
    parquet = tmp_path / "x.parquet"
    parquet.write_bytes(b"payload" * 100)

    fake_extractor = MagicMock()
    fake_extractor.extract.return_value = parquet
    mock_registry.__getitem__.return_value = fake_extractor

    from cnes_domain.models.extraction import ExtractionIntent, ExtractionParams
    from dump_agent.worker.streaming_executor import stream_to_storage

    params = ExtractionParams(
        intent=ExtractionIntent.ESTABELECIMENTOS,
        competencia="2026-03",
        cod_municipio="354130",
    )
    stream_to_storage(MagicMock(), params, "null://sink")

    assert mock_reg.call_count == 1
    assert mock_unreg.call_count == 1
    registered_path = mock_reg.call_args[0][0]
    unregistered_path = mock_unreg.call_args[0][0]
    assert isinstance(registered_path, Path)
    assert registered_path == unregistered_path


@patch("dump_agent.worker.streaming_executor.REGISTRY")
@patch("dump_agent.worker.streaming_executor.pre_flight_check")
@patch("dump_agent.worker.streaming_executor.unregister_temp_dir")
@patch("dump_agent.worker.streaming_executor.register_temp_dir")
def test_stream_to_storage_desregistra_mesmo_em_excecao(
    mock_reg, mock_unreg, mock_preflight, mock_registry, tmp_path,
):
    fake_extractor = MagicMock()
    fake_extractor.extract.side_effect = RuntimeError("extractor_boom")
    mock_registry.__getitem__.return_value = fake_extractor

    from cnes_domain.models.extraction import ExtractionIntent, ExtractionParams
    from dump_agent.worker.streaming_executor import stream_to_storage

    params = ExtractionParams(
        intent=ExtractionIntent.ESTABELECIMENTOS,
        competencia="2026-03",
        cod_municipio="354130",
    )
    try:
        stream_to_storage(MagicMock(), params, "null://sink")
    except RuntimeError:
        pass

    assert mock_unreg.call_count == 1
