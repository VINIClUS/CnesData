"""Testes de snapshot_local — persistência de snapshots parquet por competência."""
import json
from pathlib import Path

import pandas as pd
import pytest

from storage.snapshot_local import (
    SnapshotLocal,
    carregar_snapshot,
    salvar_snapshot,
    snapshot_existe,
)


def _df_prof() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS": ["123456789012345"],
        "CPF": ["12345678901"],
        "NOME_PROFISSIONAL": ["Ana Silva"],
        "SEXO": ["F"],
        "CBO": ["515105"],
        "CNES": ["2795001"],
        "TIPO_VINCULO": ["30"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [20],
        "CH_OUTRAS": [10],
        "CH_HOSPITALAR": [10],
        "FONTE": ["LOCAL"],
        "ALERTA_STATUS_CH": ["OK"],
        "DESCRICAO_CBO": ["Agente Comunitário"],
    })


def _df_estab() -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": ["2795001"],
        "NOME_FANTASIA": ["UBS Centro"],
        "TIPO_UNIDADE": ["01"],
        "CNPJ_MANTENEDORA": ["55293427000117"],
        "NATUREZA_JURIDICA": ["1023"],
        "COD_MUNICIPIO": ["354130"],
        "VINCULO_SUS": ["S"],
        "FONTE": ["LOCAL"],
    })


class TestSnapshotExiste:
    def test_falso_quando_sem_arquivos(self, tmp_path):
        assert not snapshot_existe("2026-03", tmp_path)

    def test_verdadeiro_apos_salvar(self, tmp_path):
        snap = SnapshotLocal(df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={})
        salvar_snapshot("2026-03", tmp_path, snap)
        assert snapshot_existe("2026-03", tmp_path)

    def test_falso_para_competencia_diferente(self, tmp_path):
        snap = SnapshotLocal(df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={})
        salvar_snapshot("2026-03", tmp_path, snap)
        assert not snapshot_existe("2026-04", tmp_path)


class TestSalvarCarregarSnapshot:
    def test_roundtrip_df_prof(self, tmp_path):
        original = _df_prof()
        salvar_snapshot("2026-03", tmp_path, SnapshotLocal(original, _df_estab(), {}))
        loaded = carregar_snapshot("2026-03", tmp_path)
        pd.testing.assert_frame_equal(loaded.df_prof.reset_index(drop=True), original.reset_index(drop=True))

    def test_roundtrip_df_estab(self, tmp_path):
        original = _df_estab()
        salvar_snapshot("2026-03", tmp_path, SnapshotLocal(_df_prof(), original, {}))
        loaded = carregar_snapshot("2026-03", tmp_path)
        pd.testing.assert_frame_equal(loaded.df_estab.reset_index(drop=True), original.reset_index(drop=True))

    def test_roundtrip_cbo_lookup(self, tmp_path):
        lookup = {"515105": "Agente Comunitário", "225125": "Médico"}
        salvar_snapshot("2026-03", tmp_path, SnapshotLocal(_df_prof(), _df_estab(), lookup))
        loaded = carregar_snapshot("2026-03", tmp_path)
        assert loaded.cbo_lookup == lookup

    def test_cria_diretorio_competencia_se_ausente(self, tmp_path):
        snap = SnapshotLocal(df_prof=_df_prof(), df_estab=_df_estab(), cbo_lookup={})
        salvar_snapshot("2026-03", tmp_path, snap)
        assert (tmp_path / "2026-03" / "snapshot_local_prof.parquet").exists()
        assert (tmp_path / "2026-03" / "snapshot_local_estab.parquet").exists()
        assert (tmp_path / "2026-03" / "snapshot_cbo_lookup.json").exists()

    def test_sobrescreve_snapshot_existente(self, tmp_path):
        snap_v1 = SnapshotLocal(_df_prof(), _df_estab(), {"515105": "v1"})
        salvar_snapshot("2026-03", tmp_path, snap_v1)
        snap_v2 = SnapshotLocal(_df_prof(), _df_estab(), {"515105": "v2"})
        salvar_snapshot("2026-03", tmp_path, snap_v2)
        loaded = carregar_snapshot("2026-03", tmp_path)
        assert loaded.cbo_lookup["515105"] == "v2"
