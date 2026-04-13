"""test_config.py — Testes unitários do módulo de configuração."""

import os
from pathlib import Path

import pytest
from cnes_infra import config


class TestVariaveisEager:

    def test_cod_mun_ibge_valor_correto(self):
        assert config.COD_MUN_IBGE == "354130"

    def test_cnpj_mantenedora_valor_correto(self):
        assert config.CNPJ_MANTENEDORA == "55293427000117"

    def test_cnpj_mantenedora_somente_digitos(self):
        assert config.CNPJ_MANTENEDORA.isdigit()

    def test_cnpj_mantenedora_comprimento(self):
        assert len(config.CNPJ_MANTENEDORA) == 14


class TestCaminhosDoProjeto:

    def test_raiz_projeto_e_diretorio(self):
        assert config.RAIZ_PROJETO.is_dir()

    def test_output_path_tem_extensao_csv(self):
        assert config.OUTPUT_PATH.suffix == ".csv"

    def test_logs_dir_e_path(self):
        assert isinstance(config.LOGS_DIR, Path)

    def test_log_file_nome_correto(self):
        assert config.LOG_FILE.name == "cnes_exporter.log"


class TestVariaveisLazy:

    @pytest.mark.skipif(
        not os.getenv("DB_PATH"), reason="DB_PATH não configurado"
    )
    def test_db_path_nao_vazio(self):
        assert config.DB_PATH
        assert isinstance(config.DB_PATH, str)

    @pytest.mark.skipif(
        not os.getenv("DB_PASSWORD"), reason="DB_PASSWORD não configurado"
    )
    def test_db_password_nao_vazio(self):
        assert config.DB_PASSWORD

    @pytest.mark.skipif(
        not os.getenv("DB_PATH"), reason="DB_PATH não configurado"
    )
    def test_db_dsn_formato_correto(self):
        assert ":" in config.DB_DSN
        host, caminho = config.DB_DSN.split(":", 1)
        assert len(host) > 0
        assert len(caminho) > 0

    @pytest.mark.skipif(
        not os.getenv("FIREBIRD_DLL"), reason="FIREBIRD_DLL não configurado"
    )
    def test_firebird_dll_configurado(self):
        assert config.FIREBIRD_DLL
        assert isinstance(config.FIREBIRD_DLL, str)


class TestLazyFalhaSemVar:

    def test_db_path_levanta_erro_quando_ausente(self, monkeypatch):
        monkeypatch.delenv("DB_PATH", raising=False)
        config._firebird_db_path.cache_clear()
        with pytest.raises(EnvironmentError, match="não encontrada"):
            _ = config.DB_PATH

    def test_firebird_dll_levanta_erro_quando_ausente(self, monkeypatch):
        monkeypatch.delenv("FIREBIRD_DLL", raising=False)
        config._firebird_dll.cache_clear()
        with pytest.raises(EnvironmentError, match="não encontrada"):
            _ = config.FIREBIRD_DLL


class TestExigir:

    def test_exigir_levanta_environment_error(self):
        with pytest.raises(EnvironmentError, match="não encontrada"):
            config._exigir("VARIAVEL_INEXISTENTE_PARA_TESTE")


class TestApiConfig:

    def test_api_host_padrao(self):
        assert config.API_HOST == os.getenv("API_HOST", "0.0.0.0")

    def test_api_port_padrao(self):
        assert isinstance(config.API_PORT, int)
        assert config.API_PORT > 0


def test_dlq_threshold_padrao():
    assert config.DLQ_THRESHOLD == 0.05
