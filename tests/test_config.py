"""
test_config.py — Testes Unitários do Módulo de Configuração

Objetivo: Verificar que o config.py lê e expõe corretamente
todas as variáveis de ambiente e que falha de forma clara
quando variáveis obrigatórias estão ausentes.

Estes testes são 100% unitários: não precisam de banco de dados,
não fazem I/O de rede e rodam em milissegundos.
"""

import os
import sys
from pathlib import Path
import pytest

# conftest.py já adicionou src/ ao sys.path
import config


class TestCarregamentoDotenv:
    """Testa que as variáveis do .env são lidas corretamente."""

    def test_db_path_nao_vazio(self):
        """DB_PATH deve ser uma string com valor."""
        assert config.DB_PATH, "DB_PATH não pode ser vazio"
        assert isinstance(config.DB_PATH, str)

    def test_db_password_nao_vazio(self):
        """DB_PASSWORD deve estar presente (credencial obrigatória)."""
        assert config.DB_PASSWORD, "DB_PASSWORD não pode ser vazio"

    def test_db_dsn_formato_correto(self):
        """DB_DSN deve ter o formato 'host:caminho'."""
        assert ":" in config.DB_DSN, "DB_DSN deve conter ':' separando host do caminho"
        host, caminho = config.DB_DSN.split(":", 1)
        assert len(host) > 0, "Host do DSN não pode ser vazio"
        assert len(caminho) > 0, "Caminho do banco no DSN não pode ser vazio"

    def test_firebird_dll_configurado(self):
        """FIREBIRD_DLL deve ser um caminho de string não vazio."""
        assert config.FIREBIRD_DLL, "FIREBIRD_DLL não pode ser vazio"
        assert isinstance(config.FIREBIRD_DLL, str)

    def test_cod_mun_ibge_valor_correto(self):
        """COD_MUN_IBGE deve ser o IBGE de Presidente Epitácio."""
        assert config.COD_MUN_IBGE == "354130", (
            f"Código IBGE esperado: '354130', encontrado: '{config.COD_MUN_IBGE}'"
        )

    def test_cnpj_mantenedora_valor_correto(self):
        """CNPJ_MANTENEDORA deve ser o CNPJ da Prefeitura Municipal."""
        assert config.CNPJ_MANTENEDORA == "55293427000117", (
            f"CNPJ esperado '55293427000117', encontrado: '{config.CNPJ_MANTENEDORA}'"
        )

    def test_cnpj_mantenedora_somente_digitos(self):
        """CNPJ no banco está sem pontuação — apenas números."""
        assert config.CNPJ_MANTENEDORA.isdigit(), (
            "CNPJ_MANTENEDORA deve conter apenas dígitos (sem pontuação)"
        )

    def test_cnpj_mantenedora_comprimento(self):
        """CNPJ deve ter exatamente 14 dígitos."""
        assert len(config.CNPJ_MANTENEDORA) == 14, (
            f"CNPJ deve ter 14 dígitos, mas tem {len(config.CNPJ_MANTENEDORA)}"
        )


class TestCaminhosDoProjeto:
    """Testa que os caminhos gerados pelo config são válidos."""

    def test_raiz_projeto_e_diretorio(self):
        """RAIZ_PROJETO deve apontar para um diretório existente."""
        assert config.RAIZ_PROJETO.is_dir(), (
            f"Raiz do projeto não encontrada: {config.RAIZ_PROJETO}"
        )

    def test_output_path_tem_extensao_csv(self):
        """OUTPUT_PATH deve apontar para um arquivo .csv."""
        assert config.OUTPUT_PATH.suffix == ".csv", (
            f"OUTPUT_PATH deve ter extensão .csv, encontrado: {config.OUTPUT_PATH.suffix}"
        )

    def test_output_path_diretorio_pai_configurado(self):
        """O diretório pai do OUTPUT_PATH deve ser configurável."""
        assert config.OUTPUT_PATH.parent is not None

    def test_logs_dir_e_path(self):
        """LOGS_DIR deve ser um objeto Path."""
        assert isinstance(config.LOGS_DIR, Path)

    def test_log_file_nome_correto(self):
        """LOG_FILE deve ter o nome cnes_exporter.log."""
        assert config.LOG_FILE.name == "cnes_exporter.log"


class TestFalhaSemVariaveisObrigatorias:
    """
    Testa o comportamento quando variáveis obrigatórias estão ausentes.

    Usa monkeypatch para simular a ausência de variáveis de ambiente
    sem alterar o .env real. Após o teste, o ambiente é restaurado automaticamente.
    """

    def test_exigir_levanta_environment_error(self, monkeypatch):
        """
        A função interna _exigir() deve levantar EnvironmentError com mensagem clara
        quando a variável requisitada não existe.
        """
        monkeypatch.delenv("DB_PATH", raising=False)
        monkeypatch.delenv("DB_PASSWORD", raising=False)

        with pytest.raises(EnvironmentError, match="não encontrada"):
            config._exigir("VARIAVEL_INEXISTENTE_PARA_TESTE")
