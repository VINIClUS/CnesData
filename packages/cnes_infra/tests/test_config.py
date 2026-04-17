"""Testes do módulo config — helpers de leitura de variáveis de ambiente."""

import pytest


class TestExigir:

    def test_variavel_presente_retorna_valor(self, monkeypatch):
        from cnes_infra.config import _exigir
        monkeypatch.setenv("_TEST_VAR_INFRA", "valor_teste")
        assert _exigir("_TEST_VAR_INFRA") == "valor_teste"

    def test_variavel_ausente_levanta_os_error(self, monkeypatch):
        from cnes_infra.config import _exigir
        monkeypatch.delenv("_TEST_VAR_INFRA", raising=False)
        with pytest.raises(OSError, match="_TEST_VAR_INFRA"):
            _exigir("_TEST_VAR_INFRA")


class TestExigirInteiro:

    def test_valor_inteiro_valido(self, monkeypatch):
        from cnes_infra.config import _exigir_inteiro
        monkeypatch.setenv("_TEST_INT_INFRA", "42")
        assert _exigir_inteiro("_TEST_INT_INFRA", 0) == 42

    def test_valor_padrao_quando_ausente(self, monkeypatch):
        from cnes_infra.config import _exigir_inteiro
        monkeypatch.delenv("_TEST_INT_INFRA", raising=False)
        assert _exigir_inteiro("_TEST_INT_INFRA", 99) == 99

    def test_valor_nao_inteiro_levanta_os_error(self, monkeypatch):
        from cnes_infra.config import _exigir_inteiro
        monkeypatch.setenv("_TEST_INT_INFRA", "nao_inteiro")
        with pytest.raises(OSError, match="tipo_esperado=int"):
            _exigir_inteiro("_TEST_INT_INFRA", 0)


class TestSanitizarDbUrl:

    def test_url_com_porta(self):
        from cnes_infra.config import _sanitizar_db_url
        url = "postgresql://user:pass@localhost:5433/db"
        result = _sanitizar_db_url(url)
        assert "5433" in result
        assert "postgresql+psycopg" in result

    def test_url_sem_porta(self):
        from cnes_infra.config import _sanitizar_db_url
        url = "postgresql://user:pass@localhost/db"
        result = _sanitizar_db_url(url)
        assert "postgresql+psycopg" in result


class TestLazyAttrs:

    def test_minio_access_key_default(self, monkeypatch):
        monkeypatch.delenv("MINIO_ACCESS_KEY", raising=False)
        import cnes_infra.config as cfg
        assert cfg.MINIO_ACCESS_KEY == "minioadmin"

    def test_minio_secret_key_default(self, monkeypatch):
        monkeypatch.delenv("MINIO_SECRET_KEY", raising=False)
        import cnes_infra.config as cfg
        assert cfg.MINIO_SECRET_KEY == "minioadmin"  # noqa: S105

    def test_atributo_inexistente_levanta_attribute_error(self):
        import cnes_infra.config as cfg
        with pytest.raises(AttributeError, match="nao_existe"):
            _ = cfg.nao_existe

    def test_db_path_levanta_os_error_sem_env(self, monkeypatch):
        monkeypatch.delenv("DB_PATH", raising=False)
        import cnes_infra.config as cfg
        with pytest.raises(OSError):
            _ = cfg.DB_PATH

    def test_db_password_levanta_os_error_sem_env(self, monkeypatch):
        monkeypatch.delenv("DB_PASSWORD", raising=False)
        import cnes_infra.config as cfg
        with pytest.raises(OSError):
            _ = cfg.DB_PASSWORD

    def test_firebird_dll_levanta_os_error_sem_env(self, monkeypatch):
        monkeypatch.delenv("FIREBIRD_DLL", raising=False)
        import cnes_infra.config as cfg
        with pytest.raises(OSError):
            _ = cfg.FIREBIRD_DLL

    def test_gcp_project_id_levanta_os_error_sem_env(self, monkeypatch):
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        import cnes_infra.config as cfg
        with pytest.raises(OSError):
            _ = cfg.GCP_PROJECT_ID
