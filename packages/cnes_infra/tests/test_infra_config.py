"""Testes do módulo config — helpers de leitura de variáveis de ambiente."""

import importlib

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

    def test_atributo_inexistente_levanta_attribute_error(self):
        import cnes_infra.config as cfg
        with pytest.raises(AttributeError, match="nao_existe"):
            _ = cfg.nao_existe

    def test_db_path_levanta_os_error_sem_env(self, monkeypatch):
        import cnes_infra.config as cfg
        cfg._firebird_db_path.cache_clear()
        monkeypatch.delenv("DB_PATH", raising=False)
        with pytest.raises(OSError):
            _ = cfg.DB_PATH

    def test_db_password_levanta_os_error_sem_env(self, monkeypatch):
        import cnes_infra.config as cfg
        cfg._firebird_db_password.cache_clear()
        monkeypatch.delenv("DB_PASSWORD", raising=False)
        with pytest.raises(OSError):
            _ = cfg.DB_PASSWORD

    def test_firebird_dll_levanta_os_error_sem_env(self, monkeypatch):
        import cnes_infra.config as cfg
        cfg._firebird_dll.cache_clear()
        monkeypatch.delenv("FIREBIRD_DLL", raising=False)
        with pytest.raises(OSError):
            _ = cfg.FIREBIRD_DLL

    def test_gcp_project_id_levanta_os_error_sem_env(self, monkeypatch):
        import cnes_infra.config as cfg
        cfg._gcp_project_id.cache_clear()
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        with pytest.raises(OSError):
            _ = cfg.GCP_PROJECT_ID


@pytest.fixture
def reload_config(monkeypatch):
    """Restaura `cnes_infra.config` mesmo se a assertion do teste falhar.

    `monkeypatch` é dependência desta fixture: seu teardown roda depois do
    `undo()` explícito abaixo, então a ordem de finalizers não importa. Um
    `importlib.reload(config)` solto no fim do corpo do teste rodaria com o
    monkeypatch ainda ativo (teardown do pytest é posterior ao return) e
    vazaria a env patchada para o resto da sessão.
    """
    yield
    monkeypatch.undo()
    from cnes_infra import config
    importlib.reload(config)


class TestS3PublicEndpointUrl:
    """S3_PUBLIC_ENDPOINT_URL precisa cair para S3_ENDPOINT_URL tanto quando
    a env var está ausente quanto quando está presente mas vazia — o segundo
    caso é o que docker-compose produz (`${S3_PUBLIC_ENDPOINT_URL}` sem
    default no compose, variável ausente do .env) e `os.getenv(key, default)`
    só cobre o primeiro (default só vale quando a env var está ausente)."""

    def test_cai_para_endpoint_interno_quando_ausente(self, monkeypatch, reload_config):
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.delenv("S3_PUBLIC_ENDPOINT_URL", raising=False)
        from cnes_infra import config
        importlib.reload(config)
        assert config.S3_PUBLIC_ENDPOINT_URL == "http://minio:9000"

    def test_cai_para_endpoint_interno_quando_vazia(self, monkeypatch, reload_config):
        """Regressão: docker-compose substitui variável ausente do .env por
        string vazia, não por variável ausente — S3_PUBLIC_ENDPOINT_URL=""
        no ambiente do container, não unset."""
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("S3_PUBLIC_ENDPOINT_URL", "")
        from cnes_infra import config
        importlib.reload(config)
        assert config.S3_PUBLIC_ENDPOINT_URL == "http://minio:9000"

    def test_respeita_override_explicito(self, monkeypatch, reload_config):
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("S3_PUBLIC_ENDPOINT_URL", "https://storage.dev.example.com")
        from cnes_infra import config
        importlib.reload(config)
        assert config.S3_PUBLIC_ENDPOINT_URL == "https://storage.dev.example.com"
