"""Testes do CachingVerificadorCnes — cache TTL para verificações DATASUS."""

import time
from unittest.mock import MagicMock


from analysis.verificacao_cache import CachingVerificadorCnes


def _verificador_mock(status: str = "CRITICO") -> MagicMock:
    m = MagicMock()
    m.verificar_estabelecimento.return_value = status
    return m


class TestCachingVerificadorCnes:

    def test_cache_miss_delega_ao_verificador_real(self, tmp_path):
        # Arrange
        verificador = _verificador_mock("CRITICO")
        cache = CachingVerificadorCnes(verificador, tmp_path / "cache.json")

        # Act
        resultado = cache.verificar_estabelecimento("1234567")

        # Assert
        assert resultado == "CRITICO"
        verificador.verificar_estabelecimento.assert_called_once_with("1234567")

    def test_cache_hit_nao_chama_verificador_real(self, tmp_path):
        # Arrange
        verificador = _verificador_mock("CRITICO")
        cache = CachingVerificadorCnes(verificador, tmp_path / "cache.json")
        cache.verificar_estabelecimento("1234567")  # popula cache

        # Act
        resultado = cache.verificar_estabelecimento("1234567")

        # Assert
        assert resultado == "CRITICO"
        assert verificador.verificar_estabelecimento.call_count == 1

    def test_cache_expirado_chama_verificador_novamente(self, tmp_path):
        # Arrange — TTL de 0 segundos: qualquer entrada já está expirada
        verificador = _verificador_mock("CRITICO")
        cache = CachingVerificadorCnes(verificador, tmp_path / "cache.json", ttl_segundos=0)
        cache.verificar_estabelecimento("1234567")  # popula cache

        # Act — deve expirar imediatamente
        time.sleep(0.01)
        cache.verificar_estabelecimento("1234567")

        # Assert
        assert verificador.verificar_estabelecimento.call_count == 2

    def test_cache_persiste_entre_instancias(self, tmp_path):
        # Arrange — primeira instância popula o cache
        caminho = tmp_path / "cache.json"
        verificador1 = _verificador_mock("CRITICO")
        CachingVerificadorCnes(verificador1, caminho).verificar_estabelecimento("9999999")

        # Act — segunda instância lê do arquivo
        verificador2 = _verificador_mock("OUTRO")
        resultado = CachingVerificadorCnes(verificador2, caminho).verificar_estabelecimento("9999999")

        # Assert — leu do cache, não chamou verificador2
        assert resultado == "CRITICO"
        verificador2.verificar_estabelecimento.assert_not_called()

    def test_arquivo_corrompido_reinicia_cache(self, tmp_path):
        # Arrange
        caminho = tmp_path / "cache.json"
        caminho.write_text("{ JSON INVÁLIDO }", encoding="utf-8")
        verificador = _verificador_mock("CRITICO")

        # Act — deve ignorar o arquivo corrompido e funcionar normalmente
        cache = CachingVerificadorCnes(verificador, caminho)
        resultado = cache.verificar_estabelecimento("1234567")

        # Assert
        assert resultado == "CRITICO"

    def test_cria_diretorio_pai_se_inexistente(self, tmp_path):
        # Arrange
        caminho = tmp_path / "subdir" / "cache.json"
        verificador = _verificador_mock("CRITICO")

        # Act
        CachingVerificadorCnes(verificador, caminho).verificar_estabelecimento("1234567")

        # Assert
        assert caminho.exists()
