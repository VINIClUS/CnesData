"""Testes do NullObjectStoragePort."""

from cnes_domain.ports.object_storage import NullObjectStoragePort


class TestNullObjectStoragePort:

    def test_generate_presigned_url_retorna_null_scheme(self):
        port = NullObjectStoragePort()
        url = port.generate_presigned_upload_url("bucket", "key/obj.parquet")
        assert url == "null://bucket/key/obj.parquet"

    def test_generate_presigned_url_inclui_bucket_e_key(self):
        port = NullObjectStoragePort()
        url = port.generate_presigned_upload_url("meu-bucket", "pasta/arq.gz")
        assert "meu-bucket" in url
        assert "pasta/arq.gz" in url

    def test_object_exists_sempre_falso(self):
        port = NullObjectStoragePort()
        assert port.object_exists("bucket", "any/key") is False

    def test_get_presigned_download_url_retorna_null_scheme(self):
        port = NullObjectStoragePort()
        url = port.get_presigned_download_url("bucket", "key/file.bin")
        assert url == "null://bucket/key/file.bin"

    def test_generate_url_expires_secs_nao_afeta_resultado(self):
        port = NullObjectStoragePort()
        url1 = port.generate_presigned_upload_url("b", "k", expires_secs=60)
        url2 = port.generate_presigned_upload_url("b", "k", expires_secs=9999)
        assert url1 == url2
