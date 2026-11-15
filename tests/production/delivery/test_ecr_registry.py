"""Adaptador real do registry: ECR por Stubber e GHCR por HTTP fake, sem rede."""

from __future__ import annotations

import json
import logging
from importlib import import_module

import httpx
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from scripts.promotion.promote_ecr import promote_image, verify_promoted_image
from tests.production.delivery._phase_a_fixtures import (
    CDN,
    ECR,
    GHCR_NAME,
    GHCR_PAT,
    REPOSITORY,
    FakeEcr,
    FakeGhcr,
    RegistryBoundary,
    _digest,
    _promotion,
    ecr_client,
)

DOCKER = "application/vnd.docker.distribution.manifest.v2+json"
OCI = "application/vnd.oci.image.manifest.v1+json"
MEDIA = [DOCKER, OCI]
UNAVAILABLE = {"layers": [{"layerDigest": "x", "layerAvailability": "UNAVAILABLE"}]}


def _module():
    return import_module("scripts.promotion.ecr_registry")


def _http(handler):
    return httpx.Client(transport=httpx.MockTransport(handler), trust_env=False)


def _no_http(request):
    raise AssertionError(f"http_chamado url={request.url}")


def _adapter(client=None, handler=_no_http):
    return _module().EcrRegistry(client or ecr_client(), _http(handler), GHCR_PAT)


@pytest.fixture
def ecr():
    client = ecr_client()
    with Stubber(client) as stubber:
        yield client, stubber
        stubber.assert_no_pending_responses()


def _fail(code):
    return {"service_error_code": code, "service_message": code}


def _lookup(stubber, digest, tags, by):
    manifest = RegistryBoundary().manifest
    details = {"imageDigest": digest, **({"imageTags": tags} if tags is not None else {})}
    stubber.add_response(
        "describe_images",
        {"imageDetails": [details]},
        {"repositoryName": REPOSITORY, "imageIds": [by]},
    )
    stubber.add_response(
        "batch_get_image",
        {"images": [{"imageManifest": manifest.decode()}], "failures": []},
        {
            "repositoryName": REPOSITORY,
            "imageIds": [{"imageDigest": digest}],
            "acceptedMediaTypes": MEDIA,
        },
    )
    return manifest


def test_describe_repository_devolve_a_forma_que_promote_consome(ecr):
    client, stubber = ecr
    repository = {
        "registryId": "123456789012",
        "repositoryName": REPOSITORY,
        "repositoryUri": ECR,
        "imageTagMutability": "IMMUTABLE",
    }
    stubber.add_response(
        "describe_repositories", {"repositories": [repository]}, {"repositoryNames": [REPOSITORY]}
    )
    assert _adapter(client).describe_repository() == RegistryBoundary().describe_repository()


def test_recusa_regiao_diferente_antes_de_qualquer_chamada():
    client = ecr_client("us-east-1")
    with Stubber(client) as stubber, pytest.raises(ValueError, match="ecr=region"):
        _adapter(client)
    stubber.assert_no_pending_responses()


def test_recusa_repositorio_diferente_antes_de_qualquer_chamada():
    client = ecr_client()
    with Stubber(client) as stubber, pytest.raises(ValueError, match="ecr=repository"):
        _module().EcrRegistry(client, _http(_no_http), GHCR_PAT, repository="outro-repo")
    stubber.assert_no_pending_responses()


def test_describe_image_por_digest_devolve_digest_manifest_e_tags_ordenadas(ecr):
    client, stubber = ecr
    digest = _digest(RegistryBoundary().manifest)
    manifest = _lookup(stubber, digest, ["z-tag", "a-tag"], {"imageDigest": digest})
    assert _adapter(client).describe_image(digest) == {
        "digest": digest,
        "manifest": manifest,
        "tags": ["a-tag", "z-tag"],
    }


def test_describe_image_por_tag_consulta_pela_tag_e_devolve_o_digest(ecr):
    client, stubber = ecr
    digest = _digest(RegistryBoundary().manifest)
    _lookup(stubber, digest, ["release"], {"imageTag": "release"})
    observed = _adapter(client).describe_image("release")
    assert observed["digest"] == digest
    assert observed["tags"] == ["release"]


def test_describe_image_sem_tags_devolve_lista_vazia(ecr):
    client, stubber = ecr
    digest = _digest(RegistryBoundary().manifest)
    _lookup(stubber, digest, None, {"imageDigest": digest})
    assert _adapter(client).describe_image(digest)["tags"] == []


@pytest.mark.parametrize("reference", ["release", _digest(b"x")])
def test_describe_image_devolve_none_quando_a_imagem_nao_existe(ecr, reference):
    client, stubber = ecr
    stubber.add_client_error("describe_images", **_fail("ImageNotFoundException"))
    assert _adapter(client).describe_image(reference) is None


def test_describe_image_devolve_none_quando_a_lista_vem_vazia(ecr):
    client, stubber = ecr
    stubber.add_response("describe_images", {"imageDetails": []})
    assert _adapter(client).describe_image("release") is None


def _found(stubber, digest="sha256:" + "a" * 64):
    stubber.add_response("describe_images", {"imageDetails": [{"imageDigest": digest}]})
    return digest


def test_describe_image_devolve_none_quando_batch_get_reporta_image_not_found(ecr):
    client, stubber = ecr
    digest = _found(stubber)
    failures = [{"failureCode": "ImageNotFound"}]
    stubber.add_response("batch_get_image", {"images": [], "failures": failures})
    assert _adapter(client).describe_image(digest) is None


@pytest.mark.parametrize("code", ["AccessDeniedException", "RepositoryNotFoundException"])
def test_describe_image_propaga_qualquer_outro_erro_do_ecr(ecr, code):
    client, stubber = ecr
    stubber.add_client_error("describe_images", **_fail(code))
    with pytest.raises(ClientError):
        _adapter(client).describe_image("release")


def test_describe_image_propaga_falha_de_batch_get_que_nao_e_not_found(ecr):
    client, stubber = ecr
    digest = _found(stubber)
    stubber.add_response(
        "batch_get_image", {"images": [], "failures": [{"failureCode": "KmsError"}]}
    )
    with pytest.raises(ValueError, match="ecr=batch_get_failure code=KmsError"):
        _adapter(client).describe_image(digest)


def test_describe_image_recusa_resposta_sem_manifest(ecr):
    client, stubber = ecr
    digest = _found(stubber)
    stubber.add_response("batch_get_image", {"images": [], "failures": []})
    with pytest.raises(ValueError, match="ecr=manifest_missing"):
        _adapter(client).describe_image(digest)


def _manifest(media=DOCKER):
    return json.dumps({"schemaVersion": 2, "mediaType": media}).encode()


@pytest.mark.parametrize("media", MEDIA)
def test_put_manifest_envia_bytes_exatos_com_tag_digest_e_media_type(ecr, media):
    client, stubber = ecr
    manifest = _manifest(media)
    digest = _digest(manifest)
    expected = {
        "repositoryName": REPOSITORY,
        "imageManifest": manifest.decode(),
        "imageManifestMediaType": media,
        "imageDigest": digest,
        "imageTag": "release",
    }
    response = {"image": {"imageId": {"imageDigest": digest, "imageTag": "release"}}}
    stubber.add_response("put_image", response, expected)
    _adapter(client).put_manifest(digest, manifest, "release")


def test_put_manifest_recusa_digest_divergente_na_resposta(ecr):
    client, stubber = ecr
    stubber.add_response("put_image", {"image": {"imageId": {"imageDigest": _digest(b"outro")}}})
    with pytest.raises(ValueError, match="ecr=put_digest"):
        _adapter(client).put_manifest(_digest(_manifest()), _manifest(), "release")


def test_put_manifest_propaga_conflito_de_tag_imutavel(ecr):
    client, stubber = ecr
    stubber.add_client_error("put_image", **_fail("ImageTagAlreadyExistsException"))
    with pytest.raises(ClientError):
        _adapter(client).put_manifest(_digest(_manifest()), _manifest(), "release")


def _layer(tmp_path, data, availability="UNAVAILABLE", stubber=None):
    path = tmp_path / "layer"
    path.write_bytes(data)
    digest = _digest(data)
    layers = [{"layerDigest": digest, "layerAvailability": availability}]
    stubber.add_response(
        "batch_check_layer_availability",
        {"layers": layers, "failures": []},
        {"repositoryName": REPOSITORY, "layerDigests": [digest]},
    )
    return digest, path


def test_copy_blob_nao_envia_quando_a_camada_ja_esta_disponivel(ecr, tmp_path):
    client, stubber = ecr
    digest, path = _layer(tmp_path, b"abcdefghij", "AVAILABLE", stubber)
    _adapter(client).copy_blob(digest, path)


def test_copy_blob_envia_em_partes_do_tamanho_devolvido_e_completa(ecr, tmp_path):
    client, stubber = ecr
    data = b"abcdefghij"
    digest, path = _layer(tmp_path, data, stubber=stubber)
    stubber.add_response(
        "initiate_layer_upload",
        {"uploadId": "up-1", "partSize": 4},
        {"repositoryName": REPOSITORY},
    )
    for first, last in ((0, 3), (4, 7), (8, 9)):
        expected = {
            "repositoryName": REPOSITORY,
            "uploadId": "up-1",
            "partFirstByte": first,
            "partLastByte": last,
            "layerPartBlob": data[first : last + 1],
        }
        stubber.add_response(
            "upload_layer_part", {"uploadId": "up-1", "lastByteReceived": last}, expected
        )
    stubber.add_response(
        "complete_layer_upload",
        {"uploadId": "up-1", "layerDigest": digest},
        {"repositoryName": REPOSITORY, "uploadId": "up-1", "layerDigests": [digest]},
    )
    _adapter(client).copy_blob(digest, path)


def test_copy_blob_recusa_digest_divergente_ao_completar(ecr, tmp_path):
    client, stubber = ecr
    digest, path = _layer(tmp_path, b"abcd", stubber=stubber)
    stubber.add_response("initiate_layer_upload", {"uploadId": "up-1", "partSize": 8})
    stubber.add_response("upload_layer_part", {"uploadId": "up-1", "lastByteReceived": 3})
    other = {"uploadId": "up-1", "layerDigest": _digest(b"outro")}
    stubber.add_response("complete_layer_upload", other)
    with pytest.raises(ValueError, match="ecr=layer_digest"):
        _adapter(client).copy_blob(digest, path)


def test_copy_blob_propaga_falha_do_upload(ecr, tmp_path):
    client, stubber = ecr
    digest, path = _layer(tmp_path, b"abcd", stubber=stubber)
    stubber.add_response("initiate_layer_upload", {"uploadId": "up-1", "partSize": 8})
    stubber.add_client_error("upload_layer_part", **_fail("InvalidLayerPartException"))
    with pytest.raises(ClientError):
        _adapter(client).copy_blob(digest, path)


def _signed(stubber, digest, url):
    stubber.add_response(
        "get_download_url_for_layer",
        {"downloadUrl": url, "layerDigest": digest},
        {"repositoryName": REPOSITORY, "layerDigest": digest},
    )


def test_download_destination_blob_le_a_url_assinada_sem_credencial(ecr, tmp_path):
    client, stubber = ecr
    data, seen = b"blob-bytes", []
    url = "https://s3.us-east-2.amazonaws.com/signed?X=1"
    _signed(stubber, _digest(data), url)

    def handler(request):
        seen.append(request)
        return httpx.Response(200, content=data)

    _adapter(client, handler).download_destination_blob(_digest(data), tmp_path / "out")
    assert (tmp_path / "out").read_bytes() == data
    assert "authorization" not in seen[0].headers
    assert str(seen[0].url) == url


def test_download_destination_blob_recusa_url_que_nao_e_https(ecr, tmp_path):
    client, stubber = ecr
    _signed(stubber, _digest(b"x"), "http://s3.example/signed")
    with pytest.raises(ValueError, match="http=scheme"):
        _adapter(client).download_destination_blob(_digest(b"x"), tmp_path / "out")


@pytest.fixture
def source():
    registry = RegistryBoundary()
    return registry, FakeGhcr(registry)


def _image(registry):
    return f"ghcr.io/{GHCR_NAME}@{_digest(registry.manifest)}"


def test_read_manifest_troca_o_token_e_pede_os_dois_media_types(source):
    registry, ghcr = source
    assert _adapter(handler=ghcr).read_manifest(_image(registry)) == registry.manifest
    accepted = ghcr.requests[-1].headers["accept"].split(", ")
    assert sorted(accepted) == sorted(MEDIA)


def test_download_blob_segue_o_redirect_sem_enviar_credencial(source, tmp_path):
    registry, ghcr = source
    _adapter(handler=ghcr).download_blob(
        _image(registry), _digest(registry.layer), tmp_path / "blob"
    )
    assert (tmp_path / "blob").read_bytes() == registry.layer
    assert str(ghcr.requests[-1].url).startswith(CDN)
    assert "authorization" not in ghcr.requests[-1].headers


def test_download_blob_aceita_resposta_direta_do_ghcr(source, tmp_path):
    registry, ghcr = source
    ghcr.redirect = False
    _adapter(handler=ghcr).download_blob(
        _image(registry), _digest(registry.config), tmp_path / "blob"
    )
    assert (tmp_path / "blob").read_bytes() == registry.config


def test_download_blob_recusa_redirect_para_http(source, tmp_path):
    registry, ghcr = source

    def insecure(request):
        if "/blobs/" in request.url.path:
            return httpx.Response(307, headers={"location": "http://cdn.example/blob"})
        return ghcr(request)

    with pytest.raises(ValueError, match="http=scheme"):
        _adapter(handler=insecure).download_blob(
            _image(registry), _digest(registry.layer), tmp_path / "blob"
        )


@pytest.mark.parametrize(
    "image",
    [
        "docker.io/viniclus/cnesdata/data_processor@sha256:" + "a" * 64,
        "ghcr.io/outro/cnesdata/data_processor@sha256:" + "a" * 64,
        "ghcr.io/viniclus/cnesdata/data_processor:latest",
        "ghcr.io/viniclus/cnesdata/data_processor@sha256:xyz",
    ],
)
def test_recusa_imagem_de_origem_fora_do_ghcr_do_projeto_sem_requisicao(image, tmp_path):
    with pytest.raises(ValueError, match="ghcr=image"):
        _adapter().read_manifest(image)
    with pytest.raises(ValueError, match="ghcr=image"):
        _adapter().download_blob(image, "sha256:" + "a" * 64, tmp_path / "blob")


def test_download_blob_recusa_digest_de_blob_malformado(source, tmp_path):
    with pytest.raises(ValueError, match="ghcr=digest"):
        _adapter().download_blob(_image(source[0]), "../etc/passwd", tmp_path / "blob")


def _ghcr_replies(token_status, status, body=None):
    def handler(request):
        if request.url.path == "/token":
            reply = {"token": "bearer-synthetic"} if token_status == 200 else {}
            return httpx.Response(token_status, json=body if body is not None else reply)
        return httpx.Response(status, text=GHCR_PAT)

    return handler


@pytest.mark.parametrize("status", [401, 403, 404, 500])
def test_status_inesperado_do_ghcr_falha_sem_vazar_o_token(source, status, caplog):
    handler = _ghcr_replies(200, status)
    with caplog.at_level(logging.DEBUG), pytest.raises(ValueError, match="ghcr=status") as info:
        _adapter(handler=handler).read_manifest(_image(source[0]))
    leaked = [GHCR_PAT, "bearer-synthetic"]
    assert not any(item in str(info.value) or item in caplog.text for item in leaked)


def test_falha_na_troca_de_token_nao_vaza_credencial(source):
    handler = _ghcr_replies(403, 200)
    with pytest.raises(ValueError, match="ghcr=token_status") as info:
        _adapter(handler=handler).read_manifest(_image(source[0]))
    assert GHCR_PAT not in str(info.value)


def test_resposta_de_token_sem_campo_token_e_recusada(source):
    handler = _ghcr_replies(200, 200, body={})
    with pytest.raises(ValueError, match="ghcr=token_missing"):
        _adapter(handler=handler).read_manifest(_image(source[0]))


def test_manifest_acima_do_limite_e_recusado(source):
    def huge(request):
        if request.url.path == "/token":
            return httpx.Response(200, json={"token": "bearer-synthetic"})
        return httpx.Response(200, content=b"x" * (4 * 1024**2 + 1))

    with pytest.raises(ValueError, match="ghcr=manifest_size"):
        _adapter(handler=huge).read_manifest(_image(source[0]))


def test_blob_acima_do_limite_e_recusado(source, tmp_path, monkeypatch):
    registry, ghcr = source
    monkeypatch.setattr(_module(), "MAX_BLOB_BYTES", 4)
    with pytest.raises(ValueError, match="http=size"):
        _adapter(handler=ghcr).download_blob(
            _image(registry), _digest(registry.layer), tmp_path / "blob"
        )


def test_connect_exige_token_no_ambiente(monkeypatch):
    for name in ("GH_TOKEN", "GITHUB_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValueError, match="github=token_missing"):
        _module().connect()


def test_connect_monta_adaptador_us_east_2_sem_expor_o_token(monkeypatch):
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.setenv("GITHUB_TOKEN", GHCR_PAT)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "synthetic")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "synthetic")
    adapter = _module().connect()
    assert isinstance(adapter, _module().EcrRegistry)
    assert GHCR_PAT not in repr(adapter)


def _wired(source):
    registry, ghcr = source
    fake = FakeEcr()

    def handler(request):
        if request.url.host == "s3.example":
            return httpx.Response(200, content=fake.layers[request.url.path[1:]])
        return ghcr(request)

    return registry, fake, _module().EcrRegistry(fake, _http(handler), GHCR_PAT)


def test_promote_image_conclui_com_o_adaptador_real_sobre_fakes(source):
    registry, fake, adapter = _wired(source)
    context = _promotion(registry)
    result = promote_image(context, adapter)
    digest = _digest(registry.manifest)
    assert result["image_digest"] == digest
    assert result["ecr_image"] == ECR + "@" + digest
    assert fake.tags == {context["release_id"]: digest}
    assert set(fake.layers) == {_digest(registry.config), _digest(registry.layer)}
    verify_promoted_image(context, adapter)


def test_promote_image_repetido_e_idempotente_com_o_adaptador_real(source):
    registry, fake, adapter = _wired(source)
    context = _promotion(registry)
    first = promote_image(context, adapter)
    assert promote_image(context, adapter) == first
    assert len(fake.images) == 1


def test_promote_image_recusa_tag_ocupada_por_outro_digest_no_adaptador_real(source):
    registry, fake, adapter = _wired(source)
    context = _promotion(registry)
    fake.images["sha256:" + "e" * 64] = "{}"
    fake.tags[context["release_id"]] = "sha256:" + "e" * 64
    with pytest.raises(ValueError, match="ecr=tag_conflict"):
        promote_image(context, adapter)
