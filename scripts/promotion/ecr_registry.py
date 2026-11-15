"""Registry real da promoção: lê do GHCR e copia para o ECR cnesdata-prod-processor."""

from __future__ import annotations

import json
import os
import re
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import boto3
import httpx
from botocore.exceptions import ClientError

from release.verify_candidate import require
from scripts.promotion.promote_ecr import MANIFEST, OCI_MANIFEST

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

type Node = dict[str, Any]
REGION = "us-east-2"
REPOSITORY = "cnesdata-prod-processor"
GHCR = "https://ghcr.io"
MAX_MANIFEST_BYTES = 4 * 1024**2
MAX_BLOB_BYTES = 2 * 1024**3
CHUNK = 1024**2
ACCEPT = f"{MANIFEST}, {OCI_MANIFEST}"
IMAGE = re.compile(r"ghcr\.io/(viniclus/cnesdata/data_processor)@(sha256:[0-9a-f]{64})")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
REDIRECTS = frozenset({301, 302, 303, 307, 308})


def _chunks(response: httpx.Response, limit: int, reason: str) -> Iterator[bytes]:
    total = 0
    for chunk in response.iter_bytes(CHUNK):
        total += len(chunk)
        require(total <= limit, reason)
        yield chunk


def _https(url: str) -> str:
    require(urlsplit(url).scheme == "https", "http=scheme")
    return url


def _save(response: httpx.Response, target: Path) -> None:
    with target.open("wb") as output:
        for chunk in _chunks(response, MAX_BLOB_BYTES, "http=size"):
            output.write(chunk)


def _error_code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", ""))


class EcrRegistry:
    """Args: cliente ECR us-east-2, cliente HTTP sem redirect e token GHCR somente leitura."""

    def __init__(
        self, ecr: Any, http: httpx.Client, pat: str, repository: str = REPOSITORY
    ) -> None:
        require(repository == REPOSITORY, f"ecr=repository repository={repository}")
        require(ecr.meta.region_name == REGION, f"ecr=region region={ecr.meta.region_name}")
        self._ecr, self._http, self._pat, self._repository = ecr, http, pat, repository

    def describe_repository(self) -> Node:
        """Returns: identidade e mutabilidade do repositório de destino."""
        found = self._ecr.describe_repositories(repositoryNames=[self._repository])
        item = found["repositories"][0]
        return {
            "account_id": item["registryId"],
            "region": REGION,
            "name": item["repositoryName"],
            "uri": item["repositoryUri"],
            "image_tag_mutability": item["imageTagMutability"],
        }

    def read_manifest(self, image: str) -> bytes:
        """Returns: bytes exatos do manifest; Raises: ValueError fora do GHCR do projeto."""
        name, digest = self._source(image)
        url = f"{GHCR}/v2/{name}/manifests/{digest}"
        headers = self._auth(name) | {"Accept": ACCEPT}
        with self._http.stream("GET", url, headers=headers) as response:
            self._status(response)
            return b"".join(_chunks(response, MAX_MANIFEST_BYTES, "ghcr=manifest_size"))

    def download_blob(self, image: str, digest: str, target: Path) -> None:
        """Raises: ValueError para digest malformado, redirect inseguro ou tamanho excedido."""
        name, _ = self._source(image)
        require(bool(DIGEST.fullmatch(digest)), "ghcr=digest")
        url = f"{GHCR}/v2/{name}/blobs/{digest}"
        with self._http.stream("GET", url, headers=self._auth(name)) as response:
            if response.status_code not in REDIRECTS:
                self._status(response)
                _save(response, target)
                return
            location = _https(response.headers["location"])
        self._fetch(location, target)

    def describe_image(self, reference: str) -> Node | None:
        """Returns: digest, manifest e tags; None só quando a imagem ou a tag não existe."""
        key = (
            {"imageDigest": reference}
            if reference.startswith("sha256:")
            else {"imageTag": reference}
        )
        try:
            found = self._ecr.describe_images(repositoryName=self._repository, imageIds=[key])
        except ClientError as error:
            if _error_code(error) == "ImageNotFoundException":
                return None
            raise
        if not found["imageDetails"]:
            return None
        detail = found["imageDetails"][0]
        manifest = self._manifest(detail["imageDigest"])
        if manifest is None:
            return None
        tags = sorted(detail.get("imageTags", []))
        return {"digest": detail["imageDigest"], "manifest": manifest, "tags": tags}

    def put_manifest(self, digest: str, manifest: bytes, tag: str) -> None:
        """Raises: ValueError quando o digest devolvido pelo ECR difere do enviado."""
        media = json.loads(manifest)["mediaType"]
        require(media in (MANIFEST, OCI_MANIFEST), "ecr=manifest_media")
        done = self._ecr.put_image(
            repositoryName=self._repository,
            imageManifest=manifest.decode(),
            imageManifestMediaType=media,
            imageDigest=digest,
            imageTag=tag,
        )
        require(done["image"]["imageId"]["imageDigest"] == digest, "ecr=put_digest")

    def copy_blob(self, digest: str, path: Path) -> None:
        """Raises: ValueError quando o ECR completa a camada com outro digest."""
        checked = self._ecr.batch_check_layer_availability(
            repositoryName=self._repository, layerDigests=[digest]
        )
        require(not checked.get("failures"), "ecr=layer_check")
        states = {item["layerDigest"]: item["layerAvailability"] for item in checked["layers"]}
        if states.get(digest) == "AVAILABLE":
            return
        upload = self._ecr.initiate_layer_upload(repositoryName=self._repository)
        self._parts(upload["uploadId"], upload["partSize"], path)
        done = self._ecr.complete_layer_upload(
            repositoryName=self._repository, uploadId=upload["uploadId"], layerDigests=[digest]
        )
        require(done["layerDigest"] == digest, "ecr=layer_digest")

    def download_destination_blob(self, digest: str, target: Path) -> None:
        """Raises: ValueError para URL assinada que não seja https."""
        layer = self._ecr.get_download_url_for_layer(
            repositoryName=self._repository, layerDigest=digest
        )
        self._fetch(_https(layer["downloadUrl"]), target)

    def _parts(self, upload_id: str, size: int, path: Path) -> None:
        offset = 0
        with path.open("rb") as stream:
            while chunk := stream.read(size):
                last = offset + len(chunk) - 1
                self._ecr.upload_layer_part(
                    repositoryName=self._repository,
                    uploadId=upload_id,
                    partFirstByte=offset,
                    partLastByte=last,
                    layerPartBlob=chunk,
                )
                offset = last + 1

    def _manifest(self, digest: str) -> bytes | None:
        found = self._ecr.batch_get_image(
            repositoryName=self._repository,
            imageIds=[{"imageDigest": digest}],
            acceptedMediaTypes=[MANIFEST, OCI_MANIFEST],
        )
        for failure in found.get("failures", []):
            if failure["failureCode"] == "ImageNotFound":
                return None
            raise ValueError(f"ecr=batch_get_failure code={failure['failureCode']}")
        require(bool(found["images"]), "ecr=manifest_missing")
        return found["images"][0]["imageManifest"].encode()

    def _source(self, image: str) -> tuple[str, str]:
        parsed = IMAGE.fullmatch(image)
        if parsed is None:
            raise ValueError("ghcr=image")
        return parsed[1], parsed[2]

    def _auth(self, name: str) -> dict[str, str]:
        response = self._http.get(
            f"{GHCR}/token",
            params={"service": "ghcr.io", "scope": f"repository:{name}:pull"},
            auth=("token", self._pat),
        )
        require(response.status_code == 200, f"ghcr=token_status status={response.status_code}")
        token = response.json().get("token")
        require(bool(token), "ghcr=token_missing")
        return {"Authorization": f"Bearer {token}"}

    def _status(self, response: httpx.Response) -> None:
        require(response.status_code == 200, f"ghcr=status status={response.status_code}")

    def _fetch(self, url: str, target: Path) -> None:
        with self._http.stream("GET", url) as response:
            require(response.status_code == 200, f"http=status status={response.status_code}")
            _save(response, target)


def connect() -> EcrRegistry:
    """Returns: registry com o token GHCR do ambiente; Raises: ValueError sem token."""
    pat = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not pat:
        raise ValueError("github=token_missing")
    http = httpx.Client(timeout=60, follow_redirects=False, trust_env=False)
    return EcrRegistry(boto3.client("ecr", region_name=REGION), http, pat)
