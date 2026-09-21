"""S3 adapter — implementação de ObjectStoragePort via presigned URLs."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from typing import Any

logger = logging.getLogger(__name__)


def build_s3_client(
    region_name: str,
    endpoint_url: str | None = None,
    addressing_style: str = "auto",
) -> Any:
    """Client boto3 pronto para presign — SigV4 fixo (obrigatório para
    endpoints customizados: LocalStack/AIStor caem em SigV2 sem isso).

    Raises:
        ValueError: endpoint_url definido (LocalStack/AIStor) sem
            AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY explícitos. Sem essa
            checagem, o boto3 cai silenciosamente para ~/.aws/credentials
            (ou IMDS) e assina contra o endpoint local com uma credencial
            AWS real de quem estiver rodando — reproduzido manualmente:
            403 assinado com uma access key alheia em vez de falhar alto.
    """
    import boto3
    from botocore.config import Config

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if endpoint_url is not None and not (access_key and secret_key):
        raise ValueError(
            "s3_endpoint_url=set aws_credentials=missing — "
            "defina AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY explicitamente "
            "para endpoints não-AWS; nunca herde de ~/.aws/credentials",
        )

    return boto3.client(
        "s3",
        region_name=region_name,
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key if endpoint_url is not None else None,
        aws_secret_access_key=secret_key if endpoint_url is not None else None,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": addressing_style},
        ),
    )

# head_object sem s3:ListBucket na raiz do bucket devolve 403 para uma chave
# ausente, não 404 — é o comportamento documentado do S3 quando a policy
# nega list na raiz (deliberado: a role da aplicação não tem list/create).
_ABSENT_HTTP_STATUS = {403, 404}


def _is_absent(error: ClientError) -> bool:
    status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return status in _ABSENT_HTTP_STATUS


class S3PresignedStorage:
    """Implementação de ObjectStoragePort sobre um client boto3 injetado.

    O client deve ser construído com ``Config(signature_version="s3v4")`` —
    sem isso o boto3 gera presign em SigV2 contra endpoints customizados
    (LocalStack/AIStor), que rejeitam com SignatureDoesNotMatch.

    ``public_client`` assina as URLs presigned entregues ao edge agent com
    um host diferente do usado nas chamadas internas (head_object). Sem
    conectividade de rede — generate_presigned_url é assinatura local —
    então o client público só precisa do endpoint_url certo, nunca de
    alcançar o servidor de fato. Default: mesmo client (comportamento
    anterior). Porta o split endpoint/public_endpoint do MinioWrapper
    (PR #230, H9): o hostname Docker-interno do MinIO/AIStor nunca é
    alcançável por um agente numa rede municipal.
    """

    def __init__(self, client: Any, public_client: Any | None = None) -> None:
        self._client = client
        self._public_client = public_client or client

    def generate_presigned_upload_url(
        self, bucket: str, object_key: str,
        expires_secs: int = 3600,
    ) -> str:
        return self._public_client.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket, "Key": object_key},
            ExpiresIn=expires_secs,
        )

    def object_exists(
        self, bucket: str, object_key: str,
    ) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=object_key)
            return True
        except ClientError as error:
            if _is_absent(error):
                return False
            raise

    def get_presigned_download_url(
        self, bucket: str, object_key: str,
        expires_secs: int = 3600,
    ) -> str:
        return self._public_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": object_key},
            ExpiresIn=expires_secs,
        )
