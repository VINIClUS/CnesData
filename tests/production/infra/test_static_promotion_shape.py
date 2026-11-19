"""Gramática e grants das policies nativas com listas opcionais vazias."""

from copy import deepcopy

import pytest

from tests.production.infra._static_promotion_iam_model import documentos_do_plano
from tests.production.infra.test_static_promotion_iam import (
    VETORES,
    _grants,
    _plano,
    fixture_estatico,  # noqa: F401
)


def _verificar_campos_nao_vazios(documento, campos):
    assert documento["Version"] == "2012-10-17"
    assert isinstance(documento["Statement"], list)
    assert documento["Statement"]
    for declaracao in documento["Statement"]:
        assert declaracao["Effect"] == "Allow"
        for campo in campos:
            valores = declaracao[campo]
            valores = valores if isinstance(valores, list) else [valores]
            assert valores, f"campo_vazio={campo} declaracao={declaracao}"
            assert all(isinstance(valor, str) and valor for valor in valores)


@pytest.mark.parametrize(("cenario", "acoes_ausentes"), [
    ("operacional", set()),
    ("sem_baseline", {"s3:GetObjectVersion", "s3:PutObject"}),
    ("sem_multipart", {"s3:ListMultipartUploadParts", "s3:AbortMultipartUpload"}),
    ("sem_retiradas", {"s3:DeleteObjectVersion"}),
    ("sem_listas_opcionais", {"s3:GetObjectVersion", "s3:PutObject",
                              "s3:ListMultipartUploadParts", "s3:AbortMultipartUpload",
                              "s3:DeleteObjectVersion"}),
])
def test_listas_opcionais_preservam_gramatica_e_grants_nativos(estatico, cenario, acoes_ausentes):
    documentos, confiancas = documentos_do_plano(_plano(estatico, cenario))
    for documento in documentos.values():
        _verificar_campos_nao_vazios(documento, ("Action", "Resource"))
    for confianca in confiancas.values():
        _verificar_campos_nao_vazios(confianca, ("Action",))
    esperados = deepcopy(VETORES["control_policies"])
    for alias in ("normal", "recovery"):
        removidas = acoes_ausentes & ({"s3:DeleteObjectVersion"} if alias == "normal" else {
            "s3:GetObjectVersion", "s3:PutObject", "s3:ListMultipartUploadParts",
            "s3:AbortMultipartUpload",
        })
        esperados[alias]["Statement"] = [
            declaracao for declaracao in esperados[alias]["Statement"]
            if not removidas.intersection(
                declaracao["Action"] if isinstance(declaracao["Action"], list)
                else [declaracao["Action"]]
            )
        ]
    for alias, documento in documentos.items():
        assert _grants(documento) == _grants(esperados[alias])
