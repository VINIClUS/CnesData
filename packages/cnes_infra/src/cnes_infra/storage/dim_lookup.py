"""PostgresDimLookup: cached surrogate key resolver + dim upsert helpers."""
from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


class PostgresDimLookup:

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._cache: dict[tuple[str, str], int] = {}

    def _lookup(
        self, table: str, key_col: str, key: str, sk_col: str,
    ) -> int | None:
        ck = (table, key)
        if ck in self._cache:
            return self._cache[ck]
        row = self._conn.execute(
            text(
                f"SELECT {sk_col} FROM gold.{table} WHERE {key_col} = :k"  # noqa: S608 - identifiers hardcoded in callers
            ),
            {"k": key},
        ).first()
        if row:
            self._cache[ck] = row[0]
            return row[0]
        return None

    def sk_profissional_por_cpf_hash(self, cpf_hash: str) -> int | None:
        return self._lookup(
            "dim_profissional", "cpf_hash", cpf_hash, "sk_profissional",
        )

    def sk_estabelecimento_por_cnes(self, cnes: str) -> int | None:
        return self._lookup(
            "dim_estabelecimento", "cnes", cnes, "sk_estabelecimento",
        )

    def sk_cbo_por_codigo(self, cod_cbo: str) -> int | None:
        return self._lookup("dim_cbo", "cod_cbo", cod_cbo, "sk_cbo")

    def sk_competencia_por_yyyymm(self, yyyymm: int) -> int | None:
        ck = ("dim_competencia", str(yyyymm))
        if ck in self._cache:
            return self._cache[ck]
        row = self._conn.execute(
            text(
                "SELECT sk_competencia FROM gold.dim_competencia "
                "WHERE competencia = :c"
            ),
            {"c": yyyymm},
        ).first()
        if row:
            self._cache[ck] = row[0]
            return row[0]
        return None


def _jsonize(obj: object) -> str:
    return json.dumps(obj or {})


def upsert_dim_profissional(conn: Connection, payload: dict) -> int:
    row = conn.execute(
        text("""
            INSERT INTO gold.dim_profissional (cpf_hash, nome, cns, fontes)
            VALUES (:cpf_hash, :nome, :cns, CAST(:fontes AS JSONB))
            ON CONFLICT (cpf_hash) DO UPDATE
            SET nome = EXCLUDED.nome,
                cns = COALESCE(EXCLUDED.cns, gold.dim_profissional.cns),
                fontes = gold.dim_profissional.fontes || EXCLUDED.fontes,
                atualizado_em = NOW()
            RETURNING sk_profissional
        """),
        {
            "cpf_hash": payload["cpf_hash"],
            "nome": payload["nome"],
            "cns": payload.get("cns"),
            "fontes": _jsonize(payload.get("fontes")),
        },
    ).first()
    return row[0]


def upsert_dim_estabelecimento(conn: Connection, payload: dict) -> int:
    row = conn.execute(
        text("""
            INSERT INTO gold.dim_estabelecimento (
                cnes, nome, cnpj_mantenedora, tp_unid, sk_municipio, fontes
            )
            VALUES (
                :cnes, :nome, :cnpj_mantenedora, :tp_unid, :sk_municipio,
                CAST(:fontes AS JSONB)
            )
            ON CONFLICT (cnes) DO UPDATE
            SET nome = EXCLUDED.nome,
                cnpj_mantenedora = COALESCE(
                    EXCLUDED.cnpj_mantenedora,
                    gold.dim_estabelecimento.cnpj_mantenedora
                ),
                tp_unid = EXCLUDED.tp_unid,
                sk_municipio = EXCLUDED.sk_municipio,
                fontes = gold.dim_estabelecimento.fontes || EXCLUDED.fontes,
                atualizado_em = NOW()
            RETURNING sk_estabelecimento
        """),
        {
            "cnes": payload["cnes"],
            "nome": payload["nome"],
            "cnpj_mantenedora": payload.get("cnpj_mantenedora"),
            "tp_unid": payload["tp_unid"],
            "sk_municipio": payload["sk_municipio"],
            "fontes": _jsonize(payload.get("fontes")),
        },
    ).first()
    return row[0]


def upsert_dim_cbo(conn: Connection, payload: dict) -> int:
    row = conn.execute(
        text("""
            INSERT INTO gold.dim_cbo (cod_cbo, descricao)
            VALUES (:cod_cbo, :descricao)
            ON CONFLICT (cod_cbo) DO UPDATE
            SET descricao = EXCLUDED.descricao
            RETURNING sk_cbo
        """),
        {"cod_cbo": payload["cod_cbo"], "descricao": payload["descricao"]},
    ).first()
    return row[0]


def upsert_dim_cid10(conn: Connection, payload: dict) -> int:
    row = conn.execute(
        text("""
            INSERT INTO gold.dim_cid10 (cod_cid, descricao, capitulo)
            VALUES (:cod_cid, :descricao, :capitulo)
            ON CONFLICT (cod_cid) DO UPDATE
            SET descricao = EXCLUDED.descricao,
                capitulo = EXCLUDED.capitulo
            RETURNING sk_cid
        """),
        {
            "cod_cid": payload["cod_cid"],
            "descricao": payload["descricao"],
            "capitulo": payload["capitulo"],
        },
    ).first()
    return row[0]


def upsert_dim_municipio(conn: Connection, payload: dict) -> int:
    row = conn.execute(
        text("""
            INSERT INTO gold.dim_municipio (
                ibge6, ibge7, nome, uf, populacao_estimada, teto_pab_cents
            )
            VALUES (
                :ibge6, :ibge7, :nome, :uf,
                :populacao_estimada, :teto_pab_cents
            )
            ON CONFLICT (ibge6) DO UPDATE
            SET nome = EXCLUDED.nome,
                populacao_estimada = COALESCE(
                    EXCLUDED.populacao_estimada,
                    gold.dim_municipio.populacao_estimada
                ),
                teto_pab_cents = COALESCE(
                    EXCLUDED.teto_pab_cents,
                    gold.dim_municipio.teto_pab_cents
                )
            RETURNING sk_municipio
        """),
        {
            "ibge6": payload["ibge6"],
            "ibge7": payload["ibge7"],
            "nome": payload["nome"],
            "uf": payload["uf"],
            "populacao_estimada": payload.get("populacao_estimada"),
            "teto_pab_cents": payload.get("teto_pab_cents"),
        },
    ).first()
    return row[0]


def upsert_dim_procedimento_sus(conn: Connection, payload: dict) -> int:
    row = conn.execute(
        text("""
            INSERT INTO gold.dim_procedimento_sus (
                cod_sigtap, descricao, complexidade, financiamento, modalidade,
                competencia_vigencia_ini, competencia_vigencia_fim
            )
            VALUES (
                :cod_sigtap, :descricao, :complexidade, :financiamento,
                :modalidade, :competencia_vigencia_ini, :competencia_vigencia_fim
            )
            ON CONFLICT (cod_sigtap) DO UPDATE
            SET descricao = EXCLUDED.descricao,
                complexidade = COALESCE(
                    EXCLUDED.complexidade,
                    gold.dim_procedimento_sus.complexidade
                ),
                financiamento = COALESCE(
                    EXCLUDED.financiamento,
                    gold.dim_procedimento_sus.financiamento
                ),
                modalidade = COALESCE(
                    EXCLUDED.modalidade,
                    gold.dim_procedimento_sus.modalidade
                )
            RETURNING sk_procedimento
        """),
        {
            "cod_sigtap": payload["cod_sigtap"],
            "descricao": payload["descricao"],
            "complexidade": payload.get("complexidade"),
            "financiamento": payload.get("financiamento"),
            "modalidade": payload.get("modalidade"),
            "competencia_vigencia_ini": payload.get("competencia_vigencia_ini"),
            "competencia_vigencia_fim": payload.get("competencia_vigencia_fim"),
        },
    ).first()
    return row[0]
