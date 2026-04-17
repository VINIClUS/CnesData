"""Regressão: upsert de fontes em JSONB-object é idempotente e faz união por chave."""
import pytest
from sqlalchemy import text

from cnes_infra.storage.repositories.estabelecimento_repo import (
    EstabelecimentoRepository,
)

pytestmark = pytest.mark.postgres


def _row(cnes: str, fontes: dict) -> dict:
    return {
        "tenant_id": "355030",
        "cnes": cnes,
        "fontes": fontes,
    }


class TestFontesIdempotency:
    def test_tres_upserts_mesma_fonte_nao_duplicam(self, uow) -> None:
        with uow:
            repo = EstabelecimentoRepository(uow._con)
            for _ in range(3):
                repo.gravar([_row("0985333", {"WEB": True})])

        with uow._engine.connect() as con:
            result = con.execute(text(
                "SELECT fontes FROM gold.dim_estabelecimento "
                "WHERE tenant_id='355030' AND cnes='0985333'"
            )).scalar()

        assert result == {"WEB": True}

    def test_fontes_diferentes_fazem_uniao(self, uow) -> None:
        with uow:
            repo = EstabelecimentoRepository(uow._con)
            repo.gravar([_row("0985334", {"WEB": True})])
            repo.gravar([_row("0985334", {"LOCAL": True})])

        with uow._engine.connect() as con:
            result = con.execute(text(
                "SELECT fontes FROM gold.dim_estabelecimento "
                "WHERE tenant_id='355030' AND cnes='0985334'"
            )).scalar()

        assert result == {"WEB": True, "LOCAL": True}

    def test_fontes_vazio_preserva_existente(self, uow) -> None:
        with uow:
            repo = EstabelecimentoRepository(uow._con)
            repo.gravar([_row("0985335", {"WEB": True})])
            repo.gravar([_row("0985335", {})])

        with uow._engine.connect() as con:
            result = con.execute(text(
                "SELECT fontes FROM gold.dim_estabelecimento "
                "WHERE tenant_id='355030' AND cnes='0985335'"
            )).scalar()

        assert result == {"WEB": True}


class TestFontesSchemaContract:
    def test_coluna_fontes_jsonb_tipo_objeto_via_typeof(self, uow) -> None:
        with uow:
            repo = EstabelecimentoRepository(uow._con)
            repo.gravar([_row("0985336", {"WEB": True})])

        with uow._engine.connect() as con:
            tipo = con.execute(text(
                "SELECT jsonb_typeof(fontes) FROM gold.dim_estabelecimento "
                "WHERE tenant_id='355030' AND cnes='0985336'"
            )).scalar()

        assert tipo == "object", (
            f"fontes deve ser JSONB object; virou {tipo}. "
            "Regressão de schema — revisar semântica do merge `||`."
        )
