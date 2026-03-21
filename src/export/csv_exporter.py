"""
csv_exporter.py — Camada de Exportação: Geração de Relatórios CSV

Responsabilidade única: receber um DataFrame processado e persistir
em disco no formato CSV compatível com ferramentas brasileiras.

Convenções de exportação aplicadas:
  - Separador `;`: padrão BR, compatível com Excel pt-BR sem ajuste.
  - Encoding `utf-8-sig` (UTF-8 com BOM): garante leitura correta de
    caracteres especiais no Excel em Windows.
  - `index=False`: o índice interno do pandas não tem significado de negócio.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def exportar_csv(df: pd.DataFrame, caminho: Path) -> None:
    """
    Persiste o DataFrame em um arquivo CSV no padrão brasileiro.

    Cria os diretórios intermediários automaticamente se não existirem.

    Args:
        df: DataFrame limpo e validado, pronto para exportação.
        caminho: Caminho absoluto do arquivo de saída (incluindo nome e extensão).

    Raises:
        OSError: Se não for possível criar o diretório pai ou escrever o arquivo.
    """
    caminho.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Exportando %d registros para: %s", len(df), caminho)
    df.to_csv(caminho, index=False, sep=";", encoding="utf-8-sig")
    logger.info("Exportação CSV concluída com sucesso.")
