"""
conftest.py — Configuração e Fixtures Globais do Pytest

Este arquivo é carregado automaticamente pelo pytest antes de qualquer teste.
Aqui ficam:
  - Fixtures compartilhadas entre múltiplos arquivos de teste.
  - Configuração do sys.path para que `import config` e `import cnes_exporter`
    funcionem corretamente sem precisar instalar o pacote.
  - Marcadores personalizados (`integration`) para separar testes lentos dos rápidos.
"""

import sys
from pathlib import Path
import pytest

# Adiciona src/ ao sys.path para que os módulos do projeto
# (config, cnes_exporter) possam ser importados nos testes normalmente.
SRC_DIR = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))


import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures: dados de exemplo para testes unitários
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def df_profissionais_bruto() -> pd.DataFrame:
    """
    DataFrame que simula os dados brutos retornados pela query do banco,
    incluindo nulos nas colunas de equipe (resultado natural do LEFT JOIN).
    """
    return pd.DataFrame({
        "CPF":               ["11716723817 ", " 22730768866", "27943602803"],
        "NOME_PROFISSIONAL": ["ZELIA APARECIDA RIBEIRO LIMA ", "VANESSA COSTA PAIXAO", " MARCELA NUNES BERNARDES LUZ"],
        "CBO":               ["514225 ", "322245", "225142"],
        "CARGA_HORARIA":     [40, 40, 40],
        "COD_CNES":          ["0985333 ", "0985333", "0985333"],
        "ESTABELECIMENTO":   ["ESF VILA GERONIMO", "ESF VILA GERONIMO  ", "ESF VILA GERONIMO"],
        "TIPO_ESTAB":        ["02", "02", "02"],
        "COD_INE_EQUIPE":    [None, None, None],
        "NOME_EQUIPE":       [None, None, None],
        "TIPO_EQUIPE":       [None, None, None],
    })


@pytest.fixture
def df_com_equipe() -> pd.DataFrame:
    """
    DataFrame que simula profissionais que JÁ TÊM equipe vinculada
    (o LEFT JOIN retornou dados para as colunas de equipe).
    """
    return pd.DataFrame({
        "CPF":               ["22730768866"],
        "NOME_PROFISSIONAL": ["VANESSA COSTA PAIXAO"],
        "CBO":               ["322245"],
        "CARGA_HORARIA":     [40],
        "COD_CNES":          ["0985333"],
        "ESTABELECIMENTO":   ["ESF VILA GERONIMO"],
        "TIPO_ESTAB":        ["02"],
        "COD_INE_EQUIPE":    ["0001365993"],
        "NOME_EQUIPE":       ["ESF VILA GERONIMO"],
        "TIPO_EQUIPE":       ["01"],
    })
