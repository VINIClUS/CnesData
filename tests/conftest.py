"""
conftest.py — Configuração e Fixtures Globais do Pytest

Este arquivo é carregado automaticamente pelo pytest antes de qualquer teste.
Aqui ficam:
  - Configuração do sys.path para que imports de src/ funcionem sem instalação.
  - Fixtures compartilhadas entre múltiplos arquivos de teste.
  - Todos os nomes de coluna seguem os aliases da Query Master validada
    (data_dictionary.md), e não os aliases do módulo legado cnes_exporter.py.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Adiciona src/ ao sys.path para que todos os módulos do projeto
# (config, ingestion.cnes_client, processing.transformer, etc.)
# possam ser importados normalmente em qualquer subdiretório de tests/.
SRC_DIR = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures: dados brutos simulando o retorno da camada de ingestão
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def df_profissionais_bruto() -> pd.DataFrame:
    """
    DataFrame que simula os dados brutos retornados por extrair_profissionais().

    Características:
      - Strings com espaços extras (comportamento real do Firebird CHAR padding).
      - Colunas de equipe com None (resultado do LEFT JOIN sem correspondência).
      - CPFs válidos (11 dígitos após strip).
      - Colunas alinhadas com os aliases da Query Master (data_dictionary.md).
    """
    return pd.DataFrame({
        "CPF":                ["11716723817 ", " 22730768866", "27943602803"],
        "CNS":                ["702002887429583", "700402168923850", "708609133020390"],
        "NOME_PROFISSIONAL":  [
            "ZELIA APARECIDA RIBEIRO LIMA ",
            "VANESSA COSTA PAIXAO",
            " MARCELA NUNES BERNARDES LUZ",
        ],
        "NOME_SOCIAL":        [None, None, None],
        "SEXO":               ["F ", "F", " F"],
        "DATA_NASCIMENTO":    ["1975-04-12", "1988-09-23", "1990-11-05"],
        "CBO":                ["514225 ", "322245", "225142"],
        "TIPO_VINCULO":        ["010101", "010101", "010500"],
        "SUS":        ["S", "S", "S"],
        "CH_TOTAL": [40, 40, 40],
        "CH_AMBULATORIAL":    [40, 40, 40],
        "CH_OUTRAS":          [0, 0, 0],
        "CH_HOSPITALAR":      [0, 0, 0],
        "CNES":           ["0985333 ", "0985333", "0985333"],
        "ESTABELECIMENTO":    ["ESF VILA GERONIMO", "ESF VILA GERONIMO  ", "ESF VILA GERONIMO"],
        "TIPO_UNIDADE":   ["02", "02", "02"],
        "COD_MUNICIPIO":     ["354130", "354130", "354130"],
        "INE":     [None, None, None],
        "NOME_EQUIPE":        [None, None, None],
        "TIPO_EQUIPE":    [None, None, None],
    })


@pytest.fixture
def df_com_equipe() -> pd.DataFrame:
    """
    DataFrame que simula um profissional com equipe vinculada.

    O LEFT JOIN retornou dados para as colunas de equipe — não há None.
    Usado para verificar que o fillna() não sobrescreve valores já presentes.
    """
    return pd.DataFrame({
        "CPF":                ["22730768866"],
        "CNS":                ["700402168923850"],
        "NOME_PROFISSIONAL":  ["VANESSA COSTA PAIXAO"],
        "NOME_SOCIAL":        [None],
        "SEXO":               ["F"],
        "DATA_NASCIMENTO":    ["1988-09-23"],
        "CBO":                ["322245"],
        "TIPO_VINCULO":        ["010101"],
        "SUS":        ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL":    [40],
        "CH_OUTRAS":          [0],
        "CH_HOSPITALAR":      [0],
        "CNES":           ["0985333"],
        "ESTABELECIMENTO":    ["ESF VILA GERONIMO"],
        "TIPO_UNIDADE":   ["02"],
        "COD_MUNICIPIO":     ["354130"],
        "INE":     ["0001365993"],
        "NOME_EQUIPE":        ["ESF VILA GERONIMO"],
        "TIPO_EQUIPE":    ["70"],
    })


@pytest.fixture
def df_com_cpf_invalido() -> pd.DataFrame:
    """
    DataFrame com CPFs inválidos para testar RQ-002.

    Inclui: CPF None, CPF com comprimento errado (9 dígitos), CPF válido.
    Após transformar(), apenas o terceiro registro deve sobreviver.
    """
    return pd.DataFrame({
        "CPF":                [None, "123456789", "27943602803"],
        "CNS":                [None, None, "708609133020390"],
        "NOME_PROFISSIONAL":  ["NOME COM CPF NULO", "NOME COM CPF CURTO", "NOME VALIDO"],
        "NOME_SOCIAL":        [None, None, None],
        "SEXO":               ["F", "M", "F"],
        "DATA_NASCIMENTO":    ["1980-01-01", "1985-06-15", "1990-11-05"],
        "CBO":                ["515105", "322260", "225142"],
        "TIPO_VINCULO":        ["010101", "010101", "010500"],
        "SUS":        ["S", "S", "S"],
        "CH_TOTAL": [40, 40, 40],
        "CH_AMBULATORIAL":    [40, 40, 40],
        "CH_OUTRAS":          [0, 0, 0],
        "CH_HOSPITALAR":      [0, 0, 0],
        "CNES":           ["0985333", "0985334", "0985335"],
        "ESTABELECIMENTO":    ["UBS A", "UBS B", "UBS C"],
        "TIPO_UNIDADE":   ["01", "02", "02"],
        "COD_MUNICIPIO":     ["354130", "354130", "354130"],
        "INE":     [None, None, None],
        "NOME_EQUIPE":        [None, None, None],
        "TIPO_EQUIPE":    [None, None, None],
    })


@pytest.fixture
def df_com_carga_horaria_zero() -> pd.DataFrame:
    """
    DataFrame com um registro de carga horária zero para testar RQ-003.

    O primeiro registro tem CARGA_HORARIA_TOTAL = 0 (Vínculo Zumbi).
    O segundo tem carga normal. Após transformar(), o primeiro deve ter
    ALERTA_STATUS_CH = 'ATIVO_SEM_CH' e o segundo 'OK'.
    """
    return pd.DataFrame({
        "CPF":                ["11111111111", "22222222222"],
        "CNS":                ["702002887429583", "700402168923850"],
        "NOME_PROFISSIONAL":  ["PROF ZUMBI", "PROF NORMAL"],
        "NOME_SOCIAL":        [None, None],
        "SEXO":               ["M", "F"],
        "DATA_NASCIMENTO":    ["1975-01-01", "1985-01-01"],
        "CBO":                ["515105", "515105"],
        "TIPO_VINCULO":        ["010101", "010101"],
        "SUS":        ["S", "S"],
        "CH_TOTAL": [0, 40],
        "CH_AMBULATORIAL":    [0, 40],
        "CH_OUTRAS":          [0, 0],
        "CH_HOSPITALAR":      [0, 0],
        "CNES":           ["0985333", "0985333"],
        "ESTABELECIMENTO":    ["UBS A", "UBS A"],
        "TIPO_UNIDADE":   ["01", "01"],
        "COD_MUNICIPIO":     ["354130", "354130"],
        "INE":     [None, None],
        "NOME_EQUIPE":        [None, None],
        "TIPO_EQUIPE":    [None, None],
    })
