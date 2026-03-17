"""
test_exporter_integration.py — Testes de Integração do Pipeline CNES

Objetivo: Verificar que as funções do cnes_exporter funcionam
corretamente com o banco de dados real.

IMPORTANTE: Estes testes são marcados com @pytest.mark.integration.
Eles requerem:
  - O banco CNES.GDB em execução em localhost
  - A DLL do Firebird 64-bits no caminho configurado no .env
  - As variáveis do .env corretamente configuradas

Para rodar APENAS os testes unitários (rápidos, sem banco):
  pytest tests/ -m "not integration" -v

Para rodar TODOS os testes (incluindo integração):
  pytest tests/ -v

Para rodar APENAS os testes de integração:
  pytest tests/ -m integration -v
"""

import pytest
import pandas as pd

# conftest.py adicionou src/ ao sys.path
import config
from cnes_exporter import conectar, executar_query, transformar, exportar_csv


# Marca todos os testes neste módulo como "integration"
# Isso permite excluí-los com: pytest -m "not integration"
pytestmark = pytest.mark.integration


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures de Integração
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def conexao_real():
    """
    Abre UMA conexão com o banco real compartilhada entre todos os testes
    deste módulo (scope="module" = uma vez por arquivo, não uma por teste).
    Isso melhora a performance e evita sobrecarregar o banco.
    """
    con = conectar()
    yield con  # Entrega a conexão para os testes
    con.close()  # Executa após todos os testes do módulo terminarem


@pytest.fixture(scope="module")
def df_extraido(conexao_real):
    """
    Executa a query uma única vez e compartilha o resultado entre os testes.
    Isso evita múltiplas queries idênticas ao banco durante os testes.
    """
    return executar_query(conexao_real)


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 1: Testes de Conexão
# ─────────────────────────────────────────────────────────────────────────────

class TestConexao:

    def test_conectar_retorna_objeto_conexao(self):
        """conectar() deve retornar um objeto de conexão fdb não-nulo."""
        import fdb
        con = conectar()
        try:
            assert con is not None
            assert isinstance(con, fdb.Connection)
        finally:
            con.close()

    def test_conexao_permite_executar_query_simples(self):
        """A conexão deve ser capaz de executar uma query SELECT básica."""
        con = conectar()
        try:
            cur = con.cursor()
            cur.execute("SELECT 1 FROM RDB$DATABASE")
            resultado = cur.fetchone()
            assert resultado is not None
            assert resultado[0] == 1
        finally:
            con.close()

    def test_dll_nao_encontrada_levanta_file_not_found(self, monkeypatch):
        """Se a DLL não existir no caminho configurado, deve levantar FileNotFoundError."""
        monkeypatch.setattr(config, "FIREBIRD_DLL", r"C:\caminho\inexistente\fbembed.dll")
        with pytest.raises(FileNotFoundError, match="DLL do Firebird"):
            conectar()


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 2: Testes de Extração (Query)
# ─────────────────────────────────────────────────────────────────────────────

class TestExtracao:

    def test_query_retorna_dataframe(self, df_extraido):
        """executar_query() deve retornar um objeto pd.DataFrame."""
        assert isinstance(df_extraido, pd.DataFrame)

    def test_query_retorna_pelo_menos_100_registros(self, df_extraido):
        """O município de Presidente Epitácio deve ter pelo menos 100 profissionais."""
        assert len(df_extraido) >= 100, (
            f"Esperado >= 100 registros, obtido: {len(df_extraido)}"
        )

    def test_query_possui_colunas_obrigatorias(self, df_extraido):
        """O DataFrame deve conter todas as colunas definidas na query SQL."""
        colunas_esperadas = {
            "CPF", "NOME_PROFISSIONAL", "CBO", "CARGA_HORARIA",
            "COD_CNES", "ESTABELECIMENTO", "TIPO_ESTAB",
            "COD_INE_EQUIPE", "NOME_EQUIPE", "TIPO_EQUIPE",
        }
        colunas_ausentes = colunas_esperadas - set(df_extraido.columns)
        assert not colunas_ausentes, (
            f"Colunas ausentes no resultado: {colunas_ausentes}"
        )

    def test_todos_registros_sao_do_municipio_correto(self, conexao_real):
        """
        Verifica que NENHUM estabelecimento fora do município gestor 354130
        foi retornado pela query, garantindo a integridade do filtro WHERE.
        """
        import warnings
        # Query de verificação: conta quantos registros vieram de outro município
        query_verificacao = """
            SELECT COUNT(*) AS total_incorretos
            FROM LFCES021 vinc
            INNER JOIN LFCES004 est ON est.UNIDADE_ID = vinc.UNIDADE_ID
            INNER JOIN LFCES018 prof ON prof.PROF_ID = vinc.PROF_ID
            WHERE est.CODMUNGEST != '354130'
              AND est.CNPJ_MANT = '55293427000117'
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df_check = pd.read_sql(query_verificacao, conexao_real)
        assert df_check["TOTAL_INCORRETOS"].iloc[0] == 0, (
            "A query retornou registros de municípios incorretos!"
        )

    def test_query_vazia_levanta_value_error(self, conexao_real, monkeypatch):
        """
        Se os filtros não retornarem dados (ex: CNPJ inválido),
        executar_query() deve levantar ValueError com mensagem descritiva.
        """
        monkeypatch.setattr(config, "CNPJ_MANTENEDORA", "00000000000000")
        with pytest.raises(ValueError, match="não retornou dados"):
            executar_query(conexao_real)


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 3: Testes do Pipeline Completo (E2E — End to End)
# ─────────────────────────────────────────────────────────────────────────────

class TestPipelineCompleto:

    def test_pipeline_gera_arquivo_csv(self, tmp_path, monkeypatch):
        """
        O pipeline completo deve gerar um arquivo CSV no caminho configurado.
        Usa tmp_path (fixture nativa do pytest) para isolar o arquivo de saída.
        """
        csv_temporario = tmp_path / "teste_relatorio.csv"
        monkeypatch.setattr(config, "OUTPUT_PATH", csv_temporario)

        from cnes_exporter import pipeline
        pipeline()

        assert csv_temporario.exists(), "O arquivo CSV não foi criado pelo pipeline"

    def test_csv_gerado_tem_conteudo(self, tmp_path, monkeypatch):
        """O CSV gerado deve ter pelo menos 100 linhas de dados (além do cabeçalho)."""
        csv_temporario = tmp_path / "teste_relatorio.csv"
        monkeypatch.setattr(config, "OUTPUT_PATH", csv_temporario)

        from cnes_exporter import pipeline
        pipeline()

        df_csv = pd.read_csv(csv_temporario, sep=";", encoding="utf-8-sig")
        assert len(df_csv) >= 100, (
            f"CSV gerado com apenas {len(df_csv)} linhas — esperado >= 100"
        )

    def test_csv_gerado_nao_tem_nulos_em_colunas_obrigatorias(self, tmp_path, monkeypatch):
        """
        Após a transformação, as colunas CPF e NOME_PROFISSIONAL
        não devem ter valores nulos no CSV final.
        """
        csv_temporario = tmp_path / "teste_relatorio.csv"
        monkeypatch.setattr(config, "OUTPUT_PATH", csv_temporario)

        from cnes_exporter import pipeline
        pipeline()

        df_csv = pd.read_csv(csv_temporario, sep=";", encoding="utf-8-sig")
        assert df_csv["CPF"].isna().sum() == 0, "CPF não pode ter valores nulos"
        assert df_csv["NOME_PROFISSIONAL"].isna().sum() == 0, "NOME não pode ter nulos"
