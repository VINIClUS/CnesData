import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

# No TDD estrito, importamos a classe que ainda será criada para garantir
# que o design atenda às nossas assinaturas de testes.
from ingestion.cnes_client import CnesClient


@pytest.fixture
def mock_fdb_cursor():
    """
    Simula o cursor do banco de dados fdb, garantindo os cenários das descobertas:
    - Um profissional padrão.
    - Um profissional com múltiplos vínculos (para testar a flag MULTIPLOS_VINCULOS).
    - Um caso de ACS alocado corretamente para validarmos os domínios.
    """
    cursor_mock = MagicMock()
    
    # Simula o attribute 'description' que contém as definições das colunas (nome da coluna no índice 0)
    cursor_mock.description = [
        ("CPF", None), 
        ("PROF_ID", None), 
        ("NOME_PROFISSIONAL", None),
        ("CBO", None), 
        ("TP_UNID", None), 
        ("IND_VINC", None)
    ]
    
    # Retorno simulado do fetchall() em conformidade estrita aos requisitos
    cursor_mock.fetchall.return_value = [
        # Caso 1: Profissional padrão
        ("11111111111", "ID_1", "PROFISSIONAL PADRAO", "225125", "02", "1"),
        # Caso 2 e 3: Profissional com Múltiplos Vínculos (mesmo CPF e PROF_ID)
        ("22222222222", "ID_2", "PROFISSIONAL MULTIPLO", "225124", "02", "1"),
        ("22222222222", "ID_2", "PROFISSIONAL MULTIPLO", "225203", "04", "2"),
        # Caso 4: Profissional ACS (CBO 515105)
        ("33333333333", "ID_3", "AGENTE COMUNITARIO", "515105", "02", "1"),
    ]
    
    return cursor_mock


@pytest.fixture
def mock_fdb_connect(mock_fdb_cursor):
    """
    Fixture que faz o patch mandatório de `fdb.connect`, e fornece a conexão
    mockada e seu cursor.
    """
    with patch("ingestion.cnes_client.fdb.connect") as mock_connect:
        conn_mock = MagicMock()
        conn_mock.cursor.return_value = mock_fdb_cursor
        mock_connect.return_value = conn_mock
        yield mock_connect, conn_mock, mock_fdb_cursor


class TestCnesClient:
    
    def test_extract_safe_query_bypasses_pandas_read_sql(self, mock_fdb_connect):
        """
        Diretriz 1 (Extração Segura): Garante que a ingestão não use pd.read_sql
        devido a falhas documentadas com fdb (-501 left joins). Deve gerenciar
        o cursor manualmente via execute, fetchall, description.
        """
        mock_connect, mock_conn, mock_cursor = mock_fdb_connect
        
        # O construtor provisório pede as strings de conexão
        client = CnesClient(dsn="fake:cnes.gdb", user="sysdba", password="key")
        
        # Emulando uma string de query master abstrata
        df_result = client.extract_safe("SELECT MOCK")
        
        # Validação do Mocking Mandatório
        mock_connect.assert_called_once_with(dsn="fake:cnes.gdb", user="sysdba", password="key")
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT MOCK")
        mock_cursor.fetchall.assert_called_once()
        
        # Validação do DataFrame instanciado corretamente do cursor abstrato
        assert isinstance(df_result, pd.DataFrame)
        assert len(df_result) == 4
        assert list(df_result.columns) == ["CPF", "PROF_ID", "NOME_PROFISSIONAL", "CBO", "TP_UNID", "IND_VINC"]

    def test_decode_ind_vinc(self):
        """
        Diretriz 2A: Testar a decodificação do campo IND_VINC com o dicionário
        mapeado nas descobertas (script 3).
        """
        df_mock = pd.DataFrame({
            "IND_VINC": ["1", "2"]
        })
        client = CnesClient(dsn="", user="", password="")
        
        df_result = client.decode_ind_vinc(df_mock)
        
        assert "IND_VINC_DESC" in df_result.columns
        # Assumindo que o dev vai implementar uma lógica de map(), checamos
        # apenas se existem outputs validados/não nulos.
        assert not df_result["IND_VINC_DESC"].isnull().all()

    def test_apply_multiple_vinculos_flag(self):
        """
        Diretriz 2B: Verifica se flag MULTIPLOS_VINCULOS é extraída gerando True/False
        Agrupando contagem por CPF ou PROF_ID ativo.
        """
        # Dados de exemplo contendo duplicidades
        df_mock = pd.DataFrame({
            "CPF": ["111", "222", "222", "333"],
            "PROF_ID": ["ID_1", "ID_2", "ID_2", "ID_3"]
        })
        client = CnesClient(dsn="", user="", password="")
        
        df_result = client.apply_multiple_vinculos_flag(df_mock)
        
        assert "MULTIPLOS_VINCULOS" in df_result.columns
        assert df_result["MULTIPLOS_VINCULOS"].dtype == bool
        
        # CPF 222 tem duas ocorrências: MULTIPLOS_VINCULOS = True
        mask_multiplo = df_result["CPF"] == "222"
        assert bool(df_result.loc[mask_multiplo, "MULTIPLOS_VINCULOS"].all()) is True
        
        # CPF 111 e 333 têm uma ocorrência: MULTIPLOS_VINCULOS = False
        mask_unico = df_result["CPF"] != "222"
        assert bool(df_result.loc[mask_unico, "MULTIPLOS_VINCULOS"].all()) is False

    def test_validate_domain_rules_cbo_acs_ace(self):
        """
        Diretriz 3 (RQs): Teste estrutural para regras de domínio garantindo
        as validações da NFCES026 (ACS = 515105, ACE = 515140) x TP_UNID.
        """
        df_mock = pd.DataFrame({
            "CBO": ["515105", "515140", "225125"],
            "TP_UNID": ["02", "02", "04"] # Assume 02 como um TP válido para estes CBOs
        })
        client = CnesClient(dsn="", user="", password="")
        
        df_result = client.validate_domain_rules(df_mock)
        
        # Flag ou coluna analítica de validação
        assert "RQ_DOMINIO_VALIDO" in df_result.columns
        # O CBO 515105 para TP_UNID=02 deve ser qualificado e retornado corretamente
        # Neste estágio garantimos apenas que não lança erro e adiciona a respectiva validação
        assert not df_result["RQ_DOMINIO_VALIDO"].isnull().any()
