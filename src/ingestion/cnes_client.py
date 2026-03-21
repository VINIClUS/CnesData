"""
cnes_client.py - Cliente de Ingestão do CNES
Encapsula a conexão e regras de extração com o banco Firebird, contornando
problemas conhecidos do driver fdb com o pandas.
"""

import logging
import fdb
import pandas as pd

logger = logging.getLogger(__name__)

class CnesClient:
    """
    Cliente para extração segura e enriquecimento básico de dados do CNES.
    """
    def __init__(self, dsn: str, user: str, password: str):
        self.dsn = dsn
        self.user = user
        self.password = password

    def extract_safe(self, query: str) -> pd.DataFrame:
        """
        Diretriz 1: Extração Segura.
        Executa a query contornando as limitações do pandas.read_sql
        com joins esquerdos (LEFT JOIN) complexos no Firebird (Erro -501).
        Gerencia o cursor manualmente, garantindo consistência.
        """
        logger.debug("Conectando ao banco CNES para extração segura: %s", self.dsn)
        conn = None
        try:
            conn = fdb.connect(dsn=self.dsn, user=self.user, password=self.password)
            cursor = conn.cursor()
            
            logger.debug("Executando a query na base de dados...")
            cursor.execute(query)
            
            # Recupera todos os dados do cursor manualmente
            rows = cursor.fetchall()
            
            # Recupera os nomes das colunas de forma explícita via cursor.description
            # description retorna uma lista de tuplas onde index 0 é o header
            col_names = [desc[0] for desc in cursor.description]
            
            logger.info("Extração concluída com sucesso. Registros carregados: %d", len(rows))
            
            # Instancia o DataFrame depois que a conexão do banco já completou o seu lado
            return pd.DataFrame(rows, columns=col_names)
            
        finally:
            if conn:
                conn.close()

    def decode_ind_vinc(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Diretriz 2A: Decodificação Python-Side.
        Decodifica o código IND_VINC para o domínio legível mapeado no Dicionário de Dados.
        """
        # Mapeamento estático dos valores mapeados no discovery (Cnes)
        MAPA_VINCULOS = {
            "1": "SIM",
            "2": "NAO",
            "3": "AFASTADO",
            # Outros códigos seriam adicionados conforme o Mapeamento Master
        }
        
        df_out = df.copy()
        if "IND_VINC" in df_out.columns:
            # Mapeamento dos valores transformando para string primeiro,
            # com fallback para "DESCONHECIDO" em valores não estipulados
            df_out["IND_VINC_DESC"] = (
                df_out["IND_VINC"]
                .astype(str)
                .map(MAPA_VINCULOS)
                .fillna("NAO MAPEADO")
            )
        return df_out

    def apply_multiple_vinculos_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Diretriz 2B: Enriquecimento Python-Side.
        Infere a flag MULTIPLOS_VINCULOS identificando profissionais vinculados mais de
        uma vez pela contagem de 'CPF' ou 'PROF_ID'.
        """
        df_out = df.copy()
        
        # Prioriza o CPF e PROF_ID dependendo de qual coluna estiver preenchida corretamente
        chave_agrupamento = "CPF" if "CPF" in df_out.columns else "PROF_ID"
        
        if chave_agrupamento in df_out.columns:
            # Conta o número de ocorrências por Chave
            contagem = df_out.groupby(chave_agrupamento)[chave_agrupamento].transform("size")
            # Verdadeiro para todo mundo que obteve contagem maior do que 1
            df_out["MULTIPLOS_VINCULOS"] = contagem > 1
        else:
            # Fall-back passivo se a query base não tiver a chave (não quebre a execução)
            df_out["MULTIPLOS_VINCULOS"] = False
            
        return df_out

    def validate_domain_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Diretriz 3: Validação Algorítmica de Domínio (RQs)
        Valida criticamente a veracidade de alguns campos chaves baseados
        em cruzamentos CBO x TP_UNID conforme normativo do Ministério da Saúde.
        """
        df_out = df.copy()
        
        # Só opera essa qualidade se os dois campos vitais existirem
        if "CBO" in df_out.columns and "TP_UNID" in df_out.columns:
            
            # Subconjunto de validação de Agentes
            cbo_acs_ace = ["515105", "515140"]
            tp_unid_validos_acs = ["02", "04"] # Tipos simulados do ESF na Prefeitura
            
            mascara_eh_acs_ace = df_out["CBO"].isin(cbo_acs_ace)
            mascara_tp_valido = df_out["TP_UNID"].isin(tp_unid_validos_acs)
            
            # Regra lógica de inconsistência: 
            # SE (Agente ACS) ENTÃO (Deve estar em Tipo Unidade Válida)
            # Todo o restante da base passará direto: ~mascara_eh_acs_ace
            df_out["RQ_DOMINIO_VALIDO"] = ~mascara_eh_acs_ace | (mascara_eh_acs_ace & mascara_tp_valido)
        else:
            # Passa no teste de qualidade se os campos vitais não foram requisitados
            df_out["RQ_DOMINIO_VALIDO"] = True
            
        return df_out
