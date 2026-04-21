# Dicionário de Dados — SIHD2 (AIH)

Banco Firebird local: `C:\Datasus\SIHD2\BDSIHD2.GDB`
ODS: 10.1 | Dialeto: 1 | Firebird: 1.5 | Charset: WIN1252

---

## RELAÇÃO DAS TABELAS PRINCIPAIS

| **TABELA** | **DESCRIÇÃO** |
| :--- | :--- |
| TB_AIH | AIHs em processamento (movimento) |
| TB_HAIH | AIHs processadas (histórico por competência) |
| TB_PA | Procedimentos da AIH (movimento) |
| TB_HPA | Procedimentos da AIH (histórico por competência) |
| TB_C_D | Tabelas de domínio codificadas |
| TB_MUN | Municípios (código IBGE) |
| TU_CID | CID-10 (diagnósticos) |
| TU_PROCEDIMENTO | Tabela de procedimentos SUS |

---

## TB_AIH — AIHs em Processamento

Chave primária: `PRIMARY KEY (AH_OE_GESTOR, AH_SEQ)`

Após encerramento da competência, o conteúdo é movido para TB_HAIH.

## TB_HAIH — AIHs Histórico

Chave primária: `PRIMARY KEY (AH_CMPT, AH_OE_GESTOR, AH_SEQ)`

Layout idêntico ao de TB_AIH.

### Notas

- A chave de acesso NÃO é o número da AIH mas o sequencial `AH_SEQ` criado na importação.
- `AH_SITUACAO`: 0 = aprovada, 1 = rejeitada.

### Estrutura de Campos (TB_AIH / TB_HAIH)

| **NOME DO CAMPO** | **TIPO** | **TAM** | **NULL** | **DESCRIÇÃO** | **DOMÍNIOS** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| AH_ACDTRAB_CBOR | CHAR | 6 | NULL | CBO do acidente de trabalho | |
| AH_ACDTRAB_CNAER | CHAR | 3 | NULL | Código do acidente de trabalho | |
| AH_ACDTRAB_CNPJ_EMP | CHAR | 14 | NULL | CGC do empregador | |
| AH_ACDTRAB_VINC_PREV | CHAR | 1 | NULL | Vínculo com a Previdência | 1=autônomo; 2=desempregado; 3=aposentado; 4=não segurado; 5=empregado; 6=empregador |
| AH_AUTORIZADOR_DOC | CHAR | 15 | NULL | Documento do médico autorizador | |
| AH_AUTORIZADOR_IDENT | CHAR | 1 | NULL | Tipo documento do autorizador | 1=CPF; 2=CNS |
| AH_CAR_INTERNACAO | CHAR | 2 | NULL | Caráter da internação | TB_C_D tabela 28 |
| AH_CMPT | CHAR | 6 | NOT NULL | Competência (AAAAMM) | |
| AH_CNES | CHAR | 7 | NULL | Código CNES do estabelecimento | |
| AH_COD_SOL_LIB | CHAR | 3 | NULL | Código solicitação de liberação | TB_C_D tabela 30 |
| AH_COMPLEXIDADE | CHAR | 2 | NULL | Complexidade da internação | TB_C_D tabela 25 |
| AH_CONTRATO | CHAR | 4 | NULL | Número do contrato de gestão e metas | |
| AH_CRC | CHAR | 10 | NULL | Uso restrito ao sistema | |
| AH_CS | CHAR | 15 | NULL | Código de segurança (calculado) | |
| AH_DIAG_COMP | CHAR | 4 | NULL | Diagnóstico complementar (CID) | TU_CID |
| AH_DIAG_OBITO | CHAR | 4 | NULL | Diagnóstico do óbito (CID) | TU_CID |
| AH_DIAG_PRI | CHAR | 4 | NULL | Diagnóstico principal (CID) | TU_CID |
| AH_DIAG_SEC a AH_DIAG_SEC_9 | CHAR | 4 | NULL | Diagnóstico secundário (CID) | TU_CID |
| AH_DIAG_SEC_1_CLASS a AH_DIAG_SEC_9_CLASS | CHAR | 1 | NULL | Classificação do diagnóstico secundário | 0=campo anterior em branco; 1=PREEXISTENTE; 2=ADQUIRIDO |
| AH_DIARIAS | INTEGER | 4 | NULL | Total geral de diárias | |
| AH_DIARIAS_UI | INTEGER | 4 | NULL | Diárias de unidade intermediária (UI) | |
| AH_DIARIAS_UTI | INTEGER | 4 | NULL | Diárias de UTI | |
| AH_DIR_CLINICO_DOC | CHAR | 15 | NULL | Documento do diretor clínico | |
| AH_DIR_CLINICO_IDENT | CHAR | 1 | NULL | Tipo documento do diretor clínico | 1=CPF; 2=CNS |
| AH_DT_EMISSAO | CHAR | 8 | NULL | Data emissão AIH (AAAAMMDD) | |
| AH_DT_INTERNACAO | CHAR | 8 | NULL | Data internação (AAAAMMDD) | |
| AH_DT_SAIDA | CHAR | 8 | NULL | Data saída (AAAAMMDD) | |
| AH_ENFERMARIA | CHAR | 4 | NULL | Número da enfermaria | |
| AH_ESPECIALIDADE | CHAR | 2 | NULL | Especialidade do leito | TB_C_D tabela 7 |
| AH_FINANCIAMENTO | CHAR | 2 | NULL | Tipo de financiamento | TB_C_D tabela 4 (União/Estado/Município) |
| AH_GESTOR_DOC | CHAR | 15 | NULL | Documento do gestor | |
| AH_GESTOR_IDENT | CHAR | 1 | NULL | Tipo documento do gestor | 1=CPF; 2=CNS |
| AH_IDENT | CHAR | 2 | NULL | Tipo de AIH | 1=normal; 3=continuação; 4=registro civil; 5=longa permanência |
| AH_IN_GER_INF | CHAR | 1 | NULL | Flag AIH gerenciada | Gestor pode bloquear/liberar |
| AH_IVD_SH | NUMERIC | 4,2 | NULL | Índice valorização serviços hospitalares | |
| AH_IVD_SP | NUMERIC | 4,2 | NULL | Índice valorização serviços prestados (desuso) | |
| AH_LAQVAS_GESTACAO_RISCO | CHAR | 1 | NULL | Gestação de alto risco | 0=sim; 1=não |
| AH_LAQVAS_CID_INDICACAO | CHAR | 4 | NULL | CID indicação laqueadura/vasectomia | |
| AH_LAQVAS_GRAU_INSTRUC | CHAR | 1 | NULL | Grau de instrução do paciente | 1=analfabeto; 2=1º grau; 3=2º grau; 4=3º grau |
| AH_LAQVAS_MET_CONTRACEP1 | CHAR | 2 | NULL | Método contraceptivo 1 | |
| AH_LAQVAS_MET_CONTRACEP2 | CHAR | 2 | NULL | Método contraceptivo 2 | |
| AH_LAQVAS_QTD_FILHOS | CHAR | 2 | NULL | Quantidade de filhos | Laqueadura/vasectomia |
| AH_LEITO | CHAR | 4 | NULL | Número do leito | |
| AH_LOTE | CHAR | 8 | NULL | Número do lote | |
| AH_LOTE_APRES | CHAR | 6 | NULL | Competência de apresentação (AAAAMM) | |
| AH_MED_RESP_DOC | CHAR | 15 | NULL | Documento do médico responsável | |
| AH_MED_RESP_IDENT | CHAR | 1 | NULL | Tipo documento médico responsável | 1=CPF; 2=CNS |
| AH_MED_SOL_DOC | CHAR | 15 | NULL | Documento do médico solicitante | |
| AH_MED_SOL_IDENT | CHAR | 1 | NULL | Tipo documento médico solicitante | 1=CPF; 2=CNS |
| AH_MODALIDADE_INTERNACAO | CHAR | 2 | NULL | Modalidade da internação | 02=hospitalar; 03=hospital dia; 04=internação domiciliar |
| AH_MOT_BLOQ | CHAR | 2 | NULL | Motivo do bloqueio | TB_C_D tabela 24 |
| AH_MOT_SAIDA | CHAR | 2 | NULL | Motivo da alta | TB_C_D tabela 23 |
| AH_MUN_HOSP | CHAR | 6 | NULL | Código IBGE município do hospital | TB_MUN |
| AH_NUM_AIH | CHAR | 13 | NULL | Número da AIH | |
| AH_NUM_AIH_ANT | CHAR | 13 | NULL | Número da AIH anterior (parto) | |
| AH_NUM_AIH_PROX | CHAR | 13 | NULL | Número da próxima AIH (parto) | |
| AH_OE_AIH | CHAR | 10 | NULL | Código órgão emissor da AIH | |
| AH_OE_GESTOR | CHAR | 10 | NOT NULL | Código órgão emissor do gestor | |
| AH_OE_REGIONAL | CHAR | 10 | NULL | Código órgão emissor da regional | |
| AH_PACIENTE_DT_NASCIMENTO | CHAR | 8 | NULL | Data nascimento paciente (AAAAMMDD) | |
| AH_PACIENTE_ETNIA | CHAR | 4 | NULL | Etnia do paciente | TB_C_D |
| AH_PACIENTE_IDADE | INTEGER | 4 | NULL | Idade do paciente | |
| AH_PACIENTE_IDENT_DOC | CHAR | 1 | NULL | Tipo documento do paciente | 1=PIS/PASEP; 2=RG; 3=Certidão nascimento; 4=CPF; 5=Ignorado |
| AH_PACIENTE_LOGR | VARCHAR | 50 | NULL | Logradouro do paciente | |
| AH_PACIENTE_LOGR_BAIRRO | VARCHAR | 30 | NULL | Bairro do paciente | |
| AH_PACIENTE_LOGR_CEP | CHAR | 8 | NULL | CEP do paciente | |
| AH_PACIENTE_LOGR_COMPL | VARCHAR | 15 | NULL | Complemento endereço | |
| AH_PACIENTE_LOGR_MUNICIPIO | CHAR | 6 | NULL | Código IBGE município do paciente | TB_MUN |
| AH_PACIENTE_LOGR_NUMERO | VARCHAR | 7 | NULL | Número do endereço | |
| AH_PACIENTE_LOGR_UF | CHAR | 2 | NULL | UF do paciente | |
| AH_PACIENTE_MUN_ORIGEM | CHAR | 6 | NULL | Código IBGE município de origem | TB_MUN |
| AH_PACIENTE_NACIONALIDADE | CHAR | 2 | NULL | Nacionalidade do paciente | TB_C_D tabela 29 |
| AH_PACIENTE_NOME | VARCHAR | 70 | NULL | Nome do paciente | |
| AH_PACIENTE_NOME_MAE | VARCHAR | 70 | NULL | Nome da mãe | |
| AH_PACIENTE_NOME_RESP | VARCHAR | 70 | NULL | Nome do responsável pela internação | |
| AH_PACIENTE_NUMERO_CNS | CHAR | 15 | NULL | CNS do paciente | |
| AH_PACIENTE_NUMERO_DOC | CHAR | 11 | NULL | Número documento do paciente | |
| AH_PACIENTE_RACA_COR | CHAR | 1 | NULL | Raça/cor do paciente | TB_C_D tabela 26 |
| AH_PACIENTE_SEXO | CHAR | 1 | NULL | Sexo do paciente | M ou F |
| AH_PACIENTE_TEL_DDD | CHAR | 2 | NULL | DDD do telefone | |
| AH_PACIENTE_TEL_NUM | CHAR | 9 | NULL | Número do telefone | |
| AH_PACIENTE_TIPO_LOGR | CHAR | 3 | NULL | Tipo logradouro | TB_C_D tabela 21 |
| AH_PARTO_NUM_PRENATAL | CHAR | 10 | NULL | Número da gestante no Pré-Natal | |
| AH_PARTO_QTD_ALTA | CHAR | 1 | NULL | Nascidos que tiveram alta | |
| AH_PARTO_QTD_NASC_MORTOS | CHAR | 1 | NULL | Nascidos mortos | |
| AH_PARTO_QTD_NASC_VIVOS | CHAR | 1 | NULL | Nascidos vivos | |
| AH_PARTO_QTD_OBITO | CHAR | 1 | NULL | Nascidos vivos que morreram + natimortos | |
| AH_PARTO_QTD_TRAN | CHAR | 1 | NULL | Nascidos transferidos | |
| AH_PROC_REALIZADO | CHAR | 10 | NULL | Código procedimento realizado | TU_PROCEDIMENTO |
| AH_PROC_SOLICITADO | CHAR | 10 | NULL | Código procedimento solicitado | TU_PROCEDIMENTO |
| AH_PRONTUARIO | VARCHAR | 15 | NULL | Número do prontuário | |
| AH_SEQ | INTEGER | 4 | NOT NULL | Sequencial da AIH no lote | |
| AH_SEQ_AIH5 | CHAR | 3 | NULL | Sequencial AIH tipo 5 (longa permanência) | |
| AH_SITUACAO | CHAR | 1 | NULL | Situação da AIH | 0=aprovada; 1=rejeitada |
| AH_ST_AGRAVO | CHAR | 1 | NULL | Marca de bloqueio de agravo | 1=bloqueada; 2=cancelada; 3=paga; 4=reservada |
| AH_ST_BLOQUEIO | CHAR | 1 | NULL | Marca de bloqueio | |
| AH_ST_CATETERISMO_ANEST | CHAR | 1 | NULL | Utilização de anestesia | 0=não; 1=sim |
| AH_ST_DUPLICIDADE | CHAR | 1 | NULL | Marca de homônimo | 0=não; 1=sim |
| AH_ST_INTO | CHAR | 1 | NULL | AIH do INTO | 0=não; 1=sim |
| AH_ST_MUDA_PROC | CHAR | 1 | NULL | Mudança de procedimento | 1=sim; 2=não |
| AH_ST_ONCO | CHAR | 1 | NULL | Uso reservado ao sistema | |
| AH_ST_ORTO | CHAR | 1 | NULL | Uso reservado ao sistema | |
| AH_ST_NEURO | CHAR | 1 | NULL | Uso reservado ao sistema | |
| AH_ST_MENTAL | CHAR | 1 | NULL | Uso reservado ao sistema | |
| AH_STATUS_PR | CHAR | 1 | NULL | Uso reservado ao sistema | |
| AH_STATUS_PR1 | CHAR | 1 | NULL | Uso reservado ao sistema | |
| AH_TIPO_FAEC | CHAR | 5 | NULL | Tipo de FAEC | TB_C_D tabela 13 |
| AH_UTINEO_MESES_GESTACAO | CHAR | 1 | NULL | Meses de gestação (UTI Neonatal) | |
| AH_UTINEO_MOT_SAIDA | CHAR | 1 | NULL | Motivo saída UTI Neonatal | |
| AH_UTINEO_PESO | CHAR | 4 | NULL | Peso UTI Neonatal | |
| AH_VERSAO_SISAIH01 | CHAR | 4 | NULL | Versão do SISAIH01 | |

---

## TB_PA — Procedimentos da AIH (Movimento)

Chave primária: `PRIMARY KEY (PA_OE_GESTOR, PA_SEQ_PRINC, PA_INDX)`

Após encerramento da competência, o conteúdo é movido para TB_HPA.

## TB_HPA — Procedimentos da AIH (Histórico)

Chave primária: `PRIMARY KEY (PA_CMPT, PA_OE_GESTOR, PA_SEQ_PRINC, PA_INDX)`

Layout idêntico ao de TB_PA.

### Notas

- A chave NÃO é o número da AIH mas o sequencial `PA_SEQ_PRINC`, igual ao `AH_SEQ` de TB_AIH/TB_HAIH.
- JOIN entre TB_HAIH e TB_HPA (ou TB_AIH e TB_PA): `AH_OE_GESTOR = PA_OE_GESTOR AND AH_SEQ = PA_SEQ_PRINC AND AH_CMPT = PA_CMPT`
- `PA_INDX` é um sequencial dos procedimentos dentro da AIH.

### Exemplo SQL — Valor Total de uma AIH

```sql
SELECT ah_seq, ah_num_aih, SUM(pa_valor)
FROM tb_haih, tb_hpa
WHERE ah_seq = :seq
  AND ah_cmpt = :cmpt
  AND ah_oe_gestor = pa_oe_gestor
  AND ah_seq = pa_seq_princ
  AND ah_cmpt = pa_cmpt
  AND ah_situacao = '0'
GROUP BY ah_seq, ah_num_aih
```

### Estrutura de Campos (TB_PA / TB_HPA)

| **NOME DO CAMPO** | **TIPO** | **TAM** | **NULL** | **DESCRIÇÃO** | **DOMÍNIOS** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| PA_CMPT | CHAR | 6 | NOT NULL | Competência (AAAAMM) | |
| PA_CMPT_UTI | CHAR | 6 | NULL | Competência na UTI (AAAAMM) | |
| PA_CNES | CHAR | 7 | NULL | Código CNES do estabelecimento | |
| PA_COMPLEXIDADE | CHAR | 2 | NULL | Complexidade do procedimento | TB_C_D tabela 28 |
| PA_CONTRATO | CHAR | 4 | NULL | Contrato de gestão e metas | |
| PA_CREDITO_DOC | CHAR | 15 | NULL | Documento de quem recebe o crédito | |
| PA_CREDITO_IDENT | CHAR | 1 | NULL | Tipo documento do creditado | |
| PA_EXEC_DOC | CHAR | 15 | NULL | Documento do executante | |
| PA_EXEC_IDENT | CHAR | 1 | NULL | Tipo documento do executante | |
| PA_FINANCIAMENTO | CHAR | 2 | NULL | Tipo de financiamento | TB_C_D tabela 4 |
| PA_FO | CHAR | 2 | NULL | Forma de organização | TB_C_D tabela 3 |
| PA_GRUPO | CHAR | 2 | NULL | Grupo do procedimento | TB_C_D tabela 1 |
| PA_IND_ORIGEM | CHAR | 1 | NULL | Indicador de origem | 1=importa AIH; 2=sistema |
| PA_IND_PRESTADOR | CHAR | 1 | NULL | Tipo de prestador | 1=Próprio; 2=Terceiro |
| PA_IND_RATEIO | CHAR | 1 | NULL | Indicador de rateio | 1=direto; 2=rateio |
| PA_IND_TIPO_VALOR | CHAR | 1 | NULL | Tipo de valor | 1=SH; 2=SP; 3=SH federal; 4=SP federal; 5=SH local; 6=SP local |
| PA_INDX | INTEGER | 4 | NOT NULL | Sequencial do procedimento na AIH | |
| PA_NUM_AIH | CHAR | 13 | NULL | Número da AIH | |
| PA_OE_GESTOR | CHAR | 10 | NOT NULL | Código órgão emissor do gestor | |
| PA_OE_REGIONAL | CHAR | 10 | NULL | Código órgão emissor regional | |
| PA_PF_CBO | CHAR | 6 | NULL | CBO do profissional | |
| PA_PF_DOC | CHAR | 15 | NULL | Documento pessoa física | |
| PA_PF_EQUIPE | CHAR | - | NULL | Papel do profissional na equipe | |
| PA_PF_IDENT | CHAR | 1 | NULL | Tipo identificação pessoa física | 1=CPF; 2=CNS |
| PA_PJ_DOC | CHAR | 15 | NULL | Documento pessoa jurídica | |
| PA_PJ_IDENT | CHAR | 1 | NULL | Tipo identificação PJ | 3=CNPJ; 5=CNES |
| PA_PONTO_QTD | INTEGER | 4 | NULL | Quantidade de pontos realizados | |
| PA_PROCEDIMENTO | CHAR | 10 | NULL | Código do procedimento | TU_PROCEDIMENTO |
| PA_PROCEDIMENTO_QTD | INTEGER | 4 | NULL | Quantidade de procedimentos | |
| PA_SEQ_PRINC | INTEGER | 4 | NOT NULL | Sequencial da AIH principal | |
| PA_SUBGRUPO | CHAR | 2 | NULL | Subgrupo do procedimento | TB_C_D tabela 2 |
| PA_TIPO_FAEC | CHAR | 5 | NULL | Tipo de FAEC | TB_C_D tabela 13 |
| PA_VALOR | DOUBLE PRECISION | 8 | NULL | Valor da parcela (R$) | |

---

## Tabelas de Domínio

### TB_C_D — Tabelas Codificadas

Tabelas de domínio referenciadas nos campos acima:

| **TABELA** | **DESCRIÇÃO** |
| :--- | :--- |
| 1 | Grupo do procedimento |
| 2 | Subgrupo do procedimento |
| 3 | Forma de organização |
| 4 | Tipo de financiamento (União/Estado/Município) |
| 7 | Especialidade do leito |
| 13 | Tipo de FAEC |
| 21 | Tipo de logradouro |
| 23 | Motivo da alta |
| 24 | Motivo do bloqueio |
| 25 | Complexidade da internação |
| 26 | Raça/cor |
| 28 | Caráter da internação / complexidade do procedimento |
| 29 | Nacionalidade |
| 30 | Solicitação de liberação |

### TB_MUN — Municípios

Código IBGE de 6 dígitos.

### TU_CID — CID-10

Tabela de diagnósticos (Classificação Internacional de Doenças).

### TU_PROCEDIMENTO — Procedimentos SUS

Tabela unificada de procedimentos do SUS.

---

## Informações de Conexão

| **Parâmetro** | **Valor** |
| :--- | :--- |
| DB_PATH | C:\Datasus\SIHD2\BDSIHD2.GDB |
| Firebird | 1.5 (32-bit) |
| ODS | 10.1 |
| Dialeto SQL | 1 |
| Charset | WIN1252 |
| Servidor | localhost:3050 |
| Binários | C:\Program Files (x86)\Firebird\Firebird_1_5\bin |

### Problema Conhecido: Conflito de Role SYSDBA

O banco possui um SQL ROLE chamado "SYSDBA" que impede login do usuário SYSDBA.
Usuários alternativos (FCES, SIHD_READ) não possuem permissão em tabelas de sistema.
As tabelas de dados (TB_AIH, etc.) são criadas pelo aplicativo SIHD2.exe na primeira importação de AIHs.

### Queries Úteis para o Adapter

```sql
-- AIHs aprovadas de um estabelecimento
SELECT *
FROM TB_HAIH
WHERE AH_CNES = :cnes
  AND AH_CMPT = :competencia
  AND AH_SITUACAO = '0'
ORDER BY AH_NUM_AIH

-- Valor total por AIH
SELECT ah_num_aih, SUM(pa_valor) AS valor_total
FROM TB_HAIH h
JOIN TB_HPA p ON h.AH_OE_GESTOR = p.PA_OE_GESTOR
                AND h.AH_SEQ = p.PA_SEQ_PRINC
                AND h.AH_CMPT = p.PA_CMPT
WHERE h.AH_CMPT = :competencia
  AND h.AH_SITUACAO = '0'
GROUP BY ah_num_aih

-- AIHs rejeitadas
SELECT AH_NUM_AIH, AH_SITUACAO, AH_DIAG_PRI, AH_PROC_REALIZADO
FROM TB_HAIH
WHERE AH_CMPT = :competencia
  AND AH_SITUACAO = '1'
```

---

## §Gold v2 Mapping

| Origem SIHD | → | Gold v2 | Observação |
|---|---|---|---|
| `TB_HAIH` (AIH processadas) | → | `fato_internacao` | AH_OE_GESTOR+AH_SEQ → num_aih CHAR(13); valores em centavos |
| `TB_HPA` (procedimentos AIH histórico) | → | `fato_procedimento_aih` | referência sk_aih via num_aih lookup |
| `TU_PROCEDIMENTO` | → | `dim_procedimento_sus` | sync periódico SIGTAP |
| `TU_CID` | → | `dim_cid10` | sync |
| `TB_MUN` | → | reconciliação com `dim_municipio` | COD_MUN → ibge6 |
| `TB_AIH` + `TB_PA` (em processamento) | → | **NÃO mapear para gold** | só histórico (post-close); evita fatos mutáveis |

Ingestão: `dump_agent_go` já tem `sihd_producao` intent. Mapper `data_processor` 
a criar (fora de escopo Spec B).
