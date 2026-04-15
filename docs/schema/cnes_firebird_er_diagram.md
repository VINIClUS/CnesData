# Diagrama ER — CNES Firebird (CNES.GDB)

> Gerado em 2026-04-05 a partir dos resultados de `data/discovery/01_indices_e_constraints.csv`.
> Relacionamentos confirmados via `RDB$RELATION_CONSTRAINTS`. Joins implícitos marcados com linha tracejada.

## Diagrama

```mermaid
erDiagram
    LFCES018 {
        varchar PROF_ID PK
        varchar CPF_PROF
        varchar COD_CNS
        varchar NOME_PROF
        varchar NO_SOCIAL
        text STATUSMOV
        date DATA_NASC
        text SEXO
    }
    LFCES004 {
        varchar UNIDADE_ID PK
        varchar CNES
        varchar CNPJ_MANT
        varchar NOME_FANTA
        varchar TP_UNID_ID FK
        varchar CODMUNGEST FK
        varchar CO_NATUREZ
        text STATUSMOV
    }
    LFCES021 {
        varchar UNIDADE_ID PK
        varchar PROF_ID PK
        varchar COD_CBO PK
        varchar IND_VINC PK
        text TP_SUS_NAO PK
        long CG_HORAAMB
        long CGHORAOUTR
        long CGHORAHOSP
        text STATUS
        text STATUSMOV
    }
    LFCES048 {
        varchar COD_MUN
        varchar CPF_PROF
        varchar COD_CBO
        varchar COD_AREA
        long SEQ_EQUIPE
    }
    LFCES060 {
        varchar COD_MUN
        varchar COD_AREA
        long SEQ_EQUIPE
        varchar TP_EQUIPE FK
        varchar CNES
        varchar INE
        varchar DS_AREA
        varchar CD_SEGMENT
        varchar DS_SEGMENT
    }
    NFCES026 {
        varchar COD_CBO PK
        varchar DESCRICAO
    }
    NFCES058 {
        varchar IND_VINC PK
        varchar CD_VINCULA
        varchar TP_VINCULO
        varchar TP_SUBVINC
        varchar DS_SUBVINC
        varchar DS_CONCEIT
    }
    NFCES005 {
        varchar COD_MUN PK
        varchar NOME_MUN
        varchar UF
        int64 POPULACAO
        varchar NU_LATITUD
        varchar NU_LONGITU
    }
    NFCES010 {
        varchar TP_UNID_ID PK
        varchar DESCRICAO
    }
    NFCES046 {
        varchar TP_EQUIPE PK
        varchar DS_EQUIPE
        varchar CO_GRUPO_E
    }
    LFCES020 {
        varchar UNIDADE_ID PK
        varchar COD_EQUIP PK
        varchar CODTPEQUIP PK
        long QTDE_EXIST
        long QTDE_USO
        text IND_SUS
        text STATUS
    }
    LFCES027 {
        varchar UNIDADE_ID PK
        varchar NOMRHEMOT
        varchar CPFMRHEMOT
        text STATUS
        text STATUSMOV
    }

    LFCES018 ||--o{ LFCES021 : "PROF_ID (FK confirmada)"
    LFCES004 ||--o{ LFCES021 : "UNIDADE_ID (FK confirmada)"
    NFCES026 ||--o{ LFCES021 : "COD_CBO (FK confirmada)"
    NFCES058 ||--o{ LFCES021 : "IND_VINC (FK confirmada)"
    NFCES005 ||--o{ LFCES004 : "CODMUNGEST (FK confirmada)"
    NFCES010 ||--o{ LFCES004 : "TP_UNID_ID (FK confirmada)"
    LFCES004 ||--o{ LFCES020 : "UNIDADE_ID (FK confirmada)"
    LFCES004 ||--o{ LFCES027 : "UNIDADE_ID (FK confirmada)"
    LFCES021 }o..o{ LFCES048 : "CPF_PROF+COD_CBO+COD_MUN (join implícito)"
    LFCES048 }o..o{ LFCES060 : "SEQ_EQUIPE+COD_AREA+COD_MUN (join implícito)"
    NFCES046 ||--o{ LFCES060 : "TP_EQUIPE (lookup)"
```

## Notas

| Símbolo | Significado |
|---|---|
| `PK` | Chave primária |
| `FK` | Chave estrangeira declarada (confirmada via `RDB$RELATION_CONSTRAINTS`) |
| `||--o{` | Um-para-muitos com FK declarada |
| `}o..o{` | Join implícito — sem FK declarada no schema Firebird |

### Tabelas Omitidas

- **NFCES088** — Snapshot desnormalizado (profissional × estabelecimento). Vista somente-leitura; não possui relações FK próprias.
- **LFCES000** — Configuração técnica da instalação local. Sem relação com dados de auditoria.
- Demais 240+ tabelas LFCES/NFCES — fora do escopo do pipeline municipal.

### Joins Implícitos (sem FK declarada)

O Firebird **não** impõe integridade referencial nestas junções:

1. `LFCES021 → LFCES048`: via `CPF_PROF + COD_CBO + COD_MUN`
2. `LFCES048 → LFCES060`: via `SEQ_EQUIPE + COD_AREA + COD_MUN`

Em `cnes_client.py`, estes joins são executados como `LEFT JOIN` para tolerar ausência de correspondência.
