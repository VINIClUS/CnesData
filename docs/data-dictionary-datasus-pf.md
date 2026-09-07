# Dicionário do DATASUS CNES PF

Status: schema de origem e projeção raw ratificados por CND-029.

Fonte exclusiva:
`CNES/200508_/Dados/PF/PF{UF}{YYMM}.dbc`. A conversão DBC produz DBF em Latin-1. A ordem,
tipo e largura abaixo são obrigatórios. `Sim` marca os 12 campos lidos para filtro, validação ou
projeção; os demais 28 campos são validados no layout, mas não são projetados.

## Layout PF de 40 campos

| # | Campo | Tipo DBF | Tam. | Consumido | Uso |
|---:|---|---|---:|---|---|
| 1 | `CNES` | C | 7 | Sim | Código do estabelecimento |
| 2 | `CODUFMUN` | C | 6 | Sim | Município do estabelecimento |
| 3 | `REGSAUDE` | C | 4 | Não | Região de saúde |
| 4 | `MICR_REG` | C | 6 | Não | Microrregião |
| 5 | `DISTRSAN` | C | 4 | Não | Distrito sanitário |
| 6 | `DISTRADM` | C | 4 | Não | Distrito administrativo |
| 7 | `TPGESTAO` | C | 1 | Não | Tipo de gestão |
| 8 | `PF_PJ` | C | 1 | Não | Indicador pessoa física/jurídica |
| 9 | `CPF_CNPJ` | C | 14 | Não | CPF/CNPJ do estabelecimento |
| 10 | `NIV_DEP` | C | 1 | Não | Nível de dependência |
| 11 | `CNPJ_MAN` | C | 14 | Não | CNPJ da mantenedora |
| 12 | `ESFERA_A` | C | 2 | Não | Esfera administrativa |
| 13 | `ATIVIDAD` | C | 2 | Não | Atividade de ensino |
| 14 | `RETENCAO` | C | 2 | Não | Retenção de tributos |
| 15 | `NATUREZA` | C | 2 | Não | Natureza da organização |
| 16 | `CLIENTEL` | C | 2 | Não | Fluxo de clientela |
| 17 | `TP_UNID` | C | 2 | Não | Tipo de unidade |
| 18 | `TURNO_AT` | C | 2 | Não | Turno de atendimento |
| 19 | `NIV_HIER` | C | 2 | Não | Nível de hierarquia |
| 20 | `TERCEIRO` | C | 1 | Não | Indicador de terceiro |
| 21 | `CPF_PROF` | C | 11 | Sim | CPF do profissional |
| 22 | `CPFUNICO` | C | 1 | Não | Indicador de CPF único |
| 23 | `CBO` | C | 6 | Sim | Ocupação CBO 2002 |
| 24 | `CBOUNICO` | C | 6 | Não | CBO único |
| 25 | `NOMEPROF` | C | 60 | Sim | Nome do profissional |
| 26 | `CNS_PROF` | C | 15 | Sim | CNS do profissional |
| 27 | `CONSELHO` | C | 2 | Não | Conselho profissional |
| 28 | `REGISTRO` | C | 13 | Não | Registro no conselho |
| 29 | `VINCULAC` | C | 6 | Sim | Tipo de vínculo |
| 30 | `VINCUL_C` | C | 1 | Não | Componente contratual do vínculo |
| 31 | `VINCUL_A` | C | 1 | Não | Componente autônomo do vínculo |
| 32 | `VINCUL_N` | C | 1 | Não | Componente não empregatício |
| 33 | `PROF_SUS` | C | 1 | Sim | Atendimento ao SUS |
| 34 | `PROFNSUS` | C | 1 | Não | Atendimento não SUS |
| 35 | `HORAOUTR` | N | 3 | Sim | Horas em outros atendimentos |
| 36 | `HORAHOSP` | N | 3 | Sim | Horas hospitalares |
| 37 | `HORA_AMB` | N | 3 | Sim | Horas ambulatoriais |
| 38 | `COMPETEN` | C | 6 | Sim | Competência `YYYYMM` |
| 39 | `UFMUNRES` | C | 6 | Não | Município de residência |
| 40 | `NAT_JUR` | C | 4 | Não | Natureza jurídica |

`C` é caractere e `N` é numérico com zero casas decimais. Divergência de nome, ordem, tipo,
largura, escala ou quantidade é falha final de schema.

## Filtro obrigatório

Somente linhas que satisfaçam simultaneamente estas condições são emitidas:

- `CODUFMUN` igual ao IBGE6 do tenant;
- `COMPETEN` igual à competência solicitada convertida de `YYYY-MM` para `YYYYMM`.

`UFMUNRES` não substitui `CODUFMUN`. Ausência de linhas da competência e do município
solicitados é `source_not_published`, retryable, sem buscar outro mês.

## Projeção raw de 14 colunas

A saída usa exatamente esta ordem e estes tipos:

| # | Saída | Polars | Origem/regra |
|---:|---|---|---|
| 1 | `CPF` | `String` | `CPF_PROF` |
| 2 | `CNS` | `String` | `CNS_PROF` |
| 3 | `NOME_PROFISSIONAL` | `String` | `NOMEPROF` |
| 4 | `NOME_SOCIAL` | `String` | `null` tipado |
| 5 | `SEXO` | `String` | `null` tipado |
| 6 | `CBO` | `String` | `CBO`, seis dígitos |
| 7 | `CNES` | `String` | `CNES`, sete dígitos |
| 8 | `TIPO_VINCULO` | `String` | `VINCULAC`, seis dígitos |
| 9 | `SUS` | `String` | `PROF_SUS`: `1` para `S`, `0` para `N` |
| 10 | `CH_TOTAL` | `Int64` | Soma das três cargas horárias |
| 11 | `CH_AMBULATORIAL` | `Int64` | `HORA_AMB` |
| 12 | `CH_OUTRAS` | `Int64` | `HORAOUTR` |
| 13 | `CH_HOSPITALAR` | `Int64` | `HORAHOSP` |
| 14 | `FONTE` | `String` | Literal `NACIONAL` |

## Regras de conversão

- Espaços de campos `C` são removidos nas extremidades antes da validação.
- `CPF_PROF` vazio ou `99999999999` vira `null` tipado; outro valor deve ter 11 dígitos.
- `CNS_PROF` vazio ou composto por 15 noves vira `null` tipado; outro valor deve ter 15 dígitos.
- `NOMEPROF` é emitido após trim como `NOME_PROFISSIONAL`.
- `NOME_SOCIAL` e `SEXO` são sempre `null` com dtype `String`; não são inferidos.
- `CBO`, `CNES` e `VINCULAC` aceitam somente dígitos e recebem padding à esquerda com zero até,
  respectivamente, seis, sete e seis posições. Valor maior que a largura é falha final.
- `PROF_SUS=1` produz `S` e `PROF_SUS=0` produz `N`. Vazio ou qualquer outro valor é falha final.
- Cada campo de horas vira inteiro não negativo; vazio vira zero. Valor fracionário, negativo ou
  não numérico é falha final. `CH_TOTAL` é a soma após essas conversões.
- `FONTE` é sempre o literal `NACIONAL`.

Não há deduplicação: toda linha PF válida é preservada, inclusive duplicatas.
Depois da projeção, as linhas são ordenadas pelas 14 colunas, na ordem definida acima, em ordem
ascendente e com nulls last em cada coluna. A ordenação não usa índice de leitura, relógio nem
dados externos.
Linhas completamente iguais continuam repetidas.

Essas regras são somente de projeção raw. Não corrigem documento, nome, vínculo, CBO ou carga
horária e não executam reconciliação de negócio.
