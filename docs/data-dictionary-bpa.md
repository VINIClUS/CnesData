# Dicionário de Dados — BPA-Mag (BPAMAG.GDB)

> **Status:** Preliminar — derivado de PDFs Datasus. Introspecção completa do
> Firebird GDB está deferida (requer `fbclient.dll` 1.5.5, a ser extraído via
> `Firebird-1.5.5.4926-3-Win32.exe` disponível em `http://sia.datasus.gov.br`).

Fontes consultadas:

- `E:/BPA/Layout_Exportacao_BPA.pdf` — layout dos arquivos de remessa TXT gerados
  pelo BPA-Mag (cabeçalho + BPA-C + BPA-I).
- `E:/BPA/Manual_Operacional_BPA.pdf` — manual operacional do BPA-Mag v1
  (CGSI/DRAC/SAS/MS, set/2012).

## Contexto operacional (Manual_Operacional_BPA.pdf)

O **BPA-Mag** é o aplicativo de captação desktop (Windows, Firebird embarcado)
utilizado pelos prestadores do SUS para registrar produção ambulatorial **sem
exigência de autorização prévia** (procedimentos de atenção básica e média
complexidade). Instituído pela Portaria GM/MS nº 545/1993, ampliado pela
Portaria SAS/MS nº 709/2007 com a modalidade individualizada.

- **Portaria GM/MS nº 545/1993** — institui o BPA substituindo a GAP (Guia
  de Autorização Ambulatorial).
- **Portaria SAS/MS nº 709/2007** — cria o BPA-I (individualizado), somando-se
  à APAC e preservando o BPA-C para registro agregado.
- **SIGTAP define o instrumento de registro** (§8): cada procedimento no SIGTAP
  tem o atributo `Instrumento de Registro` com domínio {BPA Consolidado, BPA
  Individualizado, APAC, RAAS}. O BPA-Mag só aceita procedimentos marcados como
  BPA-C ou BPA-I.
- **Atributos complementares relevantes** (extraídos do §8):
  - `009 - Exige CNS` — torna CNS do paciente obrigatório em BPA-I.
  - `012 - Exige idade no BPA (Consolidado)` — idade obrigatória em BPA-C.
  - `021 - Não Exige CBO` — dispensa CBO do profissional.
- **Arquivos de apoio importados mensalmente:**
  - KIT SIA / BDSIA (`BDSIAaaaammv.exe`) — tabelas SIGTAP da competência.
  - Equipes CAPTAÇÃO (do CNES) — mapeamento de equipes de atenção básica.

## Layout de Exportação (Layout_Exportacao_BPA.pdf)

O arquivo de remessa exportado pelo BPA-Mag é texto plano, posicional (layout
fixo), ASCII, com terminador `CR+LF` por linha. Estrutura:

```
[CABEÇALHO — 132 bytes — 1 linha]
[LINHAS BPA-C — 50 bytes cada]
[LINHAS BPA-I — 352 bytes cada]
```

### CABEÇALHO (`cbc-*`) — 132 bytes

| Seq | Campo | Ini | Fim | Tam | Tipo | Descrição |
|---|---|---|---|---|---|---|
| 1 | `cbc-hdr` | 001 | 002 | 2 | NUM | Indicador de linha (fixo `01`) |
| 2 | `Cbc-hdr` | 003 | 007 | 5 | ALFA | Delimitador `#BPA#` |
| 3 | `cbc-mvm` | 008 | 013 | 6 | NUM | Competência AAAAMM |
| 4 | `cbc-lin` | 014 | 019 | 6 | NUM | Total de linhas BPA gravadas |
| 5 | `cbc-flh` | 020 | 025 | 6 | NUM | Total de folhas BPA gravadas |
| 6 | `cbc-smt-vrf` | 026 | 029 | 4 | NUM | Campo de controle (domínio 1111..2221, cálculo no final do PDF) |
| 7 | `cbc-rsp` | 030 | 059 | 30 | ALFA | Nome do órgão de origem |
| 8 | `cbc-sgl` | 060 | 065 | 6 | ALFA | Sigla do órgão de origem |
| 9 | `cbc-cgccpf` | 066 | 079 | 14 | NUM | CGC/CPF do prestador |
| 10 | `cbc-dst` | 080 | 119 | 40 | ALFA | Nome do órgão destino |
| 11 | `cbc-dst-in` | 120 | 120 | 1 | ALFA | Indicador destino: `E` Estadual / `M` Municipal |
| 12 | `cbc_versao` | 121 | 130 | 10 | ALFA | Versão do sistema (livre) |
| 13 | `cbc-fim` | 131 | 132 | 2 | ALFA | `CR+LF` |

### BPA-C — Boletim Consolidado (`prd-*`, ident=`02`) — 50 bytes

| Seq | Campo | Ini | Fim | Tam | Tipo | Descrição |
|---|---|---|---|---|---|---|
| 1 | `prd-ident` | 001 | 002 | 2 | NUM | Fixo `02` |
| 2 | `prd-cnes` | 003 | 009 | 7 | NUM | CNES + DV |
| 3 | `prd-cmp` | 010 | 015 | 6 | NUM | Competência AAAAMM |
| 4 | `Prd_cbo` | 016 | 021 | 6 | ALFA | CBO profissional |
| 5 | `prd-flh` | 022 | 024 | 3 | NUM | Número da folha (001..999) |
| 6 | `prd-seq` | 025 | 026 | 2 | NUM | Sequencial (01..20) |
| 7 | `prd-pa` | 027 | 036 | 10 | NUM | Procedimento SIGTAP + DV |
| 8 | `prd-ldade` | 037 | 039 | 3 | NUM | Idade (0..130) |
| 9 | `prd-qt` | 040 | 045 | 6 | NUM | Quantidade |
| 10 | `prd-org` | 046 | 048 | 3 | ALFA | Origem: `BPA`/`PNI`/`SIE`/`SIB`/`MIN`/`PAC`/`SCL`/`EXT` |
| 11 | `prd-fim` | 049 | 050 | 2 | — | `CR+LF` |

### BPA-I — Boletim Individualizado (`prd-*`, ident=`03`) — 352 bytes

| Seq | Campo | Ini | Fim | Tam | Tipo | Descrição |
|---|---|---|---|---|---|---|
| 1 | `prd-ident` | 001 | 002 | 2 | NUM | Fixo `03` |
| 2 | `prd-cnes` | 003 | 009 | 7 | NUM | CNES + DV |
| 3 | `prd-cmp` | 010 | 015 | 6 | NUM | Competência AAAAMM |
| 4 | `Prd_cnsmed` | 016 | 030 | 15 | NUM | CNS do profissional + DV |
| 5 | `Prd_cbo` | 031 | 036 | 6 | ALFA | CBO profissional |
| 6 | `Prd_dtaten` | 037 | 044 | 8 | NUM | Data atendimento AAAAMMDD |
| 7 | `prd-flh` | 045 | 047 | 3 | NUM | Número da folha (001..999) |
| 8 | `prd-seq` | 048 | 049 | 2 | NUM | Sequencial (01..99) |
| 9 | `prd-pa` | 050 | 059 | 10 | NUM | Procedimento SIGTAP + DV |
| 10 | `Prd-cnspac` | 060 | 074 | 15 | NUM | CNS do paciente (condicional ao procedimento) |
| 11 | `Prd-sexo` | 075 | 075 | 1 | ALFA | `M` ou `F` |
| 12 | `Prd-ibge` | 076 | 081 | 6 | NUM | IBGE município residência |
| 13 | `Prd-cid` | 082 | 085 | 4 | ALFA | CID-10 |
| 14 | `prd-ldade` | 086 | 088 | 3 | NUM | Idade (0..130) |
| 15 | `prd-qt` | 089 | 094 | 6 | NUM | Quantidade |
| 16 | `Prd-caten` | 095 | 096 | 2 | NUM | Caráter de atendimento |
| 17 | `Prd-naut` | 097 | 109 | 13 | NUM | Número de autorização do estabelecimento |
| 18 | `prd-org` | 110 | 112 | 3 | ALFA | Origem (mesmo domínio de BPA-C) |
| 19 | `prd-nmpac` | 113 | 142 | 30 | ALFA | Nome do paciente |
| 20 | `prd-dtnasc` | 143 | 150 | 8 | NUM | Data nascimento AAAAMMDD |
| 21 | `prd-raca` | 151 | 152 | 2 | NUM | Raça/cor: `01` Branca, `02` Preta, `03` Parda, `04` Amarela, `05` Indígena, `99` Sem info |
| 22 | `prd-etnia` | 153 | 156 | 4 | NUM | Etnia (obrigatório se raça=05, a partir de Out/2010) |
| 23 | `prd-nac` | 157 | 159 | 3 | NUM | Nacionalidade |
| 24 | `prd_srv` | 160 | 162 | 3 | NUM | Código do Serviço |
| 25 | `prd_clf` | 163 | 165 | 3 | NUM | Código da Classificação |
| 26 | `prd_equipe_Seq` | 166 | 173 | 8 | NUM | Sequencial da equipe |
| 27 | `prd_equipe_Area` | 174 | 177 | 4 | NUM | Área da equipe |
| 28 | `prd_cnpj` | 178 | 191 | 14 | NUM | CNPJ da empresa de manutenção/adaptação de OPM |
| 29 | `prd_cep_pcnte` | 192 | 199 | 8 | NUM | CEP do paciente |
| 30 | `prd_lograd_pcnte` | 200 | 202 | 3 | NUM | Código do logradouro |
| 31 | `prd_end_pcnte` | 203 | 232 | 30 | ALFA | Endereço |
| 32 | `prd_compl_pcnte` | 233 | 242 | 10 | ALFA | Complemento |
| 33 | `prd_num_pcnte` | 243 | 247 | 5 | ALFA | Número |
| 34 | `prd_bairro_pcnte` | 248 | 277 | 30 | ALFA | Bairro |
| 35 | `prd_ddtel_pcnte` | 278 | 288 | 11 | NUM | Telefone |
| 36 | `prd_email_pcnte` | 289 | 328 | 40 | ALFA | E-mail |
| 37 | `prd_ine` | 329 | 338 | 10 | NUM | INE (identificação nacional de equipes, a partir de Ago/2015) |
| 38 | `prd_cpf_pcnte` | 339 | 349 | 11 | NUM | CPF do indivíduo (condicional) |
| 38 | `prd_situacao_rua` | 350 | 350 | 1 | ALFA | `N`/`S` pessoa em situação de rua (a partir de Dez/2024) |
| 39 | `prd-fim` | 351 | 352 | 2 | — | `CR+LF` |

> Obs.: A sequência `38` aparece duplicada no PDF — `prd_cpf_pcnte` e
> `prd_situacao_rua` são campos distintos, a numeração é inconsistente no
> documento original.

### Regras adicionais (observação final do PDF)

- **Cálculo do `cbc-smt-vrf` (campo de controle):**
  1. Somar `procedimento + quantidade` de todas as linhas.
  2. Obter resto da divisão por 1111.
  3. Somar 1111 ao resto → resultado final.
- **Identificação do paciente (BPA-I):** deve-se usar **apenas um** dos
  documentos, CPF **ou** CNS. Se informado um, o outro deve ficar em branco.

## Fluxos Operacionais (Manual_Operacional_BPA.pdf)

Ordem canônica de operação do BPA-Mag em cada competência:

1. **Instalação/configuração** — Firebird 1.5.5 + BPA-Mag (§2).
2. **Preparação da competência** (§5) — cadastro de responsáveis, definição de
   AAAAMM, importação de:
   - Tabela SUS (SIGTAP) do KIT SIA — `BDSIAaaaammv.exe`;
   - Equipes CAPTAÇÃO (do CNES) — quando houver ações de equipe.
3. **Registro BPA-C** (§6) — inclusão de folhas consolidadas por
   CNES/Mês/Ano/CBO/Idade/Quantidade.
4. **Registro BPA-I** (§7) — folhas individualizadas por CNES + CNS
   Profissional, com múltiplos atendimentos (CNS paciente, CID, sexo, idade,
   quantidade, serviço/classificação, caráter de atendimento, número de
   autorização opcional).
5. **Exportação** (§9) — geração do TXT de remessa conforme layout acima,
   encaminhado ao gestor estadual/municipal para ingestão no SIA.

## Integração com Gold v2

Mapeamento alvo (multi-tenant, pilot Presidente Epitácio):

- **BPA-C (consolidado)** → `fato_producao_ambulatorial` com
  `fonte_sistema = 'SIA_BPA_C'`. Sem `sk_paciente` (agregado). Chave natural
  operacional: `(sk_estabelecimento, sk_competencia, sk_cbo, idade, sk_procedimento)`.
- **BPA-I (individualizado)** → `fato_producao_ambulatorial` com
  `fonte_sistema = 'SIA_BPA_I'`. Inclui `sk_paciente`, `dt_atendimento`,
  `cid10`, `caracter_atendimento`, `num_autorizacao`.
- **Dedup com SIA oficial (PA/AR/AB):** quando o município receber os `.dbc`
  processados pelo SIA Nacional via DATASUS após a remessa, aplicar chave:
  `(sk_estab, sk_prof, sk_procedimento, sk_competencia, dt_atendimento,
  sk_paciente)` para casar registros BPA-I locais ↔ SIA centralizado. Usar
  `fontes` JSONB (padrão do repo) para merge idempotente.
- **Origem do registro** (`prd-org`) → coluna categórica
  `origem_captacao` com domínio `{BPA, PNI, SIE, SIB, MIN, PAC, SCL, EXT}`.
- **Raça/cor, etnia, nacionalidade** → já presentes em `dim_pessoa` v2;
  preencher a partir dos campos `prd-raca`, `prd-etnia`, `prd-nac`.
- **Equipe (INE + área + sequencial)** → `dim_equipe_sus` (nova dimensão
  candidata) ou colunas desnormalizadas em `fato_producao_ambulatorial`.
- **Endereço do paciente** → `dim_endereco` (CEP, logradouro, bairro,
  telefone, e-mail) SCD2 para rastreio histórico.
- **Campo de controle `cbc-smt-vrf`** → validação de integridade pós-ingestão
  (recomputar e comparar); não persistir em Gold.

## TODO: Introspecção completa do GDB

Rodar quando `fbclient.dll` estiver disponível:

```bash
python scripts/introspect_bpa_gdb.py \
  --gdb "E:/BPA/BPAMAG.GDB" \
  --dll "<path>/fbclient.dll" \
  --output docs/data-dictionary-bpa.md
```

Saídas esperadas da introspecção:

- Lista completa de tabelas internas do BPA-Mag (cabeçalho, folhas, itens,
  domínios, usuários, config).
- Mapeamento campo ↔ atributo do layout TXT (muitas colunas GDB têm nomes
  diferentes dos códigos do layout de export).
- Constraints, domínios, triggers, stored procedures.
- Contagens de linhas por tabela para dimensionamento de ingestão.

## Arquivos de extração de PDF (gerados por `scripts/parse_datasus_pdfs.py`)

Temporários já removidos após consolidação. Reexecutar se necessário:

```bash
.venv/Scripts/python.exe scripts/parse_datasus_pdfs.py \
  --pdf "E:/BPA/Layout_Exportacao_BPA.pdf" \
  --output docs/_tmp_bpa_layout.md

.venv/Scripts/python.exe scripts/parse_datasus_pdfs.py \
  --pdf "E:/BPA/Manual_Operacional_BPA.pdf" \
  --output docs/_tmp_bpa_manual.md
```

## Local test fixture

BPAMAG.GDB is Firebird 1.5 format. To open it locally or in CI tests
without installing Firebird server, use the bundled embedded client:

```bash
git lfs pull
python scripts/fb156_setup.py
```

Output: `.cache/firebird-1.5.6/fbclient.dll` (plus supporting runtimes).

**Architecture constraint**: FB 1.5.6 client is x86-only. Consumers must
run as 32-bit process (Go `GOARCH=386`, Python `py -3.13-32`, or sidecar
bridge). See `docs/fixtures/firebird/README.md` for details.
