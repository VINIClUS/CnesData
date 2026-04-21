# Dicionario de Dados - SIA (SIASUS DBFs)

> Introspeccao automatica via `scripts/introspect_sia_dbf.py`.
> Source: `E:\siasus`
> DBFs: 105

## DBFs prioritárias

Baseado em tamanho + papel inferido (via Manual_Operacional_SIA + nome):

| DBF | Propósito | Mapeia para Gold v2 |
|---|---|---|
| `S_APA.DBF` | Autorização Procedimentos de Alta Complexidade (APAC) | `fato_producao_ambulatorial` (subtipo `SIA_APA`) |
| `S_BPI.DBF` | BPA Individualizado (procedimentos por paciente) | `fato_producao_ambulatorial` (subtipo `SIA_BPI`) |
| `S_BPIHST.DBF` | BPA-I histórico | `fato_producao_ambulatorial` histórico |
| `S_CDN.DBF` | Cadastro domínio de códigos | `dim_procedimento_sus` sync |
| `CADMUN.DBF` | Cadastro municípios | `dim_municipio` reconciliação |
| `consiste.dbf` | Regras de consistência | **Ignorar** (lógica de negócio) |

Observações da introspecção real:
- `S_APA.DBF` (1381 records, 62 fields): prefixos `APA_*` confirmam APAC individualizada (paciente, procedimento, dt_inicio/fim, CID).
- `S_BPI.DBF` (910 records, 59 fields) e `S_BPIHST.DBF` (1792 records, 59 fields): schema idêntico com prefixo `BPI_*` (CNS paciente, CNS profissional, CBO, procedimento, folha/seq). Confirma BPA-Individualizado; `HST` é histórico de competências anteriores.
- `S_CDN.DBF` (5570 records, 4 fields): schema genérico `CDN_TB/IT/DSCR/CHKSM` — tabela de domínio normalizada (dicionário de códigos). Útil para `dim_procedimento_sus` e demais dimensões que dependem de lookups SIA.
- `CADMUN.DBF` (5715 records, 8 fields): `CODUF/CODMUNIC/NOME/CONDIC/TETOPAB/CALCPAB` — cadastro municípios com tetos PAB. Complementa `dim_municipio` com metadados financeiros se necessário.
- `consiste.dbf` (469 records, 15 fields, prefixo misto `UID/CMP/TIPO/APAC/FOLHA/SEQ`): inconsistências de lote — lógica de auditoria interna do SIA, **ignorar** na Gold.

## Summary

| Arquivo | Records | Size (bytes) |
|---|---|---|
| S_CEP.DBF | 1576563 | 23,648,543 |
| S_FXAPAC.DBF | 1315705 | 52,628,330 |
| S_PACBO.DBF | 1160735 | 32,500,743 |
| S_PACID.DBF | 488085 | 13,178,490 |
| S_PAHA.DBF | 65443 | 2,028,960 |
| S_PADET.DBF | 60864 | 1,521,763 |
| S_PROCED.DBF | 32721 | 5,563,181 |
| S_PASRV.DBF | 24493 | 685,999 |
| S_PAPA.DBF | 24181 | 894,924 |
| S_VPA.DBF | 20387 | 1,977,926 |
| S_PAREGR.DBF | 19833 | 515,821 |
| S_PA.DBF | 19133 | 3,158,067 |
| S_PAP.DBF | 18097 | 3,602,667 |
| S_CID.DBF | 15611 | 1,280,521 |
| S_UPSPRF.DBF | 12870 | 1,944,044 |
| S_TPCRD.DBF | 5919 | 1,078,093 |
| CADMUN.DBF | 5715 | 457,490 |
| S_DEPARA.DBF | 5576 | 184,170 |
| S_CDN.DBF | 5570 | 317,652 |
| S_FXRAAS.DBF | 4639 | 185,690 |
| S_PRD.DBF | 3391 | 1,175,017 |
| S_PRDHST.DBF | 2596 | 70,254 |
| S_COMPFEDERAL.DBF | 2570 | 2,218,936 |
| S_BPIHST.DBF | 1792 | 770,691 |
| S_IPU.DBF | 1670 | 334,675 |
| S_APA.DBF | 1381 | 1,076,589 |
| S_EMU.DBF | 1197 | 49,367 |
| S_SRV.DBF | 1152 | 31,330 |
| S_BPI.DBF | 910 | 392,313 |
| S_PAIN.DBF | 858 | 31,083 |
| S_EQUIPE.DBF | 592 | 78,034 |
| S_UPSGES.DBF | 541 | 9,359 |
| consiste.dbf | 469 | 133,242 |
| S_CNSQTD.DBF | 452 | 64,310 |
| S_TUEMA.DBF | 99 | 15,245 |
| S_RUB.DBF | 42 | 2,104 |
| S_CFA.DBF | 38 | 1,568 |
| S_FFI.DBF | 30 | 1,604 |
| S_UPS.DBF | 27 | 10,838 |
| s_reprd.dbf | 27 | 2,537 |
| S_PRFIRR.DBF | 22 | 1,096 |
| S_RELAT.DBF | 22 | 73,600 |
| S_ENCERR.DBF | 18 | 4,333 |
| S_CGCEX.DBF | 8 | 274 |
| S_UPSHA.DBF | 8 | 474 |
| tot_cons.dbf | 8 | 947 |
| CONFIMP.DBF | 5 | 5,830 |
| S_TDIR.DBF | 5 | 414 |
| S_UPSRC.DBF | 4 | 258 |
| S_CTR.DBF | 1 | 314 |
| S_MNT.DBF | 1 | 1,006 |
| S_SS.DBF | 1 | 770 |
| s_cor.dbf | 1 | 886 |
| ADESAO.DBF | 0 | 163 |
| APCNES_TERC.DBF | 0 | 323 |
| S_CD.DBF | 0 | 162 |
| S_CDX.DBF | 0 | 162 |
| S_CIDM.DBF | 0 | 98 |
| S_CRD.DBF | 0 | 643 |
| S_DPG.DBF | 0 | 419 |
| S_FORNEC.DBF | 0 | 386 |
| S_MN.DBF | 0 | 130 |
| S_OCORR.DBF | 0 | 515 |
| S_RAPA.DBF | 0 | 1,155 |
| S_RAS.DBF | 0 | 1,603 |
| S_TRMOTC.DBF | 0 | 321 |
| S_UPSAUT.DBF | 0 | 162 |
| advertencia_sexo.dbf | 0 | 483 |
| cadgesmn.dbf | 0 | 195 |
| cpx.dbf | 0 | 962 |
| crit_lmc.dbf | 0 | 547 |
| crit_trastuzumabe.dbf | 0 | 419 |
| erro.dbf | 0 | 386 |
| faixa.dbf | 0 | 162 |
| pa.dbf | 0 | 962 |
| psico.dbf | 0 | 1,025 |
| s_ad.dbf | 0 | 1,251 |
| s_adpa.dbf | 0 | 707 |
| s_apal.dbf | 0 | 4,259 |
| s_cnesia.dbf | 0 | 130 |
| s_corpo.dbf | 0 | 1,505 |
| s_eqesf.dbf | 0 | 257 |
| s_fcd.dbf | 0 | 2,050 |
| s_fco.dbf | 0 | 2,434 |
| s_imperr.dbf | 0 | 195 |
| s_ipul.dbf | 0 | 449 |
| s_iput.dbf | 0 | 227 |
| s_prdl.dbf | 0 | 1,251 |
| s_prdlc.dbf | 0 | 355 |
| s_prdli.dbf | 0 | 1,283 |
| s_prdlt.dbf | 0 | 195 |
| s_prdrep.dbf | 0 | 1,635 |
| s_proc.dbf | 0 | 449 |
| s_rapal.dbf | 0 | 675 |
| s_sce.dbf | 0 | 162 |
| s_trab.dbf | 0 | 1,443 |
| s_upsval.dbf | 0 | 67 |
| s_varia.dbf | 0 | 161 |
| s_vpal.dbf | 0 | 385 |
| temu.dbf | 0 | 193 |
| tsrv.dbf | 0 | 161 |
| ttvpa.dbf | 0 | 385 |
| vepe.dbf | 0 | 257 |
| versaomn.dbf | 0 | 257 |
| vig.dbf | 0 | 993 |

## Detalhes por DBF

### `S_CEP.DBF`

- 1576563 records
- Size: 23,648,543 bytes
- Encoding: cp1252
- Fields: 2

| Field | Type | Length | Decimal |
|---|---|---|---|
| CEP | C(8) | 8 | 0 |
| MUNICIPIO | C(6) | 6 | 0 |

### `S_FXAPAC.DBF`

- 1315705 records
- Size: 52,628,330 bytes
- Encoding: cp1252
- Fields: 3

| Field | Type | Length | Decimal |
|---|---|---|---|
| FX_INI | C(12) | 12 | 0 |
| FX_FIM | C(12) | 12 | 0 |
| FX_SKSUM | C(15) | 15 | 0 |

### `S_PACBO.DBF`

- 1160735 records
- Size: 32,500,743 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| PACBO_CMP | C(6) | 6 | 0 |
| PACBO_PA | C(9) | 9 | 0 |
| PACBO_CBO | C(6) | 6 | 0 |
| PACBOCHKSM | C(6) | 6 | 0 |

### `S_PACID.DBF`

- 488085 records
- Size: 13,178,490 bytes
- Encoding: cp1252
- Fields: 5

| Field | Type | Length | Decimal |
|---|---|---|---|
| PACID_CMP | C(6) | 6 | 0 |
| PACID_PA | C(9) | 9 | 0 |
| PACID_CID | C(4) | 4 | 0 |
| PACID_PRIN | C(1) | 1 | 0 |
| PACIDCHKSM | C(6) | 6 | 0 |

### `S_PAHA.DBF`

- 65443 records
- Size: 2,028,960 bytes
- Encoding: cp1252
- Fields: 6

| Field | Type | Length | Decimal |
|---|---|---|---|
| PAHA_CMP | C(6) | 6 | 0 |
| PAHA_PA | C(9) | 9 | 0 |
| PAHA_GRP | C(4) | 4 | 0 |
| PAHA_HA | C(4) | 4 | 0 |
| N | C(1) | 1 | 0 |
| PAHA_CHKSM | C(6) | 6 | 0 |

### `S_PADET.DBF`

- 60864 records
- Size: 1,521,763 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| DET_CMP | C(6) | 6 | 0 |
| DET_PA | C(9) | 9 | 0 |
| DET_COD | C(3) | 3 | 0 |
| DET_CHKSM | C(6) | 6 | 0 |

### `S_PROCED.DBF`

- 32721 records
- Size: 5,563,181 bytes
- Encoding: cp1252
- Fields: 18

| Field | Type | Length | Decimal |
|---|---|---|---|
| PA_CMP | C(6) | 6 | 0 |
| PA_ID | C(9) | 9 | 0 |
| PA_DV | C(1) | 1 | 0 |
| PA_DC | C(60) | 60 | 0 |
| PA_RUB | C(4) | 4 | 0 |
| PA_CPX | C(4) | 4 | 0 |
| PA_CTF | C(4) | 4 | 0 |
| PA_IDADEMX | N(3) | 3 | 0 |
| PA_IDADEMN | N(3) | 3 | 0 |
| PA_SEXO | C(1) | 1 | 0 |
| PA_QTDMAX | N(6) | 6 | 0 |
| PA_ELETIVA | C(1) | 1 | 0 |
| PA_SH | N(12,2) | 12 | 2 |
| PA_SP | N(12,2) | 12 | 2 |
| PA_SA | N(12,2) | 12 | 2 |
| PA_TPREG | C(10) | 10 | 0 |
| PA_SKSUM | C(15) | 15 | 0 |
| PA_CHKSM | C(6) | 6 | 0 |

### `S_PASRV.DBF`

- 24493 records
- Size: 685,999 bytes
- Encoding: cp1252
- Fields: 5

| Field | Type | Length | Decimal |
|---|---|---|---|
| PASRV_CMP | C(6) | 6 | 0 |
| PASRV_PA | C(9) | 9 | 0 |
| PASRV_SRV | C(3) | 3 | 0 |
| PASRV_CSF | C(3) | 3 | 0 |
| PASRVCHKSM | C(6) | 6 | 0 |

### `S_PAPA.DBF`

- 24181 records
- Size: 894,924 bytes
- Encoding: cp1252
- Fields: 6

| Field | Type | Length | Decimal |
|---|---|---|---|
| PAPA_CMP | C(6) | 6 | 0 |
| PAPA_TRAT | C(2) | 2 | 0 |
| PAPA_PRINC | C(9) | 9 | 0 |
| PAPA_SECUN | C(9) | 9 | 0 |
| PAPA_QTMAX | C(4) | 4 | 0 |
| PAPA_CHKSM | C(6) | 6 | 0 |

### `S_VPA.DBF`

- 20387 records
- Size: 1,977,926 bytes
- Encoding: cp1252
- Fields: 11

| Field | Type | Length | Decimal |
|---|---|---|---|
| VPA_PA | C(9) | 9 | 0 |
| VPA_CMP | C(6) | 6 | 0 |
| VPA_SP | N(15,2) | 15 | 2 |
| VPA_SA | N(15,2) | 15 | 2 |
| VPA_SH | N(15,2) | 15 | 2 |
| VPA_TOTAL | N(15,2) | 15 | 2 |
| VPA_MUN | C(6) | 6 | 0 |
| VPA_TIPO | C(1) | 1 | 0 |
| VPA_CTF | C(2) | 2 | 0 |
| VPA_RUB | C(6) | 6 | 0 |
| VPA_MVM | C(6) | 6 | 0 |

### `S_PAREGR.DBF`

- 19833 records
- Size: 515,821 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| REG_CMP | C(6) | 6 | 0 |
| REG_PA | C(9) | 9 | 0 |
| REG_REGRA | C(4) | 4 | 0 |
| REG_CHKSM | C(6) | 6 | 0 |

### `S_PA.DBF`

- 19133 records
- Size: 3,158,067 bytes
- Encoding: cp1252
- Fields: 34

| Field | Type | Length | Decimal |
|---|---|---|---|
| PA_CMP | C(6) | 6 | 0 |
| PA_ID | C(9) | 9 | 0 |
| PA_DV | C(1) | 1 | 0 |
| PA_PAB | C(1) | 1 | 0 |
| PA_TOTAL | N(12,2) | 12 | 2 |
| PA_FAEC | C(1) | 1 | 0 |
| PA_DC | C(60) | 60 | 0 |
| PA_RUB | C(4) | 4 | 0 |
| PA_TPCC | C(1) | 1 | 0 |
| PA_AUX | C(20) | 20 | 0 |
| PA_CPX | C(4) | 4 | 0 |
| PA_CTF | C(4) | 4 | 0 |
| PA_DOC | C(1) | 1 | 0 |
| PA_IDADEMX | N(3) | 3 | 0 |
| PA_IDADEMN | N(3) | 3 | 0 |
| PA_SEXO | C(1) | 1 | 0 |
| PA_QTDMAX | N(6) | 6 | 0 |
| PA_LAUDO | C(2) | 2 | 0 |
| PA_PRINC | C(1) | 1 | 0 |
| PA_SECUN | C(1) | 1 | 0 |
| PA_IDEBPA | C(1) | 1 | 0 |
| PA_CNSPCN | C(1) | 1 | 0 |
| PA_CNRAC | C(1) | 1 | 0 |
| PA_CCMANAL | C(2) | 2 | 0 |
| PA_ELETIVA | C(1) | 1 | 0 |
| PA_APACONT | C(1) | 1 | 0 |
| PA_EXIGCBO | C(1) | 1 | 0 |
| PA_PROCCEO | C(1) | 1 | 0 |
| PA_6MESES | C(1) | 1 | 0 |
| PA_EXIGAUT | C(1) | 1 | 0 |
| PA_PERMAN | N(4) | 4 | 0 |
| PA_EXIGCAS | C(1) | 1 | 0 |
| PA_SECOBRI | C(1) | 1 | 0 |
| PA_CHKSM | C(6) | 6 | 0 |

### `S_PAP.DBF`

- 18097 records
- Size: 3,602,667 bytes
- Encoding: cp1252
- Fields: 31

| Field | Type | Length | Decimal |
|---|---|---|---|
| PAP_UID | C(7) | 7 | 0 |
| PAP_CMP | C(6) | 6 | 0 |
| PAP_NUM | C(13) | 13 | 0 |
| PAP_PA | C(10) | 10 | 0 |
| PAP_SEQ | C(2) | 2 | 0 |
| PAP_CBO | C(6) | 6 | 0 |
| PAP_IDADE | N(3) | 3 | 0 |
| PAP_QT_P | N(6) | 6 | 0 |
| PAP_QT_A | N(6) | 6 | 0 |
| PAP_MVM | C(6) | 6 | 0 |
| PAP_ORG | C(3) | 3 | 0 |
| PAP_FLPA | C(1) | 1 | 0 |
| PAP_FLEMA | C(1) | 1 | 0 |
| PAP_FLCBO | C(1) | 1 | 0 |
| PAP_FLQT | C(1) | 1 | 0 |
| PAP_FLER | C(1) | 1 | 0 |
| PAP_CNPJ | C(14) | 14 | 0 |
| PAP_NFISC | C(6) | 6 | 0 |
| PAP_CIDPRI | C(6) | 6 | 0 |
| PAP_CIDSEC | C(6) | 6 | 0 |
| PAP_EQUIPE | C(12) | 12 | 0 |
| PAP_VL_FED | N(15,2) | 15 | 2 |
| PAP_VL_LOC | N(15,2) | 15 | 2 |
| PAP_VL_INC | N(15,2) | 15 | 2 |
| PAP_INCOUT | C(4) | 4 | 0 |
| PAP_INCURG | C(4) | 4 | 0 |
| PAP_RUB | C(6) | 6 | 0 |
| PAP_TPFIN | C(1) | 1 | 0 |
| PAP_CPX | C(1) | 1 | 0 |
| PAP_RC | C(4) | 4 | 0 |
| PAP_UNTERC | C(7) | 7 | 0 |

### `S_CID.DBF`

- 15611 records
- Size: 1,280,521 bytes
- Encoding: cp1252
- Fields: 12

| Field | Type | Length | Decimal |
|---|---|---|---|
| CD_COD | C(4) | 4 | 0 |
| OPC | C(1) | 1 | 0 |
| CAT | C(1) | 1 | 0 |
| SUBCAT | C(1) | 1 | 0 |
| CD_DESCR | C(50) | 50 | 0 |
| RESTRSEXO | C(1) | 1 | 0 |
| CAMPOS_RAD | C(3) | 3 | 0 |
| ESTADIO | C(1) | 1 | 0 |
| REPETE_RAD | C(1) | 1 | 0 |
| CMP_INI | C(6) | 6 | 0 |
| CMP_FIM | C(6) | 6 | 0 |
| CID_CHKSM | C(6) | 6 | 0 |

### `S_UPSPRF.DBF`

- 12870 records
- Size: 1,944,044 bytes
- Encoding: cp1252
- Fields: 20

| Field | Type | Length | Decimal |
|---|---|---|---|
| PRF_CMP | C(6) | 6 | 0 |
| PRF_CNES | C(7) | 7 | 0 |
| PRF_MUN | C(6) | 6 | 0 |
| PRF_AREA | C(4) | 4 | 0 |
| PRF_SEQ | C(8) | 8 | 0 |
| PRF_CNS | C(15) | 15 | 0 |
| PRF_DTINI | C(8) | 8 | 0 |
| PRF_DTFIM | C(8) | 8 | 0 |
| PRF_CBO | C(6) | 6 | 0 |
| PRF_IDVINC | C(6) | 6 | 0 |
| PRF_TPNSUS | C(1) | 1 | 0 |
| PRF_EQPMIN | C(1) | 1 | 0 |
| PRF_MICROA | C(2) | 2 | 0 |
| PRF_QT_HR | N(5) | 5 | 0 |
| PRF_SRVTER | C(3) | 3 | 0 |
| PRF_TERCEI | C(7) | 7 | 0 |
| PRF_NOME | C(40) | 40 | 0 |
| PRF_LOCNAC | C(1) | 1 | 0 |
| PRF_CHKSM | C(6) | 6 | 0 |
| PRF_INE | C(10) | 10 | 0 |

### `S_TPCRD.DBF`

- 5919 records
- Size: 1,078,093 bytes
- Encoding: cp1252
- Fields: 25

| Field | Type | Length | Decimal |
|---|---|---|---|
| TPCRDIFJ | C(1) | 1 | 0 |
| TPCRD_RAD | C(8) | 8 | 0 |
| TPCRDLIAL | C(6) | 6 | 0 |
| TPCRD_BCO | C(3) | 3 | 0 |
| TPCRD_AB | C(6) | 6 | 0 |
| TPCRD_CC | C(14) | 14 | 0 |
| TPCRD_IR | C(1) | 1 | 0 |
| TPCRD_ID | C(7) | 7 | 0 |
| TPCRD_CMP | C(6) | 6 | 0 |
| TPCRD_FLH | C(13) | 13 | 0 |
| TPCRD_SEQ | C(2) | 2 | 0 |
| TPCRD_PA | C(9) | 9 | 0 |
| TPCRD_ORG | C(3) | 3 | 0 |
| TPCRD_CR | C(1) | 1 | 0 |
| TPCRD_VL | N(13,2) | 13 | 2 |
| TPCRD_INCC | C(1) | 1 | 0 |
| TPCRD_CNPJ | C(14) | 14 | 0 |
| TPCRD_NFIS | C(6) | 6 | 0 |
| TPCRD_RUB | C(6) | 6 | 0 |
| TPCRD_CNS | C(15) | 15 | 0 |
| TPCRD_CBO | C(6) | 6 | 0 |
| TPCRD_CPX | C(1) | 1 | 0 |
| TPCRD_VFED | N(13,2) | 13 | 2 |
| TPCRD_VLOC | N(13,2) | 13 | 2 |
| TPCRD_VINC | N(13,2) | 13 | 2 |

### `CADMUN.DBF`

- 5715 records
- Size: 457,490 bytes
- Encoding: cp1252
- Fields: 8

| Field | Type | Length | Decimal |
|---|---|---|---|
| CODUF | C(2) | 2 | 0 |
| CODMUNIC | C(4) | 4 | 0 |
| NOME | C(40) | 40 | 0 |
| CONDIC | C(2) | 2 | 0 |
| TETOPAB | N(12,2) | 12 | 2 |
| CALCPAB | N(12,2) | 12 | 2 |
| DTHABIL | C(6) | 6 | 0 |
| CIB_SAS | C(1) | 1 | 0 |

### `S_DEPARA.DBF`

- 5576 records
- Size: 184,170 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| ORIGEM | C(10) | 10 | 0 |
| DESTINO | C(10) | 10 | 0 |
| CMP_INI | C(6) | 6 | 0 |
| CMP_FIM | C(6) | 6 | 0 |

### `S_CDN.DBF`

- 5570 records
- Size: 317,652 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| CDN_TB | C(2) | 2 | 0 |
| CDN_IT | C(8) | 8 | 0 |
| CDN_DSCR | C(40) | 40 | 0 |
| CDN_CHKSM | C(6) | 6 | 0 |

### `S_FXRAAS.DBF`

- 4639 records
- Size: 185,690 bytes
- Encoding: cp1252
- Fields: 3

| Field | Type | Length | Decimal |
|---|---|---|---|
| FX_INI | C(12) | 12 | 0 |
| FX_FIM | C(12) | 12 | 0 |
| FX_SKSUM | C(15) | 15 | 0 |

### `S_PRD.DBF`

- 3391 records
- Size: 1,175,017 bytes
- Encoding: cp1252
- Fields: 53

| Field | Type | Length | Decimal |
|---|---|---|---|
| PRD_UID | C(7) | 7 | 0 |
| PRD_CMP | C(6) | 6 | 0 |
| PRD_FLH | C(3) | 3 | 0 |
| PRD_SEQ | C(2) | 2 | 0 |
| PRD_PA | C(10) | 10 | 0 |
| PRD_CBO | C(6) | 6 | 0 |
| PRD_IDADE | N(3) | 3 | 0 |
| PRD_QT_P | N(6) | 6 | 0 |
| PRD_QT_A | N(6) | 6 | 0 |
| PRD_VL_P | N(15,2) | 15 | 2 |
| PRD_VL_A | N(15,2) | 15 | 2 |
| PRD_MVM | C(6) | 6 | 0 |
| PRD_ORG | C(3) | 3 | 0 |
| PRD_FLPA | C(1) | 1 | 0 |
| PRD_FLCBO | C(1) | 1 | 0 |
| PRD_FLCA | C(1) | 1 | 0 |
| PRD_FLIDA | C(1) | 1 | 0 |
| PRD_FLQT | C(1) | 1 | 0 |
| PRD_FLER | C(1) | 1 | 0 |
| PRD_APANUM | C(13) | 13 | 0 |
| PRD_CNSMED | C(15) | 15 | 0 |
| PRD_RMS | C(4) | 4 | 0 |
| PRD_CNPJ | C(14) | 14 | 0 |
| PRD_NFIS | C(6) | 6 | 0 |
| PRD_RESID | C(6) | 6 | 0 |
| PRD_RUB | C(6) | 6 | 0 |
| PRD_TPFIN | C(1) | 1 | 0 |
| PRD_CPX | C(1) | 1 | 0 |
| PRD_QTDATR | N(6) | 6 | 0 |
| PRD_QTDATU | N(6) | 6 | 0 |
| PRD_RC | C(4) | 4 | 0 |
| PRD_CIDPRI | C(6) | 6 | 0 |
| PRD_CIDSEC | C(6) | 6 | 0 |
| PRD_CIDCAS | C(6) | 6 | 0 |
| PRD_INCOUT | C(4) | 4 | 0 |
| PRD_INCURG | C(4) | 4 | 0 |
| PRD_INSRG | C(3) | 3 | 0 |
| PRD_CPFPCT | C(11) | 11 | 0 |
| PRD_CNSPCN | C(15) | 15 | 0 |
| PRD_DTINI | C(8) | 8 | 0 |
| PRD_DTREA | C(8) | 8 | 0 |
| PRD_SRV | C(3) | 3 | 0 |
| PRD_CSF | C(3) | 3 | 0 |
| PRD_EQUIP | C(12) | 12 | 0 |
| PRD_VL_FED | N(10,2) | 10 | 2 |
| PRD_VL_LOC | N(10,2) | 10 | 2 |
| PRD_VL_INC | N(10,2) | 10 | 2 |
| PRD_RUBFED | C(6) | 6 | 0 |
| PRD_LREX | C(1) | 1 | 0 |
| PRD_INE | C(10) | 10 | 0 |
| PRD_UNTERC | C(7) | 7 | 0 |
| PRD_CHKSM | C(6) | 6 | 0 |
| PRD_TEMP | C(20) | 20 | 0 |

### `S_PRDHST.DBF`

- 2596 records
- Size: 70,254 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| PRD_CMP | C(6) | 6 | 0 |
| PRD_UID | C(7) | 7 | 0 |
| PRD_PA | C(10) | 10 | 0 |
| PRD_INSRG | C(3) | 3 | 0 |

### `S_COMPFEDERAL.DBF`

- 2570 records
- Size: 2,218,936 bytes
- Encoding: cp1252
- Fields: 31

| Field | Type | Length | Decimal |
|---|---|---|---|
| CODPA | C(10) | 10 | 0 |
| DTINI | C(6) | 6 | 0 |
| DTFIM | C(6) | 6 | 0 |
| CAMPO_FED | C(30) | 30 | 0 |
| CAMPO_AC | C(30) | 30 | 0 |
| CAMPO_AL | C(30) | 30 | 0 |
| CAMPO_AP | C(30) | 30 | 0 |
| CAMPO_AM | C(30) | 30 | 0 |
| CAMPO_BA | C(30) | 30 | 0 |
| CAMPO_CE | C(30) | 30 | 0 |
| CAMPO_DF | C(30) | 30 | 0 |
| CAMPO_ES | C(30) | 30 | 0 |
| CAMPO_GO | C(30) | 30 | 0 |
| CAMPO_MA | C(30) | 30 | 0 |
| CAMPO_MT | C(30) | 30 | 0 |
| CAMPO_MS | C(30) | 30 | 0 |
| CAMPO_MG | C(30) | 30 | 0 |
| CAMPO_PA | C(30) | 30 | 0 |
| CAMPO_PB | C(30) | 30 | 0 |
| CAMPO_PR | C(30) | 30 | 0 |
| CAMPO_PE | C(30) | 30 | 0 |
| CAMPO_PI | C(30) | 30 | 0 |
| CAMPO_RJ | C(30) | 30 | 0 |
| CAMPO_RN | C(30) | 30 | 0 |
| CAMPO_RS | C(30) | 30 | 0 |
| CAMPO_RO | C(30) | 30 | 0 |
| CAMPO_RR | C(30) | 30 | 0 |
| CAMPO_SC | C(30) | 30 | 0 |
| CAMPO_SP | C(30) | 30 | 0 |
| CAMPO_SE | C(30) | 30 | 0 |
| CAMPO_TO | C(30) | 30 | 0 |

### `S_BPIHST.DBF`

- 1792 records
- Size: 770,691 bytes
- Encoding: cp1252
- Fields: 59

| Field | Type | Length | Decimal |
|---|---|---|---|
| BPI_UID | C(7) | 7 | 0 |
| BPI_CMP | C(6) | 6 | 0 |
| BPI_CNSMED | C(15) | 15 | 0 |
| BPI_CBO | C(6) | 6 | 0 |
| BPI_FLH | C(3) | 3 | 0 |
| BPI_SEQ | C(2) | 2 | 0 |
| BPI_PA | C(10) | 10 | 0 |
| BPI_CPFPCT | C(11) | 11 | 0 |
| BPI_CNSPAC | C(15) | 15 | 0 |
| BPI_NMPAC | C(30) | 30 | 0 |
| BPI_DTNASC | C(8) | 8 | 0 |
| BPI_SEXO | C(1) | 1 | 0 |
| BPI_IBGE | C(6) | 6 | 0 |
| BPI_DTATEN | C(8) | 8 | 0 |
| BPI_CID | C(4) | 4 | 0 |
| BPI_CATEN | C(2) | 2 | 0 |
| BPI_NAUT | C(13) | 13 | 0 |
| BPI_QT_P | N(6) | 6 | 0 |
| BPI_QT_A | N(6) | 6 | 0 |
| BPI_IDADE | N(3) | 3 | 0 |
| BPI_MVM | C(6) | 6 | 0 |
| BPI_ORG | C(3) | 3 | 0 |
| BPI_TPFIN | C(1) | 1 | 0 |
| BPI_RMS | C(4) | 4 | 0 |
| BPI_FLPA | C(1) | 1 | 0 |
| BPI_FLCID | C(1) | 1 | 0 |
| BPI_FLCBO | C(1) | 1 | 0 |
| BPI_FLCA | C(1) | 1 | 0 |
| BPI_FLIDA | C(1) | 1 | 0 |
| BPI_FLQT | C(1) | 1 | 0 |
| BPI_FLER | C(1) | 1 | 0 |
| BPI_RACA | C(2) | 2 | 0 |
| BPI_ETNIA | C(4) | 4 | 0 |
| BPI_NACIO | C(3) | 3 | 0 |
| BPI_SRV | C(3) | 3 | 0 |
| BPI_CSF | C(3) | 3 | 0 |
| BPI_EQUIPE | C(12) | 12 | 0 |
| BPI_CNPJ | C(14) | 14 | 0 |
| BPI_CEPPCN | C(8) | 8 | 0 |
| BPI_CDLOGR | C(3) | 3 | 0 |
| BPI_LOGPCN | C(30) | 30 | 0 |
| BPI_CPLPCN | C(10) | 10 | 0 |
| BPI_NUMPCN | C(5) | 5 | 0 |
| BPI_BAIRRO | C(30) | 30 | 0 |
| BPI_DDD | C(2) | 2 | 0 |
| BPI_TEL | C(9) | 9 | 0 |
| BPI_EMAIL | C(40) | 40 | 0 |
| BPI_VL_FED | N(10,2) | 10 | 2 |
| BPI_VL_LOC | N(10,2) | 10 | 2 |
| BPI_VL_INC | N(10,2) | 10 | 2 |
| BPI_INCOUT | C(4) | 4 | 0 |
| BPI_INCURG | C(4) | 4 | 0 |
| BPI_RUB | C(6) | 6 | 0 |
| BPI_CPX | C(1) | 1 | 0 |
| BPI_RC | C(4) | 4 | 0 |
| BPI_CHKSM | C(6) | 6 | 0 |
| BPI_INE | C(10) | 10 | 0 |
| BPI_STRUA | C(1) | 1 | 0 |
| BPI_ADVSEX | C(1) | 1 | 0 |

### `S_IPU.DBF`

- 1670 records
- Size: 334,675 bytes
- Encoding: cp1252
- Fields: 20

| Field | Type | Length | Decimal |
|---|---|---|---|
| IPU_UID | C(7) | 7 | 0 |
| IPU_CMP | C(6) | 6 | 0 |
| IPU_PA | C(9) | 9 | 0 |
| IPU_TPFIN | C(1) | 1 | 0 |
| IPU_NAPU | C(1) | 1 | 0 |
| IPU_QT_O | N(8) | 8 | 0 |
| IPU_VU_O | N(15,2) | 15 | 2 |
| IPU_VL_O | N(15,2) | 15 | 2 |
| IPU_QT_P | N(8) | 8 | 0 |
| IPU_VL_P | N(15,2) | 15 | 2 |
| IPU_QT_A | N(8) | 8 | 0 |
| IPU_VL_A | N(15,2) | 15 | 2 |
| IPU_VLAEST | N(15,2) | 15 | 2 |
| IPU_VLPEST | N(15,2) | 15 | 2 |
| IPU_VLOE | N(15,2) | 15 | 2 |
| IPU_VL_J | N(12,2) | 12 | 2 |
| IPU_MVM | C(6) | 6 | 0 |
| IPU_AUX | C(21) | 21 | 0 |
| IPU_FPOMAG | C(1) | 1 | 0 |
| IPU_CHKSM | C(6) | 6 | 0 |

### `S_APA.DBF`

- 1381 records
- Size: 1,076,589 bytes
- Encoding: cp1252
- Fields: 62

| Field | Type | Length | Decimal |
|---|---|---|---|
| APA_UID | C(7) | 7 | 0 |
| APA_NUM | C(13) | 13 | 0 |
| APA_EMISSA | C(8) | 8 | 0 |
| APA_DTINIC | C(8) | 8 | 0 |
| APA_DTFIM | C(8) | 8 | 0 |
| APA_TPATEN | C(2) | 2 | 0 |
| APA_TPAPAC | C(1) | 1 | 0 |
| APA_NMPCN | C(30) | 30 | 0 |
| APA_UFPCN | C(3) | 3 | 0 |
| APA_MAEPCN | C(30) | 30 | 0 |
| APA_LOGPCN | C(30) | 30 | 0 |
| APA_NUMPCN | C(5) | 5 | 0 |
| APA_CPLPCN | C(10) | 10 | 0 |
| APA_CEPPCN | C(8) | 8 | 0 |
| APA_MUNPCN | C(7) | 7 | 0 |
| APA_DTNASC | C(8) | 8 | 0 |
| APA_SEXPCN | C(1) | 1 | 0 |
| APA_VARIA | C(141) | 141 | 0 |
| APA_CPFRES | C(11) | 11 | 0 |
| APA_NMRES | C(30) | 30 | 0 |
| APA_MOTCOB | C(2) | 2 | 0 |
| APA_DTOBAL | C(8) | 8 | 0 |
| APA_CPFDIR | C(11) | 11 | 0 |
| APA_NMDIR | C(30) | 30 | 0 |
| APA_CMP | C(6) | 6 | 0 |
| APA_MVM | C(6) | 6 | 0 |
| APA_RMS | C(4) | 4 | 0 |
| APA_DTGER | C(8) | 8 | 0 |
| APA_FLER | C(10) | 10 | 0 |
| APA_INERPP | C(1) | 1 | 0 |
| APA_PRIPAL | C(9) | 9 | 0 |
| APA_CPFPCT | C(11) | 11 | 0 |
| APA_CNSPCT | C(15) | 15 | 0 |
| APA_CNSRES | C(15) | 15 | 0 |
| APA_CNSDIR | C(15) | 15 | 0 |
| APA_CIDCA | C(4) | 4 | 0 |
| APA_NPRONT | C(10) | 10 | 0 |
| APA_CODSOL | C(7) | 7 | 0 |
| APA_DTSOL | C(8) | 8 | 0 |
| APA_DTAUT | C(8) | 8 | 0 |
| APA_CODEMI | C(10) | 10 | 0 |
| APA_CATEND | C(2) | 2 | 0 |
| APA_APACAN | C(14) | 14 | 0 |
| APA_RACA | C(2) | 2 | 0 |
| APA_NOMERE | C(30) | 30 | 0 |
| APA_ETNIA | C(4) | 4 | 0 |
| APA_ADVLMC | C(1) | 1 | 0 |
| APA_ADVTZM | C(1) | 1 | 0 |
| APA_SRV | C(3) | 3 | 0 |
| APA_CSF | C(3) | 3 | 0 |
| APA_CDLOGR | C(3) | 3 | 0 |
| APA_BAIRRO | C(30) | 30 | 0 |
| APA_DDD | C(2) | 2 | 0 |
| APA_TEL | C(9) | 9 | 0 |
| APA_EMAIL | C(40) | 40 | 0 |
| APA_CNSEXE | C(15) | 15 | 0 |
| APA_INE | C(10) | 10 | 0 |
| APA_ADVSEX | C(1) | 1 | 0 |
| APA_EXPMAE | C(1) | 1 | 0 |
| APA_STRUA | C(1) | 1 | 0 |
| APA_FNTORC | C(2) | 2 | 0 |
| APA_EMEPAR | C(1) | 1 | 0 |

### `S_EMU.DBF`

- 1197 records
- Size: 49,367 bytes
- Encoding: cp1252
- Fields: 8

| Field | Type | Length | Decimal |
|---|---|---|---|
| EMU_UID | C(7) | 7 | 0 |
| EMU_CMP | C(6) | 6 | 0 |
| EMU_EMA | C(6) | 6 | 0 |
| EMU_QT_PR | N(3) | 3 | 0 |
| EMU_QT_HR | N(5) | 5 | 0 |
| EMU_LOCNAC | C(1) | 1 | 0 |
| EMU_MVM | C(6) | 6 | 0 |
| EMU_CHKSM | C(6) | 6 | 0 |

### `S_SRV.DBF`

- 1152 records
- Size: 31,330 bytes
- Encoding: cp1252
- Fields: 6

| Field | Type | Length | Decimal |
|---|---|---|---|
| SRV_UID | C(7) | 7 | 0 |
| SRV_CMP | C(6) | 6 | 0 |
| SRV_SR | C(3) | 3 | 0 |
| SRV_CSF | C(3) | 3 | 0 |
| SRV_LOCNAC | C(1) | 1 | 0 |
| SRV_CHKSM | C(6) | 6 | 0 |

### `S_BPI.DBF`

- 910 records
- Size: 392,313 bytes
- Encoding: cp1252
- Fields: 59

| Field | Type | Length | Decimal |
|---|---|---|---|
| BPI_UID | C(7) | 7 | 0 |
| BPI_CMP | C(6) | 6 | 0 |
| BPI_CNSMED | C(15) | 15 | 0 |
| BPI_CBO | C(6) | 6 | 0 |
| BPI_FLH | C(3) | 3 | 0 |
| BPI_SEQ | C(2) | 2 | 0 |
| BPI_PA | C(10) | 10 | 0 |
| BPI_CPFPCT | C(11) | 11 | 0 |
| BPI_CNSPAC | C(15) | 15 | 0 |
| BPI_NMPAC | C(30) | 30 | 0 |
| BPI_DTNASC | C(8) | 8 | 0 |
| BPI_SEXO | C(1) | 1 | 0 |
| BPI_IBGE | C(6) | 6 | 0 |
| BPI_DTATEN | C(8) | 8 | 0 |
| BPI_CID | C(4) | 4 | 0 |
| BPI_CATEN | C(2) | 2 | 0 |
| BPI_NAUT | C(13) | 13 | 0 |
| BPI_QT_P | N(6) | 6 | 0 |
| BPI_QT_A | N(6) | 6 | 0 |
| BPI_IDADE | N(3) | 3 | 0 |
| BPI_MVM | C(6) | 6 | 0 |
| BPI_ORG | C(3) | 3 | 0 |
| BPI_TPFIN | C(1) | 1 | 0 |
| BPI_RMS | C(4) | 4 | 0 |
| BPI_FLPA | C(1) | 1 | 0 |
| BPI_FLCID | C(1) | 1 | 0 |
| BPI_FLCBO | C(1) | 1 | 0 |
| BPI_FLCA | C(1) | 1 | 0 |
| BPI_FLIDA | C(1) | 1 | 0 |
| BPI_FLQT | C(1) | 1 | 0 |
| BPI_FLER | C(1) | 1 | 0 |
| BPI_RACA | C(2) | 2 | 0 |
| BPI_ETNIA | C(4) | 4 | 0 |
| BPI_NACIO | C(3) | 3 | 0 |
| BPI_SRV | C(3) | 3 | 0 |
| BPI_CSF | C(3) | 3 | 0 |
| BPI_EQUIPE | C(12) | 12 | 0 |
| BPI_CNPJ | C(14) | 14 | 0 |
| BPI_CEPPCN | C(8) | 8 | 0 |
| BPI_CDLOGR | C(3) | 3 | 0 |
| BPI_LOGPCN | C(30) | 30 | 0 |
| BPI_CPLPCN | C(10) | 10 | 0 |
| BPI_NUMPCN | C(5) | 5 | 0 |
| BPI_BAIRRO | C(30) | 30 | 0 |
| BPI_DDD | C(2) | 2 | 0 |
| BPI_TEL | C(9) | 9 | 0 |
| BPI_EMAIL | C(40) | 40 | 0 |
| BPI_VL_FED | N(10,2) | 10 | 2 |
| BPI_VL_LOC | N(10,2) | 10 | 2 |
| BPI_VL_INC | N(10,2) | 10 | 2 |
| BPI_INCOUT | C(4) | 4 | 0 |
| BPI_INCURG | C(4) | 4 | 0 |
| BPI_RUB | C(6) | 6 | 0 |
| BPI_CPX | C(1) | 1 | 0 |
| BPI_RC | C(4) | 4 | 0 |
| BPI_CHKSM | C(6) | 6 | 0 |
| BPI_INE | C(10) | 10 | 0 |
| BPI_STRUA | C(1) | 1 | 0 |
| BPI_ADVSEX | C(1) | 1 | 0 |

### `S_PAIN.DBF`

- 858 records
- Size: 31,083 bytes
- Encoding: cp1252
- Fields: 5

| Field | Type | Length | Decimal |
|---|---|---|---|
| PAIN_CMP | C(6) | 6 | 0 |
| PAIN_PA | C(9) | 9 | 0 |
| PAIN_HA | C(4) | 4 | 0 |
| PAIN_IN | N(10,2) | 10 | 2 |
| PAIN_CHKSM | C(6) | 6 | 0 |

### `S_EQUIPE.DBF`

- 592 records
- Size: 78,034 bytes
- Encoding: cp1252
- Fields: 14

| Field | Type | Length | Decimal |
|---|---|---|---|
| EQP_CMP | C(6) | 6 | 0 |
| EQP_CNES | C(7) | 7 | 0 |
| EQP_AREA | C(4) | 4 | 0 |
| EQP_SEQ | C(8) | 8 | 0 |
| EQP_MUN | C(6) | 6 | 0 |
| EQP_TP | C(2) | 2 | 0 |
| EQP_NOME | C(60) | 60 | 0 |
| EQP_DTINI | C(8) | 8 | 0 |
| EQP_DTFIM | C(8) | 8 | 0 |
| EQP_MOTFIM | C(2) | 2 | 0 |
| EQP_TPFIM | C(2) | 2 | 0 |
| EQP_LOCNAC | C(1) | 1 | 0 |
| EQP_CHKSM | C(6) | 6 | 0 |
| EQP_INE | C(10) | 10 | 0 |

### `S_UPSGES.DBF`

- 541 records
- Size: 9,359 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| GES_CMP | C(6) | 6 | 0 |
| GES_UID | C(7) | 7 | 0 |
| GES_CPX | C(2) | 2 | 0 |
| GES_IN | C(1) | 1 | 0 |

### `consiste.dbf`

- 469 records
- Size: 133,242 bytes
- Encoding: cp1252
- Fields: 15

| Field | Type | Length | Decimal |
|---|---|---|---|
| UID | C(7) | 7 | 0 |
| CMP | C(6) | 6 | 0 |
| TIPO | C(1) | 1 | 0 |
| APAC | C(13) | 13 | 0 |
| FOLHA | C(3) | 3 | 0 |
| SEQ | C(2) | 2 | 0 |
| PROCEDIM | C(10) | 10 | 0 |
| CBO | C(6) | 6 | 0 |
| CNSMED | C(15) | 15 | 0 |
| CNSPCT | C(15) | 15 | 0 |
| DTINIC | C(8) | 8 | 0 |
| COD_ERRO | C(4) | 4 | 0 |
| DESCR_ERRO | C(80) | 80 | 0 |
| ERRO_67 | C(100) | 100 | 0 |
| EQUIPE | C(12) | 12 | 0 |

### `S_CNSQTD.DBF`

- 452 records
- Size: 64,310 bytes
- Encoding: cp1252
- Fields: 17

| Field | Type | Length | Decimal |
|---|---|---|---|
| CNSPCT | C(15) | 15 | 0 |
| CMP | C(6) | 6 | 0 |
| PROCEDIM | C(10) | 10 | 0 |
| QTD | N(8) | 8 | 0 |
| SUBTOT | N(15) | 15 | 0 |
| UID | C(7) | 7 | 0 |
| TIPO | C(1) | 1 | 0 |
| APAC | C(13) | 13 | 0 |
| FOLHA | C(3) | 3 | 0 |
| SEQ | C(2) | 2 | 0 |
| CBO | C(6) | 6 | 0 |
| CNSMED | C(15) | 15 | 0 |
| MVM | C(6) | 6 | 0 |
| ORG | C(3) | 3 | 0 |
| PRINCIPAL | C(9) | 9 | 0 |
| ERRO | C(1) | 1 | 0 |
| X | C(20) | 20 | 0 |

### `S_TUEMA.DBF`

- 99 records
- Size: 15,245 bytes
- Encoding: cp1252
- Fields: 2

| Field | Type | Length | Decimal |
|---|---|---|---|
| TUEMA_TUP | C(2) | 2 | 0 |
| TUEMA_EMA | C(150) | 150 | 0 |

### `S_RUB.DBF`

- 42 records
- Size: 2,104 bytes
- Encoding: cp1252
- Fields: 3

| Field | Type | Length | Decimal |
|---|---|---|---|
| RUB_ID | C(4) | 4 | 0 |
| RUB_DC | C(40) | 40 | 0 |
| RUB_TOTAL | C(2) | 2 | 0 |

### `S_CFA.DBF`

- 38 records
- Size: 1,568 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| CFAINICIAL | C(12) | 12 | 0 |
| CFAFINAL | C(12) | 12 | 0 |
| CFAAMINI | C(6) | 6 | 0 |
| CFAAMFIN | C(6) | 6 | 0 |

### `S_FFI.DBF`

- 30 records
- Size: 1,604 bytes
- Encoding: cp1252
- Fields: 5

| Field | Type | Length | Decimal |
|---|---|---|---|
| FFI_IN_PF | C(1) | 1 | 0 |
| FFI_CGCCPF | C(14) | 14 | 0 |
| FFI_MVM | C(6) | 6 | 0 |
| FFI_VL | N(13,2) | 13 | 2 |
| FFI_IR | N(12,2) | 12 | 2 |

### `S_UPS.DBF`

- 27 records
- Size: 10,838 bytes
- Encoding: cp1252
- Fields: 44

| Field | Type | Length | Decimal |
|---|---|---|---|
| UPS_ID | C(7) | 7 | 0 |
| UPS_UF | C(2) | 2 | 0 |
| UPS_RZSC | C(35) | 35 | 0 |
| UPS_NMFN | C(35) | 35 | 0 |
| UPS_IN_ATV | C(1) | 1 | 0 |
| UPS_IN_FJ | C(1) | 1 | 0 |
| UPS_CGCCPF | C(14) | 14 | 0 |
| UPS_IR | C(1) | 1 | 0 |
| UPS_LOGR | C(30) | 30 | 0 |
| UPS_NUM | C(10) | 10 | 0 |
| UPS_COMPL | C(15) | 15 | 0 |
| UPS_BAIRRO | C(15) | 15 | 0 |
| UPS_DDD | C(5) | 5 | 0 |
| UPS_TELE | C(7) | 7 | 0 |
| UPS_CEP | C(8) | 8 | 0 |
| UPS_MN | C(6) | 6 | 0 |
| UPS_DS | C(3) | 3 | 0 |
| UPS_RS | C(3) | 3 | 0 |
| UPS_AB | C(6) | 6 | 0 |
| UPS_NU_CC | C(14) | 14 | 0 |
| UPS_IN_MN | C(1) | 1 | 0 |
| UPS_NU_CT | C(8) | 8 | 0 |
| UPS_DT_IN | D(8) | 8 | 0 |
| UPS_DT_CT | D(8) | 8 | 0 |
| UPS_DT_PR | D(8) | 8 | 0 |
| UPS_TUP | C(2) | 2 | 0 |
| UPS_TP | C(2) | 2 | 0 |
| UPS_QT_CM | N(3) | 3 | 0 |
| UPS_QT_EO | N(3) | 3 | 0 |
| UPS_QT_SG | N(2) | 2 | 0 |
| UPS_QT_SPC | N(2) | 2 | 0 |
| UPS_QT_SCA | N(2) | 2 | 0 |
| UPS_TA | C(2) | 2 | 0 |
| UPS_FC | C(2) | 2 | 0 |
| UPS_CNES | C(7) | 7 | 0 |
| UPS_IDANT | C(7) | 7 | 0 |
| UPS_NH | C(2) | 2 | 0 |
| UPSATUCNES | C(1) | 1 | 0 |
| UPS_BANCO | C(3) | 3 | 0 |
| UPS_MNT | C(14) | 14 | 0 |
| UPS_NATJUR | C(4) | 4 | 0 |
| UNIDADE_ID | C(31) | 31 | 0 |
| UPS_LOCNAC | C(1) | 1 | 0 |
| UPS_CHKSM | C(6) | 6 | 0 |

### `s_reprd.dbf`

- 27 records
- Size: 2,537 bytes
- Encoding: cp1252
- Fields: 9

| Field | Type | Length | Decimal |
|---|---|---|---|
| RE_CUNID | C(7) | 7 | 0 |
| RE_CNOME | C(35) | 35 | 0 |
| RE_CCOMP | C(6) | 6 | 0 |
| RE_NQTFL | N(4) | 4 | 0 |
| RE_NQTBPI | N(4) | 4 | 0 |
| RE_NQAPA | N(7) | 7 | 0 |
| RE_NQAD | N(5) | 5 | 0 |
| RE_NQPSI | N(5) | 5 | 0 |
| RE_MENSAGE | C(8) | 8 | 0 |

### `S_PRFIRR.DBF`

- 22 records
- Size: 1,096 bytes
- Encoding: cp1252
- Fields: 5

| Field | Type | Length | Decimal |
|---|---|---|---|
| PRF_MVM | C(6) | 6 | 0 |
| PRF_CNES | C(7) | 7 | 0 |
| PRF_CPF | C(11) | 11 | 0 |
| PRF_CNS | C(15) | 15 | 0 |
| PRF_FLG | C(1) | 1 | 0 |

### `S_RELAT.DBF`

- 22 records
- Size: 73,600 bytes
- Encoding: cp1252
- Fields: 24

| Field | Type | Length | Decimal |
|---|---|---|---|
| CODIGO | C(8) | 8 | 0 |
| DESCR | C(40) | 40 | 0 |
| CABEC | C(200) | 200 | 0 |
| HEADER | C(200) | 200 | 0 |
| IND_ORDEM | C(100) | 100 | 0 |
| QUEBRA1 | C(100) | 100 | 0 |
| HEAD_1 | C(200) | 200 | 0 |
| FOOT_1 | C(200) | 200 | 0 |
| QUEBRA2 | C(100) | 100 | 0 |
| HEAD_2 | C(200) | 200 | 0 |
| FOOT_2 | C(250) | 250 | 0 |
| QUEBRA3 | C(100) | 100 | 0 |
| HEAD_3 | C(200) | 200 | 0 |
| FOOT_3 | C(200) | 200 | 0 |
| DETALHE | C(250) | 250 | 0 |
| DETALHE_CO | C(200) | 200 | 0 |
| SOMA1 | C(100) | 100 | 0 |
| SOMA2 | C(100) | 100 | 0 |
| SOMA3 | C(100) | 100 | 0 |
| SOMA4 | C(100) | 100 | 0 |
| SOMA5 | C(100) | 100 | 0 |
| FILTRO | C(30) | 30 | 0 |
| TOT_GER | C(200) | 200 | 0 |
| I_TEMP_PRD | C(30) | 30 | 0 |

### `S_ENCERR.DBF`

- 18 records
- Size: 4,333 bytes
- Encoding: cp1252
- Fields: 28

| Field | Type | Length | Decimal |
|---|---|---|---|
| ENC_MVM | C(6) | 6 | 0 |
| ENC_BDSIA | C(7) | 7 | 0 |
| ENC_SIASUS | C(5) | 5 | 0 |
| ENC_TDTINI | C(8) | 8 | 0 |
| ENC_THRINI | C(6) | 6 | 0 |
| ENC_TDTFIM | C(8) | 8 | 0 |
| ENC_THRFIM | C(6) | 6 | 0 |
| ENC_CDTINI | C(8) | 8 | 0 |
| ENC_CHRINI | C(6) | 6 | 0 |
| ENC_CDTFIM | C(8) | 8 | 0 |
| ENC_CHRFIM | C(6) | 6 | 0 |
| ENC_BDTINI | C(8) | 8 | 0 |
| ENC_BHRINI | C(6) | 6 | 0 |
| ENC_BDTFIM | C(8) | 8 | 0 |
| ENC_BHRFIM | C(6) | 6 | 0 |
| ENC_VDTINI | C(8) | 8 | 0 |
| ENC_VHRINI | C(6) | 6 | 0 |
| ENC_VDTFIM | C(8) | 8 | 0 |
| ENC_VHRFIM | C(6) | 6 | 0 |
| ENC_RDTINI | C(8) | 8 | 0 |
| ENC_RHRINI | C(6) | 6 | 0 |
| ENC_RDTFIM | C(8) | 8 | 0 |
| ENC_RHRFIM | C(6) | 6 | 0 |
| ENC_TTOTH | C(6) | 6 | 0 |
| ENC_CTOTH | C(6) | 6 | 0 |
| ENC_BTOTH | C(6) | 6 | 0 |
| ENC_VTOTH | C(6) | 6 | 0 |
| ENC_RTOTH | C(6) | 6 | 0 |

### `S_CGCEX.DBF`

- 8 records
- Size: 274 bytes
- Encoding: cp1252
- Fields: 3

| Field | Type | Length | Decimal |
|---|---|---|---|
| CGC_NUM | C(14) | 14 | 0 |
| CGC_UF | C(2) | 2 | 0 |
| CGC_FINANC | C(1) | 1 | 0 |

### `S_UPSHA.DBF`

- 8 records
- Size: 474 bytes
- Encoding: cp1252
- Fields: 6

| Field | Type | Length | Decimal |
|---|---|---|---|
| UPSHA_UPS | C(7) | 7 | 0 |
| UPSHA_HA | C(4) | 4 | 0 |
| UPSHA_INI | C(6) | 6 | 0 |
| UPSHA_FIM | C(6) | 6 | 0 |
| UHA_LOCNAC | C(1) | 1 | 0 |
| UHA_CHKSM | C(6) | 6 | 0 |

### `tot_cons.dbf`

- 8 records
- Size: 947 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| GRUPO | C(3) | 3 | 0 |
| COD_ERRO | C(4) | 4 | 0 |
| DESCR_ERRO | C(80) | 80 | 0 |
| QTD_TOTAL | N(10) | 10 | 0 |

### `CONFIMP.DBF`

- 5 records
- Size: 5,830 bytes
- Encoding: cp1252
- Fields: 15

| Field | Type | Length | Decimal |
|---|---|---|---|
| NOME_IMP | C(20) | 20 | 0 |
| SEQ_INIT | C(80) | 80 | 0 |
| SEQ_RESET | C(80) | 80 | 0 |
| TAM_1 | C(80) | 80 | 0 |
| TAM_2 | C(80) | 80 | 0 |
| TAM_3 | C(80) | 80 | 0 |
| TAM_4 | C(80) | 80 | 0 |
| TAM_5 | C(80) | 80 | 0 |
| NEGRITO | C(80) | 80 | 0 |
| DNEGRITO | C(80) | 80 | 0 |
| CARTA | C(80) | 80 | 0 |
| DCARTA | C(80) | 80 | 0 |
| EXPANDIDO | C(80) | 80 | 0 |
| DEXPANDIDO | C(80) | 80 | 0 |
| MAXLIN | N(2) | 2 | 0 |

### `S_TDIR.DBF`

- 5 records
- Size: 414 bytes
- Encoding: cp1252
- Fields: 5

| Field | Type | Length | Decimal |
|---|---|---|---|
| TDIR_FJ | C(1) | 1 | 0 |
| TDIR_DE | N(12,2) | 12 | 2 |
| TDIR_ATE | N(12,2) | 12 | 2 |
| TDIR_TX | N(6,2) | 6 | 2 |
| TDIR_PARC | N(12,2) | 12 | 2 |

### `S_UPSRC.DBF`

- 4 records
- Size: 258 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| UPSRC_UPS | C(7) | 7 | 0 |
| UPSRC_RC | C(4) | 4 | 0 |
| UPSRC_INI | C(6) | 6 | 0 |
| UPSRC_FIM | C(6) | 6 | 0 |

### `S_CTR.DBF`

- 1 records
- Size: 314 bytes
- Encoding: cp1252
- Fields: 8

| Field | Type | Length | Decimal |
|---|---|---|---|
| CTR_MVM | C(6) | 6 | 0 |
| CTR_CAD | C(1) | 1 | 0 |
| CTR_FPO | C(1) | 1 | 0 |
| CTR_BDP | C(1) | 1 | 0 |
| CTR_PRD | C(1) | 1 | 0 |
| CTR_CRD | C(1) | 1 | 0 |
| CTR_CONS | C(1) | 1 | 0 |
| CTR_VERSAO | C(10) | 10 | 0 |

### `S_MNT.DBF`

- 1 records
- Size: 1,006 bytes
- Encoding: cp1252
- Fields: 24

| Field | Type | Length | Decimal |
|---|---|---|---|
| MNT_ID | C(8) | 8 | 0 |
| MNT_CGCFI | C(4) | 4 | 0 |
| MNT_CGCDV | C(2) | 2 | 0 |
| MNT_IR | C(1) | 1 | 0 |
| MNT_LOGR | C(30) | 30 | 0 |
| MNT_NUM | C(5) | 5 | 0 |
| MNT_COMPL | C(10) | 10 | 0 |
| MNT_BAIRRO | C(15) | 15 | 0 |
| MNT_CEP | C(8) | 8 | 0 |
| MNT_DDD | C(5) | 5 | 0 |
| MNT_TELE | C(8) | 8 | 0 |
| MNT_DT_PR | D(8) | 8 | 0 |
| MNT_DT_IN | D(8) | 8 | 0 |
| MNT_DT_CT | D(8) | 8 | 0 |
| MNT_DT_UA | D(8) | 8 | 0 |
| MNT_RZSC | C(35) | 35 | 0 |
| MNT_AB | C(6) | 6 | 0 |
| MNT_NU_CC | C(14) | 14 | 0 |
| MNT_MN | C(6) | 6 | 0 |
| MNT_RS | C(3) | 3 | 0 |
| MNT_TPCC | C(1) | 1 | 0 |
| MNT_BANCO | C(3) | 3 | 0 |
| MNT_LOCNAC | C(1) | 1 | 0 |
| MNT_CHKSM | C(6) | 6 | 0 |

### `S_SS.DBF`

- 1 records
- Size: 770 bytes
- Encoding: cp1252
- Fields: 17

| Field | Type | Length | Decimal |
|---|---|---|---|
| SS_UFIBGE | C(2) | 2 | 0 |
| SS_UFSIGL | C(2) | 2 | 0 |
| SS_UFNOME | C(30) | 30 | 0 |
| SS_NM | C(40) | 40 | 0 |
| SS_SIGL | C(10) | 10 | 0 |
| SS_CGC | C(14) | 14 | 0 |
| SS_IN | C(1) | 1 | 0 |
| SS_ABDV | C(5) | 5 | 0 |
| SS_CCDV | C(10) | 10 | 0 |
| SS_CONDIC | C(2) | 2 | 0 |
| SS_GESTAO | C(6) | 6 | 0 |
| SS_CONVBB | C(6) | 6 | 0 |
| SS_DPCNES | C(10) | 10 | 0 |
| SS_TXTCNE | C(10) | 10 | 0 |
| SS_CRD | C(14) | 14 | 0 |
| SS_DEPARA | C(14) | 14 | 0 |
| SS_CFCES0 | C(14) | 14 | 0 |

### `s_cor.dbf`

- 1 records
- Size: 886 bytes
- Encoding: cp1252
- Fields: 14

| Field | Type | Length | Decimal |
|---|---|---|---|
| QUE_LINHA | N(2) | 2 | 0 |
| PAD_NOME | C(40) | 40 | 0 |
| CABEC | C(30) | 30 | 0 |
| MENSAGEM | C(30) | 30 | 0 |
| CAMPOGET | C(30) | 30 | 0 |
| CAMPOSAY | C(30) | 30 | 0 |
| MENU | C(30) | 30 | 0 |
| PMENU | C(30) | 30 | 0 |
| PISCA | C(30) | 30 | 0 |
| ASTER | C(30) | 30 | 0 |
| SOMBRA | C(30) | 30 | 0 |
| OUTRO1 | C(30) | 30 | 0 |
| OUTRO2 | C(30) | 30 | 0 |
| OUTRO3 | C(30) | 30 | 0 |

### `ADESAO.DBF`

- 0 records
- Size: 163 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| FINANC | C(4) | 4 | 0 |
| GESTOR | C(6) | 6 | 0 |
| CMP_INI | C(6) | 6 | 0 |
| CMP_FIM | C(6) | 6 | 0 |

### `APCNES_TERC.DBF`

- 0 records
- Size: 323 bytes
- Encoding: cp1252
- Fields: 9

| Field | Type | Length | Decimal |
|---|---|---|---|
| UID | C(7) | 7 | 0 |
| CMP | C(6) | 6 | 0 |
| APAC | C(13) | 13 | 0 |
| PROC_PRIC | C(10) | 10 | 0 |
| SEQ | C(2) | 2 | 0 |
| PROC_SEC | C(10) | 10 | 0 |
| UID_TERC | C(7) | 7 | 0 |
| QTD | N(6) | 6 | 0 |
| MVM | C(6) | 6 | 0 |

### `S_CD.DBF`

- 0 records
- Size: 162 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| CD_TB | C(2) | 2 | 0 |
| CD_IT | C(8) | 8 | 0 |
| CD_DSCR | C(40) | 40 | 0 |
| CD_MEMO | M(10) | 10 | 0 |

### `S_CDX.DBF`

- 0 records
- Size: 162 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| CDX_TB1 | C(2) | 2 | 0 |
| CDX_TB2 | C(2) | 2 | 0 |
| CDX_IT1 | C(9) | 9 | 0 |
| CDX_IT2 | C(9) | 9 | 0 |

### `S_CIDM.DBF`

- 0 records
- Size: 98 bytes
- Encoding: cp1252
- Fields: 2

| Field | Type | Length | Decimal |
|---|---|---|---|
| CDN_COD | C(6) | 6 | 0 |
| CDN_DESCR | C(68) | 68 | 0 |

### `S_CRD.DBF`

- 0 records
- Size: 643 bytes
- Encoding: cp1252
- Fields: 19

| Field | Type | Length | Decimal |
|---|---|---|---|
| CRD_IN_PF | C(1) | 1 | 0 |
| CRD_CGCCPF | C(14) | 14 | 0 |
| CRD_MVM | C(6) | 6 | 0 |
| CRD_BCO | C(3) | 3 | 0 |
| CRD_AB | C(6) | 6 | 0 |
| CRD_CC | C(14) | 14 | 0 |
| CRD_MN | C(6) | 6 | 0 |
| CRD_VL | N(13,2) | 13 | 2 |
| CRDPAB | N(13,2) | 13 | 2 |
| CRDMEDIA | N(13,2) | 13 | 2 |
| CRDESTRAT | N(13,2) | 13 | 2 |
| CRDALTA | N(13,2) | 13 | 2 |
| CRDBDP | N(13,2) | 13 | 2 |
| CRDBDP_OUT | N(13,2) | 13 | 2 |
| CRD_IR | N(13,2) | 13 | 2 |
| CRD_DESCIR | C(1) | 1 | 0 |
| CRD_VL_FED | N(13,2) | 13 | 2 |
| CRD_VL_LOC | N(13,2) | 13 | 2 |
| CRD_VL_INC | N(13,2) | 13 | 2 |

### `S_DPG.DBF`

- 0 records
- Size: 419 bytes
- Encoding: cp1252
- Fields: 12

| Field | Type | Length | Decimal |
|---|---|---|---|
| DPG_CMP | C(6) | 6 | 0 |
| DPG_FLH | C(3) | 3 | 0 |
| DPG_SEQ | C(2) | 2 | 0 |
| DPG_UID | C(7) | 7 | 0 |
| DPG_PA | C(10) | 10 | 0 |
| DPG_QT | N(6) | 6 | 0 |
| DPG_VL | N(13,2) | 13 | 2 |
| DPG_VL_EF | N(13,2) | 13 | 2 |
| DPG_RC | C(4) | 4 | 0 |
| DPG_MVM | C(6) | 6 | 0 |
| DPG_CL | C(1) | 1 | 0 |
| DPG_CHKSM | C(6) | 6 | 0 |

### `S_FORNEC.DBF`

- 0 records
- Size: 386 bytes
- Encoding: cp1252
- Fields: 11

| Field | Type | Length | Decimal |
|---|---|---|---|
| FOR_CNPJ | C(14) | 14 | 0 |
| FOR_RZSOC | C(80) | 80 | 0 |
| FOR_FANTA | C(80) | 80 | 0 |
| FOR_BANCO | C(3) | 3 | 0 |
| FOR_AG | C(6) | 6 | 0 |
| FOR_CC | C(14) | 14 | 0 |
| FOR_END | C(60) | 60 | 0 |
| FOR_BAIRRO | C(20) | 20 | 0 |
| FOR_CEP | C(20) | 20 | 0 |
| FOR_IBGE | C(6) | 6 | 0 |
| FOR_TMP | C(1) | 1 | 0 |

### `S_MN.DBF`

- 0 records
- Size: 130 bytes
- Encoding: cp1252
- Fields: 3

| Field | Type | Length | Decimal |
|---|---|---|---|
| MN_IBGE | C(6) | 6 | 0 |
| MN_NOME | C(40) | 40 | 0 |
| MN_UF | C(2) | 2 | 0 |

### `S_OCORR.DBF`

- 0 records
- Size: 515 bytes
- Encoding: cp1252
- Fields: 15

| Field | Type | Length | Decimal |
|---|---|---|---|
| ORR_CMP | C(6) | 6 | 0 |
| ORR_INSRG | C(3) | 3 | 0 |
| ORR_UID | C(7) | 7 | 0 |
| ORR_FLH | C(3) | 3 | 0 |
| ORR_SEQ | C(2) | 2 | 0 |
| ORR_APANUM | C(13) | 13 | 0 |
| ORR_CNSMED | C(15) | 15 | 0 |
| ORR_CBO | C(6) | 6 | 0 |
| ORR_CNSPCN | C(15) | 15 | 0 |
| ORR_DTINI | C(8) | 8 | 0 |
| ORR_PA | C(10) | 10 | 0 |
| ORR_TIP | C(3) | 3 | 0 |
| ORR_COD | C(6) | 6 | 0 |
| ORR_DSCR | C(100) | 100 | 0 |
| ORR_MVM | C(6) | 6 | 0 |

### `S_RAPA.DBF`

- 0 records
- Size: 1,155 bytes
- Encoding: cp1252
- Fields: 35

| Field | Type | Length | Decimal |
|---|---|---|---|
| RAPA_INSRG | C(3) | 3 | 0 |
| RAPA_UID | C(7) | 7 | 0 |
| RAPA_CMP | C(6) | 6 | 0 |
| RAPA_CPFPC | C(11) | 11 | 0 |
| RAPA_CNSPC | C(15) | 15 | 0 |
| RAPA_DTINI | C(8) | 8 | 0 |
| RAPA_PA | C(10) | 10 | 0 |
| RAPA_CBO | C(6) | 6 | 0 |
| RAPA_CNSEX | C(15) | 15 | 0 |
| RAPA_DTREA | C(8) | 8 | 0 |
| RAPA_SRV | C(3) | 3 | 0 |
| RAPA_CSF | C(3) | 3 | 0 |
| RAPA_EQUIP | C(12) | 12 | 0 |
| RAPA_QT_P | N(6) | 6 | 0 |
| RAPA_QT_A | N(6) | 6 | 0 |
| RAPA_ORG | C(3) | 3 | 0 |
| RAPA_CID | C(4) | 4 | 0 |
| RAPA_LREX | C(1) | 1 | 0 |
| RAPA_CHKSU | C(4) | 4 | 0 |
| RAPA_MVM | C(6) | 6 | 0 |
| RAPA_FLPA | C(1) | 1 | 0 |
| RAPA_FLEMA | C(1) | 1 | 0 |
| RAPA_FLCBO | C(1) | 1 | 0 |
| RAPA_FLQT | C(1) | 1 | 0 |
| RAPA_FLER | C(1) | 1 | 0 |
| RAPAVL_FED | N(10,2) | 10 | 2 |
| RAPAVL_LOC | N(10,2) | 10 | 2 |
| RAPAVL_INC | N(10,2) | 10 | 2 |
| RAPAINCOUT | C(4) | 4 | 0 |
| RAPAINCURG | C(4) | 4 | 0 |
| RAPA_RUB | C(6) | 6 | 0 |
| RAPA_TPFIN | C(1) | 1 | 0 |
| RAPA_CPX | C(1) | 1 | 0 |
| RAPA_RC | C(4) | 4 | 0 |
| RAPA_INE | C(10) | 10 | 0 |

### `S_RAS.DBF`

- 0 records
- Size: 1,603 bytes
- Encoding: cp1252
- Fields: 49

| Field | Type | Length | Decimal |
|---|---|---|---|
| RA_INSRG | C(3) | 3 | 0 |
| RA_UID | C(7) | 7 | 0 |
| RA_CMP | C(6) | 6 | 0 |
| RA_CPFPCT | C(11) | 11 | 0 |
| RA_CNSPCT | C(15) | 15 | 0 |
| RA_DTINIC | C(8) | 8 | 0 |
| RA_DTFIM | C(8) | 8 | 0 |
| RA_NMPCN | C(30) | 30 | 0 |
| RA_NPRONT | C(10) | 10 | 0 |
| RA_NACPCN | C(3) | 3 | 0 |
| RA_MAEPCN | C(30) | 30 | 0 |
| RA_NOMERE | C(30) | 30 | 0 |
| RA_LOGPCN | C(30) | 30 | 0 |
| RA_NUMPCN | C(5) | 5 | 0 |
| RA_CPLPCN | C(10) | 10 | 0 |
| RA_CEPPCN | C(8) | 8 | 0 |
| RA_MUNPCN | C(7) | 7 | 0 |
| RA_DTNASC | C(8) | 8 | 0 |
| RA_SEXPCN | C(1) | 1 | 0 |
| RA_RACA | C(2) | 2 | 0 |
| RA_ETNIA | C(4) | 4 | 0 |
| RA_TELEF | C(11) | 11 | 0 |
| RA_CELULAR | C(11) | 11 | 0 |
| RA_MOTCOB | C(2) | 2 | 0 |
| RA_DTOBAL | C(8) | 8 | 0 |
| RA_CATEND | C(2) | 2 | 0 |
| RA_CIDPRI | C(4) | 4 | 0 |
| RA_CIDCA | C(4) | 4 | 0 |
| RA_CIDSEC1 | C(4) | 4 | 0 |
| RA_CIDSEC2 | C(4) | 4 | 0 |
| RA_CIDSEC3 | C(4) | 4 | 0 |
| RA_PCNORI | C(2) | 2 | 0 |
| RA_CODESF | C(1) | 1 | 0 |
| RA_CNESESF | C(7) | 7 | 0 |
| RA_DESTPCT | C(2) | 2 | 0 |
| RA_ORG | C(3) | 3 | 0 |
| RA_STRUA | C(1) | 1 | 0 |
| RA_USUDRGA | C(1) | 1 | 0 |
| RA_TPDRGA | C(3) | 3 | 0 |
| RA_NAUTO | C(13) | 13 | 0 |
| RA_CHKSU | C(4) | 4 | 0 |
| RA_RMS | C(4) | 4 | 0 |
| RA_DTGER | C(8) | 8 | 0 |
| RA_FLER | C(10) | 10 | 0 |
| RA_INERPP | C(1) | 1 | 0 |
| RA_MVM | C(6) | 6 | 0 |
| RA_CDLOGR | C(3) | 3 | 0 |
| RA_BAIRRO | C(30) | 30 | 0 |
| RA_EMAIL | C(40) | 40 | 0 |

### `S_TRMOTC.DBF`

- 0 records
- Size: 321 bytes
- Encoding: cp1252
- Fields: 9

| Field | Type | Length | Decimal |
|---|---|---|---|
| TRMOTC_A00 | C(6) | 6 | 0 |
| TRMOTC_A01 | C(6) | 6 | 0 |
| TRMOTC_A02 | C(13) | 13 | 0 |
| TRMOTC_A03 | C(8) | 8 | 0 |
| TRMOTC_A04 | C(30) | 30 | 0 |
| TRMOTC_A05 | C(30) | 30 | 0 |
| TRMOTC_A06 | C(30) | 30 | 0 |
| TRMOTC_A07 | C(30) | 30 | 0 |
| TRMOTC_A08 | C(5) | 5 | 0 |

### `S_UPSAUT.DBF`

- 0 records
- Size: 162 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| UPSAUT_UPS | C(6) | 6 | 0 |
| UPSAUT_HA | C(4) | 4 | 0 |
| UPSAUT_INI | C(6) | 6 | 0 |
| UPSAUT_FIM | C(6) | 6 | 0 |

### `advertencia_sexo.dbf`

- 0 records
- Size: 483 bytes
- Encoding: cp1252
- Fields: 14

| Field | Type | Length | Decimal |
|---|---|---|---|
| TIPO | C(4) | 4 | 0 |
| UID | C(7) | 7 | 0 |
| CMP | C(6) | 6 | 0 |
| CNS | C(15) | 15 | 0 |
| APAC | C(13) | 13 | 0 |
| PROCEDIM | C(9) | 9 | 0 |
| DIGITO | C(1) | 1 | 0 |
| SEXO | C(1) | 1 | 0 |
| MVM | C(6) | 6 | 0 |
| CNSMED | C(15) | 15 | 0 |
| CBO | C(6) | 6 | 0 |
| FOLHA | C(3) | 3 | 0 |
| SEQ | C(2) | 2 | 0 |
| ERRO | C(1) | 1 | 0 |

### `cadgesmn.dbf`

- 0 records
- Size: 195 bytes
- Encoding: cp1252
- Fields: 5

| Field | Type | Length | Decimal |
|---|---|---|---|
| CODUF | C(2) | 2 | 0 |
| CODMUNIC | C(4) | 4 | 0 |
| NOME | C(40) | 40 | 0 |
| CONDIC | C(2) | 2 | 0 |
| CIB_SAS | C(1) | 1 | 0 |

### `cpx.dbf`

- 0 records
- Size: 962 bytes
- Encoding: cp1252
- Fields: 29

| Field | Type | Length | Decimal |
|---|---|---|---|
| PA_CMP | C(6) | 6 | 0 |
| PA_ID | C(9) | 9 | 0 |
| PA_DV | C(1) | 1 | 0 |
| PA_PAB | C(1) | 1 | 0 |
| PA_TOTAL | N(12,2) | 12 | 2 |
| PA_FAEC | C(1) | 1 | 0 |
| PA_DC | C(60) | 60 | 0 |
| PA_RUB | C(4) | 4 | 0 |
| PA_TPCC | C(1) | 1 | 0 |
| PA_AUX | C(20) | 20 | 0 |
| PA_CPX | C(4) | 4 | 0 |
| PA_CTF | C(4) | 4 | 0 |
| PA_DOC | C(1) | 1 | 0 |
| PA_IDADEMX | N(3) | 3 | 0 |
| PA_IDADEMN | N(3) | 3 | 0 |
| PA_SEXO | C(1) | 1 | 0 |
| PA_QTDMAX | N(6) | 6 | 0 |
| PA_LAUDO | C(2) | 2 | 0 |
| PA_PRINC | C(1) | 1 | 0 |
| PA_SECUN | C(1) | 1 | 0 |
| PA_IDEBPA | C(1) | 1 | 0 |
| PA_CNSPCN | C(1) | 1 | 0 |
| PA_CNRAC | C(1) | 1 | 0 |
| PA_CCMANAL | C(2) | 2 | 0 |
| PA_ELETIVA | C(1) | 1 | 0 |
| PA_APACONT | C(1) | 1 | 0 |
| PA_EXIGCBO | C(1) | 1 | 0 |
| PA_PROCCEO | C(1) | 1 | 0 |
| PA_6MESES | C(1) | 1 | 0 |

### `crit_lmc.dbf`

- 0 records
- Size: 547 bytes
- Encoding: cp1252
- Fields: 16

| Field | Type | Length | Decimal |
|---|---|---|---|
| UID | C(7) | 7 | 0 |
| CMP | C(6) | 6 | 0 |
| APAC | C(13) | 13 | 0 |
| PROCEDIM | C(9) | 9 | 0 |
| DIGITO | C(1) | 1 | 0 |
| QTD | N(6) | 6 | 0 |
| MVM | C(6) | 6 | 0 |
| FASE | C(1) | 1 | 0 |
| LINHA | C(1) | 1 | 0 |
| SUB_TOT | N(6) | 6 | 0 |
| PERC | C(6) | 6 | 0 |
| TOTAL | N(12) | 12 | 0 |
| STOT_2L | N(6) | 6 | 0 |
| PERC_2L | C(6) | 6 | 0 |
| ERROF | C(1) | 1 | 0 |
| ERROL | C(1) | 1 | 0 |

### `crit_trastuzumabe.dbf`

- 0 records
- Size: 419 bytes
- Encoding: cp1252
- Fields: 12

| Field | Type | Length | Decimal |
|---|---|---|---|
| UID | C(7) | 7 | 0 |
| CMP | C(6) | 6 | 0 |
| APAC | C(13) | 13 | 0 |
| CRITICO | C(1) | 1 | 0 |
| PROCEDIM | C(9) | 9 | 0 |
| DIGITO | C(1) | 1 | 0 |
| QTD | N(6) | 6 | 0 |
| MVM | C(6) | 6 | 0 |
| SUB_TOT | N(6) | 6 | 0 |
| PERC | C(6) | 6 | 0 |
| TOTAL | N(12) | 12 | 0 |
| ERRO | C(1) | 1 | 0 |

### `erro.dbf`

- 0 records
- Size: 386 bytes
- Encoding: cp1252
- Fields: 11

| Field | Type | Length | Decimal |
|---|---|---|---|
| VPA_PA | C(9) | 9 | 0 |
| VPA_CMP | C(6) | 6 | 0 |
| VPA_TOTAL | N(13,2) | 13 | 2 |
| VPA_SP | N(13,2) | 13 | 2 |
| VPA_SA | N(13,2) | 13 | 2 |
| VPA_SH | N(13,2) | 13 | 2 |
| VPA_MUN | C(6) | 6 | 0 |
| VPA_TIPO | C(1) | 1 | 0 |
| VPA_CTF | C(2) | 2 | 0 |
| VPA_RUB | C(6) | 6 | 0 |
| VPA_MVM | C(6) | 6 | 0 |

### `faixa.dbf`

- 0 records
- Size: 162 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| CFAINICIAL | C(12) | 12 | 0 |
| CFAFINAL | C(12) | 12 | 0 |
| CFAAMINI | C(6) | 6 | 0 |
| CFAAMFIN | C(6) | 6 | 0 |

### `pa.dbf`

- 0 records
- Size: 962 bytes
- Encoding: cp1252
- Fields: 29

| Field | Type | Length | Decimal |
|---|---|---|---|
| PA_CMP | C(6) | 6 | 0 |
| PA_ID | C(9) | 9 | 0 |
| PA_DV | C(1) | 1 | 0 |
| PA_PAB | C(1) | 1 | 0 |
| PA_TOTAL | N(12,2) | 12 | 2 |
| PA_FAEC | C(1) | 1 | 0 |
| PA_DC | C(60) | 60 | 0 |
| PA_RUB | C(4) | 4 | 0 |
| PA_TPCC | C(1) | 1 | 0 |
| PA_AUX | C(20) | 20 | 0 |
| PA_CPX | C(4) | 4 | 0 |
| PA_CTF | C(4) | 4 | 0 |
| PA_DOC | C(1) | 1 | 0 |
| PA_IDADEMX | N(3) | 3 | 0 |
| PA_IDADEMN | N(3) | 3 | 0 |
| PA_SEXO | C(1) | 1 | 0 |
| PA_QTDMAX | N(6) | 6 | 0 |
| PA_LAUDO | C(2) | 2 | 0 |
| PA_PRINC | C(1) | 1 | 0 |
| PA_SECUN | C(1) | 1 | 0 |
| PA_IDEBPA | C(1) | 1 | 0 |
| PA_CNSPCN | C(1) | 1 | 0 |
| PA_CNRAC | C(1) | 1 | 0 |
| PA_CCMANAL | C(2) | 2 | 0 |
| PA_ELETIVA | C(1) | 1 | 0 |
| PA_APACONT | C(1) | 1 | 0 |
| PA_EXIGCBO | C(1) | 1 | 0 |
| PA_PROCCEO | C(1) | 1 | 0 |
| PA_6MESES | C(1) | 1 | 0 |

### `psico.dbf`

- 0 records
- Size: 1,025 bytes
- Encoding: cp1252
- Fields: 31

| Field | Type | Length | Decimal |
|---|---|---|---|
| PA_CMP | C(6) | 6 | 0 |
| PA_ID | C(9) | 9 | 0 |
| PA_DV | C(1) | 1 | 0 |
| PA_PAB | C(1) | 1 | 0 |
| PA_TOTAL | N(12,2) | 12 | 2 |
| PA_FAEC | C(1) | 1 | 0 |
| PA_DC | C(60) | 60 | 0 |
| PA_RUB | C(4) | 4 | 0 |
| PA_TPCC | C(1) | 1 | 0 |
| PA_AUX | C(20) | 20 | 0 |
| PA_CPX | C(4) | 4 | 0 |
| PA_CTF | C(4) | 4 | 0 |
| PA_DOC | C(1) | 1 | 0 |
| PA_IDADEMX | N(3) | 3 | 0 |
| PA_IDADEMN | N(3) | 3 | 0 |
| PA_SEXO | C(1) | 1 | 0 |
| PA_QTDMAX | N(6) | 6 | 0 |
| PA_LAUDO | C(2) | 2 | 0 |
| PA_PRINC | C(1) | 1 | 0 |
| PA_SECUN | C(1) | 1 | 0 |
| PA_IDEBPA | C(1) | 1 | 0 |
| PA_CNSPCN | C(1) | 1 | 0 |
| PA_CNRAC | C(1) | 1 | 0 |
| PA_CCMANAL | C(2) | 2 | 0 |
| PA_ELETIVA | C(1) | 1 | 0 |
| PA_APACONT | C(1) | 1 | 0 |
| PA_EXIGCBO | C(1) | 1 | 0 |
| PA_PROCCEO | C(1) | 1 | 0 |
| PA_6MESES | C(1) | 1 | 0 |
| PA_EXIGAUT | C(1) | 1 | 0 |
| PA_PERMAN | N(4) | 4 | 0 |

### `s_ad.dbf`

- 0 records
- Size: 1,251 bytes
- Encoding: cp1252
- Fields: 38

| Field | Type | Length | Decimal |
|---|---|---|---|
| AD_UID | C(7) | 7 | 0 |
| AD_CMP | C(6) | 6 | 0 |
| AD_NUM | C(15) | 15 | 0 |
| AD_CNSPCT | C(15) | 15 | 0 |
| AD_DTINIC | C(8) | 8 | 0 |
| AD_DTFIM | C(8) | 8 | 0 |
| AD_NMPCN | C(30) | 30 | 0 |
| AD_NPRONT | C(10) | 10 | 0 |
| AD_NACPCN | C(3) | 3 | 0 |
| AD_MAEPCN | C(30) | 30 | 0 |
| AD_NOMERE | C(30) | 30 | 0 |
| AD_LOGPCN | C(30) | 30 | 0 |
| AD_NUMPCN | C(5) | 5 | 0 |
| AD_CPLPCN | C(10) | 10 | 0 |
| AD_CEPPCN | C(8) | 8 | 0 |
| AD_MUNPCN | C(7) | 7 | 0 |
| AD_DTNASC | C(8) | 8 | 0 |
| AD_SEXPCN | C(1) | 1 | 0 |
| AD_RACA | C(2) | 2 | 0 |
| AD_ETNIA | C(4) | 4 | 0 |
| AD_TELEF | C(11) | 11 | 0 |
| AD_CELULAR | C(11) | 11 | 0 |
| AD_MOTCOB | C(2) | 2 | 0 |
| AD_DTOBAL | C(8) | 8 | 0 |
| AD_CATEND | C(2) | 2 | 0 |
| AD_CIDPRI | C(4) | 4 | 0 |
| AD_CIDCA | C(4) | 4 | 0 |
| AD_CIDSEC1 | C(4) | 4 | 0 |
| AD_CIDSEC2 | C(4) | 4 | 0 |
| AD_CIDSEC3 | C(4) | 4 | 0 |
| AD_PCNORI | C(1) | 1 | 0 |
| AD_CODESF | C(1) | 1 | 0 |
| AD_CNESESF | C(7) | 7 | 0 |
| AD_RMS | C(4) | 4 | 0 |
| AD_FLER | C(10) | 10 | 0 |
| AD_INERPP | C(1) | 1 | 0 |
| AD_ORG | C(3) | 3 | 0 |
| AD_MVM | C(6) | 6 | 0 |

### `s_adpa.dbf`

- 0 records
- Size: 707 bytes
- Encoding: cp1252
- Fields: 21

| Field | Type | Length | Decimal |
|---|---|---|---|
| ADPA_UID | C(7) | 7 | 0 |
| ADPA_CMP | C(6) | 6 | 0 |
| ADPA_CNSPC | C(15) | 15 | 0 |
| ADPA_NUM | C(15) | 15 | 0 |
| ADPA_DTINI | C(8) | 8 | 0 |
| ADPA_PA | C(10) | 10 | 0 |
| ADPA_CBO | C(6) | 6 | 0 |
| ADPA_CNSEX | C(15) | 15 | 0 |
| ADPA_DTREA | C(8) | 8 | 0 |
| ADPA_SRV | C(3) | 3 | 0 |
| ADPA_CSF | C(3) | 3 | 0 |
| ADPA_EQUIP | C(12) | 12 | 0 |
| ADPA_QT_P | N(6) | 6 | 0 |
| ADPA_QT_A | N(6) | 6 | 0 |
| ADPA_MVM | C(6) | 6 | 0 |
| ADPA_ORG | C(3) | 3 | 0 |
| ADPA_FLPA | C(1) | 1 | 0 |
| ADPA_FLEMA | C(1) | 1 | 0 |
| ADPA_FLCBO | C(1) | 1 | 0 |
| ADPA_FLQT | C(1) | 1 | 0 |
| ADPA_FLER | C(1) | 1 | 0 |

### `s_apal.dbf`

- 0 records
- Size: 4,259 bytes
- Encoding: cp1252
- Fields: 132

| Field | Type | Length | Decimal |
|---|---|---|---|
| APA_UF | C(2) | 2 | 0 |
| APA_UID | C(7) | 7 | 0 |
| APA_NUM | C(13) | 13 | 0 |
| APA_EMISSA | C(8) | 8 | 0 |
| APA_DTINIC | C(8) | 8 | 0 |
| APA_DTFIM | C(8) | 8 | 0 |
| APA_TPATEN | C(2) | 2 | 0 |
| APA_TPAPAC | C(1) | 1 | 0 |
| APA_NMPCN | C(30) | 30 | 0 |
| APA_FILLER | C(2) | 2 | 0 |
| APA_MAEPCN | C(30) | 30 | 0 |
| APA_LOGPCN | C(30) | 30 | 0 |
| APA_NUMPCN | C(5) | 5 | 0 |
| APA_CPLPCN | C(10) | 10 | 0 |
| APA_CEPPCN | C(8) | 8 | 0 |
| APA_MUNPCN | C(7) | 7 | 0 |
| APA_DTNASC | C(8) | 8 | 0 |
| APA_SEXPCN | C(1) | 1 | 0 |
| APA_VARIA | C(141) | 141 | 0 |
| APA_CPFRES | C(11) | 11 | 0 |
| APA_NMRES | C(30) | 30 | 0 |
| PAP_PA1 | C(10) | 10 | 0 |
| PAP_AT_P1 | C(6) | 6 | 0 |
| PAP_QT_P1 | C(7) | 7 | 0 |
| PAP_PA2 | C(10) | 10 | 0 |
| PAP_AT_P2 | C(6) | 6 | 0 |
| PAP_QT_P2 | C(7) | 7 | 0 |
| PAP_PA3 | C(10) | 10 | 0 |
| PAP_AT_P3 | C(6) | 6 | 0 |
| PAP_QT_P3 | C(7) | 7 | 0 |
| PAP_PA4 | C(10) | 10 | 0 |
| PAP_AT_P4 | C(6) | 6 | 0 |
| PAP_QT_P4 | C(7) | 7 | 0 |
| PAP_PA5 | C(10) | 10 | 0 |
| PAP_AT_P5 | C(6) | 6 | 0 |
| PAP_QT_P5 | C(7) | 7 | 0 |
| PAP_PA6 | C(10) | 10 | 0 |
| PAP_AT_P6 | C(6) | 6 | 0 |
| PAP_QT_P6 | C(7) | 7 | 0 |
| PAP_PA7 | C(10) | 10 | 0 |
| PAP_AT_P7 | C(6) | 6 | 0 |
| PAP_QT_P7 | C(7) | 7 | 0 |
| PAP_PA8 | C(10) | 10 | 0 |
| PAP_AT_P8 | C(6) | 6 | 0 |
| PAP_QT_P8 | C(7) | 7 | 0 |
| PAP_PA9 | C(10) | 10 | 0 |
| PAP_AT_P9 | C(6) | 6 | 0 |
| PAP_QT_P9 | C(7) | 7 | 0 |
| PAP_PA10 | C(10) | 10 | 0 |
| PAP_AT_P10 | C(6) | 6 | 0 |
| PAP_QT_P10 | C(7) | 7 | 0 |
| APA_MOTCOB | C(2) | 2 | 0 |
| APA_DTOBAL | C(8) | 8 | 0 |
| APA_CPFDIR | C(11) | 11 | 0 |
| APA_NMDIR | C(30) | 30 | 0 |
| APA_CONT | C(1) | 1 | 0 |
| PAP_CGC1 | C(14) | 14 | 0 |
| PAP_NF1 | C(6) | 6 | 0 |
| PAP_CGC2 | C(14) | 14 | 0 |
| PAP_NF2 | C(6) | 6 | 0 |
| PAP_CGC3 | C(14) | 14 | 0 |
| PAP_NF3 | C(6) | 6 | 0 |
| PAP_CGC4 | C(14) | 14 | 0 |
| PAP_NF4 | C(6) | 6 | 0 |
| PAP_CGC5 | C(14) | 14 | 0 |
| PAP_NF5 | C(6) | 6 | 0 |
| PAP_CGC6 | C(14) | 14 | 0 |
| PAP_NF6 | C(6) | 6 | 0 |
| PAP_CGC7 | C(14) | 14 | 0 |
| PAP_NF7 | C(6) | 6 | 0 |
| PAP_CGC8 | C(14) | 14 | 0 |
| PAP_NF8 | C(6) | 6 | 0 |
| PAP_CGC9 | C(14) | 14 | 0 |
| PAP_NF9 | C(6) | 6 | 0 |
| PAP_CGC10 | C(14) | 14 | 0 |
| PAP_NF10 | C(6) | 6 | 0 |
| APA_CNSPCT | C(15) | 15 | 0 |
| APA_CNSRES | C(15) | 15 | 0 |
| APA_CNSDIR | C(15) | 15 | 0 |
| CIDPR1 | C(4) | 4 | 0 |
| CIDSE1 | C(4) | 4 | 0 |
| CIDPR2 | C(4) | 4 | 0 |
| CIDSE2 | C(4) | 4 | 0 |
| CIDPR3 | C(4) | 4 | 0 |
| CIDSE3 | C(4) | 4 | 0 |
| CIDPR4 | C(4) | 4 | 0 |
| CIDSE4 | C(4) | 4 | 0 |
| CIDPR5 | C(4) | 4 | 0 |
| CIDSE5 | C(4) | 4 | 0 |
| CIDPR6 | C(4) | 4 | 0 |
| CIDSE6 | C(4) | 4 | 0 |
| CIDPR7 | C(4) | 4 | 0 |
| CIDSE7 | C(4) | 4 | 0 |
| CIDPR8 | C(4) | 4 | 0 |
| CIDSE8 | C(4) | 4 | 0 |
| CIDPR9 | C(4) | 4 | 0 |
| CIDSE9 | C(4) | 4 | 0 |
| CIDPR10 | C(4) | 4 | 0 |
| CIDSE10 | C(4) | 4 | 0 |
| AP_CIDCA | C(4) | 4 | 0 |
| AP_NPRONT | C(10) | 10 | 0 |
| AP_CODSOL | C(7) | 7 | 0 |
| AP_DTSOLIC | C(8) | 8 | 0 |
| AP_DTAUTOR | C(8) | 8 | 0 |
| AP_CODEMIS | C(10) | 10 | 0 |
| AP_CATEND | C(2) | 2 | 0 |
| AP_APACANT | C(13) | 13 | 0 |
| APA_RACA | C(2) | 2 | 0 |
| APA_NOMERE | C(30) | 30 | 0 |
| APA_UFPCN | C(3) | 3 | 0 |
| APA_ETNIA | C(4) | 4 | 0 |
| AP_SRV | C(3) | 3 | 0 |
| AP_CSF | C(3) | 3 | 0 |
| AP_EQUIPE1 | C(12) | 12 | 0 |
| AP_EQUIPE2 | C(12) | 12 | 0 |
| AP_EQUIPE3 | C(12) | 12 | 0 |
| AP_EQUIPE4 | C(12) | 12 | 0 |
| AP_EQUIPE5 | C(12) | 12 | 0 |
| AP_EQUIPE6 | C(12) | 12 | 0 |
| AP_EQUIPE7 | C(12) | 12 | 0 |
| AP_EQUIPE8 | C(12) | 12 | 0 |
| AP_EQUIPE9 | C(12) | 12 | 0 |
| AP_EQUIPE0 | C(12) | 12 | 0 |
| AP_CDLOGR | C(3) | 3 | 0 |
| AP_BAIRRO | C(30) | 30 | 0 |
| AP_DDD | C(2) | 2 | 0 |
| AP_TEL | C(9) | 9 | 0 |
| AP_EMAIL | C(40) | 40 | 0 |
| AP_CNSEXE | C(15) | 15 | 0 |
| AP_INE | C(10) | 10 | 0 |
| APAL_SOMA | N(15) | 15 | 0 |
| APAL_X | C(25) | 25 | 0 |

### `s_cnesia.dbf`

- 0 records
- Size: 130 bytes
- Encoding: cp1252
- Fields: 3

| Field | Type | Length | Decimal |
|---|---|---|---|
| COD_CNES | C(7) | 7 | 0 |
| COD_SIA | C(7) | 7 | 0 |
| GESTAO | C(6) | 6 | 0 |

### `s_corpo.dbf`

- 0 records
- Size: 1,505 bytes
- Encoding: cp1252
- Fields: 46

| Field | Type | Length | Decimal |
|---|---|---|---|
| IDENTIF | C(2) | 2 | 0 |
| AP_CMP | C(6) | 6 | 0 |
| AP_NUMAPAC | C(13) | 13 | 0 |
| AP_CODUF | C(2) | 2 | 0 |
| AP_CODUNI | C(7) | 7 | 0 |
| AP_DTEMI | C(8) | 8 | 0 |
| AP_DTINVAL | C(8) | 8 | 0 |
| AP_DTFIVAL | C(8) | 8 | 0 |
| AP_TIPATE | C(2) | 2 | 0 |
| AP_TIPAPAC | C(1) | 1 | 0 |
| AP_NOMEPAC | C(30) | 30 | 0 |
| AP_NOMEMAE | C(30) | 30 | 0 |
| AP_ENDPAC | C(30) | 30 | 0 |
| AP_ENDNUM | C(5) | 5 | 0 |
| AP_ENDCOMP | C(10) | 10 | 0 |
| AP_ENDCEP | C(8) | 8 | 0 |
| AP_CODMUN | C(7) | 7 | 0 |
| AP_DTNASC | C(8) | 8 | 0 |
| AP_SEXO | C(1) | 1 | 0 |
| AP_NOMESOL | C(30) | 30 | 0 |
| AP_CDPROC0 | C(10) | 10 | 0 |
| AP_CODCOB | C(2) | 2 | 0 |
| AP_DTOCORR | C(8) | 8 | 0 |
| AP_NOMEDIR | C(30) | 30 | 0 |
| AP_CNS | C(15) | 15 | 0 |
| AP_CNSRES | C(15) | 15 | 0 |
| AP_CNSDIR | C(15) | 15 | 0 |
| AP_CIDCA | C(4) | 4 | 0 |
| AP_NPRONT | C(10) | 10 | 0 |
| AP_CODSOL | C(7) | 7 | 0 |
| AP_DTSOLIC | C(8) | 8 | 0 |
| AP_DTAUTOR | C(8) | 8 | 0 |
| AP_CODEMIS | C(10) | 10 | 0 |
| AP_CATEND | C(2) | 2 | 0 |
| AP_APACANT | C(13) | 13 | 0 |
| AP_RACA | C(2) | 2 | 0 |
| AP_NOMERES | C(30) | 30 | 0 |
| AP_UFNASC | C(3) | 3 | 0 |
| AP_ETNIA | C(4) | 4 | 0 |
| AP_CDLOGR | C(3) | 3 | 0 |
| AP_BAIRRO | C(30) | 30 | 0 |
| AP_DDD | C(2) | 2 | 0 |
| AP_TEL | C(9) | 9 | 0 |
| AP_EMAIL | C(40) | 40 | 0 |
| AP_CNSEXE | C(15) | 15 | 0 |
| AP_INE | C(10) | 10 | 0 |

### `s_eqesf.dbf`

- 0 records
- Size: 257 bytes
- Encoding: cp1252
- Fields: 7

| Field | Type | Length | Decimal |
|---|---|---|---|
| ESF_CMP | C(6) | 6 | 0 |
| ESF_IBGE | C(6) | 6 | 0 |
| ESF_CNES | C(7) | 7 | 0 |
| ESF_SEQ | C(8) | 8 | 0 |
| ESF_AREA | C(4) | 4 | 0 |
| ESF_NOME | C(60) | 60 | 0 |
| ESF_TIPO | C(2) | 2 | 0 |

### `s_fcd.dbf`

- 0 records
- Size: 2,050 bytes
- Encoding: cp1252
- Fields: 63

| Field | Type | Length | Decimal |
|---|---|---|---|
| FCD_ID | C(7) | 7 | 0 |
| FCD_SLHAG | N(2) | 2 | 0 |
| FCD_SLHAG_ | N(2) | 2 | 0 |
| FCD_SLDPI | N(2) | 2 | 0 |
| FCD_SLDPAC | N(2) | 2 | 0 |
| FCD_MQPROP | N(2) | 2 | 0 |
| FCD_MQOUTR | N(2) | 2 | 0 |
| FCD_MQDPI | N(2) | 2 | 0 |
| FCD_INDTR1 | C(1) | 1 | 0 |
| FCD_INDTR2 | C(1) | 1 | 0 |
| FCD_INDTR3 | C(1) | 1 | 0 |
| FCD_INDTR4 | C(1) | 1 | 0 |
| FCD_INDTR5 | C(1) | 1 | 0 |
| FCD_INDTR6 | C(1) | 1 | 0 |
| FCD_HTPMN | C(7) | 7 | 0 |
| FCD_HTPCGC | C(14) | 14 | 0 |
| FCD_HTPRZ | C(35) | 35 | 0 |
| FCD_LHCMN | C(7) | 7 | 0 |
| FCD_LHCCGC | C(14) | 14 | 0 |
| FCD_LHCRZ | C(35) | 35 | 0 |
| FCD_HGMN1 | C(7) | 7 | 0 |
| FCD_HGCGC1 | C(14) | 14 | 0 |
| FCD_HGRZ1 | C(35) | 35 | 0 |
| FCD_HGMN2 | C(7) | 7 | 0 |
| FCD_HGCGC2 | C(14) | 14 | 0 |
| FCD_HGRZ2 | C(35) | 35 | 0 |
| FCD_PLMN1 | C(7) | 7 | 0 |
| FCD_PLCGC1 | C(14) | 14 | 0 |
| FCD_PLRZ1 | C(35) | 35 | 0 |
| FCD_PLMN2 | C(7) | 7 | 0 |
| FCD_PLCGC2 | C(14) | 14 | 0 |
| FCD_PLRZ2 | C(35) | 35 | 0 |
| FCD_MEDMN | C(7) | 7 | 0 |
| FCD_MEDCGC | C(14) | 14 | 0 |
| FCD_MEDRZ | C(35) | 35 | 0 |
| FCD_MTAMN | C(7) | 7 | 0 |
| FCD_MTACGC | C(14) | 14 | 0 |
| FCD_MTARZ | C(35) | 35 | 0 |
| FCD_LAAMN | C(7) | 7 | 0 |
| FCD_LAACGC | C(14) | 14 | 0 |
| FCD_LAARZ | C(35) | 35 | 0 |
| FCD_RDMN1 | C(7) | 7 | 0 |
| FCD_RDCGC1 | C(14) | 14 | 0 |
| FCD_RDRZ1 | C(35) | 35 | 0 |
| FCD_RDMN2 | C(7) | 7 | 0 |
| FCD_RDCGC2 | C(14) | 14 | 0 |
| FCD_RDRZ2 | C(35) | 35 | 0 |
| FCD_USMN1 | C(7) | 7 | 0 |
| FCD_USCGC1 | C(14) | 14 | 0 |
| FCD_USRZ1 | C(35) | 35 | 0 |
| FCD_USMN2 | C(7) | 7 | 0 |
| FCD_USCGC2 | C(14) | 14 | 0 |
| FCD_USRZ2 | C(35) | 35 | 0 |
| FCD_APMN1 | C(7) | 7 | 0 |
| FCD_APCGC1 | C(14) | 14 | 0 |
| FCD_APRZ1 | C(35) | 35 | 0 |
| FCD_APMN2 | C(7) | 7 | 0 |
| FCD_APCGC2 | C(14) | 14 | 0 |
| FCD_APRZ2 | C(35) | 35 | 0 |
| FCD_NFRNM | C(35) | 35 | 0 |
| FCD_NFRCPF | C(11) | 11 | 0 |
| FCD_DIRNM | C(35) | 35 | 0 |
| FCD_DIRCPF | C(11) | 11 | 0 |

### `s_fco.dbf`

- 0 records
- Size: 2,434 bytes
- Encoding: cp1252
- Fields: 75

| Field | Type | Length | Decimal |
|---|---|---|---|
| FCO_ID | C(7) | 7 | 0 |
| FCO_SLRSIM | N(2) | 2 | 0 |
| FCO_SLRPLA | N(2) | 2 | 0 |
| FCO_SLRARF | N(2) | 2 | 0 |
| FCO_SLRCOM | N(2) | 2 | 0 |
| FCO_SLRMOL | N(2) | 2 | 0 |
| FCO_SLRBLP | N(2) | 2 | 0 |
| FCO_SLQARM | N(2) | 2 | 0 |
| FCO_SLQPRE | N(2) | 2 | 0 |
| FCO_SLQCDU | N(2) | 2 | 0 |
| FCO_SLQLDU | N(2) | 2 | 0 |
| FCO_SLQCFL | N(2) | 2 | 0 |
| FCO_RSIMUL | N(2) | 2 | 0 |
| FCO_RAL6MV | N(2) | 2 | 0 |
| FCO_RALM6S | N(2) | 2 | 0 |
| FCO_RALM6C | N(2) | 2 | 0 |
| FCO_RO50K | N(2) | 2 | 0 |
| FCO_RO150K | N(2) | 2 | 0 |
| FCO_RO500K | N(2) | 2 | 0 |
| FCO_RUNCOB | N(2) | 2 | 0 |
| FCO_RBRAQB | N(2) | 2 | 0 |
| FCO_RBRAQM | N(2) | 2 | 0 |
| FCO_RBRAQA | N(2) | 2 | 0 |
| FCO_RMONAR | N(2) | 2 | 0 |
| FCO_RMONIN | N(2) | 2 | 0 |
| FCO_RSICPL | N(2) | 2 | 0 |
| FCO_RDOSCL | N(2) | 2 | 0 |
| FCO_RFONSE | N(2) | 2 | 0 |
| FCO_RADMN | C(7) | 7 | 0 |
| FCO_RADCGC | C(14) | 14 | 0 |
| FCO_RADRZ | C(35) | 35 | 0 |
| FCO_LHCMN | C(7) | 7 | 0 |
| FCO_LHCCGC | C(14) | 14 | 0 |
| FCO_LHCRZ | C(35) | 35 | 0 |
| FCO_TACMN | C(7) | 7 | 0 |
| FCO_TACCGC | C(14) | 14 | 0 |
| FCO_TACRZ | C(35) | 35 | 0 |
| FCO_RMMN | C(7) | 7 | 0 |
| FCO_RMCGC | C(14) | 14 | 0 |
| FCO_RMRZ | C(35) | 35 | 0 |
| FCO_APCMN | C(7) | 7 | 0 |
| FCO_APCCGC | C(14) | 14 | 0 |
| FCO_APCRZ | C(35) | 35 | 0 |
| FCO_PCMN | C(7) | 7 | 0 |
| FCO_PCCGC | C(14) | 14 | 0 |
| FCO_PCRZ | C(35) | 35 | 0 |
| FCO_USMN | C(7) | 7 | 0 |
| FCO_USCGC | C(14) | 14 | 0 |
| FCO_USRZ | C(35) | 35 | 0 |
| FCO_MNMN | C(7) | 7 | 0 |
| FCO_MNCGC | C(14) | 14 | 0 |
| FCO_MNRZ | C(35) | 35 | 0 |
| FCO_PRMN | C(7) | 7 | 0 |
| FCO_PRCGC | C(14) | 14 | 0 |
| FCO_PRRZ | C(35) | 35 | 0 |
| FCO_MEMN | C(7) | 7 | 0 |
| FCO_MECGC | C(14) | 14 | 0 |
| FCO_MERZ | C(35) | 35 | 0 |
| FCO_QTMN | C(7) | 7 | 0 |
| FCO_QTCGC | C(14) | 14 | 0 |
| FCO_QTRZ | C(35) | 35 | 0 |
| FCO_QTMN1 | C(7) | 7 | 0 |
| FCO_QTCGC1 | C(14) | 14 | 0 |
| FCO_QTRZ1 | C(35) | 35 | 0 |
| FCO_QTMN2 | C(7) | 7 | 0 |
| FCO_QTCGC2 | C(14) | 14 | 0 |
| FCO_QTRZ2 | C(35) | 35 | 0 |
| FCO_MRACPF | C(11) | 11 | 0 |
| FCO_MRANM | C(35) | 35 | 0 |
| FCO_MROCPF | C(11) | 11 | 0 |
| FCO_MRONM | C(35) | 35 | 0 |
| FCO_MOCCPF | C(11) | 11 | 0 |
| FCO_MOCNM | C(35) | 35 | 0 |
| FCO_MRRCPF | C(11) | 11 | 0 |
| FCO_MRRNM | C(35) | 35 | 0 |

### `s_imperr.dbf`

- 0 records
- Size: 195 bytes
- Encoding: cp1252
- Fields: 5

| Field | Type | Length | Decimal |
|---|---|---|---|
| ID_ERRO | C(6) | 6 | 0 |
| AP_CMP | C(6) | 6 | 0 |
| APA_UF | C(2) | 2 | 0 |
| AP_NUMAPAC | C(13) | 13 | 0 |
| AP_CDPROC | C(10) | 10 | 0 |

### `s_ipul.dbf`

- 0 records
- Size: 449 bytes
- Encoding: cp1252
- Fields: 13

| Field | Type | Length | Decimal |
|---|---|---|---|
| IPUL_UPS | C(7) | 7 | 0 |
| IPUL_CMP | C(6) | 6 | 0 |
| IPUL_TPFIN | C(1) | 1 | 0 |
| IPUL_PA | C(9) | 9 | 0 |
| IPUL_QT_O | N(8) | 8 | 0 |
| IPUL_NAPU | C(1) | 1 | 0 |
| IPUL_VU_O | N(12,2) | 12 | 2 |
| IPUL_VL_O | N(12,2) | 12 | 2 |
| IPUL_ARQ | C(12) | 12 | 0 |
| IPUL_VERS | C(5) | 5 | 0 |
| IPUL_CHK1 | C(4) | 4 | 0 |
| IPUL_CHK2 | C(4) | 4 | 0 |
| IPUL_LINE | N(6) | 6 | 0 |

### `s_iput.dbf`

- 0 records
- Size: 227 bytes
- Encoding: cp1252
- Fields: 6

| Field | Type | Length | Decimal |
|---|---|---|---|
| IPUL_UPS | C(7) | 7 | 0 |
| IPUL_CMP | C(6) | 6 | 0 |
| IPUL_PA | C(9) | 9 | 0 |
| IPUL_QT_O | N(8) | 8 | 0 |
| IPUL_NAPU | C(1) | 1 | 0 |
| IPUL_ARQ | C(12) | 12 | 0 |

### `s_prdl.dbf`

- 0 records
- Size: 1,251 bytes
- Encoding: cp1252
- Fields: 38

| Field | Type | Length | Decimal |
|---|---|---|---|
| PRDL_UPS | C(7) | 7 | 0 |
| PRDL_CMP | C(6) | 6 | 0 |
| PRDL_CNSME | C(15) | 15 | 0 |
| PRDL_CBO | C(6) | 6 | 0 |
| PRDL_DTATE | C(8) | 8 | 0 |
| PRDL_FLH | C(3) | 3 | 0 |
| PRDL_SEQ | C(2) | 2 | 0 |
| PRDL_PA | C(10) | 10 | 0 |
| PRDL_CNSPA | C(15) | 15 | 0 |
| PRDL_SEXO | C(1) | 1 | 0 |
| PRDL_IBGE | C(6) | 6 | 0 |
| PRDL_CID | C(4) | 4 | 0 |
| PRDL_IDADE | N(3) | 3 | 0 |
| PRDL_QT_P | N(6) | 6 | 0 |
| PRDL_CATEN | C(2) | 2 | 0 |
| PRDL_NAUT | C(13) | 13 | 0 |
| PRDL_ORG | C(3) | 3 | 0 |
| PRDL_NMPAC | C(30) | 30 | 0 |
| PRDL_DTNAS | C(8) | 8 | 0 |
| PRDL_TIPO | C(1) | 1 | 0 |
| PRDL_RACA | C(2) | 2 | 0 |
| PRDL_ETNIA | C(4) | 4 | 0 |
| PRDL_NACIO | C(3) | 3 | 0 |
| PRDL_SRV | C(3) | 3 | 0 |
| PRDL_CSF | C(3) | 3 | 0 |
| PRDL_EQUIP | C(12) | 12 | 0 |
| PRDL_CNPJ | C(14) | 14 | 0 |
| BPI_CEPPCN | C(8) | 8 | 0 |
| BPI_CDLOGR | C(3) | 3 | 0 |
| BPI_LOGPCN | C(30) | 30 | 0 |
| BPI_CPLPCN | C(10) | 10 | 0 |
| BPI_NUMPCN | C(5) | 5 | 0 |
| BPI_BAIRRO | C(30) | 30 | 0 |
| BPI_DDD | C(2) | 2 | 0 |
| BPI_TEL | C(9) | 9 | 0 |
| BPI_EMAIL | C(40) | 40 | 0 |
| BPI_INE | C(40) | 40 | 0 |
| CBO_ANT | C(6) | 6 | 0 |

### `s_prdlc.dbf`

- 0 records
- Size: 355 bytes
- Encoding: cp1252
- Fields: 10

| Field | Type | Length | Decimal |
|---|---|---|---|
| IDENTIF | C(2) | 2 | 0 |
| PRDL_UPS | C(7) | 7 | 0 |
| PRDL_CMP | C(6) | 6 | 0 |
| PRDL_CBO | C(6) | 6 | 0 |
| PRDL_FLH | C(3) | 3 | 0 |
| PRDL_SEQ | C(2) | 2 | 0 |
| PRDL_PA | C(10) | 10 | 0 |
| PRDL_IDADE | N(3) | 3 | 0 |
| PRDL_QT_P | N(6) | 6 | 0 |
| PRDL_ORG | C(3) | 3 | 0 |

### `s_prdli.dbf`

- 0 records
- Size: 1,283 bytes
- Encoding: cp1252
- Fields: 39

| Field | Type | Length | Decimal |
|---|---|---|---|
| IDENTIF | C(2) | 2 | 0 |
| PRDL_UPS | C(7) | 7 | 0 |
| PRDL_CMP | C(6) | 6 | 0 |
| PRDL_CNSME | C(15) | 15 | 0 |
| PRDL_CBO | C(6) | 6 | 0 |
| PRDL_DTATE | C(8) | 8 | 0 |
| PRDL_FLH | C(3) | 3 | 0 |
| PRDL_SEQ | C(2) | 2 | 0 |
| PRDL_PA | C(10) | 10 | 0 |
| PRDL_CNSPA | C(15) | 15 | 0 |
| PRDL_SEXO | C(1) | 1 | 0 |
| PRDL_IBGE | C(6) | 6 | 0 |
| PRDL_CID | C(4) | 4 | 0 |
| PRDL_IDADE | N(3) | 3 | 0 |
| PRDL_QT_P | N(6) | 6 | 0 |
| PRDL_CATEN | C(2) | 2 | 0 |
| PRDL_NAUT | C(13) | 13 | 0 |
| PRDL_ORG | C(3) | 3 | 0 |
| PRDL_NMPAC | C(30) | 30 | 0 |
| PRDL_DTNAS | C(8) | 8 | 0 |
| PRDL_RACA | C(2) | 2 | 0 |
| PRDL_ETNIA | C(4) | 4 | 0 |
| PRDL_NACIO | C(3) | 3 | 0 |
| PRDL_SRV | C(3) | 3 | 0 |
| PRDL_CSF | C(3) | 3 | 0 |
| PRDL_EQUIP | C(12) | 12 | 0 |
| PRDL_CNPJ | C(14) | 14 | 0 |
| BPI_CEPPCN | C(8) | 8 | 0 |
| BPI_CDLOGR | C(3) | 3 | 0 |
| BPI_LOGPCN | C(30) | 30 | 0 |
| BPI_CPLPCN | C(10) | 10 | 0 |
| BPI_NUMPCN | C(5) | 5 | 0 |
| BPI_BAIRRO | C(30) | 30 | 0 |
| BPI_DDD | C(2) | 2 | 0 |
| BPI_TEL | C(9) | 9 | 0 |
| BPI_EMAIL | C(40) | 40 | 0 |
| BPI_INE | C(10) | 10 | 0 |
| T1 | N(15) | 15 | 0 |
| T2 | N(2) | 2 | 0 |

### `s_prdlt.dbf`

- 0 records
- Size: 195 bytes
- Encoding: cp1252
- Fields: 5

| Field | Type | Length | Decimal |
|---|---|---|---|
| ID | N(1) | 1 | 0 |
| PRDL_PA | N(10) | 10 | 0 |
| PRDL_QT_P | N(6) | 6 | 0 |
| T1 | N(2) | 2 | 0 |
| S1 | N(17) | 17 | 0 |

### `s_prdrep.dbf`

- 0 records
- Size: 1,635 bytes
- Encoding: cp1252
- Fields: 50

| Field | Type | Length | Decimal |
|---|---|---|---|
| PRD_UID | C(7) | 7 | 0 |
| PRD_CMP | C(6) | 6 | 0 |
| PRD_FLH | C(3) | 3 | 0 |
| PRD_SEQ | C(2) | 2 | 0 |
| PRD_PA | C(10) | 10 | 0 |
| PRD_CBO | C(6) | 6 | 0 |
| PRD_IDADE | N(3) | 3 | 0 |
| PRD_QT_P | N(6) | 6 | 0 |
| PRD_QT_A | N(6) | 6 | 0 |
| PRD_VL_P | N(15,2) | 15 | 2 |
| PRD_VL_A | N(15,2) | 15 | 2 |
| PRD_MVM | C(6) | 6 | 0 |
| PRD_ORG | C(3) | 3 | 0 |
| PRD_FLPA | C(1) | 1 | 0 |
| PRD_FLCBO | C(1) | 1 | 0 |
| PRD_FLCA | C(1) | 1 | 0 |
| PRD_FLIDA | C(1) | 1 | 0 |
| PRD_FLQT | C(1) | 1 | 0 |
| PRD_FLER | C(1) | 1 | 0 |
| PRD_APANUM | C(13) | 13 | 0 |
| PRD_CNSMED | C(15) | 15 | 0 |
| PRD_RMS | C(4) | 4 | 0 |
| PRD_CNPJ | C(14) | 14 | 0 |
| PRD_NFIS | C(6) | 6 | 0 |
| PRD_RESID | C(6) | 6 | 0 |
| PRD_RUB | C(6) | 6 | 0 |
| PRD_TPFIN | C(1) | 1 | 0 |
| PRD_CPX | C(1) | 1 | 0 |
| PRD_QTDATR | N(6) | 6 | 0 |
| PRD_QTDATU | N(6) | 6 | 0 |
| PRD_RC | C(4) | 4 | 0 |
| PRD_CIDPRI | C(6) | 6 | 0 |
| PRD_CIDSEC | C(6) | 6 | 0 |
| PRD_CIDCAS | C(6) | 6 | 0 |
| PRD_INCOUT | C(4) | 4 | 0 |
| PRD_INCURG | C(4) | 4 | 0 |
| PRD_INSRG | C(3) | 3 | 0 |
| PRD_CNSPCN | C(15) | 15 | 0 |
| PRD_DTINI | C(8) | 8 | 0 |
| PRD_DTREA | C(8) | 8 | 0 |
| PRD_SRV | C(3) | 3 | 0 |
| PRD_CSF | C(3) | 3 | 0 |
| PRD_EQUIP | C(12) | 12 | 0 |
| PRD_VL_FED | N(10,2) | 10 | 2 |
| PRD_VL_LOC | N(10,2) | 10 | 2 |
| PRD_VL_INC | N(10,2) | 10 | 2 |
| PRD_RUBFED | C(6) | 6 | 0 |
| PRD_LREX | C(1) | 1 | 0 |
| PRD_TEMP | C(20) | 20 | 0 |
| PRD_MVMORI | C(6) | 6 | 0 |

### `s_proc.dbf`

- 0 records
- Size: 449 bytes
- Encoding: cp1252
- Fields: 13

| Field | Type | Length | Decimal |
|---|---|---|---|
| IDENTIF | C(2) | 2 | 0 |
| AP_CMP | C(6) | 6 | 0 |
| AP_NUMAPAC | C(13) | 13 | 0 |
| AP_CDPROC | C(10) | 10 | 0 |
| AP_CDATIV | C(6) | 6 | 0 |
| AP_QTPROC | C(7) | 7 | 0 |
| AP_CNPJ | C(14) | 14 | 0 |
| AP_NFISC | C(6) | 6 | 0 |
| AP_CIDPRI | C(4) | 4 | 0 |
| AP_CIDSEC | C(4) | 4 | 0 |
| AP_SRV | C(3) | 3 | 0 |
| AP_CSF | C(3) | 3 | 0 |
| AP_EQUIPE | C(12) | 12 | 0 |

### `s_rapal.dbf`

- 0 records
- Size: 675 bytes
- Encoding: cp1252
- Fields: 20

| Field | Type | Length | Decimal |
|---|---|---|---|
| COD_LIN | C(2) | 2 | 0 |
| RAPA_UF | C(2) | 2 | 0 |
| RAPA_CMP | C(6) | 6 | 0 |
| RAPA_UID | C(7) | 7 | 0 |
| RAPA_CNSPC | C(15) | 15 | 0 |
| RAPA_DTINI | C(8) | 8 | 0 |
| RAPA_PA | C(10) | 10 | 0 |
| RAPA_CBO | C(6) | 6 | 0 |
| RAPA_CNSEX | C(15) | 15 | 0 |
| RAPA_DTREA | C(8) | 8 | 0 |
| RAPA_SRV | C(3) | 3 | 0 |
| RAPA_CSF | C(3) | 3 | 0 |
| RAPA_EQUIP | C(12) | 12 | 0 |
| RAPA_QT_P | N(6) | 6 | 0 |
| RAPA_ORG | C(3) | 3 | 0 |
| RAPA_CID | C(4) | 4 | 0 |
| RAPA_CHKSU | C(4) | 4 | 0 |
| RAPA_MVM | C(6) | 6 | 0 |
| X | C(30) | 30 | 0 |
| RAPA_INSRG | C(3) | 3 | 0 |

### `s_sce.dbf`

- 0 records
- Size: 162 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| SCE_SRV | C(3) | 3 | 0 |
| SCE_CSF | C(3) | 3 | 0 |
| SCE_EMAS | C(200) | 200 | 0 |
| SCE_TEMP | C(3) | 3 | 0 |

### `s_trab.dbf`

- 0 records
- Size: 1,443 bytes
- Encoding: cp1252
- Fields: 44

| Field | Type | Length | Decimal |
|---|---|---|---|
| TRAB_INSRG | C(3) | 3 | 0 |
| TRAB_UID | C(7) | 7 | 0 |
| TRAB_CMP | C(6) | 6 | 0 |
| TRAB_CNSPC | C(15) | 15 | 0 |
| TRAB_DTINI | C(8) | 8 | 0 |
| TRAB_DTFIM | C(8) | 8 | 0 |
| TRAB_NMPCN | C(30) | 30 | 0 |
| TRAB_NPRON | C(10) | 10 | 0 |
| TRAB_NACPC | C(3) | 3 | 0 |
| TRAB_MAEPC | C(30) | 30 | 0 |
| TRAB_NOMER | C(30) | 30 | 0 |
| TRAB_LOGPC | C(30) | 30 | 0 |
| TRAB_NUMPC | C(5) | 5 | 0 |
| TRAB_CPLPC | C(10) | 10 | 0 |
| TRAB_CEPPC | C(8) | 8 | 0 |
| TRAB_MUNPC | C(7) | 7 | 0 |
| TRAB_DTNAS | C(8) | 8 | 0 |
| TRAB_SEXPC | C(1) | 1 | 0 |
| TRAB_RACA | C(2) | 2 | 0 |
| TRAB_ETNIA | C(4) | 4 | 0 |
| TRAB_TELEF | C(11) | 11 | 0 |
| TRAB_CELUL | C(11) | 11 | 0 |
| TRAB_CDLOG | C(3) | 3 | 0 |
| TRAB_BAIRR | C(30) | 30 | 0 |
| TRAB_EMAIL | C(40) | 40 | 0 |
| TRAB_MOTCO | C(2) | 2 | 0 |
| TRAB_DTOBA | C(8) | 8 | 0 |
| TRAB_CATEN | C(2) | 2 | 0 |
| TRAB_CIDPR | C(4) | 4 | 0 |
| TRAB_CIDCA | C(4) | 4 | 0 |
| TRAB_CIDS1 | C(4) | 4 | 0 |
| TRAB_CIDS2 | C(4) | 4 | 0 |
| TRAB_CIDS3 | C(4) | 4 | 0 |
| TRAB_PCNOR | C(1) | 1 | 0 |
| TRAB_CODES | C(1) | 1 | 0 |
| TRAB_CNESE | C(7) | 7 | 0 |
| TRAB_DESTP | C(2) | 2 | 0 |
| TRAB_ORG | C(3) | 3 | 0 |
| TRAB_CHKSU | C(4) | 4 | 0 |
| TRAB_RMS | C(4) | 4 | 0 |
| TRAB_DTGER | C(8) | 8 | 0 |
| TRAB_FLER | C(10) | 10 | 0 |
| TRAB_INERP | C(1) | 1 | 0 |
| TRAB_MVM | C(6) | 6 | 0 |

### `s_upsval.dbf`

- 0 records
- Size: 67 bytes
- Encoding: cp1252
- Fields: 1

| Field | Type | Length | Decimal |
|---|---|---|---|
| CO_CNES | C(7) | 7 | 0 |

### `s_varia.dbf`

- 0 records
- Size: 161 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| IDENTIF | C(2) | 2 | 0 |
| AP_CMP | C(6) | 6 | 0 |
| AP_NUMAPAC | C(13) | 13 | 0 |
| AP_VARIA | C(140) | 140 | 0 |

### `s_vpal.dbf`

- 0 records
- Size: 385 bytes
- Encoding: cp1252
- Fields: 11

| Field | Type | Length | Decimal |
|---|---|---|---|
| VPAL_CMP | C(6) | 6 | 0 |
| VPAL_PA | C(9) | 9 | 0 |
| VPAL_ID | C(1) | 1 | 0 |
| VPA_MUN | C(6) | 6 | 0 |
| VPA_ZERO | C(1) | 1 | 0 |
| VPA_TIPO | C(1) | 1 | 0 |
| VPAL_SA | N(8) | 8 | 0 |
| VPAL_SH | N(8) | 8 | 0 |
| VPAL_SP | N(8) | 8 | 0 |
| VPA_CTF | C(2) | 2 | 0 |
| VPA_RUB | C(6) | 6 | 0 |

### `temu.dbf`

- 0 records
- Size: 193 bytes
- Encoding: cp1252
- Fields: 5

| Field | Type | Length | Decimal |
|---|---|---|---|
| EMU_UID | C(7) | 7 | 0 |
| EMU_CMP | C(6) | 6 | 0 |
| EMU_EMA | C(6) | 6 | 0 |
| EMU_QT_PR | N(3) | 3 | 0 |
| EMU_QT_HR | N(5) | 5 | 0 |

### `tsrv.dbf`

- 0 records
- Size: 161 bytes
- Encoding: cp1252
- Fields: 4

| Field | Type | Length | Decimal |
|---|---|---|---|
| SRV_UID | C(7) | 7 | 0 |
| SRV_CMP | C(6) | 6 | 0 |
| SRV_SR | C(3) | 3 | 0 |
| SRV_CSF | C(3) | 3 | 0 |

### `ttvpa.dbf`

- 0 records
- Size: 385 bytes
- Encoding: cp1252
- Fields: 11

| Field | Type | Length | Decimal |
|---|---|---|---|
| VPA_PA | C(9) | 9 | 0 |
| VPA_CMP | C(6) | 6 | 0 |
| VPA_TOTAL | N(13,2) | 13 | 2 |
| VPA_SP | N(13,2) | 13 | 2 |
| VPA_SA | N(13,2) | 13 | 2 |
| VPA_SH | N(13,2) | 13 | 2 |
| VPA_MUN | C(6) | 6 | 0 |
| VPA_TIPO | C(1) | 1 | 0 |
| VPA_CTF | C(2) | 2 | 0 |
| VPA_RUB | C(6) | 6 | 0 |
| VPA_MVM | C(6) | 6 | 0 |

### `vepe.dbf`

- 0 records
- Size: 257 bytes
- Encoding: cp1252
- Fields: 7

| Field | Type | Length | Decimal |
|---|---|---|---|
| ORDEM | C(6) | 6 | 0 |
| REGRAC | C(4) | 4 | 0 |
| TP_FINANC | C(2) | 2 | 0 |
| CD_RUB | C(4) | 4 | 0 |
| CPX | C(1) | 1 | 0 |
| TTOTAL | N(15,2) | 15 | 2 |
| DESCRICAO | C(40) | 40 | 0 |

### `versaomn.dbf`

- 0 records
- Size: 257 bytes
- Encoding: cp1252
- Fields: 7

| Field | Type | Length | Decimal |
|---|---|---|---|
| CMP | C(6) | 6 | 0 |
| FILL1 | C(1) | 1 | 0 |
| DEPARA | C(5) | 5 | 0 |
| FILL2 | C(1) | 1 | 0 |
| BDSIA | C(7) | 7 | 0 |
| FILL3 | C(1) | 1 | 0 |
| SIA | C(5) | 5 | 0 |

### `vig.dbf`

- 0 records
- Size: 993 bytes
- Encoding: cp1252
- Fields: 30

| Field | Type | Length | Decimal |
|---|---|---|---|
| PA_CMP | C(6) | 6 | 0 |
| PA_ID | C(9) | 9 | 0 |
| PA_DV | C(1) | 1 | 0 |
| PA_PAB | C(1) | 1 | 0 |
| PA_TOTAL | N(12,2) | 12 | 2 |
| PA_FAEC | C(1) | 1 | 0 |
| PA_DC | C(60) | 60 | 0 |
| PA_RUB | C(4) | 4 | 0 |
| PA_TPCC | C(1) | 1 | 0 |
| PA_AUX | C(20) | 20 | 0 |
| PA_CPX | C(4) | 4 | 0 |
| PA_CTF | C(4) | 4 | 0 |
| PA_DOC | C(1) | 1 | 0 |
| PA_IDADEMX | N(3) | 3 | 0 |
| PA_IDADEMN | N(3) | 3 | 0 |
| PA_SEXO | C(1) | 1 | 0 |
| PA_QTDMAX | N(6) | 6 | 0 |
| PA_LAUDO | C(2) | 2 | 0 |
| PA_PRINC | C(1) | 1 | 0 |
| PA_SECUN | C(1) | 1 | 0 |
| PA_IDEBPA | C(1) | 1 | 0 |
| PA_CNSPCN | C(1) | 1 | 0 |
| PA_CNRAC | C(1) | 1 | 0 |
| PA_CCMANAL | C(2) | 2 | 0 |
| PA_ELETIVA | C(1) | 1 | 0 |
| PA_APACONT | C(1) | 1 | 0 |
| PA_EXIGCBO | C(1) | 1 | 0 |
| PA_PROCCEO | C(1) | 1 | 0 |
| PA_6MESES | C(1) | 1 | 0 |
| PA_EXIGAUT | C(1) | 1 | 0 |
