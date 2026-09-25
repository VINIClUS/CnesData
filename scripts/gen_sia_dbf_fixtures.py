"""Gera SIA DBF fixtures sintéticos com o layout real SIASUS (determinísticos via seed)."""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import random
import struct
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Layouts copied from docs/data-dictionary-sia.md (real SIASUS introspection,
# 2026-09-25) as NAME:TYPE LENGTH.DECIMALS; test_gen_sia_dbf checks the match.
_LAYOUTS: dict[str, str] = {
    "S_APA.DBF": (
    "APA_UID:C7.0 APA_NUM:C13.0 APA_EMISSA:C8.0 APA_DTINIC:C8.0 APA_DTFIM:C8.0 "
    "APA_TPATEN:C2.0 APA_TPAPAC:C1.0 APA_NMPCN:C30.0 APA_UFPCN:C3.0 APA_MAEPCN:C30.0 "
    "APA_LOGPCN:C30.0 APA_NUMPCN:C5.0 APA_CPLPCN:C10.0 APA_CEPPCN:C8.0 APA_MUNPCN:C7.0 "
    "APA_DTNASC:C8.0 APA_SEXPCN:C1.0 APA_VARIA:C141.0 APA_CPFRES:C11.0 APA_NMRES:C30.0 "
    "APA_MOTCOB:C2.0 APA_DTOBAL:C8.0 APA_CPFDIR:C11.0 APA_NMDIR:C30.0 APA_CMP:C6.0 "
    "APA_MVM:C6.0 APA_RMS:C4.0 APA_DTGER:C8.0 APA_FLER:C10.0 APA_INERPP:C1.0 "
    "APA_PRIPAL:C9.0 APA_CPFPCT:C11.0 APA_CNSPCT:C15.0 APA_CNSRES:C15.0 APA_CNSDIR:C15.0 "
    "APA_CIDCA:C4.0 APA_NPRONT:C10.0 APA_CODSOL:C7.0 APA_DTSOL:C8.0 APA_DTAUT:C8.0 "
    "APA_CODEMI:C10.0 APA_CATEND:C2.0 APA_APACAN:C14.0 APA_RACA:C2.0 APA_NOMERE:C30.0 "
    "APA_ETNIA:C4.0 APA_ADVLMC:C1.0 APA_ADVTZM:C1.0 APA_SRV:C3.0 APA_CSF:C3.0 "
    "APA_CDLOGR:C3.0 APA_BAIRRO:C30.0 APA_DDD:C2.0 APA_TEL:C9.0 APA_EMAIL:C40.0 "
    "APA_CNSEXE:C15.0 APA_INE:C10.0 APA_ADVSEX:C1.0 APA_EXPMAE:C1.0 APA_STRUA:C1.0 "
    "APA_FNTORC:C2.0 APA_EMEPAR:C1.0 APA_SEMCPF:C1.0 "
    ),
    "S_PRD.DBF": (
    "PRD_UID:C7.0 PRD_CMP:C6.0 PRD_FLH:C3.0 PRD_SEQ:C2.0 PRD_PA:C10.0 PRD_CBO:C6.0 "
    "PRD_IDADE:N3.0 PRD_QT_P:N6.0 PRD_QT_A:N6.0 PRD_VL_P:N15.2 PRD_VL_A:N15.2 "
    "PRD_MVM:C6.0 PRD_ORG:C3.0 PRD_FLPA:C1.0 PRD_FLCBO:C1.0 PRD_FLCA:C1.0 PRD_FLIDA:C1.0 "
    "PRD_FLQT:C1.0 PRD_FLER:C1.0 PRD_APANUM:C13.0 PRD_CNSMED:C15.0 PRD_RMS:C4.0 "
    "PRD_CNPJ:C14.0 PRD_NFIS:C6.0 PRD_RESID:C6.0 PRD_RUB:C6.0 PRD_TPFIN:C1.0 "
    "PRD_CPX:C1.0 PRD_QTDATR:N6.0 PRD_QTDATU:N6.0 PRD_RC:C4.0 PRD_CIDPRI:C6.0 "
    "PRD_CIDSEC:C6.0 PRD_CIDCAS:C6.0 PRD_INCOUT:C4.0 PRD_INCURG:C4.0 PRD_INSRG:C3.0 "
    "PRD_CPFPCT:C11.0 PRD_CNSPCN:C15.0 PRD_DTINI:C8.0 PRD_DTREA:C8.0 PRD_SRV:C3.0 "
    "PRD_CSF:C3.0 PRD_EQUIP:C12.0 PRD_VL_FED:N10.2 PRD_VL_LOC:N10.2 PRD_VL_INC:N10.2 "
    "PRD_VL_CRD:N10.2 PRD_RUBFED:C6.0 PRD_LREX:C1.0 PRD_INE:C10.0 PRD_UNTERC:C7.0 "
    "PRD_STCRED:C1.0 PRD_CHKSM:C6.0 PRD_TEMP:C20.0 "
    ),
    "S_BPI.DBF": (
    "BPI_UID:C7.0 BPI_CMP:C6.0 BPI_CNSMED:C15.0 BPI_CBO:C6.0 BPI_FLH:C3.0 BPI_SEQ:C2.0 "
    "BPI_PA:C10.0 BPI_CPFPCT:C11.0 BPI_CNSPAC:C15.0 BPI_NMPAC:C30.0 BPI_DTNASC:C8.0 "
    "BPI_SEXO:C1.0 BPI_IBGE:C6.0 BPI_DTATEN:C8.0 BPI_CID:C4.0 BPI_CATEN:C2.0 "
    "BPI_NAUT:C13.0 BPI_QT_P:N6.0 BPI_QT_A:N6.0 BPI_IDADE:N3.0 BPI_MVM:C6.0 BPI_ORG:C3.0 "
    "BPI_TPFIN:C1.0 BPI_RMS:C4.0 BPI_FLPA:C1.0 BPI_FLCID:C1.0 BPI_FLCBO:C1.0 "
    "BPI_FLCA:C1.0 BPI_FLIDA:C1.0 BPI_FLQT:C1.0 BPI_FLER:C1.0 BPI_RACA:C2.0 "
    "BPI_ETNIA:C4.0 BPI_NACIO:C3.0 BPI_SRV:C3.0 BPI_CSF:C3.0 BPI_EQUIPE:C12.0 "
    "BPI_CNPJ:C14.0 BPI_CEPPCN:C8.0 BPI_CDLOGR:C3.0 BPI_LOGPCN:C30.0 BPI_CPLPCN:C10.0 "
    "BPI_NUMPCN:C5.0 BPI_BAIRRO:C30.0 BPI_DDD:C2.0 BPI_TEL:C9.0 BPI_EMAIL:C40.0 "
    "BPI_VL_FED:N10.2 BPI_VL_LOC:N10.2 BPI_VL_INC:N10.2 BPI_INCOUT:C4.0 BPI_INCURG:C4.0 "
    "BPI_RUB:C6.0 BPI_CPX:C1.0 BPI_RC:C4.0 BPI_CHKSM:C6.0 BPI_INE:C10.0 BPI_STRUA:C1.0 "
    "BPI_SEMCPF:C1.0 BPI_ADVSEX:C1.0 "
    ),
    "S_PA.DBF": (
    "PA_CMP:C6.0 PA_ID:C9.0 PA_DV:C1.0 PA_PAB:C1.0 PA_TOTAL:N12.2 PA_FAEC:C1.0 "
    "PA_DC:C60.0 PA_RUB:C4.0 PA_TPCC:C1.0 PA_AUX:C20.0 PA_CPX:C4.0 PA_CTF:C4.0 "
    "PA_DOC:C1.0 PA_IDADEMX:N3.0 PA_IDADEMN:N3.0 PA_SEXO:C1.0 PA_QTDMAX:N6.0 "
    "PA_LAUDO:C2.0 PA_PRINC:C1.0 PA_SECUN:C1.0 PA_IDEBPA:C1.0 PA_CPFPCN:C1.0 "
    "PA_CNSPCN:C1.0 PA_CNRAC:C1.0 PA_CCMANAL:C2.0 PA_ELETIVA:C1.0 PA_APACONT:C1.0 "
    "PA_EXIGCBO:C1.0 PA_PROCCEO:C1.0 PA_6MESES:C1.0 PA_EXIGAUT:C1.0 PA_PERMAN:N4.0 "
    "PA_EXIGCAS:C1.0 PA_SECOBRI:C1.0 PA_CHKSM:C6.0 "
    ),
    "CADMUN.DBF": (
    "CODUF:C2.0 CODMUNIC:C4.0 NOME:C40.0 CONDIC:C2.0 TETOPAB:N12.2 CALCPAB:N12.2 "
    "DTHABIL:C6.0 CIB_SAS:C1.0 "
    ),
}
_LAYOUTS["S_BPIHST.DBF"] = _LAYOUTS["S_BPI.DBF"]

type Field = tuple[str, str, int, int]
type Record = dict[str, str | int | float | None]

_CNES = "2269481"
_CMP = "202601"
_CMP_ANTERIOR = "202512"
_PROCEDIMENTOS = {
    "0301010056": ("CONSULTA MEDICA EM ATENCAO BASICA", "1", "01"),
    "0301010064": ("CONSULTA MEDICA EM ATENCAO ESPECIALIZADA", "2", "06"),
    "0304100021": ("QUIMIOTERAPIA PALIATIVA - ADULTO", "3", "06"),
    "0305010107": ("HEMODIALISE (MAXIMO 3 SESSOES POR SEMANA)", "3", "04"),
}


def layout(name: str) -> list[Field]:
    """Campos (nome, tipo, tamanho, decimais) do DBF real."""
    fields: list[Field] = []
    for token in _LAYOUTS[name].split():
        field, spec = token.split(":")
        size, decimals = spec[1:].split(".")
        fields.append((field, spec[0], int(size), int(decimals)))
    return fields


def _encode(ftype: str, size: int, decimals: int, value: object) -> bytes:
    if value is None or value == "":
        return b" " * size
    if ftype == "N":
        text = f"{value:.{decimals}f}" if decimals else str(int(value))
        return text.encode("ascii").rjust(size, b" ")[:size]
    return str(value).encode("cp1252")[:size].ljust(size, b" ")


def _write_dbf(path: Path, fields: list[Field], records: list[Record]) -> None:
    header_len = 32 + 32 * len(fields) + 1
    record_len = 1 + sum(f[2] for f in fields)
    known = {f[0] for f in fields}
    with path.open("wb") as f:
        f.write(struct.pack(
            "<BBBBLHH20x", 0x03, 126, 1, 1, len(records), header_len, record_len,
        ))
        for name, ftype, size, decimals in fields:
            f.write(name.encode("ascii").ljust(11, b"\x00") + ftype.encode()
                    + b"\x00" * 4 + bytes([size, decimals]) + b"\x00" * 14)
        f.write(b"\x0D")
        for rec in records:
            unknown = set(rec) - known
            if unknown:
                raise ValueError(f"dbf_unknown_field file={path.name} field={sorted(unknown)[0]}")
            f.write(b" ")
            for name, ftype, size, decimals in fields:
                f.write(_encode(ftype, size, decimals, rec.get(name)))
        f.write(b"\x1A")


def _cns(rng: random.Random) -> str:
    return f"7{rng.randint(10**13, 10**14 - 1):014d}"


def _date(cmp: str, rng: random.Random) -> str:
    return dt.date(int(cmp[:4]), int(cmp[4:]), rng.randint(1, 28)).strftime("%Y%m%d")


def _gen_apa(rng: random.Random) -> list[Record]:
    headers = [(f"35261000000{i:02d}", _CMP) for i in range(1, 4)]
    headers.append(("3526100000001", _CMP_ANTERIOR))
    return [
        {
            "APA_UID": _CNES, "APA_NUM": num, "APA_CMP": cmp, "APA_TPAPAC": "1",
            "APA_DTINIC": _date(cmp, rng), "APA_DTFIM": _date(cmp, rng),
            "APA_CNSEXE": _cns(rng), "APA_PRIPAL": "030410002",
            "APA_NMPCN": "PACIENTE FICTICIO", "APA_CPFPCT": "12345678909",
            "APA_CNSPCT": _cns(rng), "APA_DTNASC": "19700101",
        }
        for num, cmp in headers
    ]


def _prd(apanum: str, procedimento: str, qt_a: int | None) -> Record:
    return {
        "PRD_UID": _CNES, "PRD_CMP": _CMP, "PRD_FLH": "001", "PRD_SEQ": "01",
        "PRD_PA": procedimento, "PRD_CBO": "225125", "PRD_ORG": "APA" if apanum else "BPA",
        "PRD_APANUM": apanum, "PRD_CIDPRI": "C509" if apanum else "",
        "PRD_QT_P": 2, "PRD_QT_A": qt_a, "PRD_VL_P": 150.25, "PRD_VL_A": 150.25 if qt_a else None,
        "PRD_CPFPCT": "12345678909", "PRD_CNSPCN": "700000000000001",
    }


def _gen_prd() -> list[Record]:
    return [
        _prd("3526100000001", "0304100021", 2),
        _prd("3526100000002", "0304100021", 2),
        _prd("3526100000003", "0305010107", None),
        _prd("3526100000099", "0305010107", 1),
        _prd("", "0301010056", 2),
        _prd("", "0301010064", 1),
    ]


def _gen_bpi(rng: random.Random, competencias: list[str]) -> list[Record]:
    return [
        {
            "BPI_UID": _CNES, "BPI_CMP": cmp, "BPI_CNSMED": _cns(rng), "BPI_CBO": "225125",
            "BPI_FLH": "001", "BPI_SEQ": f"{seq:02d}",
            "BPI_PA": rng.choice(["0301010056", "0301010064"]), "BPI_CID": "J00",
            "BPI_DTATEN": _date(cmp, rng), "BPI_QT_P": rng.randint(1, 5), "BPI_QT_A": 1,
            "BPI_ORG": "BPA", "BPI_CNSPAC": _cns(rng), "BPI_NMPAC": "PACIENTE FICTICIO",
            "BPI_CPFPCT": "12345678909", "BPI_DTNASC": "19800101",
        }
        for seq, cmp in enumerate(competencias, start=1)
    ]


def _gen_pa() -> list[Record]:
    rows: list[Record] = []
    for cmp in (_CMP_ANTERIOR, _CMP):
        for code, (descricao, complexidade, financiamento) in _PROCEDIMENTOS.items():
            rows.append({
                "PA_CMP": cmp, "PA_ID": code[:9], "PA_DV": code[9], "PA_DC": descricao,
                "PA_CPX": complexidade, "PA_CTF": financiamento, "PA_TOTAL": 10.0,
            })
    return rows


def _gen_cadmun() -> list[Record]:
    return [
        {"CODUF": "35", "CODMUNIC": "4130", "NOME": "PRESIDENTE EPITACIO", "CONDIC": "PB",
         "TETOPAB": 1234567.89, "CALCPAB": 0.0, "DTHABIL": "200001"},
        {"CODUF": "35", "CODMUNIC": "5030", "NOME": "SÃO PAULO", "CONDIC": "PB",
         "TETOPAB": None, "CALCPAB": None},
    ]


def generate_all(target_dir: Path, seed: int = 42) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    records = {
        "S_APA.DBF": _gen_apa(rng),
        "S_PRD.DBF": _gen_prd(),
        "S_BPI.DBF": _gen_bpi(rng, [_CMP] * 8),
        "S_BPIHST.DBF": _gen_bpi(rng, [_CMP_ANTERIOR] * 6 + [_CMP] * 6),
        "S_PA.DBF": _gen_pa(),
        "CADMUN.DBF": _gen_cadmun(),
    }
    for name, rows in records.items():
        _write_dbf(target_dir / name, layout(name), rows)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    generate_all(args.dir, seed=args.seed)
    logger.info("dbfs_generated dir=%s seed=%d", args.dir, args.seed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
