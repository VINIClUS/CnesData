"""Gera SIA DBF fixtures sintéticos (determinísticos via seed)."""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import random
import struct
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_S_APA_FIELDS = [
    ("APA_CMP", "C", 6),
    ("APA_CNES", "C", 7),
    ("APA_CNSPCT", "C", 15),
    ("APA_CNSEXE", "C", 15),
    ("APA_PROC", "C", 10),
    ("APA_CBO", "C", 6),
    ("APA_CID", "C", 4),
    ("APA_DTINI", "D", 8),
    ("APA_DTFIN", "D", 8),
    ("APA_QTAPR", "N", 8),
    ("APA_VLAPR", "N", 14),
]

_S_BPI_FIELDS = [
    ("BPI_CMP", "C", 6),
    ("BPI_CNES", "C", 7),
    ("BPI_CNSPAC", "C", 15),
    ("BPI_CNSMED", "C", 15),
    ("BPI_CBO", "C", 6),
    ("BPI_PROC", "C", 10),
    ("BPI_CID", "C", 4),
    ("BPI_DTATEN", "D", 8),
    ("BPI_QT", "N", 8),
    ("BPI_FOLHA", "N", 3),
    ("BPI_SEQ", "N", 2),
]

_S_CDN_FIELDS = [
    ("CDN_TB", "C", 4),
    ("CDN_IT", "C", 10),
    ("CDN_DSCR", "C", 100),
    ("CDN_CHKSM", "C", 8),
]

_CADMUN_FIELDS = [
    ("CODUF", "C", 2),
    ("CODMUNIC", "C", 6),
    ("NOME", "C", 60),
    ("CONDIC", "C", 1),
    ("TETOPAB", "N", 14),
    ("CALCPAB", "C", 1),
]


def _write_dbf(path: Path, fields: list, records: list) -> None:
    header_len = 32 + 32 * len(fields) + 1
    record_len = 1 + sum(f[2] for f in fields)

    with path.open("wb") as f:
        today = dt.date.today()
        f.write(struct.pack(
            "<BBBBLHH20x", 0x03,
            today.year - 1900, today.month, today.day,
            len(records), header_len, record_len,
        ))
        for name, ftype, size in fields:
            if len(name) > 10:
                raise ValueError(f"dbf_field_name_too_long name={name} max=10")
            name_b = name.encode("cp1252").ljust(11, b"\x00")
            f.write(name_b + ftype.encode() + b"\x00" * 4
                    + bytes([size, 0]) + b"\x00" * 14)
        f.write(b"\x0D")

        for rec in records:
            f.write(b" ")
            for (_name, ftype, size), val in zip(fields, rec, strict=True):
                if ftype == "C":
                    s = str(val).encode("cp1252")[:size].ljust(size, b" ")
                elif ftype == "D":
                    s = val.strftime("%Y%m%d").encode("ascii")
                elif ftype == "N":
                    s = str(val).encode("ascii")[:size].rjust(size, b" ")
                else:
                    s = b" " * size
                f.write(s)
        f.write(b"\x1A")


def _gen_apa(rng: random.Random) -> list:
    return [
        (
            "202601", "2269481",
            f"7{rng.randint(10**13, 10**14 - 1):014d}",
            f"7{rng.randint(10**13, 10**14 - 1):014d}",
            rng.choice(["0301010056", "0301010064", "0401010074"]),
            "225125", "J00",
            dt.date(2026, 1, rng.randint(1, 28)),
            dt.date(2026, 1, rng.randint(1, 28)),
            rng.randint(1, 10),
            rng.randint(1000, 50000),
        )
        for _ in range(5)
    ]


def _gen_bpi(rng: random.Random, n: int) -> list:
    return [
        (
            "202601", "2269481",
            f"7{rng.randint(10**13, 10**14 - 1):014d}",
            f"7{rng.randint(10**13, 10**14 - 1):014d}",
            "225125",
            rng.choice(["0301010056", "0301010064"]),
            "J00",
            dt.date(2026, 1, rng.randint(1, 28)),
            rng.randint(1, 5),
            1,
            rng.randint(1, 99),
        )
        for _ in range(n)
    ]


def _gen_cdn(_rng: random.Random) -> list:
    return [
        ("PROC", "0301010056", "CONSULTA MEDICA EM ATEN BAS", "00000000"),
        ("PROC", "0301010064", "CONSULTA ODONTOLOGICA", "00000000"),
        ("PROC", "0401010074", "EXAME LAB HEMOGRAMA", "00000000"),
        ("CID ", "J00       ", "RINOFARINGITE AGUDA", "00000000"),
        ("CID ", "K02       ", "CARIE DENTARIA", "00000000"),
    ]


def _gen_cadmun() -> list:
    return [
        ("35", "354130", "PRESIDENTE EPITACIO", "1", 1234567, "C"),
        ("35", "355030", "SAO PAULO", "1", 99999999, "C"),
    ]


def generate_all(target_dir: Path, seed: int = 42) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    _write_dbf(target_dir / "S_APA.DBF", _S_APA_FIELDS, _gen_apa(rng))
    _write_dbf(target_dir / "S_BPI.DBF", _S_BPI_FIELDS, _gen_bpi(rng, 8))
    _write_dbf(target_dir / "S_BPIHST.DBF", _S_BPI_FIELDS, _gen_bpi(rng, 12))
    _write_dbf(target_dir / "S_CDN.DBF", _S_CDN_FIELDS, _gen_cdn(rng))
    _write_dbf(target_dir / "CADMUN.DBF", _CADMUN_FIELDS, _gen_cadmun())


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
