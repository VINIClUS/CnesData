"""Testes de prontidão do runtime para arquivos DBC do DATASUS."""

from __future__ import annotations

import hashlib
import re
import sys
import tomllib
from pathlib import Path

import pytest
from dbfread import DBF

from cnes_contracts import RawManifest
from cnes_contracts.manifests.validation import manifest_sha256

_ROOT = Path(__file__).resolve().parents[4]
_FIXTURE = Path(__file__).parent / "fixtures" / "PFSP2601.dbc"
_GOLDEN = _ROOT / "docs" / "fixtures" / "data-plane" / "raw-manifest-v1.json"
_GOLDEN_SHA256 = "9c6005f90bbc5af3bcb3c5469474e44ea895e4e2e8ef27e014e0933e45af0e99"
_GOLDEN_BYTES = (
    b'{"manifest_version":1,"manifest_id":"fixture-cnes-nacional-v1",'
    b'"tenant_id":"354130","source_type":"CNES_NACIONAL","file_subtype":"CNES_VINCULO",'
    b'"competencia":"2026-01","agent_id":"system-datasus","agent_version":"1.0.0",'
    b'"schema_version":"cnes-profissional-v1","snapshot_mode":"FULL",'
    b'"snapshot_id":"fixture-cnes-nacional-v1","base_snapshot_id":null,"sequence":1,'
    b'"previous_manifest_sha256":null,'
    b'"object_sha256":"30a60a84af9b06c568fe6170ce72365ee33cb1a706f130cc5bc7e197aef348ec",'
    b'"row_count":5,"size_bytes":5123,'
    b'"object_key":"raw/354130/CNES_NACIONAL/2026-01/fixture-cnes-nacional-v1/data.parquet",'
    b'"created_at":"2026-02-01T00:00:00Z"}'
)
_PF_LAYOUT = (
    ("CNES", "C", 7, 0),
    ("CODUFMUN", "C", 6, 0),
    ("REGSAUDE", "C", 4, 0),
    ("MICR_REG", "C", 6, 0),
    ("DISTRSAN", "C", 4, 0),
    ("DISTRADM", "C", 4, 0),
    ("TPGESTAO", "C", 1, 0),
    ("PF_PJ", "C", 1, 0),
    ("CPF_CNPJ", "C", 14, 0),
    ("NIV_DEP", "C", 1, 0),
    ("CNPJ_MAN", "C", 14, 0),
    ("ESFERA_A", "C", 2, 0),
    ("ATIVIDAD", "C", 2, 0),
    ("RETENCAO", "C", 2, 0),
    ("NATUREZA", "C", 2, 0),
    ("CLIENTEL", "C", 2, 0),
    ("TP_UNID", "C", 2, 0),
    ("TURNO_AT", "C", 2, 0),
    ("NIV_HIER", "C", 2, 0),
    ("TERCEIRO", "C", 1, 0),
    ("CPF_PROF", "C", 11, 0),
    ("CPFUNICO", "C", 1, 0),
    ("CBO", "C", 6, 0),
    ("CBOUNICO", "C", 6, 0),
    ("NOMEPROF", "C", 60, 0),
    ("CNS_PROF", "C", 15, 0),
    ("CONSELHO", "C", 2, 0),
    ("REGISTRO", "C", 13, 0),
    ("VINCULAC", "C", 6, 0),
    ("VINCUL_C", "C", 1, 0),
    ("VINCUL_A", "C", 1, 0),
    ("VINCUL_N", "C", 1, 0),
    ("PROF_SUS", "C", 1, 0),
    ("PROFNSUS", "C", 1, 0),
    ("HORAOUTR", "N", 3, 0),
    ("HORAHOSP", "N", 3, 0),
    ("HORA_AMB", "N", 3, 0),
    ("COMPETEN", "C", 6, 0),
    ("UFMUNRES", "C", 6, 0),
    ("NAT_JUR", "C", 4, 0),
)
_PF_RECORD = {
    "CNES": "9999999",
    "CODUFMUN": "354130",
    "REGSAUDE": "0000",
    "MICR_REG": "000000",
    "DISTRSAN": "0000",
    "DISTRADM": "0000",
    "TPGESTAO": "0",
    "PF_PJ": "0",
    "CPF_CNPJ": "00000000000000",
    "NIV_DEP": "0",
    "CNPJ_MAN": "00000000000000",
    "ESFERA_A": "00",
    "ATIVIDAD": "00",
    "RETENCAO": "00",
    "NATUREZA": "00",
    "CLIENTEL": "00",
    "TP_UNID": "00",
    "TURNO_AT": "00",
    "NIV_HIER": "00",
    "TERCEIRO": "0",
    "CPF_PROF": "00000000000",
    "CPFUNICO": "0",
    "CBO": "000000",
    "CBOUNICO": "000000",
    "NOMEPROF": "PROFISSIONAL TESTE",
    "CNS_PROF": "000000000000000",
    "CONSELHO": "00",
    "REGISTRO": "0000000000000",
    "VINCULAC": "000000",
    "VINCUL_C": "0",
    "VINCUL_A": "0",
    "VINCUL_N": "0",
    "PROF_SUS": "0",
    "PROFNSUS": "0",
    "HORAOUTR": 0,
    "HORAHOSP": 0,
    "HORA_AMB": 0,
    "COMPETEN": "202601",
    "UFMUNRES": "354130",
    "NAT_JUR": "0000",
}


def _project(path: str) -> dict[str, object]:
    return tomllib.loads((_ROOT / path).read_text(encoding="utf-8"))["project"]


def test_runtime_python313_falha_sem_modulo_dbc():
    assert sys.version_info[:2] == (3, 13)

    import datasus_dbc

    assert callable(datasus_dbc.decompress)


def test_fixture_dbc_converte_para_dbf_sem_dados_sensiveis(tmp_path: Path):
    import datasus_dbc

    output = tmp_path / "PFSP2601.dbf"
    datasus_dbc.decompress(str(_FIXTURE), str(output))

    table = DBF(output, load=False, encoding="latin-1")
    records = iter(table)
    record = next(records)

    assert table.loaded is False
    assert (
        tuple((field.name, field.type, field.length, field.decimal_count) for field in table.fields)
        == _PF_LAYOUT
    )
    assert dict(record) == _PF_RECORD
    with pytest.raises(StopIteration):
        next(records)


def test_extra_national_fixa_dependencias_compativeis():
    infra = _project("packages/cnes_infra/pyproject.toml")
    central_api = _project("apps/central_api/pyproject.toml")

    assert infra["optional-dependencies"]["national"] == [
        "datasus-dbc>=0.1.3,<0.2",
        "dbfread>=2.0.7,<3",
    ]
    assert "cnes-infra[national]" in central_api["dependencies"]
    assert "cnes-infra" not in central_api["dependencies"]


def test_imagem_runtime_importa_dbc_sem_cargo():
    dockerfile = (_ROOT / "apps" / "central_api" / "Dockerfile").read_text(encoding="utf-8")
    stages = re.split(r"(?m)^FROM ", dockerfile)[1:]

    assert len(stages) == 2
    builder, runtime = stages
    assert builder.startswith("python:3.13-slim AS builder")
    assert all(re.search(rf"\b{tool}\b", builder) for tool in ("cargo", "rustc", "gcc"))
    assert builder.index("apt-get install") < builder.index("uv pip install")
    assert "apt-get install" not in runtime
    assert re.search(r"\b(?:cargo|rustc|gcc)\b", runtime) is None
    logical_runtime = runtime.replace("\\\n", " ")
    builder_copies = tuple(
        tuple(line.split()[2:])
        for line in logical_runtime.splitlines()
        if line.startswith("COPY --from=builder ")
    )
    assert builder_copies == (
        (
            "/usr/local/lib/python3.13/site-packages",
            "/usr/local/lib/python3.13/site-packages",
        ),
        ("/usr/local/bin/uvicorn", "/usr/local/bin/uvicorn"),
    )


def test_manifesto_raw_golden_preserva_bytes_canonicos():
    payload = _GOLDEN.read_bytes()
    manifest = RawManifest.model_validate_json(payload)

    assert payload == _GOLDEN_BYTES
    assert hashlib.sha256(payload).hexdigest() == _GOLDEN_SHA256
    assert manifest.model_dump_json(exclude_none=False, by_alias=False).encode() == payload
    assert manifest_sha256(manifest) == _GOLDEN_SHA256
