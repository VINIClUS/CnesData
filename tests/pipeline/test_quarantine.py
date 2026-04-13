"""Testes do QuarantineBuffer e lógica de divert-to-DLQ."""
import polars as pl
import pytest

from ingestion.quarantine import QuarantineBuffer, QuarantineRecord, quarentinar_linhas


def _make_record(**kwargs) -> QuarantineRecord:
    defaults = dict(
        competencia="2026-01",
        source_system="FIREBIRD",
        record_identifier="12345678901",
        error_category="SCHEMA_MISMATCH",
        failure_reason="coluna=CPF check=str_length",
        raw_payload={"CPF": "123"},
    )
    defaults.update(kwargs)
    return QuarantineRecord(**defaults)


def test_buffer_vazio_inicialmente():
    buf = QuarantineBuffer()
    assert len(buf) == 0


def test_append_incrementa_tamanho():
    buf = QuarantineBuffer()
    buf.append(_make_record())
    buf.append(_make_record())
    assert len(buf) == 2


def test_quarantine_ratio_sem_rejeicoes():
    buf = QuarantineBuffer()
    assert buf.quarantine_ratio(100) == 0.0


def test_quarantine_ratio_com_rejeicoes():
    buf = QuarantineBuffer()
    buf.append(_make_record())
    ratio = buf.quarantine_ratio(total_valid=9)
    assert ratio == pytest.approx(0.1)


def test_quarantine_ratio_total_zero():
    buf = QuarantineBuffer()
    assert buf.quarantine_ratio(total_valid=0) == 0.0


def test_quarentinar_linhas_popula_buffer():
    df = pl.DataFrame({"CPF": ["11111111111", "22222222222"], "CNES": ["1234567", "7654321"]})
    buf = QuarantineBuffer()
    quarentinar_linhas(
        df=df,
        indices=[0],
        buffer=buf,
        competencia="2026-01",
        source_system="FIREBIRD",
        error_category="INVALID_CPF",
        failure_reason="cpf_invalido",
        id_col="CPF",
    )
    assert len(buf) == 1
    rec = buf._records[0]
    assert rec.record_identifier == "11111111111"
    assert rec.error_category == "INVALID_CPF"
    assert rec.source_system == "FIREBIRD"


def test_flush_limpa_buffer(tmp_path):
    import duckdb

    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA quarantine")
    con.execute("""
        CREATE TABLE quarantine.records (
            id INTEGER,
            competencia VARCHAR NOT NULL,
            source_system VARCHAR NOT NULL,
            record_identifier VARCHAR,
            error_category VARCHAR NOT NULL,
            failure_reason TEXT NOT NULL,
            raw_payload JSON,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    buf = QuarantineBuffer()
    buf.append(_make_record(competencia="2026-01"))
    buf.append(_make_record(competencia="2026-01", record_identifier="99999999999"))

    count = buf.flush_to_duckdb(con)
    assert count == 2
    assert len(buf) == 0

    rows = con.execute("SELECT COUNT(*) FROM quarantine.records").fetchone()[0]
    assert rows == 2
    con.close()


def test_flush_buffer_vazio_retorna_zero(tmp_path):
    import duckdb

    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA quarantine")
    con.execute("""
        CREATE TABLE quarantine.records (
            id INTEGER,
            competencia VARCHAR NOT NULL,
            source_system VARCHAR NOT NULL,
            record_identifier VARCHAR,
            error_category VARCHAR NOT NULL,
            failure_reason TEXT NOT NULL,
            raw_payload JSON,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    buf = QuarantineBuffer()
    assert buf.flush_to_duckdb(con) == 0
    con.close()
