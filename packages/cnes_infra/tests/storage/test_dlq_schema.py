"""Testes do schema quarantine.records e inserção via QuarantineBuffer."""
import duckdb
from cnes_domain.quarantine import QuarantineBuffer, QuarantineRecord


def _criar_db(tmp_path):
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
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_quarantine_source
        ON quarantine.records (source_system, competencia)
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_quarantine_identifier
        ON quarantine.records (record_identifier)
    """)
    return con


def test_schema_quarantine_criado(tmp_path):
    con = _criar_db(tmp_path)
    tabelas = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'quarantine'"
    ).fetchall()
    assert any("records" in t[0] for t in tabelas)
    con.close()


def test_colunas_obrigatorias_existem(tmp_path):
    con = _criar_db(tmp_path)
    colunas = {
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'quarantine' AND table_name = 'records'"
        ).fetchall()
    }
    assert {"competencia", "source_system", "record_identifier",
            "error_category", "failure_reason", "raw_payload"} <= colunas
    con.close()


def test_insert_via_buffer(tmp_path):
    con = _criar_db(tmp_path)
    buf = QuarantineBuffer()
    buf.append(QuarantineRecord(
        competencia="2026-01",
        source_system="FIREBIRD",
        record_identifier="12345678901",
        error_category="INVALID_CPF",
        failure_reason="cpf_invalido",
        raw_payload={"CPF": "123"},
    ))
    buf.flush_to_duckdb(con)
    row = con.execute("SELECT source_system, error_category FROM quarantine.records").fetchone()
    assert row == ("FIREBIRD", "INVALID_CPF")
    con.close()


def test_multiplos_source_systems(tmp_path):
    con = _criar_db(tmp_path)
    buf = QuarantineBuffer()
    for src in ("FIREBIRD", "HR", "DATASUS"):
        buf.append(QuarantineRecord(
            competencia="2026-01",
            source_system=src,
            record_identifier="x",
            error_category="TYPE_ERROR",
            failure_reason="tipo_invalido",
            raw_payload={},
        ))
    buf.flush_to_duckdb(con)
    count = con.execute("SELECT COUNT(*) FROM quarantine.records").fetchone()[0]
    assert count == 3
    con.close()


def test_raw_payload_json_serializavel(tmp_path):
    con = _criar_db(tmp_path)
    buf = QuarantineBuffer()
    buf.append(QuarantineRecord(
        competencia="2026-02",
        source_system="HR",
        record_identifier="99999999999",
        error_category="NULL_REQUIRED",
        failure_reason="campo_nulo",
        raw_payload={"CPF": None, "NOME": "João", "nested": {"x": 1}},
    ))
    buf.flush_to_duckdb(con)
    payload_str = con.execute("SELECT raw_payload FROM quarantine.records").fetchone()[0]
    assert payload_str is not None
    con.close()
