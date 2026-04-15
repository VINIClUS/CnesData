"""QuarantineBuffer — coleta e persiste registros rejeitados (DLQ)."""
import json
import logging
from dataclasses import dataclass, field

import polars as pl

logger = logging.getLogger(__name__)

_INSERT_QUARANTINE = """
INSERT INTO quarantine.records
    (competencia, source_system, record_identifier, error_category, failure_reason, raw_payload)
VALUES (?, ?, ?, ?, ?, ?)
"""


@dataclass
class QuarantineRecord:
    """Representa um registro rejeitado na validação."""

    competencia: str
    source_system: str
    record_identifier: str
    error_category: str
    failure_reason: str
    raw_payload: dict


@dataclass
class QuarantineBuffer:
    """Acumula registros rejeitados e persiste em lote no DuckDB."""

    _records: list[QuarantineRecord] = field(
        default_factory=list, init=False,
    )

    def append(self, record: QuarantineRecord) -> None:
        self._records.append(record)

    def flush_to_duckdb(self, con) -> int:
        """Persiste todos os registros acumulados e limpa o buffer.

        Args:
            con: Conexão DuckDB ativa.

        Returns:
            Número de registros persistidos.
        """
        if not self._records:
            return 0
        rows = [
            (
                r.competencia, r.source_system, r.record_identifier,
                r.error_category, r.failure_reason,
                json.dumps(r.raw_payload, default=str),
            )
            for r in self._records
        ]
        con.executemany(_INSERT_QUARANTINE, rows)
        count = len(self._records)
        logger.info("quarantine_flushed total=%d", count)
        self._records.clear()
        return count

    def quarantine_ratio(self, total_valid: int) -> float:
        """Proporção de registros quarentenados vs total processado."""
        total_quarantined = len(self._records)
        total = total_valid + total_quarantined
        return total_quarantined / total if total > 0 else 0.0

    def __len__(self) -> int:
        return len(self._records)


def quarentinar_linhas(
    df: pl.DataFrame,
    indices: list[int],
    buffer: QuarantineBuffer,
    competencia: str,
    source_system: str,
    error_category: str,
    failure_reason: str,
    id_col: str = "CPF",
) -> None:
    """Converte linhas de um DataFrame em QuarantineRecords.

    Args:
        df: DataFrame fonte.
        indices: Índices (posições) das linhas rejeitadas.
        buffer: Buffer de quarentena.
        competencia: Competência no formato 'YYYY-MM'.
        source_system: 'FIREBIRD' | 'HR' | 'DATASUS'.
        error_category: Categoria do erro.
        failure_reason: Descrição do motivo de rejeição.
        id_col: Coluna usada como record_identifier.
    """
    subset = df[indices] if indices else pl.DataFrame()
    for row in subset.iter_rows(named=True):
        identifier = str(row.get(id_col, ""))
        buffer.append(
            QuarantineRecord(
                competencia=competencia,
                source_system=source_system,
                record_identifier=identifier,
                error_category=error_category,
                failure_reason=failure_reason,
                raw_payload=row,
            )
        )
