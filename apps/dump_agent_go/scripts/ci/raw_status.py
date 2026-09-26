"""Conta jobs raw concluídos no profile local para o smoke Windows."""

import sqlite3
import sys

with sqlite3.connect(sys.argv[1]) as connection:
    count = connection.execute(
        "SELECT count(*) FROM jobs WHERE tenant_id = ? AND state = 'SUCCEEDED'",
        ("354130",),
    ).fetchone()[0]
sys.stdout.write(f"{count}\n")
