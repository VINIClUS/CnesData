"""Protocolo de conexões e transações SQLite compartilhadas."""

import sqlite3
from contextlib import AbstractContextManager
from typing import Protocol


class SQLiteConnectionsPort(Protocol):
    def read_connection(self) -> AbstractContextManager[sqlite3.Connection]: ...

    def write_transaction(self) -> AbstractContextManager[sqlite3.Connection]: ...
