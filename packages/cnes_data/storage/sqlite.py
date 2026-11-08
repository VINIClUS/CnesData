"""Protocolo de conexões e transações SQLite compartilhadas."""

import sqlite3
from abc import abstractmethod
from contextlib import AbstractContextManager
from typing import Protocol


class SQLiteConnectionsPort(Protocol):
    @abstractmethod
    def read_connection(self) -> AbstractContextManager[sqlite3.Connection]:
        raise NotImplementedError

    @abstractmethod
    def write_transaction(self) -> AbstractContextManager[sqlite3.Connection]:
        raise NotImplementedError
