"""
Fake connection settings over stdlib sqlite3, for QueryExecution tests.

QueryExecution uses only `settings.engine()` (its `raw_connection()` and
`dialect`), `settings.cancel(con)` and `settings.name`. This module provides
those over an in-memory sqlite3 database, so the mechanics (batches, limits,
phases, cancel) can be tested without a database server. It does not test
driver behaviour: sqlite's types and interrupt differ from Oracle's.
"""

import sqlite3
from types import SimpleNamespace
from typing import Any

from sqlalchemy.exc import OperationalError


def rows(n: int) -> str:
    """SQL returning n rows: columns n (1..n) and s ('x1'..)."""
    return (
        f"with recursive r(n) as (select 1 union all select n + 1 from r where n < {n})\n"
        "select n, 'x' || n as s from r"
    )


# About 10 s (count over 10^8 rows); for cancel during execute. Long enough
# next to a cancel after 0.2 s, short enough if cancel stops working.
SLOW = (
    "with recursive r(n) as (select 1 union all select n + 1 from r where n < 100000000)\n"
    "select count(*) as c from r"
)


class FakeSettings:
    """
    ConnectionSettings stand-in.

    - arraysize: set as `dialect.arraysize` (like Oracle); absent if None
      (like duckdb, pymssql).
    - connect_error: `raw_connection()` raises as SQLAlchemy's pool does.
    """
    name = 'fake'

    def __init__(self, arraysize: int | None = None, connect_error: bool = False) -> None:
        self.arraysize = arraysize
        self.connect_error = connect_error
        self.connections: list[sqlite3.Connection] = []

    def engine(self) -> 'FakeEngine':
        return FakeEngine(self)

    def cancel(self, con: Any) -> None:
        con.interrupt()


class FakeEngine:
    def __init__(self, settings: FakeSettings) -> None:
        self.settings = settings
        self.dialect = SimpleNamespace(name='sqlite', loaded_dbapi=sqlite3)
        if settings.arraysize is not None:
            self.dialect.arraysize = settings.arraysize

    def raw_connection(self) -> sqlite3.Connection:
        if self.settings.connect_error:
            raise OperationalError(None, None, sqlite3.OperationalError('unable to open database file'))
        # execute_query fetches on a worker thread; tests check the connection from the main thread.
        con = sqlite3.connect(':memory:', check_same_thread=False)
        self.settings.connections.append(con)
        return con
