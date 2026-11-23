"""
Engine construction and connection settings per driver.

Connector functions (`get_..._connection`, `get_..._con_to_...`) build a
SQLAlchemy engine from explicit arguments. Settings classes (`OracleSettings`,
`MssqlSettings`, `DuckdbSettings`) read and validate a [connections.<name>]
section from the config and call the matching connector in `engine()`.

Pooling: all engines use NullPool. A connection is closed as soon as it is
released instead of being kept for reuse. Engines are created per query, so a
pooled connection would stay open until garbage collection: an idle session on
the database, and for duckdb a held file lock that blocks other processes.

Driver options: every connector takes `connect_args`, a dict passed unchanged
to `create_engine(connect_args=...)` and from there to the driver's
`connect()`. In the config these come from [connections.<name>.connect_args].
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
import sys
import urllib

from sqlalchemy import create_engine, Engine
from sqlalchemy.pool import NullPool

from query import errors
from query.config import (
    ConnectionSettings,
    setting,
    parse_str,
    parse_port,
    parse_positive_int,
    parse_existing_file,
    parse_credentials_file,
    read_credentials,
)


# region module attributes
def module_getattr(module_name: str, settings: ConnectionSettings) -> Callable[[str], Any]:
    """
    Module-level `__getattr__` (PEP 562) for a connection module. Serves
    `last_error`: the most recent error for `settings.name`, looked up on
    each access, so errors stored after import are seen.

    Usage, in the connection module:
        __getattr__ = connection.module_getattr(__name__, SETTINGS)
    """
    def __getattr__(name: str) -> Any:
        if name == 'last_error':
            return errors.last_error(settings.name)
        raise AttributeError(f"module {module_name!r} has no attribute {name!r}")
    return __getattr__


def module_dir(module_name: str) -> Callable[[], list[str]]:
    """Module-level __dir__: the module's own names plus `last_error`."""
    def __dir__() -> list[str]:
        return sorted({*vars(sys.modules[module_name]), 'last_error'})
    return __dir__


# region connectors
def get_odbc_con_to_access_db(
    dbq: str,
    connect_args: dict[str, Any] | None = None,
) -> Engine:
    """
    Create an engine for an MS Access database via ODBC.

    Parameters:
    - dbq (str): Path to the .mdb/.accdb file.
    - connect_args (dict | None): Passed to pyodbc's `connect()`.
    """
    param = r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ={dbq};"
    return create_engine(
        f"access+pyodbc:///?odbc_connect={param}",
        connect_args = connect_args or {},
        poolclass = NullPool,
    )


def get_oracledb_con_to_oracle_db(
    uid: str,
    pwd: str,
    host: str,
    port: int | str,
    dsn: str,
    arraysize: int | None = None,
    connect_args: dict[str, Any] | None = None,
) -> Engine:
    """
    Create an engine for an Oracle database (python-oracledb, thin mode).

    Parameters:
    - uid, pwd (str): Credentials.
    - host (str), port (int | str), dsn (str): Location and service name.
    - arraysize (int | None): Rows fetched per network round trip, set on
      every cursor by the SQLAlchemy dialect. None keeps the driver default
      (100), which makes large results slow.
    - connect_args (dict | None): Passed to `oracledb.connect()`, e.g.
      `tcp_connect_timeout`, `expire_time`.
    """
    encoded_pwd = urllib.parse.quote(pwd, safe='')
    param = f"oracle+oracledb://{uid}:{encoded_pwd}@{host}:{port}/{dsn}"
    return create_engine(
        param,
        arraysize = arraysize,
        connect_args = connect_args or {},
        poolclass = NullPool,
    )


def get_pymssql_con_to_msserver_db(
    uid: str,
    pwd: str,
    host: str,
    port: int | str,
    database: str,
    connect_args: dict[str, Any] | None = None,
) -> Engine:
    """
    Create an engine for an MS SQL Server database (pymssql).

    Parameters:
    - uid, pwd (str): Credentials.
    - host (str), port (int | str), database (str): Location and database name.
    - connect_args (dict | None): Passed to `pymssql.connect()`, e.g.
      `login_timeout`, `timeout`, `charset`, `tds_version`.
    """
    encoded_pwd = urllib.parse.quote(pwd, safe='')
    param = f"mssql+pymssql://{uid}:{encoded_pwd}@{host}:{port}/{database}"
    return create_engine(
        param,
        connect_args = connect_args or {},
        poolclass = NullPool,
    )


def get_sqlite_connection(
    database: str,
    connect_args: dict[str, Any] | None = None,
) -> Engine:
    """
    Create an engine for a SQLite database file.

    Parameters:
    - database (str): Path to the database file.
    - connect_args (dict | None): Passed to `sqlite3.connect()`.
    """
    return create_engine(
        f"sqlite:///{database}",
        connect_args = connect_args or {},
        poolclass = NullPool,
    )


def get_duckdb_connection(
    database: str,
    connect_args: dict[str, Any] | None = None,
) -> Engine:
    """
    Create an engine for a DuckDB database file (duckdb_engine).

    Parameters:
    - database (str): Path to the database file.
    - connect_args (dict | None): Passed to `duckdb.connect()`, e.g.
      `{'read_only': True}` or `{'config': {'threads': 4}}`.
    """
    return create_engine(
        f"duckdb:///{database}",
        connect_args = connect_args or {},
        poolclass = NullPool,
    )


# region settings
@dataclass(frozen=True, kw_only=True)
class OracleSettings(ConnectionSettings):
    """
    Settings for an Oracle connection.

    Config keys in [connections.<name>]:
    - host (str): Required.
    - port (int): Required.
    - dsn (str): Required. Service name.
    - credentials (str): Required. Path to an ini file with a [credentials]
      section containing uid and pwd.
    - arraysize (int): Required, > 0. Rows per network round trip; 10_000 is
      the tested value. Required so it is never silently left at the driver
      default of 100.
    - connect_args (table): Optional. See `ConnectionSettings`.
    """
    host: str = setting(parse_str, 'string')
    port: int = setting(parse_port, 'port number')
    dsn: str = setting(parse_str, 'string')
    credentials: Path = setting(parse_credentials_file, 'path to credentials file')
    arraysize: int = setting(parse_positive_int, 'positive int; rows per round trip, e.g. 10_000')

    def engine(self) -> Engine:
        creds = read_credentials(self.credentials)
        return get_oracledb_con_to_oracle_db(
            uid = creds['uid'],
            pwd = creds['pwd'],
            host = self.host,
            port = self.port,
            dsn = self.dsn,
            arraysize = self.arraysize,
            connect_args = self.connect_args,
        )

    def cancel(self, con: Any) -> None:
        """The running statement ends with ORA-01013."""
        con.cancel()


@dataclass(frozen=True, kw_only=True)
class MssqlSettings(ConnectionSettings):
    """
    Settings for an MS SQL Server connection.

    Config keys in [connections.<name>]:
    - host (str): Required.
    - port (int): Required.
    - database (str): Required. Database name on the server.
    - credentials (str): Required. Path to an ini file with a [credentials]
      section containing uid and pwd.
    - connect_args (table): Optional. See `ConnectionSettings`.
    """
    host: str = setting(parse_str, 'string')
    port: int = setting(parse_port, 'port number')
    database: str = setting(parse_str, 'string')
    credentials: Path = setting(parse_credentials_file, 'path to credentials file')

    def engine(self) -> Engine:
        creds = read_credentials(self.credentials)
        return get_pymssql_con_to_msserver_db(
            uid = creds['uid'],
            pwd = creds['pwd'],
            host = self.host,
            port = self.port,
            database = self.database,
            connect_args = self.connect_args,
        )


@dataclass(frozen=True, kw_only=True)
class DuckdbSettings(ConnectionSettings):
    """
    Settings for a DuckDB connection. No credentials.

    Config keys in [connections.<name>]:
    - database (str): Required. Path to an existing database file.
    - connect_args (table): Optional. See `ConnectionSettings`. For example
      `read_only = true` lets several reading processes open the file at once.
    """
    database: Path = setting(parse_existing_file, 'path to database file')

    def engine(self) -> Engine:
        return get_duckdb_connection(
            database = str(self.database),
            connect_args = self.connect_args,
        )

    def cancel(self, con: Any) -> None:
        """duckdb's interrupt(). Untested (no duckdb tests yet)."""
        con.interrupt()
