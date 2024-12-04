from typing import Callable

from configparser import ConfigParser
from pathlib import Path
import urllib

from sqlalchemy import create_engine, Connection


def get_db_credentials(path: Path | str) -> dict:
    config_file = Path(path)
    with config_file.open() as f:
        parser = ConfigParser()
        parser.read_file(f)
    return dict(parser.items('credentials'))


def get_odbc_con_to_access_db(dbq: str) -> Connection:
    param = r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ={dbq};"
    engine = create_engine(f"access+pyodbc:///?odbc_connect={param}")
    return engine


def get_oracledb_con_to_oracle_db(
    uid: str,
    pwd: str,
    host: str,
    port: str,
    dsn: str,
    **kwargs
) -> Connection:
    encoded_pwd = urllib.parse.quote(pwd, safe='')
    param = f"oracle+oracledb://{uid}:{encoded_pwd}@{host}:{port}/{dsn}"
    engine = create_engine(param)
    return engine


def get_sqlite_connection(
    database: str,
    **kwargs
) -> Connection:
    """Create SQLite connection using SQLAlchemy."""
    param = f"sqlite:///{database}"
    engine = create_engine(param)
    return engine


def get_connection_to_db(
    connector: Callable,
    path_to_credentials: Path | str,
) -> Connection:
    """
    Get database connection using specified connector.
    For SQLite, path_to_credentials can be None as credentials aren't needed.
    """
    creds = get_db_credentials(path_to_credentials)
    return connector(**creds)
