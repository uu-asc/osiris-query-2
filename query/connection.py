from configparser import ConfigParser
from pathlib import Path
import urllib

from sqlalchemy import create_engine, Connection


def get_db_credentials(path: str|Path) -> dict:
    config_file = Path(path)
    with config_file.expanduser().open() as f:
        parser = ConfigParser()
        parser.read_file(f)
    return dict(parser.items('credentials'))


def get_odbc_con_to_oracle_db(
    udn: str,
    uid: str,
    pwd: str,
    **kwargs
) -> 'pyodbc.Connection':
    import pyodbc
    param = f"DSN={udn};UID={uid};PWD={pwd};"
    return pyodbc.connect(param)


def get_odbc_con_to_access_db(dbq: str) -> Connection:
    param = r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ={dbq};"
    engine = create_engine(f"access+pyodbc:///?odbc_connect={param}")
    return engine.connect()


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
    return engine.connect()


def get_connection_to_db(connector, path_to_credentials):
    creds = get_db_credentials(path_to_credentials)
    return connector(**creds)
