from pathlib import Path

import pandas as pd
from jinja2 import Environment
from sqlalchemy import TextClause
from sqlalchemy.exc import DatabaseError

from query import connection, definition


def execute_query(
    query: TextClause|Path|str,
    /,
    connector,
    path_to_credentials: str|Path,
    *,
    env: Environment|None = None,
    parse_dates: list|dict|None = None,
    index_col: str|list[str]|None = None,
    dtype: str|dict|None = None,
    dtype_backend: str = 'numpy_nullable', # numpy_nullable / pyarrow
    **kwargs
) -> pd.DataFrame|None:

    dtype_backend = 'numpy_nullable' if dtype_backend is None else dtype_backend
    sql = definition.get_sql(query, env=env, **kwargs)
    con = connection.get_connection_to_db(connector, path_to_credentials)
    try:
        return pd.read_sql_query(
            sql,
            con,
            parse_dates = parse_dates,
            index_col = index_col,
            dtype = dtype,
            dtype_backend = dtype_backend,
        )
    except DatabaseError as e:
        error_message = str(e.orig)
        print(
f"""ORACLE error message: {error_message}

{sql.text}

ORACLE error message: {error_message}
""")
        return None
