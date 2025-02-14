from pathlib import Path
from string import Template
from typing import Callable

import pandas as pd
from jinja2 import Environment
from sqlalchemy import TextClause
from sqlalchemy.exc import DatabaseError

from query import connection, definition, utils
from query.definition import get_sql, get_params


DEFAULT_QUERY_NAME = '<n/a>'
ERROR_TEMPLATE = Template("""
Database error message: ${error_message}
Query: ${query_name}
Parameters: ${params}
Missing: ${missing}
------------------------------------------------------------------------

${sql}

------------------------------------------------------------------------
Database error message: ${error_message}
"""
)


@utils.add_to_docstring(definition.DOCSTRING)
def execute_query(
    query: TextClause|Path|str,
    /,
    connector: Callable,
    path_to_credentials: str|Path,
    *,
    env: Environment|None = None,
    parse_dates: list|dict|None = None,
    index_col: str|list[str]|None = None,
    dtype: str|dict|None = None,
    dtype_backend: str = 'numpy_nullable', # numpy_nullable / pyarrow
    squeeze: bool = True,
    **kwargs
) -> pd.DataFrame|pd.Series|None:
    """
    Execute a SQL query and return the result as a pandas DataFrame.

    Parameters:
    - query (TextClause | Path | str):
        SQL query definition, file path, or raw SQL string.
    - connector (Callable):
        A callable object that establishes a connection to the database.
    - path_to_credentials (str | Path):
        Path to the credentials file or a string containing credentials.
    - env (Environment | None, optional):
        Environment context for the query execution. Default is None.
    - parse_dates (list | dict | None, optional):
        Columns to parse as dates. Default is None.
    - index_col (str | list[str] | None, optional):
        Column(s) to set as index(MultiIndex). Default is None.
        Uses `columns` instead when aggregating if `index_col` is None.
    - dtype (str | dict | None, optional):
        Data type to force. Default is None.
    - dtype_backend (str, optional):
        Data type backend for storage. Options: 'numpy_nullable' or 'pyarrow'.
        Default is 'numpy_nullable'.
    - squeeze (bool):
        Return Series if output has only one column. Default is True.
    - **kwargs:
        Additional keyword arguments passed to the query definition.

    Returns:
    - pd.DataFrame | None: A pandas DataFrame containing the query result, or None if an error occurs.

    Raises:
    - DatabaseError: If an error occurs during the query execution.
    """
    dtype_backend = 'numpy_nullable' if dtype_backend is None else dtype_backend
    query_name = query if definition.is_path(query) else DEFAULT_QUERY_NAME
    sql = definition.get_sql(query, env=env, **kwargs)
    con = connection.get_connection_to_db(connector, path_to_credentials)
    if index_col is None:
        index_col = kwargs.get('columns')
    elif index_col == False:
        index_col = None
    try:
        df = pd.read_sql_query(
            sql,
            con,
            parse_dates = parse_dates,
            index_col = index_col,
            dtype = dtype,
            dtype_backend = dtype_backend,
        )
        if squeeze and len(df.columns) == 1:
            return df.squeeze(axis=1)
        if squeeze and len(df) == 1:
            return df.squeeze().rename(query_name)
        return df

    except DatabaseError as e:
        params = get_params(query)
        missing = [k for k in params if k not in kwargs]
        info = str(e.orig)
        error_statement = ERROR_TEMPLATE.substitute(
            sql = sql.text,
            query_name = query_name,
            error_message = info,
            params = params,
            missing = missing,
        )
        print(error_statement)
        return None
