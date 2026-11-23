"""
Execute queries on the ISIS database.

execute_query():
    Execute a SQL query and return the result as a pandas DataFrame.
"""

from pathlib import Path
from string import Template

import pandas as pd

from query import execution
from query.connections import connection
from query.definition import SQL, Query, get_sql, get_raw_sql, get_params, find_query
from query.aggspec import *

SETTINGS = connection.DuckdbSettings.from_config('isis')

# last_error: most recent error for this connection (see errors.last_error)
__getattr__ = connection.module_getattr(__name__, SETTINGS)
__dir__ = connection.module_dir(__name__)


def execute_query(
    query: SQL | Query | Path | str,
    parse_dates: list | dict | None = None,
    index_col: str | list[str] | None = None,
    dtype: str | dict | None = None,
    dtype_backend: str | None = None,
    squeeze: bool = True,
    **kwargs
) -> pd.DataFrame | pd.Series | None:
    """
    Execute a SQL query and return the result as a pandas DataFrame.

    Parameters:
    - query (SQL | Query | Path | str): Query name or file path, SQL string, a
        Query, or an SQL from `get_sql`.
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
    return execution.execute_query(
        query,
        settings = SETTINGS,
        parse_dates = parse_dates,
        index_col = index_col,
        dtype = dtype,
        dtype_backend = dtype_backend,
        squeeze = squeeze,
        **kwargs
    )
