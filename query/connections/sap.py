"""
Execute queries on the SAP database.

execute_query():
    Execute a SQL query and return the result as a pandas DataFrame.
"""

from pathlib import Path
from string import Template

import pandas as pd
from sqlalchemy import TextClause

from query import config, execution
from query.connections import connection
from query.definition import get_sql, get_raw_sql, get_params, find_query
from query.aggspec import *


def execute_query(
    query: TextClause | Path | str,
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
    - query (TextClause | Path | str): SQL query definition, file path, or raw SQL string.
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
    path_to_credentials = config.get_paths_from_config('sap', table='credentials')
    return execution.execute_query(
        query,
        connector = connection.get_pymssql_con_to_msserver_db,
        path_to_credentials = path_to_credentials,
        parse_dates = parse_dates,
        index_col = index_col,
        dtype = dtype,
        dtype_backend = dtype_backend,
        squeeze = squeeze,
        **kwargs
    )
