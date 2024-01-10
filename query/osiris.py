from pathlib import Path

import pandas as pd
from sqlalchemy import TextClause

from query import config, connection, definition, execution, utils
from query.definition import get_sql


SCHEMA = config.load_schema('osiris')


@utils.add_to_docstring(definition.DOCSTRING)
def execute_query(
    query: TextClause|Path|str,
    parse_dates: list|dict|None = None,
    index_col: str|list[str]|None = None,
    dtype: str|dict|None = None,
    dtype_backend: str|None = None,
    **kwargs
) -> pd.DataFrame|None:
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
    - **kwargs:
        Additional keyword arguments passed to the query definition.

    Returns:
    - pd.DataFrame | None: A pandas DataFrame containing the query result, or None if an error occurs.

    Raises:
    - DatabaseError: If an error occurs during the query execution.
    """
    path_to_credentials = config.get_paths_from_config(
        key = 'osiris',
        table = 'credentials',
        keep_shape = True,
    )
    return execution.execute_query(
        query,
        connector = connection.get_oracledb_con_to_oracle_db,
        path_to_credentials = path_to_credentials,
        parse_dates = parse_dates,
        index_col = index_col,
        dtype = dtype,
        dtype_backend = dtype_backend,
        **kwargs
    )


def find_table(
    *args: str,
    where: list|None = None,
    **kwargs
) -> pd.DataFrame:
    """
    Search for tables in the database based on specified criteria.

    Parameters:
    - *args (str): Substrings for matching table name.
    - where (list|None): Optional parameter allowing additional conditions for the query. Defaults to None.
    - **kwargs: Additional keyword arguments to be passed to the underlying `execute_query` function.

    Returns:
    - pd.DataFrame: A DataFrame containing all tables meeting the specified criteria.
    """
    criteria = []
    for arg in args:
        criteria.append(f"table_name like '%{arg.upper()}%'")

    where = criteria if where is None else [*criteria, *where]

    df = execute_query(
        'reference/all_tables',
        where = where,
        **kwargs
    )
    return df


def find_column(
    *args: str,
    table: str|None = None,
    data_type: str|None = None,
    where: list|None = None,
    **kwargs
) -> pd.DataFrame:
    """
    Search for columns in the database based on specified criteria.

    Parameters:
    - *args (str): Substrings for matching column name.
    - table (str|None): Optional parameter specifying the table name for additional filtering. Defaults to None.
    - data_type (str|None): Optional parameter specifying the data type for additional filtering. Defaults to None.
    - where (list|None): Optional parameter allowing additional conditions for the query. Defaults to None.
    - **kwargs: Additional keyword arguments to be passed to the underlying execute_query function.

    Returns:
    - pd.DataFrame: A DataFrame containing all columns meeting the specified criteria.
    """

    criteria = []
    for arg in args:
        criteria.append(f"column_name like '%{arg.upper()}%'")

    where = criteria if where is None else [*criteria, *where]

    if table:
        assert isinstance(table, str), "Table needs to be a string"
        where.append(f"table_name like '%{table.upper()}%'")

    if data_type:
        assert isinstance(data_type, str), "Data_type needs to be a string"
        where.append(f"data_type like '%{data_type.upper()}%'")

    df = execute_query(
        'reference/all_columns',
        where = where,
        **kwargs
    )
    return df


def peek(
    table_name: str,
    n: int = 10,
    **kwargs
) -> pd.DataFrame|None:
    """
    Peek at first `n` results from `table_name`.

    Parameters:
    - table_name (str): Name of table to peek at.
    - n (int): Number of rows to return, defaults to 10.
    - **kwargs: Additional keyword arguments to be passed to the underlying execute_query function.

    Returns:
    - pd.DataFrame: A DataFrame containing the first `n` result from `table_name`.
    """
    return execute_query(
        'reference/table',
        table = table_name,
        n = n,
        **kwargs
    )
