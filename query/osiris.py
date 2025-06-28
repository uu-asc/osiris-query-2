"""
Execute queries on the OSIRIS query database.

execute_query():
    Execute a SQL query and return the result as a pandas DataFrame.
get_table():
    Fetch all results from `table`.
peek():
    Peek at first `n` results from `table`.
get_values():
    Get distinct values from `column` in `table`.
get_sql():
    Get SQL query from `source`.
get_params():
    Get parameters used in the SQL query from `source`
describe_column():
    Return descriptive statistics for `column` in `table`.
find_table():
    Search for tables in the database based on specified criteria.
find_column():
    Search for columns in the database based on specified criteria.
sanity():
    Perform a sanity check on staleness of data.
"""

from pathlib import Path
from string import Template

import pandas as pd
from sqlalchemy import TextClause

from query import config, connection, definition, execution, utils
from query.definition import get_sql, get_raw_sql, get_params, find_query
from query.aggspec import *

SCHEMA = config.load_schema('osiris')


quickfilter_docstrings = ['    QUICK FILTERS', *[
    utils.QUICK_FILTER_TEMPLATE.substitute(column_name = column_name)
    for column_name in ['studentnummer', 'sinh_id', 'io_aanvr_id']
]]

@utils.add_to_docstring(definition.DOCSTRING, *quickfilter_docstrings)
@utils.add_quick_filter('studentnummer')
@utils.add_quick_filter('sinh_id')
@utils.add_quick_filter('io_aanvr_id')
def execute_query(
    query: TextClause|Path|str,
    parse_dates: list|dict|None = None,
    index_col: str|list[str]|None = None,
    dtype: str|dict|None = None,
    dtype_backend: str|None = None,
    squeeze: bool = True,
    **kwargs
) -> pd.DataFrame|pd.Series|None:
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
        squeeze = squeeze,
        **kwargs
    )


SEARCH_STRINGS = {
    'like': Template("${field} like '%${arg}%'"),
    'regex': Template("regexp_like(${field}, '${arg}')"),
    'exact': Template("${field} = '${arg}'"),
}


def find_table(
    *args: str,
    where: list|str|None = None,
    how: str = 'like',
    **kwargs
) -> pd.DataFrame:
    """
    Search for tables in the database based on specified criteria.

    Parameters:
    - *args (str): Substrings for matching table name.
    - where (list|None): Optional parameter allowing additional conditions for the query. Defaults to None.
    - how (str): One of:
        - 'like': use like to match
        - 'regex': use regex to match
        - 'exact': exact match
    - **kwargs: Additional keyword arguments to be passed to the underlying `execute_query` function.

    Returns:
    - pd.DataFrame: A DataFrame containing all tables meeting the specified criteria.
    """
    tpl = SEARCH_STRINGS[how]

    where = [] if where is None else where
    for arg in args:
        criterium = tpl.substitute(field='table_name', arg=arg.upper())
        where.append(criterium)

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
    how: str = 'like',
    how_data_type: str = 'like',
    how_table: str = 'exact',
    **kwargs
) -> pd.DataFrame:
    """
    Search for columns in the database based on specified criteria.

    Parameters:
    - *args (str): Substrings for matching column name.
    - table (str|None): Optional parameter specifying the table name for additional filtering. Defaults to None.
    - data_type (str|None): Optional parameter specifying the data type for additional filtering. Defaults to None.
    - where (list|None): Optional parameter allowing additional conditions for the query. Defaults to None.
    - how (str): One of:
        - 'like': use like to match
        - 'regex': use regex to match
        - 'exact': exact match
    - **kwargs: Additional keyword arguments to be passed to the underlying execute_query function.

    Returns:
    - pd.DataFrame: A DataFrame containing all columns meeting the specified criteria.
    """
    tpl = SEARCH_STRINGS[how]

    where = [] if where is None else where
    for arg in args:
        criterium = tpl.substitute(field='column_name', arg=arg.upper())
        where.append(criterium)

    if table:
        assert isinstance(table, str), "Table needs to be a string"
        tpl = SEARCH_STRINGS[how_table]
        criterium = tpl.substitute(field='table_name', arg=table.upper())
        where.append(criterium)

    if data_type:
        assert isinstance(data_type, str), "Data_type needs to be a string"
        tpl = SEARCH_STRINGS[how_data_type]
        criterium = tpl.substitute(field='data_type', arg=data_type.upper())
        where.append(criterium)

    df = execute_query(
        'reference/all_columns',
        where = where,
        **kwargs
    )
    return df


def get_table(
    table: str,
    **kwargs
) -> pd.DataFrame|None:
    """
    Fetch all results from `table`.

    Parameters:
    - table (str): Name of table to fetch.

    Returns:
    - pd.DataFrame: A DataFrame containing the data from `table`.
    """
    return execute_query(
        'reference/table',
        table = table,
        **kwargs
    )


def peek(
    table: str,
    n: int = 7,
    **kwargs
) -> pd.DataFrame|None:
    """
    Peek at first `n` results from `table`.

    Parameters:
    - table (str): Name of table to peek at.
    - n (int): Number of rows to return, defaults to 7.
    - **kwargs: Additional keyword arguments to be passed to the underlying execute_query function.

    Returns:
    - pd.DataFrame: A DataFrame containing the first `n` result from `table`.
    """
    return execute_query(
        'reference/table',
        table = table,
        n = n,
        **kwargs
    )


def get_values(
    table: str,
    column: str,
    max_results: int|None = 50
) -> pd.Series:
    """
    Retrieve unique values from a specified column in a database table.

    Parameters:
    - table (str): Name of the database table to query.
    - column (str): Name of the column from which to retrieve unique values.
    - max_results (int or None): Maximum number of results to return. If None, returns all unique values.
        Defaults to 50.

    Returns:
    - pd.Series: A pandas Series containing the unique values from the specified column, sorted in ascending order.

    Examples:
        >>> get_values('customers', 'country')
        0    Australia
        1    Canada
        2    France
        ...
        Name: country, dtype: object

        >>> get_values('orders', 'status', max_results=3)
        0    cancelled
        1    completed
        2    pending
        Name: status, dtype: object
    """
    sql = """
    select distinct {{ column }}
    from {{ table }}
    order by {{ column }}
    """
    return execute_query(sql, table=table, column=column, n=max_results)


def describe_table(table: str) -> pd.DataFrame:
    """
    Returns descriptive statistics for a table; including column names, data types, data length, precision, number of distinct values, null count, null percentage and number of rows.

    Parameters:
    - table (str): Name of the table to analyze.

    Returns
    - pd.DataFrame: statistics about the table.
    """
    df = execute_query('reference/table_stats', table = table)
    return df


def describe_column(
    table: str,
    column: str,
    n_sample_values: int = 7
) -> pd.Series:
    """
    Returns descriptive statistics and value samples for a column; including row counts, distinct value counts, nulls, most frequent values, and a sample of actual values.

    Parameters:
    - table (str): Name of the table to analyze.
    - column (str): Name of the column to analyze.
    - n_sample_values (int): Maximum number of distinct values to show in sample_values. Defaults to 7.

    Returns:
    - pd.Series: Statistics about the column, with index labels:
        - total_rows: Total number of rows in table
        - non_null_rows: Number of non-null values
        - unique_values: Number of distinct values
        - min_value: Minimum value in column
        - max_value: Maximum value in column
        - null_percentage: Fraction of rows that are null (0-1)
        - distinct_percentage: Fraction of non-null values that are unique (0-1)
        - most_frequent_value: Mode (most common value)
        - most_frequent_count: Frequency count of the mode
        - sample_values: Comma-separated string of up to n_sample_values distinct values
        Series name is set to "{table}/{column}".
    """
    s = execute_query(
        'reference/column_stats',
        table = table,
        column = column,
        n_sample_values = n_sample_values,
    )
    s = s.rename(f"{table}/{column}")
    return s


@utils.add_keyword_defaults(config.CONFIG['sanity']['osiris'])
def sanity(
    mutation_date_column: str = 'mutatie_datum',
    table: str = 'ost_student_inschrijfhist',
    threshold_in_hours: int = 1,
) -> pd.Series:
    """
    Perform sanity check on a table by identifying if the time between the last mutation and the current time exceeds a given threshold.

    Parameters:
    - mutation_date_column (str): Name of the column in the table that contains the mutation date. Default is 'mutatie_datum'.
    - table (str): Name of the table to perform the sanity check on. Default is 'ost_student_inschrijfhist'.
    - threshold_in_hours (int): Threshold value in hours. Default is 1 hour.

    Returns:
    - pd.DataFrame: A DataFrame containing the last mutation date, the time since the last mutation date, the threshold in hours and if this time is below ('Y') or above ('N') the set threshold.

    Warnings:
    - Raises a warning if the time since last mutation exceeds the threshold.
    """
    s = execute_query(
        'sanity/last_mutation',
        mutation_date_column = mutation_date_column,
        table = table,
        threshold_in_hours = threshold_in_hours
    )
    if s.loc['below_threshold'] == 'N':
        import warnings
        warnings.warn(
            f"Stale data in db\nTime since last mutation in table '{table}' exceeds threshold of {threshold_in_hours} hours.\nLast mutation was at {s.loc['max_mutation_date']}."
        )
    return s
