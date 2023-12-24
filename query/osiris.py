from pathlib import Path

import pandas as pd
from sqlalchemy import TextClause

from query import config, connection, execution
from query.definition import get_sql


SCHEMA = config.load_schema('osiris')


def execute_query(
    query: TextClause|Path|str,
    parse_dates: list|dict|None = None,
    index_col: str|list[str]|None = None,
    dtype: str|dict|None = None,
    dtype_backend: str|None = None,
    **kwargs
) -> pd.DataFrame|None:
    path_to_credentials = config.get_path_from_config('osiris', 'credentials')
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
