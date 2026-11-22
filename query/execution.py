from pathlib import Path
from typing import Any, NamedTuple

import pandas as pd
from jinja2 import Environment
from sqlalchemy import TextClause
from sqlalchemy.exc import DBAPIError, StatementError
from pandas.errors import DatabaseError as PandasDatabaseError

from query import definition, errors, utils
from query.config import ConnectionSettings
from query.definition import (
    AGG_KEYWORDS,
    DEFAULT_QUERY_NAME,
    UTIL_KEYWORDS,
    QuerySource,
    get_params,
)
from query.errors import ConnectError, KeywordError, QueryError, QueryNotFound, TemplateError


@utils.add_to_docstring(definition.DOCSTRING)
def execute_query(
    query: TextClause | Path | str,
    /,
    settings: ConnectionSettings,
    *,
    env: Environment | None = None,
    parse_dates: list | dict | None = None,
    index_col: str | list[str] | None = None,
    dtype: str | dict | None = None,
    dtype_backend: str = 'numpy_nullable', # numpy_nullable / pyarrow
    squeeze: bool = True,
    raise_errors: bool = False,
    **kwargs
) -> pd.DataFrame | pd.Series | None:
    """
    Execute a SQL query and return the result as a pandas DataFrame.

    Parameters:
    - query (TextClause | Path | str):
        SQL query definition, file path, or raw SQL string.
    - settings (ConnectionSettings):
        Validated connection settings; `settings.engine()` creates the engine.
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
    - raise_errors (bool):
        Raise ConnectError/QueryError instead of showing it and returning
        None. Default is False.
    - **kwargs:
        Additional keyword arguments passed to the query definition.

    Returns:
    - pd.DataFrame | pd.Series | None: The query result, or None if the
        database side failed (ConnectError, QueryError) and `raise_errors` is
        False. The error is then shown and available as
        `<connection module>.last_error`.

    Raises:
    - QueryNotFound, TemplateError, KeywordError: Always; nothing was sent.
    - ConnectError, QueryError: Only with `raise_errors=True`.
    All errors are stored as `<connection module>.last_error`.
    """
    dtype_backend = 'numpy_nullable' if dtype_backend is None else dtype_backend
    is_template = not isinstance(query, TextClause)
    try:
        if is_template:
            source = definition.load_query(query, env=env)
            sql = definition.get_sql(source, env=env, **kwargs)
        else:
            source = QuerySource(text=query.text, name=DEFAULT_QUERY_NAME, path=None)
            sql = query
    except (QueryNotFound, TemplateError, KeywordError) as e:
        errors.remember(settings.name, e)
        raise
    con = settings.engine()
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
            return df.squeeze().rename(source.name)
        return df

    except (DBAPIError, PandasDatabaseError) as e:
        error = _database_error(e, settings, source, sql, is_template, kwargs, env)
        if error is None:
            raise           # not an error from the driver: leave it as it is
        errors.remember(settings.name, error)
        if raise_errors:
            raise error from e
        errors.report(error)
        return None


# region database errors
class DriverError(NamedTuple):
    """Error details as reported by the driver."""
    code: str | None
    message: str
    offset: int | None


def _driver_error(orig: BaseException) -> DriverError:
    """
    Details of a driver exception. oracledb puts an error object in args[0]
    with full_code, offset and message; for other drivers code and offset are
    None and the message is str(orig).
    """
    info = orig.args[0] if orig.args else None
    return DriverError(
        code=getattr(info, 'full_code', None),
        message=getattr(info, 'message', None) or str(orig),
        offset=getattr(info, 'offset', None),
    )


def _statement_error(e: BaseException | None) -> StatementError | None:
    """
    First SQLAlchemy StatementError in the __cause__ chain of `e`. pandas
    wraps the SQLAlchemy error for SQL errors, not for connect errors.
    """
    while e is not None:
        if isinstance(e, StatementError):
            return e
        e = e.__cause__
    return None


def _param_report(
    source: QuerySource,
    is_template: bool,
    kwargs: dict[str, Any],
    env: Environment | None,
) -> tuple[list[str] | None, list[str] | None]:
    """
    (not_passed, unused) for QueryError; (None, None) without a template.
    A failure in the analysis never replaces the original error.
    """
    if not is_template:
        return None, None
    try:
        variables = get_params(source, env=env)
    except Exception:
        return None, None
    passed = set(kwargs)
    not_passed = sorted(variables - passed)
    unused = sorted(passed - variables - set(UTIL_KEYWORDS) - set(AGG_KEYWORDS))
    return not_passed, unused


def _database_error(
    e: BaseException,
    settings: ConnectionSettings,
    source: QuerySource,
    sql: TextClause,
    is_template: bool,
    kwargs: dict[str, Any],
    env: Environment | None,
) -> ConnectError | QueryError | None:
    """
    ConnectError if no statement was sent (SQLAlchemy's `statement is None`),
    otherwise QueryError. None if there is no StatementError in the chain:
    then the error didn't come from the driver.
    """
    stmt = _statement_error(e)
    if stmt is None:
        return None
    driver = _driver_error(stmt.orig if stmt.orig is not None else stmt)
    if stmt.statement is None:
        return ConnectError(
            connection=settings.name,
            code=driver.code,
            db_message=driver.message,
        )
    not_passed, unused = _param_report(source, is_template, kwargs, env)
    return QueryError(
        query_name=source.name,
        connection=settings.name,
        sql=stmt.statement,
        code=driver.code,
        db_message=driver.message,
        raw_offset=driver.offset,
        passed=dict(kwargs),
        not_passed=not_passed,
        unused=unused,
        path=source.path,
    )
