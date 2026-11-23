from pathlib import Path
from typing import NamedTuple

import pandas as pd
from jinja2 import Environment
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, StatementError
from pandas.errors import DatabaseError as PandasDatabaseError

from query import definition, errors, utils
from query.config import ConnectionSettings
from query.definition import AGG_KEYWORDS, SQL, UTIL_KEYWORDS, Query
from query.errors import ConnectError, KeywordError, QueryError, QueryNotFound, TemplateError


@utils.add_to_docstring(definition.DOCSTRING)
def execute_query(
    query: SQL | Query | Path | str,
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
    - query (SQL | Query | Path | str):
        Query name or file path, SQL string, a Query, or an SQL from
        `get_sql` (then no keyword arguments for the template may be passed).
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
    - TypeError: `query` is an SQL and template keywords were passed.
    The errors of this library are stored as `<connection module>.last_error`.
    """
    dtype_backend = 'numpy_nullable' if dtype_backend is None else dtype_backend
    try:
        sql = definition.get_sql(query, env=env, **kwargs)
    except (QueryNotFound, TemplateError, KeywordError) as e:
        errors.remember(settings.name, e)
        raise
    con = settings.engine()
    if index_col is None:
        index_col = sql.params.get('columns')
    elif index_col == False:
        index_col = None
    try:
        df = pd.read_sql_query(
            text(sql.text),     # old path only; QueryExecution passes the string as it is
            con,
            parse_dates = parse_dates,
            index_col = index_col,
            dtype = dtype,
            dtype_backend = dtype_backend,
        )
        if squeeze and len(df.columns) == 1:
            return df.squeeze(axis=1)
        if squeeze and len(df) == 1:
            return df.squeeze().rename(sql.query.name)
        return df

    except (DBAPIError, PandasDatabaseError) as e:
        error = _database_error(e, settings, sql)
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


def _exact_offset(settings: ConnectionSettings, sql: str, code: str) -> int | None:
    """
    Error position in `sql`, from parsing it again with cursor_sharing = exact.

    The osiris database runs with cursor_sharing = force: before parsing,
    Oracle replaces every literal by a bind placeholder, and the error position
    it reports refers to that rewritten text. In

        select 1 from no_such_table

    the table name is at 14, but Oracle reports 23: '1' became ':"SYS_B_0"'.
    Each literal before the error shifts the position, so the reported offset
    is wrong in most real queries (rendered parameters are literals).
    Parsing again in a session with cursor_sharing = exact (this connection
    only; nothing is executed) gives the position in the SQL as written.

    Only called for errors that get a caret (see errors.caret_offset), so it
    costs an extra connection only for compile-time errors. Returns None if
    the connection isn't Oracle, or the parse fails differently or not at all.
    Never raises: a failure here only loses the caret.
    """
    try:
        engine = settings.engine()
        if engine.dialect.name != 'oracle':
            return None
        con = engine.raw_connection()
        try:
            cur = con.cursor()
            cur.execute('alter session set cursor_sharing = exact')
            cur.parse(sql)
        finally:
            con.close()
    except Exception as e:
        driver = _driver_error(e)
        return driver.offset if driver.code == code else None
    return None


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


def _param_report(sql: SQL) -> tuple[list[str] | None, list[str] | None]:
    """
    (not_passed, unused) for QueryError, from the query `sql` was rendered
    from. (None, None) if the analysis fails: it never replaces the original
    error.
    """
    try:
        variables = sql.query.params()
    except Exception:
        return None, None
    passed = set(sql.params)
    not_passed = sorted(variables - passed)
    unused = sorted(passed - variables - set(UTIL_KEYWORDS) - set(AGG_KEYWORDS))
    return not_passed, unused


def _database_error(
    e: BaseException,
    settings: ConnectionSettings,
    sql: SQL,
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
    raw_offset = driver.offset
    if errors.caret_offset(driver.code, raw_offset, stmt.statement) is not None:
        raw_offset = _exact_offset(settings, stmt.statement, driver.code)
    not_passed, unused = _param_report(sql)
    return QueryError(
        query_name=sql.query.name,
        connection=settings.name,
        sql=stmt.statement,
        code=driver.code,
        db_message=driver.message,
        raw_offset=raw_offset,
        passed=dict(sql.params),
        not_passed=not_passed,
        unused=unused,
        path=sql.query.path,
    )
