import os
import threading
import time
import warnings
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, NamedTuple

import pandas as pd
from pandas.io.sql import _wrap_result
from jinja2 import Environment
from sqlalchemy.exc import DBAPIError

from query import definition, errors, utils
from query.config import ConnectionSettings
from query.definition import AGG_KEYWORDS, EXECUTION_KEYWORDS, SQL, UTIL_KEYWORDS, Query
from query.errors import (
    ConnectError,
    KeywordError,
    QueryCancelled,
    QueryError,
    QueryNotFound,
    TemplateError,
)
from query.status import StatusLine


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
    max_rows: int | None = None,
    progress: bool = True,
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
    - max_rows (int | None):
        Stop fetching after this many rows. A client-side limit: the query is
        not changed (unlike the `n` keyword). Default is None (no limit).
    - progress (bool):
        Show a status line for queries that run longer than 2 s: updated in
        place in a notebook, a final line on stderr elsewhere. A truncated
        result (max_rows) always gets the final line. Default is True.
    - **kwargs:
        Additional keyword arguments passed to the query definition. A
        template variable with the name of one of this function's keywords
        (definition.EXECUTION_KEYWORDS) gets no value; this gives a warning.

    Returns:
    - pd.DataFrame | pd.Series | None: The query result, or None if the
        database side failed (ConnectError, QueryError) and `raise_errors` is
        False. The error is then shown and available as
        `<connection module>.last_error`.

    Raises:
    - QueryNotFound, TemplateError, KeywordError: Always; nothing was sent.
    - ConnectError, QueryError: Only with `raise_errors=True`.
    - TypeError: `query` is an SQL and template keywords were passed, or the
        statement returned no result set (after it has run).
    - KeyboardInterrupt: On an interrupt, after the statement was cancelled.
        A second interrupt stops waiting for the cancel; the statement may
        then still be running on the database.
    The errors of this library are stored as `<connection module>.last_error`.
    """
    dtype_backend = 'numpy_nullable' if dtype_backend is None else dtype_backend
    try:
        sql = definition.get_sql(query, env=env, **kwargs)
    except (QueryNotFound, TemplateError, KeywordError) as e:
        errors.remember(settings.name, e)
        raise
    _warn_clash(sql)
    if index_col is None:
        index_col = sql.params.get('columns')
    elif index_col == False:
        index_col = None
    ex = QueryExecution(sql, settings=settings)
    status = StatusLine(settings.name, sql.query.name, live=progress)
    try:
        result = _fetch_watched(
            ex, status,
            max_rows = max_rows,
            parse_dates = parse_dates,
            index_col = index_col,
            dtype = dtype,
            dtype_backend = dtype_backend,
        )
    except (ConnectError, QueryError) as error:
        errors.remember(settings.name, error)
        if raise_errors:
            raise
        errors.report(error)
        return None
    status.done(result)
    df = result.df
    if squeeze and len(df.columns) == 1:
        return df.squeeze(axis=1)
    if squeeze and len(df) == 1:
        return df.squeeze().rename(sql.query.name)
    return df


def _fetch_watched(ex: 'QueryExecution', status: StatusLine, **fetch_options: Any) -> 'FetchResult':
    """
    Run `ex.fetch(**fetch_options)` on a worker thread. This thread waits,
    updates the status line and handles an interrupt: cancel, wait for the
    worker, re-raise KeyboardInterrupt. A second interrupt stops waiting; the
    worker (a daemon thread) then finishes on its own and closes its
    connection.

    The worker is needed because on Windows an interrupt doesn't reach a
    thread that is waiting in the driver. Waiting is on an Event, never on
    join() or is_alive(): after an interrupted join, is_alive() can report a
    running thread as stopped (measured on Windows, Python 3.12).

    Errors from the worker are re-raised here. A result that arrives together
    with an interrupt is discarded: the interrupt wins.
    """
    outcome: dict[str, Any] = {}
    ended = threading.Event()

    def work() -> None:
        try:
            outcome['result'] = ex.fetch(**fetch_options)
        except BaseException as e:
            outcome['error'] = e
        finally:
            ended.set()

    threading.Thread(target=work, name=f'oq2 {ex.sql.query.name}', daemon=True).start()
    try:
        while not ended.wait(timeout=0.5):
            status.update(ex.progress)
    except KeyboardInterrupt:
        status.cancel_requested(ex.progress)
        ex.cancel()
        try:
            while not ended.wait(timeout=0.5):
                status.update(ex.progress)
        except KeyboardInterrupt:
            status.stopped_waiting(ex.progress)
            raise
        status.update(ex.progress)          # cancelled after ...
        raise
    status.update(ex.progress)              # failed after ..., if the line was showing
    if 'error' in outcome:
        raise outcome['error']
    return outcome['result']


# region QueryExecution
DEFAULT_CHUNKSIZE = 10_000      # batch size when the dialect has no arraysize


class Phase(StrEnum):
    PENDING = 'pending'
    CONNECTING = 'connecting'
    EXECUTING = 'executing'
    FETCHING = 'fetching'
    DONE = 'done'
    FAILED = 'failed'
    CANCELLED = 'cancelled'


_ENDED = frozenset({Phase.DONE, Phase.FAILED, Phase.CANCELLED})


@dataclass(frozen=True)
class Progress:
    """
    State of an execution. `rows` counts the rows fetched from the database
    (with max_rows this can be one more than returned); `elapsed` runs from
    the start of the run (0 before it).
    """
    phase: Phase
    rows: int
    elapsed: float


@dataclass
class FetchResult:
    """
    Result of `QueryExecution.fetch()`. `truncated` is True if the result had
    more than max_rows rows; `nbytes` is the frame's memory use.
    """
    df: pd.DataFrame
    truncated: bool
    rows: int
    nbytes: int
    elapsed: float


class QueryExecution:
    """
    One execution of one query: render, connect, execute, fetch. Used once.

    Rendering happens here, so QueryNotFound, TemplateError and KeywordError
    are raised on creation; `sql` can be inspected before running. `fetch()`
    returns one DataFrame, `chunks()` yields one per batch. `cancel()` and
    `progress` can be used from another thread.

    Phases: pending -> connecting -> executing -> fetching -> done, or failed
    / cancelled from any running phase.

    Errors (always raised): ConnectError, QueryError, QueryCancelled. A cancel
    gives no partial result; use max_rows or chunks() to stop and keep rows.
    A statement without a result set raises TypeError after it has run.

    Parameters:
    - query (SQL | Query | Path | str): Query name or file path, SQL string,
        a Query, or an SQL (then `params` must be empty).
    - settings (ConnectionSettings): `engine()` to connect, `cancel()` to
        stop a running statement, `name` for errors.
    - params (dict | None): Template parameters.
    - env (Environment | None): Jinja2 environment for a name or string.
    """

    def __init__(
        self,
        query: SQL | Query | Path | str,
        *,
        settings: ConnectionSettings,
        params: dict[str, Any] | None = None,
        env: Environment | None = None,
    ) -> None:
        if isinstance(query, SQL):
            if params:
                raise TypeError(
                    f"query is already rendered; parameters {sorted(params)} would be ignored"
                )
            self.sql = query
        else:
            self.sql = definition.as_query(query, env).render(**(params or {}))
        self.settings = settings
        self._lock = threading.Lock()   # guards the fields below against cancel() and progress
        self._phase = Phase.PENDING
        self._rows = 0
        self._start: float | None = None
        self._end: float | None = None
        self._cancelled = False
        self._dbapi_con: Any = None     # set while the statement runs, for cancel()
        self._columns: list[str] = []

    @property
    def progress(self) -> Progress:
        with self._lock:
            if self._start is None:
                elapsed = 0.0
            else:
                elapsed = (self._end or time.monotonic()) - self._start
            return Progress(self._phase, self._rows, elapsed)

    def cancel(self) -> None:
        """
        Stop the execution; thread-safe. While executing, the driver is asked
        to stop the statement (settings.cancel); otherwise a flag stops the
        execution at the next step. No-op once the execution has ended.
        """
        with self._lock:
            if self._phase in _ENDED:
                return
            self._cancelled = True
            if self._phase is Phase.EXECUTING and self._dbapi_con is not None:
                self.settings.cancel(self._dbapi_con)

    def fetch(
        self,
        *,
        max_rows: int | None = None,
        chunksize: int | None = None,
        on_progress: Callable[[Progress], None] | None = None,
        dtype_backend: str = 'numpy_nullable',
        parse_dates: list | dict | None = None,
        index_col: str | list[str] | None = None,
        dtype: str | dict | None = None,
    ) -> FetchResult:
        """
        Run the query and return the result as one DataFrame.

        - max_rows: stop after this many rows; `truncated` says whether there
            were more. None: no limit.
        - chunksize: rows per batch (limit, cancel and progress act between
            batches). Default: the dialect's arraysize, otherwise 10,000.
        - on_progress: called on every phase change and after every batch,
            on this thread.
        - dtype_backend, parse_dates, index_col, dtype: as for
            pd.read_sql_query, which builds its frame the same way.
        """
        data: list[tuple] = []
        truncated = False
        batches = self._batches(chunksize, max_rows, on_progress)
        try:
            for rows in batches:
                data.extend(rows)
                if max_rows is not None and len(data) > max_rows:
                    truncated = True
                    del data[max_rows:]
                    break
        finally:
            batches.close()     # stops the generator: cursor and connection closed
        df = _frame(
            data, self._columns,
            dtype_backend=dtype_backend, parse_dates=parse_dates, index_col=index_col, dtype=dtype,
        )
        return FetchResult(
            df=df,
            truncated=truncated,
            rows=len(data),
            nbytes=int(df.memory_usage(deep=True).sum()),
            elapsed=self.progress.elapsed,
        )

    def chunks(
        self,
        *,
        chunksize: int | None = None,
        dtype_backend: str = 'numpy_nullable',
        parse_dates: list | dict | None = None,
        index_col: str | list[str] | None = None,
        dtype: str | dict | None = None,
    ) -> Iterator[pd.DataFrame]:
        """
        Run the query and yield a DataFrame per batch. Stopping early (break)
        closes the connection and ends the execution as done. Dtypes are
        inferred per chunk and can differ between chunks.
        """
        batches = self._batches(chunksize, None, None)
        try:
            for rows in batches:
                yield _frame(
                    rows, self._columns,
                    dtype_backend=dtype_backend, parse_dates=parse_dates, index_col=index_col, dtype=dtype,
                )
        finally:
            batches.close()

    def _batches(
        self,
        chunksize: int | None,
        max_rows: int | None,
        on_progress: Callable[[Progress], None] | None,
    ) -> Iterator[list[tuple]]:
        """
        Connect, execute and fetch; yield raw row batches. Shared by fetch()
        and chunks(). Keeps phase and row count; closing the generator early
        (max_rows, break) ends the execution as done.
        """
        report = on_progress or (lambda progress: None)
        self._begin()
        report(self.progress)
        engine = self.settings.engine()
        try:
            try:
                con = engine.raw_connection()
            except DBAPIError as e:
                raise self._connect_error(e) from e
            try:
                if not self._executing(con):
                    raise self._cancelled_error()     # cancelled while connecting
                report(self.progress)
                try:
                    cursor, size = _cursor(con, engine.dialect, chunksize, max_rows)
                    cursor.execute(self.sql.text)
                    if cursor.description is None:    # PL/SQL block, DML, DDL: has already run
                        raise TypeError(
                            "statement returned no result set; only queries that return rows are supported"
                        )
                    self._columns = _column_names(cursor.description, engine.dialect)
                    with self._lock:
                        self._phase = Phase.FETCHING
                    report(self.progress)
                    while not self._cancelled:
                        rows = cursor.fetchmany(size)
                        if not rows:
                            break
                        with self._lock:
                            self._rows += len(rows)
                        yield rows
                        report(self.progress)
                except engine.dialect.loaded_dbapi.Error as e:
                    # A cancel during execute always ends in a driver error.
                    if self._cancelled:
                        raise self._cancelled_error() from e
                    raise self._query_error(e) from e
                if self._cancelled:
                    raise self._cancelled_error()
            finally:
                with self._lock:
                    self._dbapi_con = None
                con.close()
        except QueryCancelled:
            self._finish(Phase.CANCELLED)
            report(self.progress)
            raise
        except GeneratorExit:       # the caller stopped early: max_rows, or break in chunks()
            self._finish(Phase.DONE)
            report(self.progress)
            raise
        except BaseException:
            self._finish(Phase.FAILED)
            report(self.progress)
            raise
        self._finish(Phase.DONE)
        report(self.progress)

    def _begin(self) -> None:
        with self._lock:
            if self._phase is not Phase.PENDING:
                raise RuntimeError("QueryExecution runs once; create a new one to run again")
            self._phase = Phase.CONNECTING
            self._start = time.monotonic()

    def _executing(self, con: Any) -> bool:
        """Phase executing with `con`; False if cancel() came while connecting."""
        with self._lock:
            if self._cancelled:
                return False
            self._phase = Phase.EXECUTING
            self._dbapi_con = con
            return True

    def _finish(self, phase: Phase) -> None:
        with self._lock:
            self._phase = phase
            self._end = time.monotonic()

    def _connect_error(self, e: DBAPIError) -> ConnectError:
        driver = _driver_error(e.orig if e.orig is not None else e)
        return ConnectError(
            connection=self.settings.name,
            code=driver.code,
            db_message=driver.message,
        )

    def _query_error(self, orig: BaseException) -> QueryError:
        driver = _driver_error(orig)
        raw_offset = driver.offset
        if errors.caret_offset(driver.code, raw_offset, self.sql.text) is not None:
            raw_offset = _exact_offset(self.settings, self.sql.text, driver.code)
        not_passed, unused = _param_report(self.sql)
        return QueryError(
            query_name=self.sql.query.name,
            connection=self.settings.name,
            sql=self.sql.text,
            code=driver.code,
            db_message=driver.message,
            raw_offset=raw_offset,
            passed=dict(self.sql.params),
            not_passed=not_passed,
            unused=unused,
            path=self.sql.query.path,
        )

    def _cancelled_error(self) -> QueryCancelled:
        return QueryCancelled(query_name=self.sql.query.name, connection=self.settings.name)


def _cursor(con: Any, dialect: Any, chunksize: int | None, max_rows: int | None) -> tuple[Any, int]:
    """
    A cursor on `con` and the batch size. Batch size: `chunksize`, else the
    dialect's arraysize (Oracle), else DEFAULT_CHUNKSIZE; with max_rows capped
    at max_rows + 1 (one extra row tells whether the result is truncated).
    fetchmany(n) makes round trips until it has n rows, so the batch size is
    capped as well as the round trip size. cursor.arraysize is set only when
    the dialect has one: only Oracle fetches in round trips of that size.
    """
    arraysize = getattr(dialect, 'arraysize', None)
    size = chunksize or arraysize or DEFAULT_CHUNKSIZE
    if max_rows is not None:
        size = min(size, max_rows + 1)
    cursor = con.cursor()
    if arraysize:
        cursor.arraysize = min(arraysize, size)
    return cursor, size


def _column_names(description: Any, dialect: Any) -> list[str]:
    """
    Column names as SQLAlchemy reports them through pd.read_sql_query. For
    dialects that normalize names (Oracle), case-insensitive uppercase names
    become lowercase (SINH_ID -> sinh_id); quoted mixed-case names stay as
    they are. Other dialects: names as the driver reports them.
    """
    names = [d[0] for d in description]
    if getattr(dialect, 'requires_name_normalize', False):
        names = [dialect.normalize_name(name) for name in names]
    return names


def _frame(data: list[tuple], columns: list[str], **options: Any) -> pd.DataFrame:
    """
    DataFrame from raw rows, built as pd.read_sql_query builds it (pandas'
    private _wrap_result), so dtypes match read_sql_query. A pandas upgrade that
    renames or changes it fails here loudly; the parity test guards behaviour.
    """
    return _wrap_result(data, columns, **options)


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


# Frames in this package; trailing separator, so a sibling folder like
# query_scripts/ does not match. Not resolved: frames carry the path as imported.
_PACKAGE_DIR = os.path.join(os.path.dirname(__file__), "")


def _warn_clash(sql: SQL) -> None:
    """
    Warn when the template uses a name execute_query consumes itself
    (EXECUTION_KEYWORDS): such a variable never gets a value through
    execute_query. A failure in the analysis is ignored: it never stops a
    query that rendered.
    """
    try:
        clash = EXECUTION_KEYWORDS & sql.query.params()
    except Exception:
        return
    if clash:
        warnings.warn(
            f"{sql.query.name}: template uses {sorted(clash)}; these are "
            "consumed by execute_query and won't reach the template",
            skip_file_prefixes=(_PACKAGE_DIR,),    # shown at the caller, past the wrappers
        )
