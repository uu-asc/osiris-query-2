"""
Errors raised or reported by osiris-query-2.

Leaf module: no pandas, SQLAlchemy or driver imports, so `config.py` and
`definition.py` can import it, and it can be tested without a database.
Driver details are read by duck typing where the errors are created.

- OsirisQueryError: base for all errors of this library.
- ConfigError: invalid or incomplete configuration (also importable from
  `query.config`).
- QueryError: the database received the SQL and rejected it, or failed
  executing it.
- ConnectError: connecting failed; no SQL was sent.
- TemplateError: rendering the template failed; nothing was sent.

The error objects hold data only. The text rendering (`__str__`) is built
from the fields with the helpers below.
"""

import reprlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


class OsirisQueryError(Exception):
    """Base for all errors raised or reported by osiris-query-2."""


class ConfigError(OsirisQueryError):
    """Invalid or incomplete configuration."""


# region position
# Compile-time errors: the database rejected the statement before running it,
# and the driver's offset points into the SQL. Any other code gets no caret:
# a missing caret loses information, a caret on a runtime error points at the
# wrong place.
_COMPILE_ERRORS = frozenset(range(900, 1000)) | {
    1747,   # invalid user.table.column, table.column, or column specification
    1789,   # query block has incorrect number of result columns
}


def caret_offset(code: str | None, raw_offset: int | None, sql: str) -> int | None:
    """
    Character offset in `sql` for the caret, or None if there is none.

    Parameters:
    - code (str | None): Error code as reported by the driver, e.g. 'ORA-00904'.
    - raw_offset (int | None): Offset as reported by the driver. oracledb
      counts bytes of the UTF-8 encoded statement. 0 means unknown.
    - sql (str): The statement as the driver received it.

    Returns:
    - int | None: Offset in characters, or None for unknown offsets, codes
      outside the compile-time allowlist and non-Oracle codes.
    """
    if not raw_offset or code is None or not code.startswith('ORA-'):
        return None
    number = code[4:]
    if not number.isdigit() or int(number) not in _COMPILE_ERRORS:
        return None
    return len(sql.encode('utf-8')[:raw_offset].decode('utf-8', errors='ignore'))


def _position(text: str, offset: int) -> tuple[int, int]:
    """(line, column) of `offset` in `text`, both 1-based."""
    before = text[:offset]
    return before.count('\n') + 1, offset - before.rfind('\n')


# region rendering helpers
TABSIZE = 4
CONTEXT_BEFORE = 5
CONTEXT_AFTER = 2


def _lines(text: str) -> list[str]:
    """Split on '\\n' (as `_position` counts), drop a trailing '\\r'."""
    return [line.removesuffix('\r') for line in text.split('\n')]


def _context(
    text: str,
    line: int,
    before: int = CONTEXT_BEFORE,
    after: int = CONTEXT_AFTER,
) -> list[tuple[int, str, bool]]:
    """
    Lines around `line` (1-based) as (lineno, text, is_error_line).
    """
    lines = _lines(text)
    start = max(1, line - before)
    end = min(len(lines), line + after)
    return [(n, lines[n - 1], n == line) for n in range(start, end + 1)]


def _caret(line_text: str, column: int, tabsize: int = TABSIZE) -> str:
    """Spaces up to `column` (1-based) of `line_text`, then '^'. Tabs expanded."""
    return ' ' * len(line_text[:column - 1].expandtabs(tabsize)) + '^'


_repr = reprlib.Repr()
_repr.maxstring = 60
_repr.maxother = 60
_repr.maxlist = 8
_repr.maxtuple = 8
_repr.maxset = 8
_repr.maxdict = 8
SHORT_WIDTH = 80


def _short(value: Any, width: int = SHORT_WIDTH) -> str:
    """Shortened repr of `value`, at most `width` characters."""
    text = _repr.repr(value)
    return text if len(text) <= width else text[:width - 3] + '...'


def _timestamp(moment: datetime) -> str:
    return f"{moment:%Y-%m-%d %H:%M:%S}"


def _numbered(
    rows: list[tuple[int, str, bool]],
    width: int,
    column: int | None = None,
) -> list[str]:
    """
    Render rows as '  12 | text'. The error line gets '>' in the gutter and,
    when `column` is given, a caret line below it.
    """
    out = []
    for lineno, text, is_error in rows:
        marker = '>' if is_error else ' '
        out.append(f"{marker} {lineno:>{width}} | {text.expandtabs(TABSIZE)}")
        if is_error and column is not None:
            out.append(f"  {'':>{width}} | {_caret(text, column)}")
    return out


def _all_rows(text: str, error_line: int | None) -> list[tuple[int, str, bool]]:
    return [(n, line, n == error_line) for n, line in enumerate(_lines(text), start=1)]


def _param_lines(
    passed: dict[str, Any],
    not_passed: list[str] | None,
    unused: list[str] | None,
) -> list[str]:
    """
    Parameter section. `not_passed` and `unused` are left out when None
    (no template) or empty.
    """
    values = ', '.join(f"{k}={_short(v)}" for k, v in passed.items()) or '(none)'
    out = [f"passed      {values}"]
    if not_passed:
        out.append(f"not passed  {', '.join(not_passed)}")
    if unused:
        out.append(f"unused      {', '.join(unused)}")
    return out


# region errors
@dataclass(eq=False)
class QueryError(OsirisQueryError):
    """
    The database received the SQL and rejected it, or failed executing it.

    Fields:
    - query_name: Query path, or '<n/a>' for inline SQL.
    - connection: Connection name (settings.name).
    - sql: The statement as the driver received it.
    - code: Driver error code, e.g. 'ORA-00904'; None if unknown.
    - db_message: Message from the driver.
    - raw_offset: Offset as reported by the driver (bytes for oracledb).
    - passed: Keyword arguments as received by `execution.execute_query`.
    - not_passed: Template variables without a value. Some may be optional.
      None if there is no template (TextClause).
    - unused: Passed, but neither a template variable nor a library keyword.
      None if there is no template.
    - raised_at: When the error occurred.
    - offset: Derived. Character offset in `sql` where the database stopped
      parsing; None if unknown or not meaningful (see `caret_offset`).
    """
    query_name: str
    connection: str
    sql: str
    code: str | None
    db_message: str
    raw_offset: int | None = None
    passed: dict[str, Any] = field(default_factory=dict)
    not_passed: list[str] | None = None
    unused: list[str] | None = None
    raised_at: datetime = field(default_factory=datetime.now)
    offset: int | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        super().__init__(self.db_message)
        self.offset = caret_offset(self.code, self.raw_offset, self.sql)

    @property
    def position(self) -> tuple[int, int] | None:
        """(line, column), 1-based, of `offset` in `sql`; None without offset."""
        if self.offset is None:
            return None
        return _position(self.sql, self.offset)

    def __repr__(self) -> str:
        return f"QueryError({self.code!r}, query={self.query_name!r})"

    def __str__(self) -> str:
        out = [
            f"QueryError [{self.connection}] {self.query_name}  ({_timestamp(self.raised_at)})",
            self.db_message,
            '',
            *_param_lines(self.passed, self.not_passed, self.unused),
            '',
        ]
        width = len(str(len(_lines(self.sql))))
        position = self.position
        if position is not None:
            line, column = position
            out.append(
                f"line {line}, column {column} "
                "(where the database stopped parsing; the mistake can be earlier)"
            )
            out.extend(_numbered(_context(self.sql, line), width, column))
            out.append('')
        error_line = position[0] if position else None
        out.append(f"full SQL ({len(_lines(self.sql))} lines)")
        out.extend(_numbered(_all_rows(self.sql, error_line), width))
        return '\n'.join(out)


@dataclass(eq=False)
class ConnectError(OsirisQueryError):
    """
    Connecting to the database failed. No SQL was sent.

    Fields:
    - connection: Connection name (settings.name).
    - code: Driver error code, e.g. 'ORA-01017', 'DPY-6005'; None if unknown.
    - db_message: Message from the driver.
    - raised_at: When the error occurred.
    """
    connection: str
    code: str | None
    db_message: str
    raised_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self) -> None:
        super().__init__(self.db_message)

    def __repr__(self) -> str:
        return f"ConnectError({self.code!r}, connection={self.connection!r})"

    def __str__(self) -> str:
        return '\n'.join([
            f"ConnectError [{self.connection}]  ({_timestamp(self.raised_at)})",
            self.db_message,
        ])


@dataclass(eq=False)
class TemplateError(OsirisQueryError):
    """
    Rendering the template failed. Nothing was sent to the database.

    Fields:
    - query_name: Query path, or '<n/a>' for inline SQL.
    - template: Template in which the error occurred: the query itself, an
      included file, or '<string>' for inline SQL.
    - message: What went wrong.
    - source: Source of `template`; None when context lines would not help
      (e.g. the `raise()` helper in library templates).
    - lineno: Line in `source`, 1-based; None if unknown.
    - raised_at: When the error occurred.
    """
    query_name: str
    template: str
    message: str
    source: str | None = None
    lineno: int | None = None
    raised_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def __repr__(self) -> str:
        return (
            f"TemplateError(query={self.query_name!r}, "
            f"template={self.template!r}, lineno={self.lineno!r})"
        )

    def __str__(self) -> str:
        header = f"TemplateError {self.query_name}  ({_timestamp(self.raised_at)})"
        out = [header]
        if self.template != self.query_name:
            out.append(f"in {self.template}")
        out.append(self.message)
        if self.source is not None and self.lineno is not None:
            width = len(str(len(_lines(self.source))))
            out.append('')
            out.append(f"line {self.lineno}")
            out.extend(_numbered(_context(self.source, self.lineno), width))
        return '\n'.join(out)


# region last error
_last_errors: dict[str, OsirisQueryError] = {}


def remember(connection: str, error: OsirisQueryError) -> None:
    """Store `error` as the most recent error for `connection`."""
    _last_errors[connection] = error


def last_error(connection: str) -> OsirisQueryError | None:
    """Most recent error for `connection`, whenever it occurred; None if none."""
    return _last_errors.get(connection)
