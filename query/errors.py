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
- TemplateError: rendering the user's template failed; nothing was sent.
- QueryNotFound: the query file was not found.
- KeywordError: library keywords (where, values, totals, ...) were passed in
  an invalid way; nothing was sent.

The error objects hold data only. Both renderings are built from the fields:
- text (`__str__`): logs, stderr, and the traceback IPython shows when the
  error is raised (`_render_traceback_`).
- HTML (`_repr_html_`, via `_repr_mimebundle_`): notebooks. Jinja templates
  and a stylesheet in `query/html/`; SQL highlighted with pygments when it is
  installed. jinja2, markupsafe, pygments and IPython are imported only when
  needed, so this module imports without them.

`report(error)` shows an error: rich display under IPython, text on stderr
otherwise.
"""

import functools
import re
import reprlib
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
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


# region html
HTML_DIR = Path(__file__).parent / 'html'

# First match wins: Comment.Preproc (Jinja delimiters) before Comment.
_TOKEN_CLASSES = (
    ('Comment.Preproc', 'tok-t'),
    ('Comment', 'tok-c'),
    ('Literal.String', 'tok-s'),
    ('Literal.Number', 'tok-n'),
    ('Keyword', 'tok-k'),
    ('Operator', 'tok-o'),
    ('Name.Builtin', 'tok-b'),
    ('Name.Variable', 'tok-v'),
)

_URL = re.compile(r'https?://[^\s<>"\']+')
_PLAIN_TOKENS = re.compile(r'\w+|\s+|\S')


@functools.cache
def _html_env() -> Any:
    """Jinja environment for the HTML renderings; autoescaping on."""
    from jinja2 import Environment, FileSystemLoader
    from markupsafe import Markup
    env = Environment(
        loader=FileSystemLoader(HTML_DIR),
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters['linkify'] = _linkify
    env.globals['css'] = Markup((HTML_DIR / 'error.css').read_text(encoding='utf-8'))
    return env


def _render_html(template: str, **context: Any) -> str:
    return _html_env().get_template(template).render(**context)


def _linkify(text: str) -> Any:
    """Escape `text`; turn http(s) URLs into links."""
    from markupsafe import Markup, escape
    parts = []
    last = 0
    for match in _URL.finditer(text):
        url = match.group().rstrip('.,;:)')
        parts.append(escape(text[last:match.start()]))
        parts.append(Markup('<a href="{0}" target="_blank" rel="noopener noreferrer">{0}</a>').format(url))
        last = match.start() + len(url)
    parts.append(escape(text[last:]))
    return Markup('').join(parts)


@functools.cache
def _token_classes() -> tuple[tuple[Any, str], ...]:
    from pygments import token
    def resolve(path: str) -> Any:
        ttype = token.Token
        for part in path.split('.'):
            ttype = getattr(ttype, part)
        return ttype
    return tuple((resolve(path), cls) for path, cls in _TOKEN_CLASSES)


def _token_class(ttype: Any) -> str:
    for parent, cls in _token_classes():
        if ttype in parent:
            return cls
    return ''


Segments = list[tuple[str, str]]     # (css class, text) per token, one line


def _segments(text: str, lexer: str) -> list[Segments]:
    """
    Highlighted `text`, one list of (css class, text) per line, split as
    `_lines` splits. Plain text (class '') if pygments is not installed or its
    output doesn't line up with `_lines`.
    """
    expected = _lines(text)
    lines: list[Segments] | None = None
    try:
        from pygments.lexers import get_lexer_by_name
    except ImportError:
        pass
    else:
        lines = [[]]
        tokens = get_lexer_by_name(lexer, stripnl=False, ensurenl=False).get_tokens(text)
        for ttype, value in tokens:
            cls = _token_class(ttype)
            for i, part in enumerate(value.split('\n')):
                if i:
                    lines.append([])
                if part:
                    lines[-1].append((cls, part))
    aligned = (
        lines is not None
        and len(lines) == len(expected)
        and all(''.join(t for _, t in segs) == line for segs, line in zip(lines, expected))
    )
    if not aligned:
        # Unhighlighted, but split into words, spaces and symbols, so the
        # underline at the error position still covers one token.
        return [[('', part) for part in _PLAIN_TOKENS.findall(line)] for line in expected]
    return lines


def _mark_range(segments: Segments, column: int) -> tuple[int, int]:
    """
    Character range on the line to underline: from `column` (1-based) to the
    end of the token there. On whitespace, the next token is included.
    """
    start = column - 1
    pos = 0
    for i, (_, text) in enumerate(segments):
        if pos <= start < pos + len(text):
            end = pos + len(text)
            if not text[start - pos:].strip() and i + 1 < len(segments):
                end += len(segments[i + 1][1])
            return start, end
        pos += len(text)
    return start, start          # past the end of the line


def _line_html(segments: Segments, mark: tuple[int, int] | None = None) -> Any:
    """One line as HTML: escaped token text in classed spans, `mark` underlined."""
    from markupsafe import Markup, escape

    def piece(cls: str, text: str, marked: bool) -> Any:
        html = escape(text)
        if marked:
            html = Markup('<span class="mark">{}</span>').format(html)
        return Markup('<span class="{}">{}</span>').format(cls, html) if cls else html

    out = []
    pos = 0
    for cls, text in segments:
        if mark is None:
            out.append(piece(cls, text, False))
        else:
            a = min(max(mark[0] - pos, 0), len(text))
            b = min(max(mark[1] - pos, 0), len(text))
            for part, marked in ((text[:a], False), (text[a:b], True), (text[b:], False)):
                if part:
                    out.append(piece(cls, part, marked))
        pos += len(text)
    if mark is not None and mark[0] >= pos:
        out.append(Markup('<span class="mark"> </span>'))
    return Markup('').join(out)


def _html_rows(
    lines: list[Segments],
    numbers: range | list[int],
    error_line: int | None = None,
    column: int | None = None,
) -> list[dict[str, Any]]:
    """Rows for the `code` macro: gutter text, line HTML, error flag."""
    width = len(str(len(lines)))
    rows = []
    for n in numbers:
        is_error = n == error_line
        mark = _mark_range(lines[n - 1], column) if is_error and column is not None else None
        rows.append({
            'gutter': f"{n:>{width}} │ ",
            'html': _line_html(lines[n - 1], mark),
            'error': is_error,
        })
    return rows


class _Displayable:
    """Display hooks shared by the reported errors (not ConfigError)."""

    def _repr_html_(self) -> str:
        raise NotImplementedError

    def _repr_mimebundle_(self, include: Any = None, exclude: Any = None) -> dict[str, str]:
        bundle = {'text/plain': str(self)}
        try:
            bundle['text/html'] = self._repr_html_()
        except Exception:       # a broken HTML rendering must not hide the error
            pass
        return bundle

    def _render_traceback_(self) -> list[str]:
        """Used by IPython instead of the traceback when the error is raised."""
        connection = getattr(self, 'connection', None)
        where = f"{connection}.last_error" if connection else 'sys.last_value'
        return [*str(self).splitlines(), '', f"(HTML view: {where})"]


# region errors
@dataclass(eq=False)
class QueryError(_Displayable, OsirisQueryError):
    """
    The database received the SQL and rejected it, or failed executing it.

    Fields:
    - query_name: Query path, or '<inline>' for inline SQL.
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
    - path: Full path of the query file; None for inline SQL.
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
    path: str | None = None
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
        out = [f"QueryError [{self.connection}] {self.query_name}  ({_timestamp(self.raised_at)})"]
        if self.path:
            out.append(self.path)
        out += [
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

    def _repr_html_(self) -> str:
        lines = _segments(self.sql, 'sql')
        position = self.position
        line, column = position if position else (None, None)
        context = []
        if line is not None:
            numbers = [n for n, _, _ in _context(self.sql, line)]
            context = _html_rows(lines, numbers, line, column)
        return _render_html(
            'query_error.html.j2',
            e=self,
            kind='QueryError',
            title=[self.connection, self.query_name],
            raised_at=_timestamp(self.raised_at),
            message=self.db_message,
            passed=[(k, _short(v)) for k, v in self.passed.items()],
            position=position,
            context=context,
            full=_html_rows(lines, range(1, len(lines) + 1), line, column),
        )


@dataclass(eq=False)
class ConnectError(_Displayable, OsirisQueryError):
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

    def _repr_html_(self) -> str:
        return _render_html(
            'base.html.j2',
            e=self,
            kind='ConnectError',
            title=[self.connection],
            raised_at=_timestamp(self.raised_at),
            message=self.db_message,
        )


@dataclass(eq=False)
class TemplateError(_Displayable, OsirisQueryError):
    """
    Rendering the template failed. Nothing was sent to the database.

    Fields:
    - query_name: Query path, or '<inline>' for inline SQL.
    - template: Template in which the error occurred: the query itself
      (equal to `query_name`, so '<inline>' for inline SQL) or an included
      file (name relative to its search path).
    - message: What went wrong.
    - source: Source of `template`; None when context lines would not help
      (e.g. the `raise()` helper).
    - lineno: Line in `source`, 1-based; None if unknown.
    - path: Full path of `template`; None for inline SQL.
    - raised_at: When the error occurred.
    """
    query_name: str
    template: str
    message: str
    source: str | None = None
    lineno: int | None = None
    path: str | None = None
    raised_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def __repr__(self) -> str:
        return (
            f"TemplateError(query={self.query_name!r}, "
            f"template={self.template!r}, lineno={self.lineno!r})"
        )

    def _title(self) -> list[str]:
        title = [self.query_name]
        if self.template != self.query_name:
            title.append(f"in {self.template}")
        return title

    def __str__(self) -> str:
        out = [f"TemplateError {self.query_name}  ({_timestamp(self.raised_at)})"]
        if self.template != self.query_name:
            out.append(f"in {self.template}")
        if self.path:
            out.append(self.path)
        out.append(self.message)
        if self.source is not None and self.lineno is not None:
            width = len(str(len(_lines(self.source))))
            out.append('')
            out.append(f"line {self.lineno}")
            out.extend(_numbered(_context(self.source, self.lineno), width))
        return '\n'.join(out)

    def _repr_html_(self) -> str:
        context = []
        if self.source is not None and self.lineno is not None:
            lines = _segments(self.source, 'sql+jinja')
            numbers = [n for n, _, _ in _context(self.source, self.lineno)]
            context = _html_rows(lines, numbers, self.lineno)
        return _render_html(
            'template_error.html.j2',
            e=self,
            kind='TemplateError',
            title=self._title(),
            raised_at=_timestamp(self.raised_at),
            message=self.message,
            context=context,
        )


@dataclass(eq=False)
class QueryNotFound(_Displayable, OsirisQueryError):
    """
    The query file was not found.

    Fields:
    - query_name: Query path as passed.
    - searched: Search paths of the template loader, in order.
    - suggestions: Close matches among the queries found there; may be empty.
    - raised_at: When the error occurred.
    """
    query_name: str
    searched: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)
    raised_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self) -> None:
        super().__init__(f"query not found: {self.query_name}")

    def __repr__(self) -> str:
        return f"QueryNotFound(query={self.query_name!r})"

    def __str__(self) -> str:
        out = [
            f"QueryNotFound {self.query_name}  ({_timestamp(self.raised_at)})",
            '',
            f"searched      {', '.join(self.searched) or '(none)'}",
        ]
        if self.suggestions:
            out.append(f"did you mean  {', '.join(self.suggestions)}")
        return '\n'.join(out)

    def _repr_html_(self) -> str:
        return _render_html(
            'query_not_found.html.j2',
            e=self,
            kind='QueryNotFound',
            title=[self.query_name],
            raised_at=_timestamp(self.raised_at),
            message='query not found',
        )


@dataclass(eq=False)
class KeywordError(_Displayable, OsirisQueryError):
    """
    Library keywords (where, select, values, totals, ...) were passed in an
    invalid way. Raised from the library templates and aggspec; nothing was
    sent to the database.

    Fields:
    - query_name: Query path, or '<inline>' for inline SQL.
    - keyword: The keyword involved, if known (e.g. 'values').
    - message: What went wrong.
    - raised_at: When the error occurred.
    """
    query_name: str
    keyword: str | None
    message: str
    raised_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def __repr__(self) -> str:
        return f"KeywordError(query={self.query_name!r}, keyword={self.keyword!r})"

    def __str__(self) -> str:
        out = [f"KeywordError {self.query_name}  ({_timestamp(self.raised_at)})"]
        if self.keyword:
            out.append(f"keyword: {self.keyword}")
        out.append(self.message)
        return '\n'.join(out)

    def _repr_html_(self) -> str:
        title = [self.query_name]
        if self.keyword:
            title.append(f"keyword {self.keyword}")
        return _render_html(
            'base.html.j2',
            e=self,
            kind='KeywordError',
            title=title,
            raised_at=_timestamp(self.raised_at),
            message=self.message,
        )


# region reporting
def report(error: OsirisQueryError) -> None:
    """
    Show `error`: rich display under IPython (HTML in notebooks, text in a
    terminal IPython), text on stderr otherwise.
    """
    shell = None
    try:
        from IPython import get_ipython
        shell = get_ipython()
    except ImportError:
        pass
    if shell is not None:
        from IPython.display import display
        display(error)
    else:
        print(error, file=sys.stderr)


# region last error
_last_errors: dict[str, OsirisQueryError] = {}


def remember(connection: str, error: OsirisQueryError) -> None:
    """Store `error` as the most recent error for `connection`."""
    _last_errors[connection] = error


def last_error(connection: str) -> OsirisQueryError | None:
    """Most recent error for `connection`, whenever it occurred; None if none."""
    return _last_errors.get(connection)
