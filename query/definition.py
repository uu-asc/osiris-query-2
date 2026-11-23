import difflib
import re
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any, TypeAlias

from jinja2 import (
    BaseLoader,
    Environment,
    FileSystemLoader,
    TemplateError as JinjaTemplateError,
    TemplateNotFound,
    TemplateSyntaxError,
    meta,
)

import sqlparse
from sqlparse.sql import Token, TokenList
from sqlparse.tokens import CTE, DML

from query import utils
from query import aggspec
from query.config import CONFIG, get_paths_from_config
from query import errors
from query.errors import KeywordError, QueryNotFound, TemplateError


SqlString: TypeAlias = str

DEFAULT_QUERY_NAME = '<inline>'


class TemplateRaise(Exception):
    """
    Raised by the `raise()` helper in templates. Internal: `get_sql` converts
    it to KeywordError (library templates) or TemplateError (the user's own).
    """


def get_template_loader(
    paths: Path|str|list[Path|str]|None = None,
) -> FileSystemLoader:
    if paths is None:
        paths = []
    elif isinstance(paths, (Path, str)):
        paths = [paths]
    paths = [*paths, '.', *get_paths_from_config('queries', flatten=True)]
    return FileSystemLoader(paths)


def get_environment(
    paths: Path|str|list[Path|str]|None = None,
    loader: BaseLoader|None = None,
) -> Environment:
    if loader is None:
        loader = get_template_loader(paths)

    def raise_helper(msg):
        # https://stackoverflow.com/a/29262304/10403856
        raise TemplateRaise(msg)

    def format_string(text: str, fmt: str, *args):
        return fmt.format(text, *args)

    env = Environment(
        loader=loader,
        trim_blocks=True,
        lstrip_blocks=True
    )
    env.globals['raise'] = raise_helper
    env.filters['format_string'] = format_string
    return env


ENV: Environment = get_environment()


UTIL_KEYWORDS: list[str] = [
    'select',
    'where',
    'order_by',
    'n',
    'random',
    'cte',
    'aggfunc',
    'values',
]
# Keywords used by the aggregation templates (utils/agg.sql). Together with
# UTIL_KEYWORDS: every keyword the library templates consume. Kept explicit;
# a test checks the templates against both lists.
AGG_KEYWORDS: list[str] = [
    'distinct',
    'columns',
    'having',
    'label_val',
    'keep_na',
    'label_na',
    'totals',
    'grouping_sets',
    'cube_totals',
    'label_totals',
]
# Defaults for the aggregation keywords, from config; applied by Query.render.
AGG_DEFAULTS: dict[str, Any] = CONFIG['defaults']['aggregation']
DOCSTRING: str = f"""
    Optional keywords:

    CTE
    - select (str|list[str]|dict[str, str]): Select only specified columns.
    - where (str|list[str]): Select only rows that meet where criteria.
    - order_by (str|list[str]): Order by.
    - n (int): Fetch only first n records.
    - random (bool): Randomize order of rows.

    AGG
    - aggfunc (str): Aggregation function to use.
    - distinct (bool):
        Whether to count only distinct rows/values. Default False.
    - columns (list[str]): Group by on columns. Optional.
    - values (str | list[str] | dict[str, str] | list[dict[str,str]]):
        Values to aggregate. Optional.
        Only works if group by is used within the aggregation.

        Can take:
        * string,
        * mapping,
        * list of strings,
        * list of mappings

        Mapping can have the following attributes:
        * aggfunc: aggregation function to apply
        * column: column to aggregate
        * case: case statement to aggregate
        * name: name for resulting aggregated column
    - label_val (str):
        What label to use for the aggregation result.
        Only relevant when not grouping.
        By default uses `aggfunc`.
    - keep_na (bool):
        Whether to include null/NA values when aggregating.
        When True, group columns will be coalesced using `label_na`.
        Default False.
    - label_na (str): Label for null/NA values. Default '{CONFIG['defaults']['aggregation']['label_na']}'.
    - totals (bool): Add totals. Default False.
        When True, uses hierarchical totals by default.
    - grouping_sets (str | list[str]):
        Custom grouping sets for totals. Overrides default hierarchical behavior.
    - cube_totals (bool):
        Use cube() for all possible combinations of totals.
        Cannot be used with grouping_sets. Default False.
    - label_totals (str): What label to use for total rows. Default '{CONFIG['defaults']['aggregation']['label_totals']}'.
"""


# region query
@dataclass(frozen=True)
class Query:
    """
    A query template: from a query file, or inline text.

    Fields:
    - text: SQL/Jinja source: file contents, or the string as passed.
    - name: As passed ('matching/eindstatus') for `from_name`, the resolved
        path for `from_path`, '<inline>' for `from_text`.
    - path: Resolved file path; None for inline SQL.
    - env: Jinja environment used for rendering and analysis.
    """
    text: str
    name: str
    path: str | None
    env: Environment = field(default=ENV, repr=False, compare=False)

    @classmethod
    def from_name(cls, name: str | Path, env: Environment | None = None) -> 'Query':
        """
        Query file `name`, found through the loader's search paths (the
        configured query folders and '.'). '.sql' is added if missing.

        Raises:
        - QueryNotFound: No such file in the search paths.
        """
        env = ENV if env is None else env
        query = _find(name, env)
        if query is None:
            raise _query_not_found(str(name), env)
        return query

    @classmethod
    def from_path(cls, path: str | Path, env: Environment | None = None) -> 'Query':
        """
        Query file at `path`, anywhere on disk (e.g. an absolute path the
        loader can't find). `{% include %}` in it still goes through the loader.

        Raises:
        - QueryNotFound: The file can't be read.
        """
        path = Path(path).resolve()
        try:
            text = path.read_text(encoding='utf-8')
        except OSError:
            raise QueryNotFound(query_name=str(path), searched=[], suggestions=[]) from None
        return cls(text, name=str(path), path=str(path), env=ENV if env is None else env)

    @classmethod
    def from_text(cls, text: str, env: Environment | None = None) -> 'Query':
        """Inline query template `text`."""
        return cls(text, name=DEFAULT_QUERY_NAME, path=None, env=ENV if env is None else env)

    def params(self) -> set[str]:
        """Template variables of the query (not following `{% include %}`)."""
        return meta.find_undeclared_variables(self.env.parse(self.text))

    def render(self, **params) -> 'SQL':
        """
        Render the query with `params`. The aggregation defaults (AGG_DEFAULTS)
        are used for rendering; `SQL.params` holds only `params`.

        Raises:
        - TemplateError: Rendering the query (or a file it includes) failed.
        - KeywordError: Library keywords (where, values, ...) are invalid.
        """
        context = AGG_DEFAULTS | params
        try:
            rendered = self.env.from_string(self.text).render(**context)
        except Exception as e:
            raise _template_error(e, self) from e
        if any(kwd in params for kwd in UTIL_KEYWORDS):
            try:
                rendered = wrap_sql(rendered, env=self.env, **context)
            except Exception as e:
                # Wrong keyword arguments are by far the most likely cause, so
                # everything is reported as KeywordError; the cause is chained.
                raise _keyword_error(e, self) from e
        return SQL(rendered, query=self, params=dict(params))


@dataclass(frozen=True, eq=False)
class SQL:
    """
    A rendered query, as sent to the database. Created by `Query.render`.
    Displays as SQL (highlighted in notebooks); `str(sql)` or `sql.text` gives
    the string.

    Fields:
    - text: The rendered SQL.
    - query: The query it was rendered from.
    - params: The parameters it was rendered with.
    """
    text: str
    query: Query
    params: dict[str, Any]

    def __str__(self) -> str:
        return self.text

    def __repr__(self) -> str:
        return self.text

    def _repr_html_(self) -> str:
        return errors.sql_html(self.text)


# Statements that return rows: a one-line string starting with one of these
# is taken as SQL. Extend when needed.
_SQL_START = frozenset({'select', 'with'})
_FIRST_WORD = re.compile(r'\s*\(*\s*(\w+)')


def _find(name: str | Path, env: Environment) -> Query | None:
    """The query file `name` resolves to through the loader; None if not found."""
    path = Path(name)
    if path.suffix != '.sql':
        path = path.with_suffix(path.suffix + '.sql')
    try:
        text, filename, _ = env.loader.get_source(env, path.as_posix())
    except TemplateNotFound:
        return None
    filename = str(Path(filename).resolve()) if filename else None
    return Query(text, name=str(name), path=filename, env=env)


def as_query(source: 'Query|Path|str', env: Environment|None = None) -> Query:
    """
    `source` as a Query. A Query is returned unchanged (with its own env). A
    string or Path is, in this order:
    1. a string with a newline: SQL;
    2. a query file the loader finds, or an existing absolute path: that file;
    3. a string whose first word is in _SQL_START: SQL;
    4. otherwise QueryNotFound.
    Anything else, including an SQL, raises TypeError: an SQL is already
    rendered, so its callers (get_sql, QueryExecution) handle it themselves.
    """
    if isinstance(source, Query):
        return source
    env = ENV if env is None else env
    if isinstance(source, str) and '\n' in source:
        return Query.from_text(source, env)
    if not isinstance(source, (str, Path)):
        raise TypeError(f"expected Query, Path or str, got {type(source).__name__}")
    query = _find(source, env)
    if query is not None:
        return query
    if Path(source).is_absolute() and Path(source).is_file():
        return Query.from_path(source, env)
    if isinstance(source, str):
        match = _FIRST_WORD.match(source)
        if match and match[1].lower() in _SQL_START:
            return Query.from_text(source, env)
    raise _query_not_found(str(source), env)


# region get sql
@utils.add_to_docstring(DOCSTRING)
def get_sql(
    source: 'SQL|Query|Path|str',
    *,
    env: Environment|None = None,
    save_to_path: Path|str|None = None,
    print_sql: bool = False,
    print_vars: bool = False,
    **kwargs
) -> SQL:
    """
    Render a query (a query name or file path, an SQL string, or a Query) and
    return it as SQL.

    Parameters:
    - source (SQL|Query|Path|str): Query name or file path, SQL string, or a
        Query. An SQL (already rendered) is returned unchanged.
    - env (Environment|None):
        Jinja2 environment. If not provided, the default environment is used.
        A Query keeps its own environment.
    - save_to_path (Path|str|None): Path to save rendered query.
    - print_sql (bool): If True, prints rendered query.
    - print_vars (bool): If True, prints template variables.
    - **kwargs: Additional keyword arguments to be passed to template.

    Returns:
    SQL: The rendered query; `.text` gives the string.

    Raises:
    - QueryNotFound: `source` is neither a query file nor SQL.
    - TemplateError: Rendering the query (or a file it includes) failed.
    - KeywordError: Library keywords (where, values, ...) are invalid.
    - TypeError: `source` is an SQL and keyword arguments were passed.
    """
    if isinstance(source, SQL):
        if kwargs:
            raise TypeError(
                f"query is already rendered; parameters {sorted(kwargs)} would be ignored"
            )
        return source
    query = as_query(source, env)

    if print_vars:
        print(query.params())

    sql = query.render(**kwargs)

    if print_sql:
        print(sql.text)
    if save_to_path:
        Path(save_to_path).write_text(sql.text)

    return sql


def get_params(
    source: 'Query|Path|str',
    *,
    env: Environment|None = None,
) -> set[str]:
    """
    Get parameters in `source`.

    Parameters:
    - source (Query|Path|str): Query name or file path, SQL string, or a Query.
    - env (Environment|None):
        Jinja2 environment. If not provided, the default environment is used.

    Returns:
    set: Set of variables in query statement.
    """
    return as_query(source, env).params()


def get_raw_sql(
    source: Path|str,
    *,
    env: Environment|None = None,
    save_to_path: Path|str|None = None,
    print_sql: bool = False,
    **kwargs
) -> str:
    raw_sql = try_path(source, env=env)
    if print_sql:
        print(raw_sql)
    if save_to_path:
        Path(save_to_path).write_text(raw_sql)
    return raw_sql


def try_path(
    source: 'Query|Path|str',
    *,
    env: Environment|None = None,
) -> SqlString:
    """
    Source text of `source`: the query file's contents, or the SQL string.

    Kept for backwards compatibility; see `Query`.

    Parameters:
    - source (Query|Path|str): Query name or file path, SQL string, or a Query.
    - env (Environment|None):
        Jinja2 environment. If not provided, the default environment is used.

    Returns:
    SqlString: A string representing a sql query.
    """
    return as_query(source, env).text


# region errors
def _searchpaths(env: Environment) -> list[Path]:
    """Search paths of the loader, resolved; empty for loaders without them."""
    paths = getattr(env.loader, 'searchpath', None) or []
    return [Path(p).resolve() for p in paths]


def _query_not_found(name: str, env: Environment) -> QueryNotFound:
    searched = _searchpaths(env)
    # The current directory is searched, but not scanned for suggestions: in
    # a notebook it can be any (large) folder.
    cwd = Path.cwd().resolve()
    known = set()
    for base in searched:
        if base == cwd or not base.is_dir():
            continue
        for file in base.rglob('*.sql'):
            known.add(file.relative_to(base).with_suffix('').as_posix())
    wanted = name.replace('\\', '/').removesuffix('.sql')
    return QueryNotFound(
        query_name=name,
        searched=[str(p) for p in searched],
        suggestions=difflib.get_close_matches(wanted, sorted(known), n=3),
    )


def _message(e: Exception) -> str:
    """Message of `e`: Jinja's own message, otherwise 'TypeName: message'."""
    if isinstance(e, TemplateRaise):
        return str(e)
    if isinstance(e, TemplateNotFound):
        return f"included template not found: {e.name}"
    if isinstance(e, JinjaTemplateError):
        return e.message or str(e)
    return f"{type(e).__name__}: {e}"


def _last_template_frame(tb: TracebackType | None) -> tuple[str | None, int | None]:
    """
    (filename, lineno) of the innermost template frame in `tb`; (None, None)
    if there is none.

    Jinja rewrites tracebacks so templates appear as frames with the
    template's file and line; templates rendered with `from_string` appear as
    '<template>'. The rewritten frames are recognised by the
    `__jinja_exception__` global Jinja gives them (jinja2.debug). Not by
    filename: with '.' as a search path, any Python file under the current
    directory would look like a template.
    """
    found: tuple[str | None, int | None] = (None, None)
    while tb is not None:
        if '__jinja_exception__' in tb.tb_frame.f_globals:
            found = (tb.tb_frame.f_code.co_filename, tb.tb_lineno)
        tb = tb.tb_next
    return found


def _relative_name(filename: str, searched: list[Path]) -> str | None:
    """`filename` relative to the first search path it lies under, or None."""
    path = Path(filename).resolve()
    for base in searched:
        if path.is_relative_to(base):
            return path.relative_to(base).as_posix()
    return None


def _template_error(e: Exception, query: Query) -> TemplateError:
    """
    TemplateError for an exception raised while rendering `query`. The
    template and line come from the exception (syntax errors) or from the
    traceback (other errors). '<template>' or no filename means the query
    itself.
    """
    message = _message(e)
    if isinstance(e, TemplateRaise):
        return TemplateError(query.name, template=query.name, message=message, path=query.path)

    if isinstance(e, TemplateSyntaxError):
        filename, lineno = e.filename, e.lineno
    else:
        filename, lineno = _last_template_frame(e.__traceback__)

    if filename in (None, '<template>'):
        return TemplateError(
            query.name, template=query.name, message=message,
            source=query.text, lineno=lineno, path=query.path,
        )

    env = query.env
    name = _relative_name(filename, _searchpaths(env)) or filename
    try:
        source, *_ = env.loader.get_source(env, name)
    except Exception:           # context lines are optional; never hide the error
        source = None
    return TemplateError(
        query.name, template=name, message=message,
        source=source, lineno=lineno, path=str(Path(filename).resolve()),
    )


def _keyword_error(e: Exception, query: Query) -> KeywordError:
    """KeywordError for an exception raised in `wrap_sql` (or aggspec)."""
    if isinstance(e, aggspec.ValidationError):
        return KeywordError(query.name, keyword='values', message=str(e))
    return KeywordError(query.name, keyword=None, message=_message(e))


# region find sql
def find_query(*keywords, how='like', **kwargs):
    """
    Search for queries across all configured query paths based on filename/path.

    Parameters:
    - *keywords (str): Substrings for matching query paths.
    - how (str): One of 'like', 'regex', 'exact' for matching strategy.

    Returns:
    - dict: Dictionary with base paths as keys and lists of matching relative paths as values.
    """
    def matches_keyword(path_str, keyword, search_type):
        if search_type == 'like':
            return keyword.lower() in path_str.lower()
        elif search_type == 'regex':
            import re
            return bool(re.search(keyword, path_str, re.IGNORECASE))
        elif search_type == 'exact':
            return keyword.lower() == path_str.lower()

    matches = {}
    query_paths = get_paths_from_config('queries', flatten=True)

    for base_path in query_paths:
        base_matches = []
        for sql_file in base_path.rglob('*.sql'):
            relative_path = sql_file.relative_to(base_path).with_suffix('')
            relative_str = str(relative_path).replace('\\', '/')

            if all(matches_keyword(relative_str, kw, how) for kw in keywords):
                base_matches.append(relative_str)

        if base_matches:
            matches[str(base_path)] = sorted(base_matches)

    return matches

# region dynamic sql
@aggspec.build_value_specs
def wrap_sql(
    sql: str,
    *,
    env: Environment|None = None,
    **kwargs
) -> str:
    """
    Wraps SQL query with a wrapper template that allows for ad hoc modifications of the query (such as: randomize order, fetch only first n rows, etc.)

    Parameters:
    - sql (str): The input SQL query.
    - env (Environment|None): The Jinja2 environment. Use default if None.
    - **kwargs: Additional keyword arguments to be passed to wrapper.

    Returns:
    str: The wrapped SQL query.
    """
    tokens = sqlparse.parse(sql)[0].tokens
    has_cte = any(token.ttype is CTE for token in tokens)
    body, main_statement = split_sql_from_tokens(tokens)

    env = ENV if env is None else env
    template = env.get_template('utils/wrapper.sql')
    rendered = template.render(
        has_cte = has_cte,
        body = body,
        main_statement = main_statement,
        **kwargs
    )
    return rendered


def split_sql_from_tokens(
    tokens: list[Token|TokenList]
) -> tuple[str, str]:
    """
    Extracts the body (including comments, CTEs, etc.) and the main statement from a SQL query.

    Parameters:
    - tokens (list[Token|TokenList]): List of tokens representing the SQL query.

    Returns:
    tuple[str, str]: A tuple containing two strings:
        - The body.
        - The main statement.
    """
    body = ''
    main_statement = ''
    is_main = False

    for token in tokens:
        is_main = is_main or token.ttype is DML
        if is_main:
            main_statement += str(token)
        else:
            body += str(token)

    return body, main_statement
