from pathlib import Path
from typing import TypeAlias

from jinja2 import Environment, BaseLoader, FileSystemLoader, meta
from sqlalchemy import text, TextClause

import sqlparse
from sqlparse.sql import Token, TokenList
from sqlparse.tokens import CTE, DML

from query import utils
from query import aggspec
from query.config import CONFIG, get_paths_from_config


SqlString: TypeAlias = str


TextClause.__repr__ = lambda self: self.text


def get_template_loader(
    paths: Path|str|list[Path|str]|None = None,
) -> FileSystemLoader:
    if paths is None:
        paths = []
    elif isinstance(paths, (Path, str)):
        paths = [paths]
    paths = [*paths, '.', *get_paths_from_config('queries')]
    return FileSystemLoader(paths)


def get_environment(
    paths: Path|str|list[Path|str]|None = None,
    loader: BaseLoader|None = None,
) -> Environment:
    if loader is None:
        loader = get_template_loader(paths)

    def raise_helper(msg):
        # https://stackoverflow.com/a/29262304/10403856
        raise ValueError(msg)

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
    - grouping_sets (str | list[str]):
        What totals to group. By default total all combinations of groups.
    - label_totals (str): What label to use for total rows. Default '{CONFIG['defaults']['aggregation']['label_totals']}'.
"""


# region get sql
@utils.add_keyword_defaults(CONFIG['defaults']['aggregation'])
@utils.add_to_docstring(DOCSTRING)
def get_sql(
    source: Path|str,
    *,
    env: Environment|None = None,
    save_to_path: Path|str|None = None,
    print_sql: bool = False,
    print_vars: bool = False,
    **kwargs
) -> TextClause:
    """
    Retrieves a SQL query template from file or string, renders it with Jinja2, and returns a TextClause.

    Parameters:
    - source (Path|str): Path to file or string containing the SQL query.
    - env (Environment|None):
        Jinja2 environment. If not provided, the default environment is used.
    - save_to_path (Path|str|None): Path to save rendered query.
    - print_sql (bool): If True, prints rendered query.
    - print_vars (bool): If True, prints template variables.
    - **kwargs: Additional keyword arguments to be passed to template.

    Returns:
    TextClause: The processed SQL query as a SQLAlchemy TextClause.
    """
    if isinstance(source, TextClause):
        return source
    env = ENV if env is None else env
    source = try_path(source, env=env)

    if print_vars:
        variables = get_params(source, env=env)
        print(variables)

    template = env.from_string(source)
    rendered = template.render(**kwargs)

    if any(kwd in kwargs for kwd in UTIL_KEYWORDS):
        rendered = wrap_sql(rendered, **kwargs)

    sql = TextClause(rendered)

    if print_sql:
        print(sql.text)
    if save_to_path:
        Path(save_to_path).write_text(sql.text)

    return sql


def get_params(
    source: Path|str,
    *,
    env: Environment|None = None,
) -> set[str]:
    """
    Get parameters in `source`.

    Parameters:
    - source (Path|str): Path to file or string containing the SQL query.
    - env (Environment|None):
        Jinja2 environment. If not provided, the default environment is used.

    Returns:
    set: Set of variables in query statement.
    """
    env = ENV if env is None else env
    source = try_path(source, env=env)

    ast = env.parse(source)
    variables = meta.find_undeclared_variables(ast)
    return variables


def try_path(
    source: Path|str,
    *,
    env: Environment|None = None,
) -> SqlString:
    """
    Test if `source` is path and if so load query statement from it. Else do nothing.

    If `source` seems to be a path then it is tested if it has a '.sql' suffix. If not then it will be added to the path.

    Parameters:
    - source (Path|str): Path to file or string containing the SQL query.
    - env (Environment|None):
        Jinja2 environment. If not provided, the default environment is used.

    Returns:
    SqlString: A string representing a sql query.
    """
    if is_path(source):
        env = ENV if env is None else env
        path = Path(source)
        if path.suffix != '.sql':
            path = path.with_suffix(path.suffix + '.sql')
        source, *_ = env.loader.get_source(None, path.as_posix())
    return source


def is_path(source: Path|str) -> bool:
    """
    Test if source is a path (and not a sql statement).
    Assumes that if source:
    - is a Path then True
    - is a string with newline then False
    - is a single string starting with 'select ' then False
    - else True

    Parameters:
    - source (Path|str):
        Source to be tested. Could be a string representing a path or a sql statement.

    Returns:
    bool: Whether the source is a path or a sql statement.
    """
    if isinstance(source, Path):
        return True
    if isinstance(source, TextClause):
        return False
    if '\n' in source:
        return False
    if source.lower().startswith('select '):
        return False
    return True


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
    query_paths = get_paths_from_config('queries')

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
