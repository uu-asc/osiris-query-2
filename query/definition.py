from pathlib import Path

from jinja2 import (
    Environment,
    ChoiceLoader,
    FileSystemLoader,
    meta,
)
from sqlalchemy import text, TextClause
import sqlparse
from sqlparse.sql import Token, TokenList
from sqlparse.tokens import CTE, DML

from query.config import get_paths_from_config
from query.utils import add_to_docstring


def get_template_loader():
    paths = get_paths_from_config('queries')
    return ChoiceLoader([
        FileSystemLoader(paths),
        FileSystemLoader('.'),
    ])


TEMPLATE_LOADER = get_template_loader()


def get_environment() -> Environment:
    def raise_helper(msg):
        # https://stackoverflow.com/a/29262304/10403856
        raise ValueError(msg)

    env = Environment(
        loader=TEMPLATE_LOADER,
        trim_blocks=True,
        lstrip_blocks=True
    )
    env.globals['raise'] = raise_helper
    return env


ENV = get_environment()
UTIL_KEYWORDS = [
    'select',
    'where',
    'order_by',
    'n',
    'random',
    'cte',
    'aggfunc',
    'values',
]
DOCSTRING = """
    Optional keywords:

    CTE
    - select (list[str]): Select only specified columns.
    - where (list[str]): Select only rows that meet where criteria.
    - order_by (list[str]): Order by.
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
    - label_na (str): Label for null/NA values. Default '<NA>'.
    - totals (bool): Add totals. Default False.
    - grouping_sets (str | list[str]):
        What totals to group. By default total all combinations of groups.
    - label_totals (str): What label to use for total rows. Default 'Totaal'.
"""


@add_to_docstring(DOCSTRING)
def get_sql(
    src: Path|str,
    *,
    env: Environment|None = None,
    save_to_path: Path|str|None = None,
    print_output: bool = False,
    print_vars: bool = False,
    **kwargs
) -> TextClause:
    """
    Retrieves a SQL query template from file or string, renders it with Jinja2,
    and returns a TextClause.

    Parameters:
    - src (Path|str): Path to file or string containing the SQL query.
    - env (Environment|None): Jinja2 environment. If not provided, the default environment is used.
    - save_to_path (Path|str|None): Path to save rendered query.
    - print_output (bool): If True, prints rendered query.
    - print_vars (bool): If True, prints template variables.
    - **kwargs: Additional keyword arguments to be passed to template.

    Returns:
    TextClause: The processed SQL query as a SQLAlchemy TextClause.
    """
    if isinstance(src, TextClause):
        return src
    env = ENV if env is None else env

    if (
        isinstance(src, str)
        and '\n' not in src
        and not src.lower().startswith('select ')
    ):
        path_to_sql = Path(src).with_suffix('.sql').as_posix()
        src, *_ = TEMPLATE_LOADER.get_source(None, path_to_sql)

    if print_vars:
        ast = env.parse(src)
        variables = meta.find_undeclared_variables(ast)
        print(variables)

    template = env.from_string(src)
    rendered = template.render(**kwargs)

    if any(kwd in kwargs for kwd in UTIL_KEYWORDS):
        rendered = wrap_sql(rendered, **kwargs)

    sql = text(rendered)

    if print_output:
        print(sql.text)
    if save_to_path:
        Path(save_to_path).write_text(sql.text)

    return sql


def wrap_sql(
    sql: str,
    *,
    env: Environment|None = None,
    **kwargs
) -> str:
    """
    Wraps SQL query with a wrapper template that allows for ad hoc
    modifications of the query (such as: randomize order, fetch only first n
    rows, etc.)

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
