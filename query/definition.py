from pathlib import Path
from functools import singledispatch, wraps

from jinja2 import (
    Environment,
    ChoiceLoader,
    PackageLoader,
    FileSystemLoader,
    meta,
)
from sqlalchemy import text, TextClause


TEMPLATE_LOADER = ChoiceLoader([
    PackageLoader('definitions', package_path='../definitions'),
    FileSystemLoader('.'),
])


def get_environment() -> Environment:
    return Environment(
        loader=TEMPLATE_LOADER,
        trim_blocks=True,
        lstrip_blocks=True
    )


def get_sql(
    src: Path|str,
    *,
    env: Environment|None = None,
    save_to_path: Path|str = None,
    print_output: bool = False,
    print_variables: bool = False,
    **kwargs
) -> TextClause:

    if isinstance(src, TextClause):
        return src
    env = get_environment() if env is None else env

    if (
        isinstance(src, str)
        and '\n' not in src
        and not src.lower().startswith('select ')
    ):
        path_to_sql = Path(src).with_suffix('.sql').as_posix()
        src, *_ = TEMPLATE_LOADER.get_source(None, path_to_sql)

    if print_variables:
        ast = env.parse(src)
        variables = meta.find_undeclared_variables(ast)
        print(variables)

    template = env.from_string(src)
    rendered = template.render(**kwargs)
    sql = text(rendered)

    if print_output:
        print(sql.text)
    if save_to_path:
        Path(save_to_path).write_text(sql.text)

    return sql
