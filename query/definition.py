from pathlib import Path

from jinja2 import (
    Environment,
    ChoiceLoader,
    FileSystemLoader,
    meta,
)
from sqlalchemy import text, TextClause

from query.config import get_paths_from_config


def get_template_loader():
    paths = get_paths_from_config('queries')
    return ChoiceLoader([
        FileSystemLoader(paths),
        FileSystemLoader('.'),
    ])


TEMPLATE_LOADER = get_template_loader()


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
