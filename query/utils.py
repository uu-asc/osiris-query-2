from string import Template
from functools import wraps
from typing import Any, Callable
from pathlib import Path

import pandas as pd


class DotDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    __dir__ = dict.keys


DOCSTRING_TEMPLATE = Template(
"""$docstring

Additional notes
----------------
$appendices"""
)


def add_to_docstring(*appendices: str) -> Callable:
    """
    A decorator that appends additional information to the docstring of a function.

    Parameters:
    - appendices (str|list[str]): Text or list of texts to append.

    Returns:
    - callable: Decorated function.
    """
    def decorator(func):
        func.__doc__ = DOCSTRING_TEMPLATE.substitute(
            docstring = func.__doc__,
            appendices = '\n'.join(appendices)
        )
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator


def add_keyword_defaults(keywords: dict[str, Any]) -> Callable:
    """
    A decorator that adds default keyword arguments to the wrapped function.

    Parameters:
    - keywords (dict[str, Any]): A dictionary containing default keyword arguments.

    Returns:
    - callable: Decorated function.

    The returned decorator, when applied to a function, merges the provided
    default keyword arguments with any arguments passed to the decorated
    function.

    Example:
    ```python
    @add_keyword_defaults({"threshold": 0.5, "verbose": True})
    def process_data(data, threshold=0.2, verbose=False):
        # Function implementation
        pass
    ```

    In the example above, the `process_data` function will have default keyword
    arguments "threshold" and "verbose" added by the decorator, with the
    provided values overridden if the caller specifies them explicitly.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            kwargs = keywords | kwargs
            return func(*args, **kwargs)
        return wrapper
    return decorator


QUICK_FILTER_TEMPLATE = Template(
"""    - $column_name (str|list|None):
        Select rows where "$column_name" in given values."""
)


def add_quick_filter(column_name: str) -> Callable:
    """Create a decorator that adds SQL-style IN clause filtering for a specified column.

    This decorator allows functions to accept an additional keyword argument (named by column_name) which gets transformed into a SQL-style IN clause and added to the 'where' parameter of the decorated function.

    Parameters
    ----------
    column_name : str
        Name of the column to filter on. This will be used both as the keyword argument name and in the generated SQL clause.

    Returns
    -------
    Callable
        A decorator that processes the specified column filter.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key: str | list[str] | None = kwargs.pop(column_name, None)
            if key is not None:
                keys = [key] if isinstance(key, str) else key
                keys_as_string = ', '.join(f"'{n}'" for n in keys)
                formatted = f"{column_name} in ({keys_as_string})"

                where = kwargs.get('where', [])
                where = [where] if isinstance(where, str) else list(where)
                where.append(formatted)
                kwargs['where'] = where

            return func(*args, **kwargs)
        return wrapper
    return decorator


def init_notebook_folder():
    from pathlib import Path
    path = Path()
    for folder in ['queries', 'output', 'data']:
        (path / folder).mkdir(exist_ok=True)


class Ts:
    @property
    def timestamp(self):
        return f"{self.now:%d-%m-%Y %H:%M}"

    @property
    def datum(self):
        return f"{self.now:%d-%m-%Y}"

    @property
    def ymd(self):
        return f"{self.now:%Y%m%d}"

    @property
    def year(self):
        return f"{self.now:%Y}"

    @property
    def daymonth(self):
        return f"{self.now:%d %B}"

    @property
    def now(self):
        return pd.Timestamp.today()


TS = Ts()


def export_to_excel(
    df: pd.DataFrame,
    fn: Path|str,
    sheet_name: str = 'data',
):
    fn = Path(fn)
    with pd.ExcelWriter(
        fn,
        date_format = 'DD-MM-YYYY',
        datetime_format = 'DD-MM-YYYY',
    ) as writer:

        n_rows, n_cols = df.shape
        idx_nlevels = df.index.nlevels
        col_nlevels = df.columns.nlevels

        df.to_excel(
            writer,
            sheet_name = sheet_name,
            freeze_panes = (col_nlevels, idx_nlevels),
        )

        sheet = writer.sheets[sheet_name]
        sheet.autofit()
        sheet.autofilter(col_nlevels - 1, 0, n_rows, idx_nlevels + n_cols - 1)
