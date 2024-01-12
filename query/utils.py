from string import Template
from functools import wraps
from typing import Any, Callable

import pandas as pd


class DotDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    __dir__ = dict.keys


TEMPLATE = """$docstring

Additional notes
----------------
$appendices"""


def add_to_docstring(appendices: str|list[str]) -> Callable:
    """
    A decorator that appends additional information to the docstring of a function.

    Parameters:
    - appendices (str|list[str]): Text or list of texts to append.

    Returns:
    - callable: Decorated function.
    """
    appendices = [appendices] if isinstance(appendices, str) else appendices
    template = Template(TEMPLATE)
    def decorator(func):
        func.__doc__ = template.substitute(
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


def init_notebook_folder():
    from pathlib import Path
    path = Path()
    for folder in  ['queries', 'output', 'data']:
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
    def daymonth(self):
        return f"{self.now:%d %B}"

    @property
    def now(self):
        return pd.Timestamp.today()


TS = Ts()
