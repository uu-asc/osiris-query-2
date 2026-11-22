import importlib
import warnings
from contextlib import suppress
from types import ModuleType
from typing import TYPE_CHECKING

import pandas as pd
from sqlparse.engine import grouping

if TYPE_CHECKING:
    from query.connections import osiris, isis, sap, csa


# Connection modules are imported on first access (PEP 562), so that
# `from query import osiris` only loads (and validates config for) osiris.
_CONNECTIONS = ('osiris', 'isis', 'sap', 'csa')


def __getattr__(name: str) -> ModuleType:
    if name in _CONNECTIONS:
        module = importlib.import_module(f'query.connections.{name}')
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted({*globals(), *_CONNECTIONS})


modules = ["flatbread", "flatbread_dataviewer", "xquery", "key_extractor"]
imported = {}

for module in modules:
    with suppress(ImportError):
        imported[module] = __import__(module)
    if module not in imported:
        warnings.warn(f"Optional dependency not found: {module}")

pd.set_option('display.max_columns', None)
pd.set_option('display.show_dimensions', True)

grouping.MAX_GROUPING_TOKENS = None
