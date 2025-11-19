import pandas as pd

import warnings

from contextlib import suppress

modules = ["flatbread", "flatbread_dataviewer", "xquery", "key_extractor"]
imported = {}

for module in modules:
    with suppress(ImportError):
        imported[module] = __import__(module)
    if module not in imported:
        warnings.warn(f"Optional dependency not found: {module}")

pd.set_option('display.max_columns', None)
pd.set_option('display.show_dimensions', True)
