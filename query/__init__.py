import pandas as pd

try:
    import flatbread
    import xquery
    import key_extractor
except ImportError:
    ...

pd.set_option('display.max_columns', None)
pd.set_option('display.show_dimensions', True)
