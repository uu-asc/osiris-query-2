import re
from functools import reduce, wraps
from typing import Callable
import pandas as pd


type BooleanIndex = list[bool] | pd.Series[bool]


@pd.api.extensions.register_dataframe_accessor("xquery")
class Xquery:
    """
    Execute one or more queries on the DataFrame. A query can be:
    - A query string
    - A boolean index
    - A callable that evaluates to True or False for each row

    A query can also contain a filter for the columns to be returned. Some statistics about the intermediate results can be inspected through the `history` attribute.

    ### Query language
    A query statement will be compiled according to registered translators. The following translators have been applied:

    translator      | input                   | output
    ----------------|-------------------------|-----------------------
    multiline       | multiline query         | single line
    equals          | '='                     | '=='
    is_na           | 'is na/null'            | '.isna()'
    is_not_na       | 'is not na/null'        | '.notna()'
    pattern_match   | "contains 'x'"          | '.str.contains('x')'
    pattern_match   | "matches 'x'"           | '.str.match('x')'
    pattern_match   | "full matches 'x'"      | '.str.fullmatch('x')'
    pattern_match   | "starts with 'x'"       | '.str.startswith('x')'
    pattern_match   | "ends with 'x'"         | '.str.endswith('x')'
    string_test     | "is alphanumeric"       | '.str.isalnum()'
    string_test     | "is alphabetic"         | '.str.isalpha()'
    string_test     | "is numeric"            | '.str.isnumeric()'
    string_test     | "is digit"              | '.str.isdigit()'
    string_test     | "is decimal"            | '.str.isdecimal()'
    string_test     | "is lowercase"          | '.str.islower()'
    string_test     | "is uppercase"          | '.str.isupper()'
    string_test     | "is titlecase"          | '.str.istitle()'
    string_test     | "is space"              | '.str.isspace()'
    is_duplicated   | "is duplicated"         | '.duplicated(False)'
    is_duplicated   | "is first duplicated"   | '.duplicated('first')'
    is_duplicated   | "is last duplicated"    | '.duplicated('last')'
    date            | 'xxxx-xx-xx'            | "'xxxx-xx-xx'"

    Pattern matching and string testing can also be negated.

    ### Queries in parallel
    Queries can be run sequentially or in parallel. The latter meaning that the result of each query will be concatenated (and afterwards deduplicated).

    Parameters:
    - *args:
        Queries to apply. Can be a query string, a boolean index, or a callable that returns True or False for each column.
    - columns (list[str]|BooleanIndex|Callable, optional):
        Columns to select. Can be a list of column names, a boolean index, or a callable that returns True or False for each column. If set to None returns all columns. Default is None.
    - in_parallel (bool):
        Execute queries in parallel or sequentially. Default False.
    - store_keys (bool):
        Store queries as keys in resulting df if queries are executed in parallel. Default False.
    - **kwargs: Additional arguments are passed to the pandas query engine.

    Returns:
    pd.DataFrame: Resulting DataFrame.
    """
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self._history = []

    @staticmethod
    def store_history_decorator(method):
        @wraps(method)
        def wrapper(self, df, query, **kwargs):
            in_parallel = kwargs.pop('in_parallel', False)
            if len(df.xquery._history) == 0:
                df.xquery._history.append({
                    'name':         '[[ START ]]',
                    'qtype':        pd.NA,
                    'timestamp':    pd.Timestamp.now(),
                    'parallel':     False,
                    'rows':         df.shape[0],
                    'cols':         df.shape[1],
                })
            result = method(self, df, query, **kwargs)
            history = df.xquery._history.copy()
            name = f'"{query}"' if isinstance(query, str) else query
            qtype = str(type(query))
            history.append({
                'name':         name,
                'qtype':        qtype,
                'timestamp':    pd.Timestamp.now(),
                'parallel':     in_parallel,
                'rows':         result.shape[0],
                'cols':         result.shape[1],
            })
            result.xquery._history = history
            return result
        return wrapper

    @property
    def history(self):
        stop_record = {
            'name':         '[[ STOP ]]',
            'qtype':        pd.NA,
            'timestamp':    pd.Timestamp.now(),
            'parallel':     False,
            'rows':         self._obj.shape[0],
            'cols':         self._obj.shape[1],
        }
        df = pd.DataFrame([*self._history, stop_record])
        return df.astype({'rows': 'Int64'}).assign(
            diff_prev = lambda df: df.rows.subtract(df.rows.shift(1)),
            diff_first = lambda df: df.rows.subtract(df.rows.iloc[0]),
            timedelta = lambda df: (
                df.timestamp
                .subtract(df.timestamp.shift(1))
                .dt.total_seconds()
            ),
        )

    def __call__(
        self,
        *args: tuple[str|BooleanIndex|Callable],
        columns: list[str]|BooleanIndex|Callable|None = None,
        in_parallel: bool = False,
        store_keys: bool = False,
        **kwargs
    ) -> pd.DataFrame:
        result = self._query(
            self._obj,
            *args,
            in_parallel = in_parallel,
            store_keys = store_keys,
            **kwargs
        )
        if columns is None:
            return result
        return result.loc[:, columns]

    def _query(
        self,
        df: pd.DataFrame,
        *args: tuple[str|BooleanIndex|Callable],
        in_parallel: bool = False,
        store_keys: bool = False,
        **kwargs
    ) -> pd.DataFrame:
        """
        Process multiple queries sequentially or in parallel.

        Parameters:
        - df (pd.DataFrame): DataFrame to query.
        - args: Queries to apply.
        - in_parallel (bool):
            Execute queries in parallel or sequentially. Default False.
        - store_keys (bool):
            Store queries as keys in resulting df if queries are executed in parallel. Default False.
        - **kwargs: Additional arguments are passed to the pandas query engine.

        Returns:
        pd.DataFrame: Resulting DataFrame.
        """
        queries = [arg for arg in args if arg is not None]
        results = []
        for query in queries:
            kwargs['in_parallel'] = in_parallel
            result = self._single_query(df, query, **kwargs)
            if not in_parallel:
                df = result
            else:
                results.append(result)
        if in_parallel:
            first = results[0].xquery._history[0]
            history = [
                item for df in results
                for item in df.xquery._history[1:]
            ]
            history.insert(0, first)
            keys = (
                [f'"{q}"' if isinstance(q, str) else q for q in queries]
                if store_keys else None
            )
            names = ['xquery', *results[0].index.names] if store_keys else None
            df = pd.concat(results, keys=keys, names=names).drop_duplicates()
            df.xquery._history.extend(history)
        return df

    @store_history_decorator
    def _single_query(
        self,
        df: pd.DataFrame,
        query: str|BooleanIndex|Callable,
        **kwargs
    ) -> pd.DataFrame:
        if isinstance(query, str):
            query = compile_query(query)
            try:
                return df.query(query, **kwargs)
            except (TypeError, ValueError):
                return df.query(query, engine='python', **kwargs)
        else:
            return df.copy().loc[query]


def compile_query(query: str) -> str:
    wrap = lambda funcs: reduce(lambda f,g: lambda x: g(f(x)), funcs)
    pipeline = wrap([
        translate_multiline,
        translate_equals,
        translate_is_na,
        translate_is_not_na,
        translate_pattern_match,
        translate_pattern_match_negation,
        translate_string_test,
        translate_string_test_negation,
        translate_is_duplicated,
        translate_date,
    ])
    return pipeline(query)


def translate_multiline(string: str) -> str:
    "Convert multiline string to single line."
    string = string.strip('\n ')
    regex = re.compile(r"[\n\s]+")
    return regex.sub(' ', string)


def translate_equals(string: str) -> str:
    """
    Convert string:
    Input    '='
    Output   '=='

    Returns:
    str
    """
    regex = re.compile(r"\s=\s")
    return regex.sub(' == ', string)


def translate_is_na(string: str) -> str:
    """
    Convert string:
    Input    'is null/na'
    Output   '.isna()'

    Returns:
    str
    """
    regex = re.compile(r"\sis\s+(?:null|na)(\s|$)", flags=re.I)
    return regex.sub('.isna()', string)


def translate_is_not_na(string: str) -> str:
    """
    Convert string:
    Input    'is not null/na'
    Output   '.isna()'

    Returns:
    str
    """
    regex = re.compile(r"\sis\s+not\s+(?:null|na)(\s|$)", flags=re.I)
    return regex.sub('.notna()', string)


def translate_pattern_match(string: str) -> str:
    """
    Convert string:
    Input    " {op} '{x}'"
    Output   "str.{op}}('{x}', na=False)"

    Returns:
    str
    """
    ops = {
        'full matches': 'fullmatch',
        'ends with': 'endswith',
        'contains': 'contains',
        'matches': 'match',
        'starts with': 'startswith',
    }
    opnames = '|'.join(ops.keys()).replace(' ', r'\s')

    regex = re.compile(rf"""
        \s
        (?P<op>{opnames})
        \s+
        (?P<phrase>['\"].*?['\"])
    """, re.VERBOSE
    )

    def convert(match):
        op = match.group('op')
        phrase = match.group('phrase')
        return f".str.{ops[op]}({phrase}, na=False)"

    return regex.sub(convert, string)


def translate_pattern_match_negation(string: str) -> str:
    """
    Convert string:
    Input    " not {col} {op} '{x}'"
    Output   "~{col}.str.{op}}('{x}', na=False)"

    Returns:
    str
    """
    ops = {
        'contains': 'contains',
        'matches': 'match',
        'full matches': 'fullmatch',
        'starts with': 'startswith',
        'ends with': 'endswith',
    }
    opnames = '|'.join(ops.keys()).replace(' ', r'\s')

    regex = re.compile(rf"""
        (^\s)
        not
        \s+
        (?P<col>[^\s]+)
        \s+
        (?P<op>{opnames})
        \s+
        (?P<phrase>['\"].*?['\"])
    """, re.VERBOSE
    )

    def convert(match):
        col = match.group('col')
        op = match.group('op')
        phrase = match.group('phrase')
        return f"~({col}).str.{ops[op]}({phrase}, na=False)"
    return regex.sub(convert, string)


def translate_string_test(string: str) -> str:
    """
    Convert string:
    Input    " {op}"
    Output   "str.{op}}()"

    Returns:
    str
    """
    ops = {
        'is alphanumeric': 'isalnum',
        'is alphabetic': 'isalpha',
        'is numeric': 'isnumeric',
        'is digit': 'isdigit',
        'is decimal': 'isdecimal',
        'is lowercase': 'islower',
        'is uppercase': 'isupper',
        'is titlecase': 'istitle',
        'is space': 'isspace',
    }
    opnames = '|'.join(ops.keys()).replace(' ', r'\s')

    regex = re.compile(rf"\s(?P<op>{opnames})(\s|$)")

    def convert(match):
        op = match.group('op')
        return f".str.{ops[op]}()"

    return regex.sub(convert, string)


def translate_string_test_negation(string: str) -> str:
    """
    Convert string:
    Input    "{col} not {op}"
    Output   "~{col}.str.{op}}()"

    Returns:
    str
    """
    ops = {
        'is not digit': 'isdigit',
        'is not lowercase': 'islower',
        'is not numeric': 'isnumeric',
        'is not space': 'isspace',
        'is not titlecase': 'istitle',
        'is not uppercase': 'isupper',
        'is not alphanumeric': 'isalnum',
        'is not alphabetic': 'isalpha',
        'is not decimal': 'isdecimal',
    }
    opnames = '|'.join(ops.keys()).replace(' ', r'\s')

    regex = re.compile(rf"""
        (^\s)
        (?P<col>[^\s]+)
        \s+
        (?P<op>{opnames})
        (\s|$)
    """, re.VERBOSE
    )

    def convert(match):
        col = match.group('col')
        op = match.group('op')
        return f"~{col}.str.{ops[op]}()"
    return regex.sub(convert, string)


def translate_is_duplicated(string: str) -> str:
    """
    Convert string:
    Input    " {op}"
    Output   ".duplicated({op})"

    Returns:
    str
    """
    ops = {
        'is duplicated': 'False',
        'is first duplicated': "'first'",
        'is last duplicated': "'last'",
    }
    opnames = '|'.join(ops.keys()).replace(' ', r'\s')

    regex = re.compile(rf"\s(?P<op>{opnames})(\s|$)")

    def convert(match):
        op = match.group('op')
        return f".duplicated({ops[op]})"

    return regex.sub(convert, string)


def translate_date(string: str) -> str:
    """
    Convert string:
    Input    "2023-12-27"
    Output   "'2023-12-27'"

    Returns:
    str
    """
    regex = re.compile(r"[^'\"](\d{4}-\d{2}-\d{2})")
    convert = lambda match: f"'{match.group(1)}'"
    return regex.sub(convert, string)
