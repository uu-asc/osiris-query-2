import re
from functools import reduce, wraps
from typing import Union, Callable
import pandas as pd


BooleanIndex = Union[list[bool], 'pd.Series[bool]']


def store_history_decorator(method):
    @wraps(method)
    def wrapper(self, df, query, **kwargs):
        if len(df.xquery._history) == 0:
            df.xquery._history.append({
                'name': 'query start',
                'rows': df.shape[0],
                'cols': df.shape[1],
            })
        result = method(self, df, query, **kwargs)
        history = df.xquery._history.copy()
        history.append({
            'name': query,
            'rows': result.shape[0],
            'cols': result.shape[1],
        })
        result.xquery._history = history
        return result
    return wrapper


@pd.api.extensions.register_dataframe_accessor("xquery")
class Query:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
        self._history = []

    @property
    def history(self):
        return pd.DataFrame([
            *self._history,
            {
                'name': 'query stop',
                'rows': self._obj.shape[0],
                'cols': self._obj.shape[1],
            }
        ]).assign(
            diff = lambda df: df.rows.subtract(df.rows.shift(1)).astype('Int64')
        )

    def __call__(
        self,
        *args: tuple[str | BooleanIndex | Callable],
        columns: list = None,
        in_parrallel = False,
        **kwargs
    ) -> pd.DataFrame:
        result = self._query(
            self._obj,
            *args,
            in_parrallel = in_parrallel,
            **kwargs
        )
        if columns is None:
            return result
        return result[columns]

    def _query(
        self,
        df: pd.DataFrame,
        *args: tuple[str | BooleanIndex | Callable],
        in_parrallel = False,
        **kwargs
    ) -> pd.DataFrame:
        queries = [arg for arg in args if arg is not None]
        results = []
        for query in queries:
            result = self._single_query(df, query, **kwargs)
            if not in_parrallel:
                df = result
            else:
                results.append(result)
        if in_parrallel:
            history = [
                item for df in results
                for item in df.xquery._history[1:]
            ]
            df = pd.concat(results).drop_duplicates()
            df.xquery._history = history
        return df

    @store_history_decorator
    def _single_query(
        self,
        df: pd.DataFrame,
        query: str | BooleanIndex | Callable,
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


def compile_query(query : str) -> str:
    wrap = lambda funcs: reduce(lambda f,g: lambda x: g(f(x)), funcs)
    pipeline = wrap([
        translate_multiline,
        translate_is_na,
        translate_is_not_na,
        translate_contains,
        translate_startswith,
        translate_date,
    ])
    return pipeline(query)


def translate_multiline(string : str) -> str:
    string = string.strip('\n ')
    regex = re.compile("[\n\s]+")
    return regex.sub(' ', string)


def translate_is_na(string : str) -> str:
    regex = re.compile("\s+is\s+(?:null|na)", flags=re.I)
    return regex.sub('.isna()', string)


def translate_is_not_na(string : str) -> str:
    regex = re.compile("\s+is\s+not\s+(?:null|na)", flags=re.I)
    return regex.sub('.notna()', string)


def translate_contains(string : str) -> str:
    regex = re.compile("\s+contains\s+(['\"].*?['\"])")
    convert = lambda match: f".str.contains({match.group(1)}, na=False)"
    return regex.sub(convert, string)


def translate_startswith(string : str) -> str:
    regex = re.compile("\s+startswith\s+(['\"].*?['\"])")
    convert = lambda match: f".str.startswith({match.group(1)}, na=False)"
    return regex.sub(convert, string)


def translate_date(string : str) -> str:
    regex = re.compile("[^'\"](\d{4}-\d{2}-\d{2})")
    convert = lambda match: f"'{match.group(1)}'"
    return regex.sub(convert, string)
