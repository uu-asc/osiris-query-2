from functools import singledispatch
from typing import TypeAlias, Literal

import pandas as pd


Axis: TypeAlias = Literal[0, 1, 2, 'index', 'columns', 'both']


def offset_date_field(
    df: pd.DataFrame,
    date_field: str,
    year_field: str,
) -> pd.DataFrame:
    offset_year = df[year_field].max()

    def shift_dates(group):
        offset = pd.DateOffset(years = offset_year - group.name)
        return group.shift(freq = offset)

    return (
        df
        .set_index(date_field, drop=False)
        .groupby(year_field, group_keys=False)
        .apply(shift_dates)
        .rename_axis(date_field + '_offs')
        .reset_index()
    )


def _custom_sort_index(
    df: pd.DataFrame,
    order: list|pd.CategoricalDtype,
    axis: Axis = 0,
    level: int|str|None = None,
) -> pd.DataFrame:
    index = df.index if axis in [0, 'index'] else df.columns
    if isinstance(index, pd.MultiIndex):
        index = index.levels[level]
    order = [i for i in order if i in index]
    return df.reindex(order, axis=axis, level=level)


def custom_sort_index(
    df: pd.DataFrame,
    order: list|pd.CategoricalDtype,
    axis: Axis = 0,
    level: int|str|None = None,
):
    sorter = lambda idx: idx.map({n:m for m,n in pd.Series(order).items()})
    return df.sort_index(axis=axis, level=level, key=sorter)


def value_counts(
    data: pd.Series,
    fillna: str = '---',
    name: str = 'aantal',
    add_pct: bool = False,
    label_pct: str = 'pct',
    ndigits: int = -1,
):
    result = data.fillna(fillna).value_counts().rename(name).pipe(add_totals)
    if add_pct:
        return result.pipe(add_percentages, label=label_pct, ndigits=ndigits)
    return result


@singledispatch
def add_percentages(
    data,
    label: str = 'pct',
    ndigits: int = -1,
):
    raise NotImplementedError('No implementation for this type')


@add_percentages.register
def _(
    data: pd.Series,
    label: str = 'pct',
    ndigits: int = -1,
) -> pd.DataFrame:
    total = data.iloc[-1]
    pcts = (
        data
        .div(total)
        .mul(100)
        .pipe(round_percentages, ndigits=ndigits)
        .rename(label)
    )
    return pd.concat([data, pcts], axis=1)


@add_percentages.register
def _(
    data: pd.DataFrame,
    axis: int = 2,
    label_n: str = 'n',
    label_pct: str = 'pct',
    ndigits: int = -1,
) -> pd.DataFrame:
    if axis == 2:
        totals = data.iloc[-1, -1]
    elif axis == 1:
        totals = data.iloc[-1, :]
    else:
        totals = data.iloc[:, -1]
    axis = axis if axis < 2 else None
    pcts = (
        data
        .div(totals, axis=axis)
        .mul(100)
        .pipe(round_percentages, ndigits=ndigits)
    )
    return pd.concat([data, pcts], keys=[label_n, label_pct], axis=1)


def round_percentages(
    s: pd.Series,
    ndigits: int = -1
) -> pd.Series:
    """
    Round percentages in a way that they always add up to 100%.
    Taken from `this SO answer <https://stackoverflow.com/a/13483486/10403856>`_
    """
    if ndigits < 0:
        return s
    cumsum = s.fillna(0).cumsum().round(ndigits)
    prev_baseline = cumsum.shift(1).fillna(0)
    return cumsum - prev_baseline


@singledispatch
def add_totals(
    data,
    label: str = 'Totaal',
    is_discrete: bool = True
):
    raise NotImplementedError('No implementation for this type')


@add_totals.register
def _(
    data: pd.Series,
    label: str = 'Totaal',
    is_discrete: bool = True
) -> pd.Series:
    output = add_agg(data, 'sum', label=label)
    return output.astype('int64[pyarrow]') if is_discrete else output


@add_totals.register
def _(
    data: pd.DataFrame,
    axis: int = 0,
    label: str = 'Totaal',
    is_discrete: bool = True
) -> pd.DataFrame:
    if axis < 2:
        output = add_agg(data, 'sum', axis=axis, label=label)
    else:
        output = (
            data
            .pipe(add_totals, axis=0, label=label)
            .pipe(add_totals, axis=1, label=label)
        )
    return output.astype('int64[pyarrow]') if is_discrete else output


@singledispatch
def add_agg(data, aggfunc, label=None):
    raise NotImplementedError('No implementation for this type')


@add_agg.register
def _(data: pd.Series, aggfunc, label=None):
    key = get_key(label, aggfunc, data.index)
    data.loc[key] = data.agg(aggfunc)
    return data


@add_agg.register
def _(data: pd.DataFrame, aggfunc, axis=0, label=None):
    agged = data.agg(aggfunc, axis=axis)
    ax = data.index if axis == 0 else data.columns
    key = get_key(label, aggfunc, ax)
    if axis == 0:
        data.loc[key, :] = agged
    elif axis == 1:
        data[key] = agged
    return data


def get_key(label, aggfunc, ax):
    "Get index key."
    label = get_label(label, aggfunc)
    padding = [''] * (ax.nlevels - 1)
    return tuple([label, *padding]) if padding else label


def get_label(label, aggfunc):
    "Get agg label to be used in index key."
    if label is not None:
        return label
    if isinstance(aggfunc, str):
        return aggfunc
    if aggfunc.__name__ != '<lambda>':
        return aggfunc.__name__
    return 'aggregation'
