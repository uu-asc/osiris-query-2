import re
from pathlib import Path
from itertools import batched, chain

import pandas as pd


class KeyExtractor:
    """
    Accessor to extract keys from a series or dataframe.
    """
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def _batch(
        self,
        s: pd.Series,
        batch_size: int = 5,
        batch_name: str = 'batch',
    ) -> pd.Series:
        def batch(n: int):
            batches = pd.RangeIndex(n) // batch_size
            return batches.rename(batch_name)

        batches = batch(len(s))
        return s.set_index(batches, append=True)

    def _preprocess(
        self,
        key = None,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
    ) -> pd.Series:
        grouper = self._get_grouper(groupby)
        key = self._obj.name if key is None else key
        cols = self._collect_groups(key, grouper)
        # reset index in order to drop duplicates correctly if necessary
        data = self._obj.reset_index().filter(cols)
        if unique: # within groups
            data = data.drop_duplicates(subset=cols)
        if groupby:
            data = data.set_index(grouper, append=True)

        if batch_size is not None:
            if groupby is None:
                data = data.pipe(
                    self._batch,
                    batch_size = batch_size,
                    batch_name = batch_name,
                )
            else:
                data = data.groupby(grouper, group_keys=False).apply(
                    self._batch,
                    batch_size = batch_size,
                    batch_name = batch_name,
                )

        if sample is not None:
            data = data.sample(sample)
        return data.squeeze()

    def _get_grouper(self, groupby):
        match groupby:
            case list():
                grouper = groupby
            case None:
                grouper = []
            case _:
                grouper = [groupby]
        return grouper

    def _collect_groups(self, *args):
        groups = [self._get_grouper(arg) for arg in args]
        flattened = list(chain.from_iterable(groups))
        return flattened

    def _stringify(
        self,
        s: pd.Series,
        groupby = None,
        batch_name = None,
        sep: str = ';'
    ):

        def convert_to_strings(s, sep=';'):
            values = s.astype(str).to_list()
            return sep.join(values)

        def traverse_data(s):
            nonlocal output
            if s.index.nlevels > 1:
                for lvl in s.index.levels[0]:
                    name = ''
                    if s.index.levels[0].name:
                        name = f'{s.index.levels[0].name}: '
                    output += f'[{name}{lvl}]\n'
                    traverse_data(s.xs(lvl))
            else:
                for key, val in s.items():
                    name = f'{s.index.name}: '
                    output += f'[{name}{key}]\n'
                    output += f'{val}\n\n'

        if groupby:
            grouper = self._collect_groups(groupby, batch_name)
            values = s.groupby(grouper).agg(convert_to_strings, sep=sep)
            output = ''
            traverse_data(values)
            return output
        elif batch_name:
            values = s.groupby(batch_name).agg(convert_to_strings, sep=sep)
            output = ''
            traverse_data(values)
            return output
        else:
            output = s.agg(convert_to_strings, sep=sep)
            return output


@pd.api.extensions.register_dataframe_accessor("askeys")
class KeyExtractorDataFrame(KeyExtractor):
    """
    Accessor to extract keys from the dataframe.
    """
    def to_series(
        self,
        key,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
    ) -> pd.Series:
        data = self._preprocess(
            key,
            unique = unique,
            sample = sample,
            groupby = groupby,
            batch_size = batch_size,
            batch_name = batch_name,
        )
        return data

    def to_list(
        self,
        key,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
    ) -> list:
        data = self._preprocess(
            key,
            unique = unique,
            sample = sample,
            groupby = groupby,
            batch_size = batch_size,
            batch_name = batch_name,
        )
        if groupby:
            def listify(groups):
                listify = lambda grp: grp.to_list()
                return {name:listify(group) for name, group in groups}
            return data.groupby(level=0).pipe(listify)
        return data.to_list()

    def to_string(
        self,
        key,
        sep: str = ';',
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = 500,
        batch_name: str = 'batch',
    ) -> str:
        batch_name = batch_name if batch_size is not None else None
        data = self._preprocess(
            key,
            unique = unique,
            sample = sample,
            groupby = groupby,
            batch_size = batch_size,
            batch_name = batch_name,
        )
        return self._stringify(
            data,
            groupby = groupby,
            batch_name = batch_name,
            sep=sep
        )

    def to_stdout(
        self,
        key,
        sep: str = ';',
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = 500,
    )-> None:
        stdout = self.to_string(
            key = key,
            sep = sep,
            unique = unique,
            sample = sample,
            groupby = groupby,
            batch_size = batch_size,
        )
        print(stdout)
        return None

    def to_clipboard(
        self,
        key,
        unique: bool = True,
        sample: int|None = None,
    ):
        keys = self._preprocess(
            key,
            unique = unique,
            sample = sample,
        )
        keys.to_clipboard(index=False)
        return None

    def to_file(
        self,
        key,
        path: Path|str,
        sep: str = '\n',
        encoding: str = 'utf8',
        unique: bool = True,
        sample: int|None = None,
    ):
        keys = self.to_string(
            key,
            sep = sep,
            unique = unique,
            sample = sample,
        )
        Path(path).write_text(keys, encoding=encoding)
        return None

    def __call__(
        self,
        key,
        /,
        *,
        to: str = 'str',
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int = 500,
        sep: str = ';',
        encoding: str = 'utf8',
        path: Path|str = None,
        **kwargs
    ) -> None|str|list|pd.Series:
        """
        Return key field in DataFrame as string, list, series or print to stdout or copy to clipboard.

        Arguments
        ---------
        to : ['str', 'clipboard', 'stdout', 'file', ...], default 'str'
            str       : return as str
            series    : return as pd.Series
            list      : return as list
            clipboard : copy output to clipboard
            stdout    : print output
            file      : export to ``path``
        sep : str, default ';'
            Separtor between values.
        path : Path of str, optioneel
            Path to file.
        unique : bool, default True
            Deduplicate keys if True.

        Returns
        -------
        str, list, pd.Series or None (prints to stdout)
        """
        match to:
            case 'str':
                return self.to_string(
                    key = key,
                    sep = sep,
                    unique = unique,
                    sample = sample,
                    groupby = groupby,
                    batch_size = batch_size,
                )
            case 'series':
                return self.to_series(
                    key = key,
                    unique = unique,
                    sample = sample,
                    groupby = groupby,
                )
            case 'list':
                return self.to_list(
                    key = key,
                    unique = unique,
                    sample = sample,
                    groupby = groupby,
                )
            case 'stdout':
                return self.to_stdout(
                    key = key,
                    sep = sep,
                    unique = unique,
                    sample = sample,
                    groupby = groupby,
                )
            case 'clipboard':
                return self.to_clipboard(
                    key = key,
                    unique = unique,
                    sample = sample,
                )
            case 'file':
                return self.to_file(
                    key = key,
                    path = path,
                    sep = sep,
                    encoding = encoding,
                    unique = unique,
                    sample = sample,
                    groupby = groupby,
                )


@pd.api.extensions.register_series_accessor("askeys")
class KeyExtractorSeries(KeyExtractor):
    """
    Accessor to extract keys from the dataframe.
    """
    def to_series(
        self,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
    ) -> pd.Series:
        data = self._preprocess(
            unique = unique,
            sample = sample,
            groupby = groupby,
            batch_size = batch_size,
            batch_name = batch_name,
        )
        return data

    def to_list(
        self,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
    ) -> list:
        data = self._preprocess(
            unique = unique,
            sample = sample,
            groupby = groupby,
            batch_size = batch_size,
            batch_name = batch_name,
        )
        if groupby:
            def listify(groups):
                listify = lambda grp: grp.to_list()
                return {name:listify(group) for name, group in groups}
            return data.groupby(level=0).pipe(listify)
        return data.to_list()

    def to_string(
        self,
        sep: str = ';',
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = 500,
        batch_name: str = 'batch',
    ) -> str:
        batch_name = batch_name if batch_size is not None else None
        data = self._preprocess(
            unique = unique,
            sample = sample,
            groupby = groupby,
            batch_size = batch_size,
            batch_name = batch_name,
        )
        return self._stringify(
            data,
            groupby = groupby,
            batch_name = batch_name,
            sep=sep
        )

    def to_stdout(
        self,
        sep: str = ';',
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = 500,
    )-> None:
        stdout = self.to_string(
            sep = sep,
            unique = unique,
            sample = sample,
            groupby = groupby,
            batch_size = batch_size,
        )
        print(stdout)
        return None

    def to_clipboard(
        self,
        unique: bool = True,
        sample: int|None = None,
    ):
        keys = self._preprocess(
            unique = unique,
            sample = sample,
        )
        keys.to_clipboard(index=False)
        return None

    def to_file(
        self,
        path: Path|str,
        sep: str = '\n',
        encoding: str = 'utf8',
        unique: bool = True,
        sample: int|None = None,
    ):
        keys = self.to_string(
            sep = sep,
            unique = unique,
            sample = sample,
        )
        Path(path).write_text(keys, encoding=encoding)
        return None

    def __call__(
        self,
        *,
        to: str = 'str',
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int = 500,
        sep: str = ';',
        encoding: str = 'utf8',
        path: Path|str = None,
        **kwargs
    ) -> None|str|list|pd.Series:
        """
        Return key field in DataFrame as string, list, series or print to stdout or copy to clipboard.

        Arguments
        ---------
        to : ['str', 'clipboard', 'stdout', 'file', ...], default 'str'
            str       : return as str
            series    : return as pd.Series
            list      : return as list
            clipboard : copy output to clipboard
            stdout    : print output
            file      : export to ``path``
        sep : str, default ';'
            Separtor between values.
        path : Path of str, optioneel
            Path to file.
        unique : bool, default True
            Deduplicate keys if True.

        Returns
        -------
        str, list, pd.Series or None (prints to stdout)
        """
        match to:
            case 'str':
                return self.to_string(
                    sep = sep,
                    unique = unique,
                    sample = sample,
                    groupby = groupby,
                    batch_size = batch_size,
                )
            case 'series':
                return self.to_series(
                    unique = unique,
                    sample = sample,
                    groupby = groupby,
                )
            case 'list':
                return self.to_list(
                    unique = unique,
                    sample = sample,
                    groupby = groupby,
                )
            case 'stdout':
                return self.to_stdout(
                    sep = sep,
                    unique = unique,
                    sample = sample,
                    groupby = groupby,
                )
            case 'clipboard':
                return self.to_clipboard(
                    unique = unique,
                    sample = sample,
                )
            case 'file':
                return self.to_file(
                    path = path,
                    sep = sep,
                    encoding = encoding,
                    unique = unique,
                    sample = sample,
                    groupby = groupby,
                )
