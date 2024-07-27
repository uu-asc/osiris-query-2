from pathlib import Path
from itertools import chain

import pandas as pd
from query.utils import TS


#region KeyExtractor
class KeyExtractor:
    """
    Accessor to extract keys from a series or dataframe.
    """
    def __init__(self, pandas_obj) -> None:
        self._obj = pandas_obj
        self._key = None
        self._groupby = None
        self._batch_size = None
        self._batch_name = None

    def __call__(
        self,
        key,
        *,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
        to: str = 'str',
        sep: str = ';',
    ) -> pd.Series|str|None:
        if to == 'series':
            return self.to_series(
                key = key,
                unique = unique,
                sample = sample,
                groupby = groupby,
                batch_size = batch_size,
                batch_name = batch_name,
            )
        if to == 'str':
            return self.to_string(
                key = key,
                unique = unique,
                sample = sample,
                groupby = groupby,
                batch_size = batch_size,
                batch_name = batch_name,
                sep = sep,
            )
        if to in ('stdout', 'print'):
            return self.to_stdout(
                key = key,
                unique = unique,
                sample = sample,
                groupby = groupby,
                batch_size = batch_size,
                batch_name = batch_name,
                sep = sep,
            )

    def _preprocessor(function):
        def wrapper(self, *args, **kwargs):
            self.s = self._preprocess(*args, **kwargs)
            return function(self, *args, **kwargs)
        return wrapper

    @_preprocessor
    def to_series(
        self,
        key,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
    ) -> pd.Series:
        return self.s

    @_preprocessor
    def to_string(
        self,
        key,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
        sep: str = ';',
    ) -> str:
        return self._stringify(self.s, self.collected_groups, sep=sep)

    @_preprocessor
    def to_stdout(
        self,
        key,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
        sep: str = ';',
    ) -> None:
        output = self._stringify(self.s, self.collected_groups, sep=sep)
        print(output)

    to_print = to_stdout

    @_preprocessor
    def to_file(
        self,
        key,
        path: Path|str,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
    ) -> None:
        path = Path(path)
        if self.collected_groups:
            for (groep, item) in self.s.groupby(self.collected_groups):
                key = '_'.join(str(i) for i in groep)
                fname = f'{TS.ymd}.{key}.{len(item)}.txt'
                item.to_csv(path / fname, index=False)
        else:
            fname = f'{TS.ymd}.{key}.{len(self.s)}.txt'
            self.s.to_csv(path / fname, index=False)

    @property
    def key(self) -> str:
        if self._key is None:
            raise ValueError('No key was given')
        return self._key

    @key.setter
    def key(self, value) -> None:
        self._key = value

    @property
    def groupby(self) -> list:
        return self._get_grouper(self._groupby)

    @groupby.setter
    def groupby(self, value) -> None:
        self._groupby = value

    @property
    def collected_groups(self) -> list:
        """
        Combines groupby with batches. This works even if either groupby or batches was not set.

        Use this when the batches need to be included in the groupby operation.
        """
        groupby = self._get_grouper(self.groupby)
        batch_name = self._get_grouper(self.batch_name)
        return self._collect_groups(groupby, batch_name)

    @property
    def batch_name(self) -> str:
        """
        Batch name if a batch size was set else None. Defaults to 'batch' if no batch name was given.
        """
        if self.batch_size > 1:
            return self._batch_name or 'batch'
        return None

    @batch_name.setter
    def batch_name(self, value) -> None:
        self._batch_name = value

    @property
    def batch_size(self) -> int:
        """
        Batch size; returns 0 if no (valid) batch size was set.
        """
        if isinstance(self._batch_size, int):
            return self._batch_size
        return 0

    @batch_size.setter
    def batch_size(self, value) -> None:
        self._batch_size = value

    def _get_grouper(self, groupby) -> list:
        """
        Get grouper for groupby operations from groupby.

        Groupby may have been set to None (no grouping) in which case the grouper should return an empty list. Groupby may have been set to a scalar in which case the grouper should return as that scalar wrapped in a list. In all other cases the groupby returns as is.
        """
        if groupby is None:
            return []
        if pd.api.types.is_scalar(groupby):
            return [groupby]
        return groupby

    def _collect_groups(self, *args) -> list:
        """
        Collect all groups from args and return them as a list of groups.
        """
        groups = [self._get_grouper(arg) for arg in args]
        flattened = list(chain.from_iterable(groups))
        return flattened

    def _preprocess(
        self,
        key = None,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = None,
        **kwargs,
    ) -> pd.Series:
        """
        Preprocess keys.

        Parameters:
        key: str, optional
            The key to extract from the data.
        unique: bool, default True
            If True, ensures that the keys are unique within the specified groups.
        sample: int or None, optional
            The number of samples to return. If None, no sampling is performed.
        groupby: list, str, or None, optional
            Columns to group by. If None, no grouping is performed.
        batch_size: int or None, optional
            The size of batches to create. If None, no batching is performed.
        batch_name: str, default 'batch'
            The index name for the batches.

        Returns:
            pd.Series
            keys which optionally are deduplicated, sampled, grouped and batched.
        """
        self._key = key
        self._groupby = groupby
        self._batch_size = batch_size
        self._batch_name = batch_name

        cols = [*self.groupby, self.key]

        # reset index in order to drop duplicates correctly if necessary
        data = self._obj.reset_index().filter(cols)
        if unique: # within groups
            data = data.drop_duplicates(subset=cols)
        if groupby:
            data = data.set_index(self.groupby, append=True)

        if batch_size is not None:
            if groupby is None:
                data = data.pipe(
                    self._add_batches,
                    batch_size = batch_size,
                    batch_name = batch_name,
                )
            else:
                data = data.groupby(self.groupby, group_keys=False).apply(
                    self._add_batches,
                    batch_size = batch_size,
                    batch_name = batch_name,
                )

        if sample is not None:
            data = data.sample(sample)
        return data.squeeze()

    def _add_batches(
        self,
        s: pd.Series,
        batch_size: int = 500,
        batch_name: str = 'batch',
    ) -> pd.Series:
        """
        Add level to index which partitions the Series into batches of specified size.

        Parameters:
        s (pd.Series):
            Series to batch.
        batch_size (int):
            The size of each batch. Default is 500.
        batch_name (str):
            The name of the new index level representing the batch. Default is 'batch'.

        Returns:
            pd.Series
        """
        def get_batches(n: int) -> pd.RangeIndex:
            batches = (pd.RangeIndex(n) // batch_size) + 1
            return batches.rename(batch_name)

        batches = get_batches(len(s))
        return s.set_index(batches, append=True)

    def _stringify(
        self,
        s: pd.Series,
        groupby = None,
        sep: str = ';',
    ) -> str:

        def traverse_data(s, sep=';'):
            nonlocal output
            if s.index.nlevels > 1:
                for lvl in s.index.levels[0]:
                    if not lvl in s.index:
                        # handle lvl not being present
                        continue
                    name = ''
                    if s.index.levels[0].name:
                        name = f'{s.index.levels[0].name}: '
                    output += f'[{name}{lvl}] ({len(s.xs(lvl))})\n'
                    traverse_data(s.xs(lvl))
            else:
                for key, val in s.items():
                    name = f'{s.index.name}: '
                    output += f'[{name}{key}] ({len(val)})\n'
                    output += f'{sep.join(val)}\n\n'

        if groupby:
            values = s.groupby(groupby).agg(list)
            output = ''
            traverse_data(values, sep=sep)
            return output
        else:
            output = s.astype(str).str.cat(sep=sep)
            return output


#region DataFrame
@pd.api.extensions.register_dataframe_accessor("askeys")
class KeyExtractorDataFrame(KeyExtractor):
    """
    Accessor to extract keys from the dataframe.
    """


#region Series
@pd.api.extensions.register_series_accessor("askeys")
class KeyExtractorSeries(KeyExtractor):
    """
    Accessor to extract keys from the series.
    """
    @property
    def key(self) -> str:
        return self._obj.name

    def __call__(
        self,
        *,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
        to: str = 'str',
        sep: str = ';',
    ) -> pd.Series|str|None:
        if to == 'series':
            return self.to_series(
                unique = unique,
                sample = sample,
                groupby = groupby,
                batch_size = batch_size,
                batch_name = batch_name,
            )
        if to == 'str':
            return self.to_string(
                unique = unique,
                sample = sample,
                groupby = groupby,
                batch_size = batch_size,
                batch_name = batch_name,
                sep = sep,
            )
        if to in ('stdout', 'print'):
            return self.to_stdout(
                unique = unique,
                sample = sample,
                groupby = groupby,
                batch_size = batch_size,
                batch_name = batch_name,
                sep = sep,
            )

    @KeyExtractor._preprocessor
    def to_series(
        self,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
    ) -> pd.Series:
        return self.s

    @KeyExtractor._preprocessor
    def to_string(
        self,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
        sep: str = ';',
    ) -> str:
        return self._stringify(self.s, self.collected_groups, sep=sep)

    @KeyExtractor._preprocessor
    def to_stdout(
        self,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
        sep: str = ';',
    ) -> None:
        output = self._stringify(self.s, self.collected_groups, sep=sep)
        print(output)

    to_print = to_stdout

    @KeyExtractor._preprocessor
    def to_file(
        self,
        path: Path|str,
        unique: bool = True,
        sample: int|None = None,
        groupby: list|str|None = None,
        batch_size: int|None = None,
        batch_name: str = 'batch',
    ) -> None:
        path = Path(path)
        if self.collected_groups:
            for (groep, item) in self.s.groupby(self.collected_groups):
                key = '_'.join(str(i) for i in groep)
                fname = f'{TS.ymd}.{key}.{len(item)}.txt'
                item.to_csv(path / fname, index=False)
        else:
            fname = f'{TS.ymd}.{key}.{len(self.s)}.txt'
            self.s.to_csv(path / fname, index=False)
