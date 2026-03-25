import io
from string import Template
from functools import wraps
from typing import Any, BinaryIO, Callable
from pathlib import Path

import pandas as pd


class DotDict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    __dir__ = dict.keys


# region docstring
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


# region keywords
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


# region quickfilter
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


# region excel
class ExcelExporter:
    """Excel exporter with automatic formatting and multi-sheet support.

    This class handles the export of pandas DataFrames to Excel with consistent
    formatting, including date formats, autofit columns, and autofilter.

    Parameters
    ----------
    date_format : str, optional
        Format string for dates, by default 'DD-MM-YYYY'
    datetime_format : str, optional
        Format string for datetime values, by default 'DD-MM-YYYY'
    """

    def __init__(
        self,
        date_format: str = 'DD-MM-YYYY',
        datetime_format: str = 'DD-MM-YYYY',
    ):
        self.date_format = date_format
        self.datetime_format = datetime_format

    def _header_row_count(self, df: pd.DataFrame, index: bool) -> int:
        """Return the number of header rows pandas writes.

        When index is shown and columns are multi-level, pandas adds an extra
        row for index names. This row should be included in the autofilter
        range so filters sit on the correct row.
        """
        col_nlevels = df.columns.nlevels
        if index and col_nlevels > 1 and any(n is not None for n in df.index.names):
            return col_nlevels + 1
        return col_nlevels

    def _format_sheet(
        self,
        sheet: object,
        col_nlevels: int,
        idx_offset: int,
        n_rows: int,
        n_cols: int,
    ) -> None:
        """Apply formatting to a worksheet.

        Parameters
        ----------
        sheet : Any
            Excel worksheet object
        col_nlevels : int
            Number of column levels
        idx_offset : int
            Number of columns to offset for index (if shown)
        n_rows : int
            Number of rows
        n_cols : int
            Number of columns
        """
        sheet.autofit()
        sheet.autofilter(
            col_nlevels - 1,
            0,
            n_rows,
            idx_offset + n_cols - 1,
        )

    def to_bytes(
        self,
        sheet_data: pd.DataFrame | dict[str, pd.DataFrame],
        index: bool = True,
        sheet_name: str = 'data',
    ) -> bytes:
        """Export multiple DataFrames to Excel and return as bytes.

        Parameters
        ----------
        sheet_data : dict[str, pd.DataFrame]
            Dictionary mapping sheet names to DataFrames
        index : bool, optional
            Whether to write index to Excel file, by default True

        Returns
        -------
        bytes
            Excel file content as bytes
        """
        if isinstance(sheet_data, pd.DataFrame):
            sheet_data = {sheet_name: sheet_data}
        buf = io.BytesIO()
        self._write(sheet_data, buf, index=index)
        return buf.getvalue()

    def export_sheet(
        self,
        df: pd.DataFrame,
        filepath: Path | str,
        sheet_name: str = 'data',
        index: bool = True,
    ) -> None:
        """Export a single DataFrame to Excel.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to export
        filepath : Path|str
            Path to save the Excel file
        sheet_name : str, optional
            Name of the worksheet, by default 'data'
        index : bool, optional
            Whether to write index to Excel file, by default True
        """
        self.export_workbook({sheet_name: df}, filepath, index=index)

    def export_workbook(
        self,
        sheet_data: dict[str, pd.DataFrame],
        filepath: Path | str,
        index: bool = True,
    ) -> None:
        """Export multiple DataFrames to a single Excel file.

        Parameters
        ----------
        sheet_data : dict[str, pd.DataFrame]
            Dictionary mapping sheet names to DataFrames
        filepath : Path|str
            Path to save the Excel file
        index : bool, optional
            Whether to write index to Excel file, by default True
        """
        self._write(sheet_data, Path(filepath), index=index)

    def _write(
        self,
        sheet_data: dict[str, pd.DataFrame],
        destination: Path | BinaryIO,
        index: bool = True,
    ) -> None:
        """Write sheet data to a file path or file-like object.

        Parameters
        ----------
        destination : Path | BinaryIO
            File path or file-like object to write to
        sheet_data : dict[str, pd.DataFrame]
            Dictionary mapping sheet names to DataFrames
        index : bool, optional
            Whether to write index to Excel file, by default True
        """
        with pd.ExcelWriter(
            destination,
            date_format=self.date_format,
            datetime_format=self.datetime_format,
        ) as writer:
            for sheet_name, df in sheet_data.items():
                n_rows, n_cols = df.shape
                idx_nlevels = df.index.nlevels if index else 0
                header_rows = self._header_row_count(df, index)

                df.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    index=index,
                    freeze_panes=(header_rows, idx_nlevels),
                )

                self._format_sheet(
                    writer.sheets[sheet_name],
                    header_rows,
                    idx_nlevels,
                    n_rows,
                    n_cols,
                )


# region other
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
