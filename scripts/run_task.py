import argparse
import json

from importlib import import_module
from pathlib import Path
from typing import Any

import pandas as pd

from query.utils import Stopwatch
from query.config import get_paths_from_config


BANNER = """
+==================================================================+
    :: RUNNING TASK ::
+------------------------------------------------------------------+
"""


def path_to_json(path: Path|str) -> dict[str, Any]:
    """
    Read JSON content from a file specified by the given path.

    Parameters:
    - path (Union[Path, str]): The path to the file.

    Returns:
    - dict[str, Any]: A dictionary representing the parsed JSON content.

    Raises:
    - FileNotFoundError: If the path does not exist or is not a file.
    - ValueError: If there is an error decoding the JSON content.
    """
    path = Path(path)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"File '{path}' does not exist/is not a file.")

    with open(path, 'r') as file:
        file_content = file.read()
        return json.loads(file_content)


def make_out_path() -> Path:
    path = get_paths_from_config('output')[0]
    return path / 'temp' / 'temp.parquet'


def run_query(
    out_path: str,
    **task
) -> pd.DataFrame:
    df = source.execute_query(**task)
    df.to_parquet(out_path)
    return df


if __name__ == "__main__":
    print(BANNER)
    stopwatch = Stopwatch()

    parser = argparse.ArgumentParser(description='Run task')
    parser.add_argument('source', help='Source')
    parser.add_argument('task_path', help='Path to task')
    parser.add_argument('-o', '--out_path', help='Output path')
    args = parser.parse_args()

    source = import_module(f"query.{args.source}")
    print(f"source:   {args.source}")

    task = path_to_json(args.task_path)
    print(f"task:     {args.task_path}\n{json.dumps(task, indent=2)}")

    out_path = make_out_path() if args.out_path is None else args.out_path
    print(f"out:      {out_path}")

    df = run_query(out_path, **task)
    rows, cols = df.shape
    print(f"\n[[... Retrieved: {rows = }, {cols = } ...]]")

    stopwatch.total()
