import json
import time
from pathlib import Path
from typing import Any

import pandas as pd

from query import get_paths_from_config


def wait_for_file(file_path: Path|str, timeout_seconds: int = 60) -> Path:
    """
    Waits for a file to appear at the specified path.

    Parameters:
        - file_path (Path|str): Path to file.
        - timeout_seconds (int): Maximum time to wait (default is 60 seconds).

    Returns:
        - Path: awaited path to file.

    Raises:
        - TimeoutError: If file does not appear within specified timeout.
    """
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        if file_path.exists():
            return file_path
        time.sleep(1)

    raise TimeoutError(f"Timeout waiting for '{file_path}'")


def execute_from_task(**task: Any) -> pd.DataFrame:
    """
    Execute a task and return result as DataFrame.

    Parameters:
        - **task (Any): Keyword arguments representing the task details.

    Returns:
        - pd.DataFrame: Resulting DataFrame.

    Raises:
        - TimeoutError: If output file does not appear within timeout.

    Note:
        - Depends on server looking for task files and executing them.
        - Uses paths specified in config.
        - Task details are written to temporary JSON file at 'task' path.
        - Output is read from configured 'output/temp/temp.parquet' path.
        - Both temporary files are deleted after reading the output.
    """
    task_path = get_paths_from_config('tasks')
    out_path = get_paths_from_config('output') / 'temp' / 'temp.parquet'
    task_path.write_text(json.dumps(task))

    try:
        wait_for_file(out_path)
        return pd.read_parquet(out_path)
    finally:
        task_path.unlink()
        out_path.unlink()
