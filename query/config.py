import json
import sys
import tomllib
from pathlib import Path
from typing import Any


LIBPATH = Path(__file__).resolve().parent.parent


def resolve_path(path: Path|str) -> Path:
    """
    Resolve the provided path to an absolute Path.

    Parameters:
    - path (Path | str): The path to be resolved. It can be either a Path
      or a string representing a file or directory path.

    Returns:
    - Path: An absolute Path after resolution.

    This function takes a Path or a string path as input and ensures that the resulting Path is absolute. If the input path is relative, it is resolved relative to the LIBPATH constant. The resolved path is checked for existence, and an assertion error is raised if the path does not exist.

    Examples:
    >>> resolve_path(Path("example.txt"))
    PosixPath('/absolute/path/to/example.txt')

    >>> resolve_path("relative/path/to/file.txt")
    PosixPath('/absolute/path/to/LIBPATH/relative/path/to/file.txt')

    Raises:
    - AssertionError: If the resolved path does not exist.
    """
    path = Path(path).expanduser()
    if not path.is_absolute():
        path = (LIBPATH / path).resolve()
    assert path.exists(), f"Config: '{path}' does not exist"
    return path


def get_paths_from_config(
    key: str,
    table: str = 'paths',
    keep_shape: bool = False,
) -> Path|list[Path]|dict[str, Path]:
    """
    Retrieve paths from CONFIG based on the specified key.

    Parameters:
    - key (str): The key to identify the paths in CONFIG.
    - table (str): The table in CONFIG to search. Default is 'paths'.
    - keep_shape (bool):
        Normally will coerce str and dict into list output. When `keep_shape` is set to True, a str will be converted to Path and a dict will be converted to a dict of Paths.

    Returns:
    list[Path]: A list of resolved Paths (default).

    If `keep_shape` is True then may also return:
    - Path: the resolved Path if input was a str.
    - dict[str, Path]: a dictionary of resolved paths if input was dict.

    Raises:
    - TypeError: If an unexpected type is encountered while reading paths.
    """
    config = CONFIG[table][key]
    match config:
        case list(config):
            return [resolve_path(path) for path in config]
        case dict(config):
            if keep_shape:
                return {key:resolve_path(path) for key, path in config.items()}
            else:
                return [resolve_path(path) for path in config.values()]
        case str(config):
            path = resolve_path(config)
            return path if keep_shape else [path]
        case _:
            raise TypeError(f'Encountered unexpected type: {type(config)} while reading paths')


def load_config() -> dict[str, Any]:
    """
    Load configuration data from the 'config.toml' file in the 'config' directory.

    If 'config.toml' is not found, it falls back to 'config.default.toml'.

    Returns:
    dict[str, Any]: A dictionary containing the loaded configuration.

    The function reads the configuration from a TOML file located in the 'config' directory. It first attempts to read 'config.toml' and, if not found, falls back to 'config.default.toml'. The configuration is returned as a dictionary.

    Example:
    >>> load_config()
    {'key1': 'value1', 'key2': 'value2', ...}
    """
    config_dir = LIBPATH / 'config'
    config_file = config_dir / 'config.toml'

    if not config_file.exists():
        config_file = config_dir / 'config.default.toml'

    with open(config_file, 'rb') as f:
        config = tomllib.load(f)

    return config


def add_library_to_sys_path() -> None:
    paths = CONFIG['paths']['library']
    if isinstance(paths, list):
        sys.path.extend(paths)
    else:
        sys.path.append(paths)


CONFIG = load_config()
add_library_to_sys_path()


def load_schema(schema: str) -> dict[str, Any]:
    """
    Load a JSON schema file based on the provided schema name.

    Parameters:
    - schema (str): The name of the JSON schema file (without extension).

    Returns:
    dict[str, Any]: A dictionary containing the loaded schema data.

    This function takes the name of a JSON schema file, resolves its path using the configured schema directory, and loads the content of the schema file. The loaded schema data is returned as a dictionary.
    """
    schema_path = resolve_path(CONFIG['paths']['schema'])
    schema_file = (schema_path / schema).with_suffix('.json')

    with open(schema_file) as f:
        schema_data = json.load(f)

    return schema_data
