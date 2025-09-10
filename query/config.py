import json
import sys
import tomllib
from pathlib import Path
from typing import Any


LIBPATH: Path = Path(__file__).resolve().parent.parent


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
    key: str|list[str],
    table: str = 'paths',
    keep_shape: bool = False,
) -> Path|list[Path]|dict[str, Any]:
    """
    Retrieve paths from CONFIG based on the specified key (supports dot notation and list of keys for nested access).

    Parameters:
    - key (str|list[str]): The key to identify the paths in CONFIG.
        - str: Simple key or dot notation (e.g. 'output' or 'output.werkvoorraad')
        - list[str]: List of keys for nested access (e.g. ['output', 'werkvoorraad'])
    - table (str): The table in CONFIG to search. Default is 'paths'.
    - keep_shape (bool):
        Normally will coerce str and dict into list output. When `keep_shape` is set to True, a str will be converted to Path and a dict will be converted to a dict of Paths.

    Returns:
    list[Path]: A list of resolved Paths (default).

    If `keep_shape` is True then may also return:
    - Path: the resolved Path if input was a str.
    - dict[str, Path]: a dictionary of resolved paths if input was dict (nested structure preserved).

    Examples:
    >>> # Simple string path
    >>> get_paths_from_config('schema')  # returns [Path('./schema')]
    >>> get_paths_from_config('schema', keep_shape=True)  # returns Path('./schema')

    >>> # Dict of paths
    >>> get_paths_from_config('output', keep_shape=True)
    >>> # returns {'main': Path('./output'), 'temp': {'data': Path('./temp/data')}}

    >>> # List of paths
    >>> get_paths_from_config('library')  # returns [Path('/path1'), Path('/path2')]

    Raises:
    - TypeError: If an unexpected type is encountered while reading paths.
    """
    # Normalize key input to list of keys
    if isinstance(key, str):
        if '.' in key:
            keys = key.split('.')
        else:
            keys = [key]
    else:
        keys = key

    # Navigate to nested config
    config = CONFIG[table]
    for k in keys:
        config = config[k]

    return _resolve_config_paths(config, keep_shape)


def _resolve_config_paths(
    config: str|list|dict,
    keep_shape: bool
) -> Path|list[Path]|dict[str, Any]:
    """
    Recursively resolve paths in config structure.

    Parameters:
    - config: Configuration value to resolve (string path, list, or dict).
    - keep_shape: Whether to preserve the original structure or flatten to list.

    Returns:
    - Path|list[Path]|dict: Resolved paths maintaining structure based on keep_shape.

    Raises:
    - TypeError: If an unexpected type is encountered while resolving paths.
    """
    match config:
        case list():
            if keep_shape:
                return [_resolve_config_paths(item, True) for item in config]
            else:
                # Flatten all items from list
                paths = []
                for item in config:
                    resolved = _resolve_config_paths(item, True)
                    paths.extend(_collect_leaf_paths(resolved))
                return paths
        case dict():
            if keep_shape:
                return {k: _resolve_config_paths(v, True) for k, v in config.items()}
            else:
                # Flatten all paths from nested dict structure
                paths = []
                for value in config.values():
                    resolved = _resolve_config_paths(value, True)
                    paths.extend(_collect_leaf_paths(resolved))
                return paths
        case str():
            path = resolve_path(config)
            return path if keep_shape else [path]
        case _:
            raise TypeError(f'Encountered unexpected type: {type(config)} while reading paths')


def _collect_leaf_paths(structure) -> list[Path]:
    """Extract all Path objects from nested structure."""
    if isinstance(structure, Path):
        return [structure]
    elif isinstance(structure, dict):
        paths = []
        for value in structure.values():
            paths.extend(_collect_leaf_paths(value))
        return paths
    elif isinstance(structure, list):
        paths = []
        for item in structure:
            paths.extend(_collect_leaf_paths(item))
        return paths
    else:
        raise TypeError(f'Unexpected type in path structure: {type(structure)}')


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
# add_library_to_sys_path()


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
