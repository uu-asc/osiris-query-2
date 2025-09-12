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
    flatten: bool = False,
) -> Path|list[Path]|dict[str, Any]:
    """
    Retrieve paths from CONFIG based on the specified key (supports dot notation and list of keys for nested access).

    Parameters:
    - key (str|list[str]): The key to identify the paths in CONFIG.
        - str: Simple key or dot notation (e.g. 'output' or 'output.werkvoorraad')
        - list[str]: List of keys for nested access (e.g. ['output', 'werkvoorraad'])
    - table (str): The table in CONFIG to search. Default is 'paths'.
    - flatten (bool): Whether to flatten nested structures into a single list. Default is False.
        - False: Preserves original structure (Path for strings, dict for dicts)
        - True: Returns list[Path] with all paths flattened

    Returns:
    - Path: Single resolved Path (when flatten=False and input is string, default).
    - dict[str, Path|dict]: Nested dict with resolved paths (when flatten=False and input is dict).
    - list[Path]: A flattened list of all resolved Paths (when flatten=True).

    Examples:
    >>> # Preserve structure (default)
    >>> get_paths_from_config('schema')  # returns Path('./schema')
    >>> get_paths_from_config('output')  # returns {'main': Path, 'temp': {...}}

    >>> # Flattened output
    >>> get_paths_from_config('output', flatten=True)  # returns [Path1, Path2, Path3, ...]

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

    return _resolve_config_paths(config, flatten)


def _resolve_config_paths(
    config: str|list|dict,
    flatten: bool
) -> Path|list[Path]|dict[str, Any]:
    """
    Recursively resolve paths in config structure.

    Parameters:
    - config: Configuration value to resolve (string path, list, or dict).
    - flatten: Whether to flatten nested structures or preserve shape.

    Returns:
    - Path|list[Path]|dict: Resolved paths - flattened list or original structure.

    Raises:
    - TypeError: If an unexpected type is encountered while resolving paths.
    """
    match config:
        case list():
            if flatten:
                # Flatten: collect all paths from list items
                paths = []
                for item in config:
                    resolved = _resolve_config_paths(item, flatten=False)  # Get structured first
                    paths.extend(_collect_leaf_paths(resolved))
                return paths
            else:
                # Preserve structure: resolve each item individually
                return [_resolve_config_paths(item, flatten=False) for item in config]
        case dict():
            if flatten:
                # Flatten: collect all paths from dict values
                paths = []
                for value in config.values():
                    resolved = _resolve_config_paths(value, flatten=False)  # Get structured first
                    paths.extend(_collect_leaf_paths(resolved))
                return paths
            else:
                # Preserve structure: resolve each value, keep dict structure
                return {k: _resolve_config_paths(v, flatten=False) for k, v in config.items()}
        case str():
            path = resolve_path(config)
            return [path] if flatten else path
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
