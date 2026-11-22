import sys
import tomllib
from configparser import ConfigParser, Error as ConfigParserError
from dataclasses import dataclass, field, fields, MISSING
from pathlib import Path
from typing import Any, Callable, Self, TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy import Engine


LIBPATH: Path = Path(__file__).resolve().parent.parent


class ConfigError(Exception):
    """Invalid or incomplete configuration."""


def absolute_path(path: Path|str) -> Path:
    """Expand `~` and resolve relative paths against LIBPATH. Does not check existence."""
    path = Path(path).expanduser()
    if not path.is_absolute():
        path = (LIBPATH / path).resolve()
    return path


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
    path = absolute_path(path)
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
    >>> get_paths_from_config('queries')  # returns Path('./definitions')
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


def get_config_file() -> Path:
    """
    Return the config file to load: 'config/config.toml', or
    'config/config.default.toml' if the former does not exist.
    """
    config_dir = LIBPATH / 'config'
    config_file = config_dir / 'config.toml'
    if not config_file.exists():
        config_file = config_dir / 'config.default.toml'
    return config_file


def load_config(config_file: Path) -> dict[str, Any]:
    """
    Load configuration data from `config_file` (TOML).

    Returns:
    dict[str, Any]: A dictionary containing the loaded configuration.

    Raises:
    - ConfigError: If the file is not valid TOML. The message names the file
      and the position of the error.

    Example:
    >>> load_config(get_config_file())
    {'key1': 'value1', 'key2': 'value2', ...}
    """
    with open(config_file, 'rb') as f:
        try:
            config = tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            raise ConfigError(f"cannot parse {config_file}: {e}") from None
    return config


def add_library_to_sys_path() -> None:
    paths = CONFIG['paths']['library']
    if isinstance(paths, list):
        sys.path.extend(paths)
    else:
        sys.path.append(paths)


CONFIG_FILE: Path = get_config_file()
CONFIG: dict[str, Any] = load_config(CONFIG_FILE)
# add_library_to_sys_path()


# region connection settings
def setting(
    parse: Callable[[Any], Any],
    expected: str,
    **kwargs: Any,
) -> Any:
    """
    Declare a field of a `ConnectionSettings` subclass that is read from config.

    Parameters:
    - parse (Callable): Receives the raw config value; returns the parsed
      value or raises ValueError with a message describing the problem.
    - expected (str): Short description of a valid value, shown when a
      required key is missing (e.g. 'positive int').
    - **kwargs: Passed to `dataclasses.field`. Give `default` or
      `default_factory` to make the key optional; without either it is required.

    Fields declared without `setting(...)` (such as `name`) are not read
    from config.
    """
    return field(metadata={'parse': parse, 'expected': expected}, **kwargs)


def parse_str(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"expected non-empty string, got {value!r}")
    return value


def parse_port(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value < 65536:
        raise ValueError(f"expected port number (int), got {value!r}")
    return value


def parse_positive_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"expected positive int, got {value!r}")
    return value


def parse_table(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"expected table, got {value!r}")
    return value


def parse_existing_file(value: Any) -> Path:
    """Path string; `~` expanded, relative paths resolved against LIBPATH; file must exist."""
    path = absolute_path(parse_str(value))
    if not path.is_file():
        raise ValueError(f"file not found: {path}")
    return path


CREDENTIALS_KEYS = {'uid', 'pwd'}


def read_credentials(path: Path) -> dict[str, str]:
    """
    Read the [credentials] section from an ini file.

    Only uid and pwd are used by the connectors. Other keys in the section
    are ignored (they are not passed to the connection).
    """
    parser = ConfigParser()
    with path.open() as f:
        parser.read_file(f)
    return dict(parser.items('credentials'))


def parse_credentials_file(value: Any) -> Path:
    """
    Path to an existing ini file with a [credentials] section containing
    uid and pwd. Other keys are allowed and ignored.

    Only the path is kept; the file is read again when connecting, so the
    password is not stored on the settings object.
    """
    path = parse_existing_file(value)
    try:
        creds = read_credentials(path)
    except ConfigParserError as e:
        raise ValueError(f"{path}: {e.message.splitlines()[0]}") from None
    missing = sorted(CREDENTIALS_KEYS - creds.keys())
    if missing:
        raise ValueError(f"{path}: {', '.join(missing)} missing")
    return path


@dataclass(frozen=True, kw_only=True)
class ConnectionSettings:
    """
    Validated settings for one connection, read from [connections.<name>].

    Subclasses (one per driver, see query.connections.connection) declare
    their fields with `setting(...)` and implement `engine()`. Each database
    module creates its settings at import, e.g.
    `SETTINGS = OracleSettings.from_config('osiris')`.

    Validation (`from_config`) reports all problems in the section at once:
    missing required keys, values that fail to parse, and unknown keys (to
    catch typos).

    Fields shared by all drivers:
    - name (str): The section name, e.g. 'osiris'. Not a config key.
    - connect_args (table, optional): Driver options passed unchanged to
      `create_engine(connect_args=...)` and from there to the driver's
      `connect()`. Contents are not validated here, since they depend on the
      driver; a wrong option shows up as a driver error on first connect.
      Example:

          [connections.isis.connect_args]
          read_only = true
    """
    name: str
    connect_args: dict[str, Any] = setting(parse_table, 'table', default_factory=dict)

    @classmethod
    def from_config(
        cls,
        name: str,
        config: dict[str, Any] | None = None,
    ) -> Self:
        """
        Build settings from [connections.<name>] in `config` (default: CONFIG).

        Raises:
        - ConfigError: listing every problem found in the section.
        """
        config = CONFIG if config is None else config
        section = config.get('connections', {}).get(name)
        header = f"[connections.{name}] ({CONFIG_FILE})"
        if section is None:
            raise ConfigError(f"missing section {header}")
        if not isinstance(section, dict):
            raise ConfigError(f"{header} must be a table")

        values, problems = cls._parse(section)
        if problems:
            lines = '\n'.join(f"  - {problem}" for problem in problems)
            raise ConfigError(f"problems in {header}:\n{lines}")
        return cls(name=name, **values)

    @classmethod
    def _parse(cls, section: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
        """Parse all fields; collect problems instead of stopping at the first."""
        values: dict[str, Any] = {}
        problems: list[str] = []
        known: set[str] = set()
        for f in fields(cls):
            if 'parse' not in f.metadata:
                continue
            known.add(f.name)
            if f.name not in section:
                required = f.default is MISSING and f.default_factory is MISSING
                if required:
                    problems.append(f"{f.name}: missing ({f.metadata['expected']})")
                continue
            try:
                values[f.name] = f.metadata['parse'](section[f.name])
            except ValueError as e:
                problems.append(f"{f.name}: {e}")
        for key in sorted(section.keys() - known):
            problems.append(f"{key}: unknown key")
        return values, problems

    def engine(self) -> 'Engine':
        """Create a SQLAlchemy engine for this connection."""
        raise NotImplementedError
