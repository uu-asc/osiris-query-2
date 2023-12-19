import json
import tomllib
from pathlib import Path


LIBPATH = Path(__file__).resolve().parent


def load_config():
    config_path = LIBPATH.parent / 'config' / 'config.toml'
    with open(config_path, 'rb') as f:
        config = tomllib.load(f)
    return config


def load_schema(schema):
    config = load_config()
    schema_path = (LIBPATH.parent / config['paths']['schema']).resolve()
    schema_file = (schema_path / schema).with_suffix('.json')
    with open(schema_file) as f:
        schema_data = json.load(f)
    return schema_data
