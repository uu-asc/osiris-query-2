import tempfile
import unittest
from pathlib import Path

from query.config import ConfigError, load_config
from query.connections.connection import (
    DuckdbSettings,
    MssqlSettings,
    OracleSettings,
)


class SettingsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def write(self, name: str, text: str) -> str:
        path = self.tmp / name
        path.write_text(text)
        return str(path)

    def creds(self, text: str = "[credentials]\nuid = u\npwd = p\n", name: str = 'creds.ini') -> str:
        return self.write(name, text)

    def oracle_section(self, **overrides) -> dict:
        section = {
            'host': 'db.example',
            'port': 1521,
            'dsn': 'svc',
            'credentials': self.creds(name='default.ini'),
            'arraysize': 10_000,
        }
        section.update(overrides)
        return {k: v for k, v in section.items() if v is not None}

    def build(self, cls, section: dict, name: str = 'db'):
        return cls.from_config(name, {'connections': {name: section}})

    def problems(self, cls, section: dict) -> str:
        with self.assertRaises(ConfigError) as ctx:
            self.build(cls, section)
        return str(ctx.exception)


class TestOracleSettings(SettingsTestCase):
    def test_valid(self) -> None:
        s = self.build(OracleSettings, self.oracle_section())
        self.assertEqual(s.name, 'db')
        self.assertEqual(s.arraysize, 10_000)
        self.assertEqual(s.port, 1521)
        self.assertTrue(s.credentials.is_absolute())

    def test_missing_section(self) -> None:
        with self.assertRaises(ConfigError) as ctx:
            OracleSettings.from_config('osiris', {'connections': {}})
        self.assertIn('missing section [connections.osiris]', str(ctx.exception))

    def test_all_problems_reported_at_once(self) -> None:
        msg = self.problems(OracleSettings, self.oracle_section(
            arraysize = None,
            port = '1521',
            credentials = str(self.tmp / 'nope.ini'),
            arraysise = 10_000,
        ))
        self.assertIn('arraysize: missing', msg)
        self.assertIn('port: expected port number', msg)
        self.assertIn('credentials: file not found', msg)
        self.assertIn('arraysise: unknown key', msg)
        self.assertEqual(msg.count('\n  - '), 4)

    def test_arraysize_rejects_bool_and_non_positive(self) -> None:
        for value in (True, 0, -1, 10.0):
            with self.subTest(value=value):
                msg = self.problems(OracleSettings, self.oracle_section(arraysize=value))
                self.assertIn('arraysize: expected positive int', msg)

    def test_credentials_file_missing_pwd(self) -> None:
        path = self.creds("[credentials]\nuid = u\n")
        msg = self.problems(OracleSettings, self.oracle_section(credentials=path))
        self.assertIn('pwd missing', msg)

    def test_credentials_file_extra_keys_ignored(self) -> None:
        path = self.creds("[credentials]\nuid = u\npwd = p\nhost = h\n")
        s = self.build(OracleSettings, self.oracle_section(credentials=path))
        self.assertEqual(s.host, 'db.example')

    def test_connect_args(self) -> None:
        s = self.build(OracleSettings, self.oracle_section())
        self.assertEqual(s.connect_args, {})
        s = self.build(OracleSettings, self.oracle_section(connect_args={'expire_time': 2}))
        self.assertEqual(s.connect_args, {'expire_time': 2})
        msg = self.problems(OracleSettings, self.oracle_section(connect_args='x'))
        self.assertIn('connect_args: expected table', msg)

    def test_credentials_file_without_section(self) -> None:
        path = self.creds("uid = u\n")
        msg = self.problems(OracleSettings, self.oracle_section(credentials=path))
        self.assertIn('credentials:', msg)


class TestMssqlSettings(SettingsTestCase):
    def test_valid(self) -> None:
        s = self.build(MssqlSettings, {
            'host': 'h', 'port': 1433, 'database': 'd', 'credentials': self.creds(),
        })
        self.assertEqual(s.database, 'd')

    def test_arraysize_is_unknown(self) -> None:
        msg = self.problems(MssqlSettings, {
            'host': 'h', 'port': 1433, 'database': 'd', 'credentials': self.creds(),
            'arraysize': 10_000,
        })
        self.assertIn('arraysize: unknown key', msg)


class TestDuckdbSettings(SettingsTestCase):
    def test_valid(self) -> None:
        db = self.write('x.duckdb', '')
        s = self.build(DuckdbSettings, {'database': db})
        self.assertEqual(str(s.database), db)

    def test_missing_file_and_stray_credentials(self) -> None:
        msg = self.problems(DuckdbSettings, {
            'database': str(self.tmp / 'nope.duckdb'),
            'credentials': self.creds(),
        })
        self.assertIn('database: file not found', msg)
        self.assertIn('credentials: unknown key', msg)


class TestLoadConfig(SettingsTestCase):
    def test_parse_error_names_file(self) -> None:
        path = Path(self.write('bad.toml', 'host = 10.0.0.1\n'))
        with self.assertRaises(ConfigError) as ctx:
            load_config(path)
        self.assertIn(str(path), str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
