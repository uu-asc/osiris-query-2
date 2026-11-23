import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from jinja2 import meta
from pandas.errors import DatabaseError as PandasDatabaseError
from sqlalchemy import text
from sqlalchemy.exc import DatabaseError, InterfaceError, OperationalError, ResourceClosedError

from query import errors, execution
from query.connections.connection import module_getattr
from query.definition import (
    AGG_KEYWORDS,
    ENV,
    UTIL_KEYWORDS,
    QuerySource,
    get_environment,
    get_sql,
    load_query,
)
from query.errors import ConnectError, QueryError, QueryNotFound, TemplateError

SETTINGS = SimpleNamespace(name='testdb', engine=lambda: object())
QUERY = 'select\n  collegejaar\nfrom t\nwhere collegejaar = {{ collegejaar }}\n{% if opleiding %}and opleiding = 1{% endif %}\n'


class DriverException(Exception):
    """Like oracledb's exceptions: an error object in args[0]."""
    def __init__(self, full_code: str, message: str, offset: int = 0) -> None:
        super().__init__(SimpleNamespace(full_code=full_code, message=message, offset=offset))


def sql_error(statement: str) -> PandasDatabaseError:
    """An SQL error as read_sql_query raises it: pandas wraps SQLAlchemy wraps the driver."""
    orig = DriverException('ORA-00904', 'ORA-00904: "COLEGEJAAR": invalid identifier', offset=7)
    sa = DatabaseError(statement, {}, orig)
    wrapper = PandasDatabaseError(f"Execution failed on sql: {sa}")
    wrapper.__cause__ = sa
    return wrapper


def connect_error() -> OperationalError:
    """A connect error: not wrapped by pandas, statement None."""
    orig = DriverException('DPY-6005', 'DPY-6005: cannot connect to database', offset=0)
    return OperationalError(None, None, orig)


class ExecutionErrorTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        (self.root / 'q.sql').write_text(QUERY, encoding='utf-8')
        (self.root / 'broken.sql').write_text('select {{ x.y }}', encoding='utf-8')
        self.env = get_environment(paths=self.root)
        errors._last_errors.clear()
        self.addCleanup(errors._last_errors.clear)
        patcher = mock.patch.object(errors, 'report')
        self.report = patcher.start()
        self.addCleanup(patcher.stop)

    def run_failing(self, exc: BaseException, query='q', **kwargs):
        def fail(sql, *args, **kw):
            raise exc
        with mock.patch.object(execution.pd, 'read_sql_query', fail):
            return execution.execute_query(query, settings=SETTINGS, env=self.env, **kwargs)


class QueryErrorTest(ExecutionErrorTest):
    def test_sql_error_is_reported_and_stored(self) -> None:
        statement = 'select\n  collegejaar\nfrom t'
        with mock.patch.object(execution, '_exact_offset', return_value=7) as exact:
            result = self.run_failing(sql_error(statement), collegejaar=2024, colegejaar=2024)
        exact.assert_called_once_with(SETTINGS, statement, 'ORA-00904')
        self.assertIsNone(result)
        err = errors.last_error('testdb')
        self.assertIsInstance(err, QueryError)
        self.report.assert_called_once_with(err)
        self.assertEqual(err.query_name, 'q')
        self.assertEqual(err.connection, 'testdb')
        self.assertEqual(err.sql, statement)                    # e.statement, not the rendered TextClause
        self.assertEqual(err.code, 'ORA-00904')
        self.assertEqual(err.db_message, 'ORA-00904: "COLEGEJAAR": invalid identifier')
        self.assertEqual((err.raw_offset, err.offset), (7, 7))
        self.assertEqual(err.passed, {'collegejaar': 2024, 'colegejaar': 2024})
        self.assertEqual(err.not_passed, ['opleiding'])
        self.assertEqual(err.unused, ['colegejaar'])
        self.assertEqual(err.path, str(self.root / 'q.sql'))

    def test_no_exact_parse_without_caret(self) -> None:
        orig = DriverException('ORA-01476', 'ORA-01476: divisor is equal to zero', offset=9)
        with mock.patch.object(execution, '_exact_offset') as exact:
            self.run_failing(DatabaseError('select 1/0 from dual', {}, orig), collegejaar=2024)
        exact.assert_not_called()
        self.assertEqual(errors.last_error('testdb').raw_offset, 9)

    def test_library_keywords_are_not_unused(self) -> None:
        self.run_failing(sql_error('select 1'), collegejaar=2024, where='a = 1', label_na='-')
        self.assertEqual(errors.last_error('testdb').unused, [])

    def test_raise_errors(self) -> None:
        exc = sql_error('select 1')
        with self.assertRaises(QueryError) as cm:
            self.run_failing(exc, collegejaar=2024, raise_errors=True)
        self.assertIs(cm.exception.__cause__, exc)
        self.assertIs(errors.last_error('testdb'), cm.exception)
        self.report.assert_not_called()

    def test_interface_error_with_statement_is_query_error(self) -> None:
        orig = DriverException('DPY-1001', 'DPY-1001: not connected to database')
        self.run_failing(InterfaceError('select 1', {}, orig), collegejaar=2024)
        err = errors.last_error('testdb')
        self.assertIsInstance(err, QueryError)
        self.assertEqual(err.code, 'DPY-1001')
        self.assertIsNone(err.offset)

    def test_driver_without_error_object(self) -> None:
        orig = Exception('Binder Error: column "x" not found')      # e.g. duckdb
        self.run_failing(DatabaseError('select x', {}, orig), collegejaar=2024)
        err = errors.last_error('testdb')
        self.assertIsNone(err.code)
        self.assertEqual(err.db_message, 'Binder Error: column "x" not found')

    def test_text_clause_has_no_parameter_report(self) -> None:
        self.run_failing(sql_error('select 1'), query=text('select {{ not_a_template }}'))
        err = errors.last_error('testdb')
        self.assertEqual(err.query_name, '<inline>')
        self.assertIsNone(err.not_passed)
        self.assertIsNone(err.unused)
        self.assertIsNone(err.path)


class ExactOffsetTest(unittest.TestCase):
    """execution._exact_offset with a fake engine: no database needed."""
    def settings(self, dialect='oracle', parse_error=None, connect_error=None):
        statements = []
        def execute(sql):
            statements.append(sql)
        def parse(sql):
            statements.append(('parse', sql))
            if parse_error is not None:
                raise parse_error
        def raw_connection():
            if connect_error is not None:
                raise connect_error
            cursor = SimpleNamespace(execute=execute, parse=parse)
            return SimpleNamespace(cursor=lambda: cursor, close=lambda: statements.append('close'))
        engine = SimpleNamespace(dialect=SimpleNamespace(name=dialect), raw_connection=raw_connection)
        return SimpleNamespace(name='testdb', engine=lambda: engine), statements

    def test_offset_from_exact_parse(self) -> None:
        settings, statements = self.settings(parse_error=DriverException('ORA-00942', 'ORA-00942', offset=14))
        self.assertEqual(execution._exact_offset(settings, 'select 1 from x', 'ORA-00942'), 14)
        self.assertEqual(statements, [
            'alter session set cursor_sharing = exact',
            ('parse', 'select 1 from x'),
            'close',
        ])

    def test_different_code(self) -> None:
        settings, _ = self.settings(parse_error=DriverException('ORA-00904', 'ORA-00904', offset=7))
        self.assertIsNone(execution._exact_offset(settings, 'select 1', 'ORA-00942'))

    def test_parse_succeeds(self) -> None:
        settings, statements = self.settings()
        self.assertIsNone(execution._exact_offset(settings, 'select 1', 'ORA-00942'))
        self.assertEqual(statements[-1], 'close')

    def test_not_oracle(self) -> None:
        settings, statements = self.settings(dialect='duckdb')
        self.assertIsNone(execution._exact_offset(settings, 'select 1', 'ORA-00942'))
        self.assertEqual(statements, [])

    def test_connect_fails(self) -> None:
        settings, _ = self.settings(connect_error=OSError('down'))
        self.assertIsNone(execution._exact_offset(settings, 'select 1', 'ORA-00942'))


class ConnectErrorTest(ExecutionErrorTest):
    def test_connect_error(self) -> None:
        self.assertIsNone(self.run_failing(connect_error(), collegejaar=2024))
        err = errors.last_error('testdb')
        self.assertIsInstance(err, ConnectError)
        self.assertEqual((err.connection, err.code), ('testdb', 'DPY-6005'))
        self.assertEqual(err._render_traceback_()[-1], '(HTML view: testdb.last_error)')

    def test_connect_error_raised(self) -> None:
        with self.assertRaises(ConnectError):
            self.run_failing(connect_error(), collegejaar=2024, raise_errors=True)


class OtherErrorsTest(ExecutionErrorTest):
    def test_no_statement_error_in_chain_is_reraised(self) -> None:
        wrapper = PandasDatabaseError('not from the driver')
        wrapper.__cause__ = ResourceClosedError('closed')
        with self.assertRaises(PandasDatabaseError) as cm:
            self.run_failing(wrapper, collegejaar=2024, raise_errors=True)
        self.assertIs(cm.exception, wrapper)
        self.assertIsNone(errors.last_error('testdb'))
        self.report.assert_not_called()

    def test_template_error_raised_and_stored(self) -> None:
        with self.assertRaises(TemplateError) as cm:
            execution.execute_query('broken', settings=SETTINGS, env=self.env)
        self.assertIs(errors.last_error('testdb'), cm.exception)
        self.report.assert_not_called()

    def test_query_not_found_raised_and_stored(self) -> None:
        with self.assertRaises(QueryNotFound) as cm:
            execution.execute_query('nope', settings=SETTINGS, env=self.env)
        self.assertIs(errors.last_error('testdb'), cm.exception)


class QuerySourcePassThroughTest(ExecutionErrorTest):
    def test_load_query_returns_query_source_unchanged(self) -> None:
        source = load_query('q', env=self.env)
        self.assertIs(load_query(source, env=self.env), source)

    def test_get_sql_does_not_read_again(self) -> None:
        source = load_query('q', env=self.env)
        with mock.patch.object(self.env.loader, 'get_source', side_effect=AssertionError('read again')):
            sql = get_sql(source, env=self.env, collegejaar=2024)
        self.assertIn('collegejaar = 2024', sql.text)


class ModuleGetattrTest(unittest.TestCase):
    def setUp(self) -> None:
        errors._last_errors.clear()
        self.addCleanup(errors._last_errors.clear)
        self.getattr = module_getattr('query.connections.testdb', SETTINGS)

    def test_last_error_follows_storage(self) -> None:
        self.assertIsNone(self.getattr('last_error'))
        err = ConnectError('testdb', None, 'down')
        errors.remember('testdb', err)
        self.assertIs(self.getattr('last_error'), err)

    def test_other_names(self) -> None:
        with self.assertRaisesRegex(AttributeError, "'query.connections.testdb' has no attribute 'nope'"):
            self.getattr('nope')


class KeywordListsTest(unittest.TestCase):
    INTERNAL = {'body', 'has_cte', 'main_statement', 'handle_where'}

    def test_library_templates_use_only_listed_keywords(self) -> None:
        keywords = set(UTIL_KEYWORDS) | set(AGG_KEYWORDS)
        for name in ['utils/wrapper.sql', 'utils/cte.sql', 'utils/agg.sql']:
            source, *_ = ENV.loader.get_source(ENV, name)
            variables = meta.find_undeclared_variables(ENV.parse(source)) - self.INTERNAL
            with self.subTest(template=name):
                self.assertLessEqual(variables, keywords)


if __name__ == '__main__':
    unittest.main()
