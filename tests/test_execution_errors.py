import inspect
import sqlite3
import tempfile
import unittest
import warnings
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
from jinja2 import meta

from query import definition, errors, execution
from query.connections.connection import module_getattr
from query.definition import (
    AGG_KEYWORDS,
    ENV,
    EXECUTION_KEYWORDS,
    UTIL_KEYWORDS,
    get_environment,
    get_sql,
)
from query.errors import ConnectError, QueryError, QueryNotFound, TemplateError
from query.execution import DriverError
from tests.fake_sqlite import FakeSettings, rows

SETTINGS = SimpleNamespace(name='testdb', engine=lambda: object())
# Table t doesn't exist: on the sqlite fake this query fails with a QueryError.
QUERY = 'select\n  collegejaar\nfrom t\nwhere collegejaar = {{ collegejaar }}\n{% if opleiding %}and opleiding = 1{% endif %}\n'


class DriverException(Exception):
    """Like oracledb's exceptions: an error object in args[0]."""
    def __init__(self, full_code: str, message: str, offset: int = 0) -> None:
        super().__init__(SimpleNamespace(full_code=full_code, message=message, offset=offset))


class ExecutionTest(unittest.TestCase):
    """execute_query over the sqlite fake (tests/fake_sqlite.py)."""
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        (self.root / 'q.sql').write_text(QUERY, encoding='utf-8')
        (self.root / 'broken.sql').write_text('select {{ x.y }}', encoding='utf-8')
        self.env = get_environment(paths=self.root)
        self.settings = FakeSettings()
        errors._last_errors.clear()
        self.addCleanup(errors._last_errors.clear)
        patcher = mock.patch.object(errors, 'report')
        self.report = patcher.start()
        self.addCleanup(patcher.stop)

    def run_query(self, query='q', settings=None, **kwargs):
        return execution.execute_query(
            query, settings=settings or self.settings, env=self.env, **kwargs
        )


class QueryErrorTest(ExecutionTest):
    def test_query_error_is_reported_and_stored(self) -> None:
        result = self.run_query(collegejaar=2024, colegejaar=2024)
        self.assertIsNone(result)
        err = errors.last_error('fake')
        self.assertIsInstance(err, QueryError)
        self.report.assert_called_once_with(err)
        self.assertEqual(err.query_name, 'q')
        self.assertEqual(err.connection, 'fake')
        self.assertEqual(err.sql, get_sql('q', env=self.env, collegejaar=2024).text)
        self.assertIsNone(err.code)                             # sqlite: no error object
        self.assertIn('no such table', err.db_message)
        self.assertIsNone(err.offset)
        self.assertEqual(err.passed, {'collegejaar': 2024, 'colegejaar': 2024})
        self.assertEqual(err.not_passed, ['opleiding'])
        self.assertEqual(err.unused, ['colegejaar'])
        self.assertEqual(err.path, str(self.root / 'q.sql'))
        self.assertIsInstance(err.__cause__, sqlite3.OperationalError)

    def test_exact_parse_for_caret(self) -> None:
        driver = DriverError('ORA-00904', 'ORA-00904: "COLEGEJAAR": invalid identifier', 7)
        with mock.patch.object(execution, '_driver_error', return_value=driver), \
             mock.patch.object(execution, '_exact_offset', return_value=7) as exact:
            self.run_query(collegejaar=2024)
        err = errors.last_error('fake')
        exact.assert_called_once_with(self.settings, err.sql, 'ORA-00904')
        self.assertEqual((err.code, err.raw_offset, err.offset), ('ORA-00904', 7, 7))

    def test_no_exact_parse_without_caret(self) -> None:
        driver = DriverError('ORA-01476', 'ORA-01476: divisor is equal to zero', 9)
        with mock.patch.object(execution, '_driver_error', return_value=driver), \
             mock.patch.object(execution, '_exact_offset') as exact:
            self.run_query(collegejaar=2024)
        exact.assert_not_called()
        self.assertEqual(errors.last_error('fake').raw_offset, 9)

    def test_library_keywords_are_not_unused(self) -> None:
        self.run_query(collegejaar=2024, where='a = 1', label_na='-')
        self.assertEqual(errors.last_error('fake').unused, [])

    def test_raise_errors(self) -> None:
        with self.assertRaises(QueryError) as cm:
            self.run_query(collegejaar=2024, raise_errors=True)
        self.assertIsInstance(cm.exception.__cause__, sqlite3.OperationalError)
        self.assertIs(errors.last_error('fake'), cm.exception)
        self.report.assert_not_called()

    def test_sql_passed_in_keeps_name_and_parameters(self) -> None:
        sql = get_sql('q', env=self.env, collegejaar=2024, colegejaar=2024)
        self.run_query(sql)
        err = errors.last_error('fake')
        self.assertEqual(err.query_name, 'q')
        self.assertEqual(err.path, str(self.root / 'q.sql'))
        self.assertEqual(err.passed, {'collegejaar': 2024, 'colegejaar': 2024})
        self.assertEqual(err.not_passed, ['opleiding'])
        self.assertEqual(err.unused, ['colegejaar'])

    def test_inline_sql_has_parameter_report(self) -> None:
        self.run_query('select {{ x }} from no_such_table')
        err = errors.last_error('fake')
        self.assertEqual(err.query_name, '<inline>')
        self.assertEqual(err.not_passed, ['x'])
        self.assertEqual(err.unused, [])
        self.assertIsNone(err.path)

    def test_get_sql_options_not_in_passed(self) -> None:
        with mock.patch('builtins.print'):
            self.run_query(collegejaar=2024, print_sql=True)
        self.assertEqual(errors.last_error('fake').passed, {'collegejaar': 2024})

    def test_sql_with_parameters_raises(self) -> None:
        sql = get_sql('q', env=self.env, collegejaar=2024)
        with self.assertRaises(TypeError):
            self.run_query(sql, collegejaar=2025)


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


class ConnectErrorTest(ExecutionTest):
    def test_connect_error(self) -> None:
        self.assertIsNone(self.run_query(settings=FakeSettings(connect_error=True), collegejaar=2024))
        err = errors.last_error('fake')
        self.assertIsInstance(err, ConnectError)
        self.assertEqual(err.connection, 'fake')
        self.assertIn('unable to open', err.db_message)
        self.report.assert_called_once_with(err)
        self.assertEqual(err._render_traceback_()[-1], '(HTML view: fake.last_error)')

    def test_connect_error_raised(self) -> None:
        with self.assertRaises(ConnectError):
            self.run_query(settings=FakeSettings(connect_error=True), collegejaar=2024, raise_errors=True)


class OtherErrorsTest(ExecutionTest):
    def test_no_result_set_raised_not_stored(self) -> None:
        with self.assertRaisesRegex(TypeError, 'no result set'):
            self.run_query('create table x (a int)\n')
        self.assertIsNone(errors.last_error('fake'))
        self.report.assert_not_called()

    def test_template_error_raised_and_stored(self) -> None:
        with self.assertRaises(TemplateError) as cm:
            self.run_query('broken')
        self.assertIs(errors.last_error('fake'), cm.exception)
        self.report.assert_not_called()

    def test_query_not_found_raised_and_stored(self) -> None:
        with self.assertRaises(QueryNotFound) as cm:
            self.run_query('nope')
        self.assertIs(errors.last_error('fake'), cm.exception)


class ResultTest(ExecutionTest):
    def test_frame(self) -> None:
        df = self.run_query(rows(3))
        self.assertEqual(list(df.columns), ['n', 's'])
        self.assertEqual(df['n'].tolist(), [1, 2, 3])

    def test_squeeze(self) -> None:
        self.assertIsInstance(self.run_query('select n from (' + rows(3) + ')\n'), pd.Series)
        row = self.run_query(rows(1))
        self.assertIsInstance(row, pd.Series)
        self.assertEqual(row.name, '<inline>')
        self.assertIsInstance(self.run_query(rows(1), squeeze=False), pd.DataFrame)

    def test_max_rows(self) -> None:
        df = self.run_query(rows(10), max_rows=3)
        self.assertEqual(df['n'].tolist(), [1, 2, 3])

    def test_max_rows_zero(self) -> None:
        df = self.run_query(rows(10), max_rows=0)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual((len(df), list(df.columns)), (0, ['n', 's']))

    def test_max_rows_is_not_a_template_parameter(self) -> None:
        self.run_query('select {{ x }} from no_such_table', max_rows=3)
        self.assertEqual(errors.last_error('fake').passed, {})


class IndexColTest(ExecutionTest):
    QUERY = 'select 1 as a, 2 as b\n'

    def test_from_columns_keyword(self) -> None:
        df = self.run_query(self.QUERY, columns=['a'], squeeze=False)
        self.assertEqual(df.index.names, ['a'])

    def test_from_sql_passed_in(self) -> None:
        sql = get_sql(self.QUERY, env=self.env, columns=['a'])
        self.assertEqual(self.run_query(sql, squeeze=False).index.names, ['a'])

    def test_false_means_none(self) -> None:
        df = self.run_query(self.QUERY, columns=['a'], index_col=False, squeeze=False)
        self.assertEqual(list(df.columns), ['a', 'b'])


class ClashWarningTest(ExecutionTest):
    def test_warns_for_execution_keyword_in_template(self) -> None:
        query = '{% if dtype %}-- {{ dtype }}{% endif %}\nselect 1 as x'
        with self.assertWarnsRegex(UserWarning, r"<inline>: template uses \['dtype'\]") as cm:
            self.run_query(query)
        self.assertEqual(cm.filename, __file__)              # the caller, not the library

    def test_no_warning_without_clash(self) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            self.run_query(rows(2))
        self.assertEqual(caught, [])


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

    def test_execution_keywords_match_signatures(self) -> None:
        def keyword_only(f) -> set[str]:
            return {
                p.name for p in inspect.signature(f).parameters.values()
                if p.kind is p.KEYWORD_ONLY
            }
        expected = keyword_only(execution.execute_query) | keyword_only(definition.get_sql)
        self.assertEqual(set(EXECUTION_KEYWORDS), expected)


if __name__ == '__main__':
    unittest.main()
