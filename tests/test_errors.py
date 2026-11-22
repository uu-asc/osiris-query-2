import unittest
from datetime import datetime

from query.errors import (
    ConfigError,
    ConnectError,
    OsirisQueryError,
    QueryError,
    TemplateError,
    _caret,
    _context,
    _short,
    caret_offset,
    last_error,
    remember,
)

AT = datetime(2026, 11, 22, 14, 3, 41)


def query_error(sql: str, code: str | None = 'ORA-00904', raw_offset: int | None = None, **kwargs) -> QueryError:
    return QueryError(
        query_name='q', connection='osiris', sql=sql, code=code,
        db_message=f'{code}: boom', raw_offset=raw_offset, raised_at=AT, **kwargs,
    )


class CaretOffsetTest(unittest.TestCase):
    def test_ascii(self) -> None:
        self.assertEqual(caret_offset('ORA-00904', 7, 'select x from t'), 7)

    def test_bytes_to_characters(self) -> None:
        sql = "select 'éé' as x, y from t"
        raw = sql.encode('utf-8').index(b'y')
        self.assertEqual(raw, sql.index('y') + 2)
        self.assertEqual(caret_offset('ORA-00904', raw, sql), sql.index('y'))

    def test_offset_inside_multibyte_character(self) -> None:
        sql = "select 'é'"
        raw = sql.encode('utf-8').index('é'.encode('utf-8')) + 1
        self.assertEqual(caret_offset('ORA-00936', raw, sql), sql.index('é'))

    def test_allowlist(self) -> None:
        for code in ('ORA-00900', 'ORA-00936', 'ORA-00942', 'ORA-00999', 'ORA-01747', 'ORA-01789'):
            with self.subTest(code=code):
                self.assertEqual(caret_offset(code, 3, 'select 1'), 3)

    def test_not_on_allowlist(self) -> None:
        for code in ('ORA-01476', 'ORA-01013', 'ORA-01722', 'ORA-00600', 'ORA-01000'):
            with self.subTest(code=code):
                self.assertIsNone(caret_offset(code, 3, 'select 1'))

    def test_unknown_offset_or_code(self) -> None:
        self.assertIsNone(caret_offset('ORA-00904', 0, 'select 1'))
        self.assertIsNone(caret_offset('ORA-00904', None, 'select 1'))
        self.assertIsNone(caret_offset(None, 3, 'select 1'))
        self.assertIsNone(caret_offset('DPY-4010', 3, 'select 1'))
        self.assertIsNone(caret_offset('ORA-x', 3, 'select 1'))


class PositionTest(unittest.TestCase):
    def test_first_line(self) -> None:
        self.assertEqual(query_error('select x from t', raw_offset=7).position, (1, 8))

    def test_later_line(self) -> None:
        sql = 'select x\nfrom t\nwhere y = 1'
        self.assertEqual(query_error(sql, raw_offset=sql.index('y')).position, (3, 7))

    def test_start_of_line(self) -> None:
        sql = 'select x\nfrom t'
        self.assertEqual(query_error(sql, raw_offset=sql.index('from')).position, (2, 1))

    def test_last_line_without_trailing_newline(self) -> None:
        sql = 'select x\nfrom t'
        self.assertEqual(query_error(sql, raw_offset=len(sql) - 1).position, (2, 6))

    def test_no_position_for_runtime_error(self) -> None:
        err = query_error('select 1/0 from dual', code='ORA-01476', raw_offset=8)
        self.assertIsNone(err.offset)
        self.assertIsNone(err.position)
        self.assertEqual(err.raw_offset, 8)


class HelperTest(unittest.TestCase):
    def test_context_middle(self) -> None:
        text = '\n'.join(f'l{n}' for n in range(1, 21))
        rows = _context(text, 10)
        self.assertEqual([n for n, _, _ in rows], list(range(5, 13)))
        self.assertEqual([n for n, _, err in rows if err], [10])

    def test_context_start_and_end(self) -> None:
        text = 'a\nb\nc'
        self.assertEqual([n for n, _, _ in _context(text, 1)], [1, 2, 3])
        self.assertEqual([n for n, _, _ in _context(text, 3)], [1, 2, 3])

    def test_context_crlf(self) -> None:
        self.assertEqual(_context('a\r\nb', 2), [(1, 'a', False), (2, 'b', True)])

    def test_caret(self) -> None:
        self.assertEqual(_caret('where x = 1', 7), '      ^')

    def test_caret_with_tab(self) -> None:
        line = '\twhere x'
        caret = _caret(line, line.index('x') + 1)
        self.assertEqual(caret.index('^'), line.expandtabs(4).index('x'))

    def test_short(self) -> None:
        self.assertEqual(_short(2024), '2024')
        self.assertLessEqual(len(_short(list(range(5000)))), 80)
        self.assertLessEqual(len(_short('x' * 500)), 80)


class QueryErrorTextTest(unittest.TestCase):
    SQL = 'select *\nfrom t\nwhere colegejaar = 2024\n  and x = 1'

    def test_with_position(self) -> None:
        err = query_error(
            self.SQL, raw_offset=self.SQL.index('colegejaar'),
            passed={'colegejaar': 2024}, not_passed=['collegejaar'], unused=['colegejaar'],
        )
        text = str(err)
        self.assertIn('QueryError [osiris] q  (2026-11-22 14:03:41)', text)
        self.assertIn('ORA-00904: boom', text)
        self.assertIn('passed      colegejaar=2024', text)
        self.assertIn('not passed  collegejaar', text)
        self.assertIn('unused      colegejaar', text)
        self.assertIn('line 3, column 7', text)
        lines = text.splitlines()
        error_line = lines.index('> 3 | where colegejaar = 2024')
        self.assertEqual(lines[error_line + 1], '    |       ^')
        self.assertIn('full SQL (4 lines)', text)

    def test_without_position(self) -> None:
        text = str(query_error(self.SQL, code='ORA-01476', raw_offset=5))
        self.assertNotIn('column', text)
        self.assertNotIn('^', text)
        self.assertIn('full SQL (4 lines)', text)
        self.assertNotIn('>', text.split('full SQL')[1])

    def test_no_template(self) -> None:
        text = str(query_error(self.SQL, passed={}, not_passed=None, unused=None))
        self.assertIn('passed      (none)', text)
        self.assertNotIn('not passed', text)
        self.assertNotIn('unused', text)

    def test_repr_is_short(self) -> None:
        self.assertEqual(repr(query_error(self.SQL)), "QueryError('ORA-00904', query='q')")

    def test_args(self) -> None:
        self.assertEqual(query_error(self.SQL).args, ('ORA-00904: boom',))


class OtherErrorsTest(unittest.TestCase):
    def test_connect_error(self) -> None:
        err = ConnectError('osiris', 'ORA-01017', 'ORA-01017: invalid username/password', raised_at=AT)
        self.assertEqual(
            str(err),
            'ConnectError [osiris]  (2026-11-22 14:03:41)\nORA-01017: invalid username/password',
        )
        self.assertNotIn('SQL', str(err))

    def test_template_error_in_query(self) -> None:
        err = TemplateError('q', 'q', "unexpected '}'", source='select\n{{ x }\nfrom t', lineno=2, raised_at=AT)
        text = str(err)
        self.assertNotIn('\nin ', text)
        self.assertIn('> 2 | {{ x }', text)

    def test_template_error_in_included_file(self) -> None:
        err = TemplateError('q', 'filters/jaar.sql', 'boom', source='a\nb', lineno=1, raised_at=AT)
        self.assertIn('\nin filters/jaar.sql\n', str(err))

    def test_template_error_message_only(self) -> None:
        err = TemplateError('q', 'utils/agg.sql', 'AGG: Cannot use both', raised_at=AT)
        self.assertEqual(
            str(err),
            'TemplateError q  (2026-11-22 14:03:41)\nin utils/agg.sql\nAGG: Cannot use both',
        )


class HierarchyTest(unittest.TestCase):
    def test_base(self) -> None:
        for cls in (ConfigError, QueryError, ConnectError, TemplateError):
            self.assertTrue(issubclass(cls, OsirisQueryError))

    def test_config_error_importable_from_config(self) -> None:
        from query.config import ConfigError as FromConfig
        self.assertIs(FromConfig, ConfigError)


class LastErrorTest(unittest.TestCase):
    def test_per_connection(self) -> None:
        a = ConnectError('a', None, 'x')
        b = ConnectError('b', None, 'y')
        remember('test-a', a)
        remember('test-b', b)
        self.assertIs(last_error('test-a'), a)
        self.assertIs(last_error('test-b'), b)
        self.assertIsNone(last_error('test-none'))

    def test_replaced_by_newer(self) -> None:
        old, new = ConnectError('c', None, 'old'), ConnectError('c', None, 'new')
        remember('test-c', old)
        remember('test-c', new)
        self.assertIs(last_error('test-c'), new)


if __name__ == '__main__':
    unittest.main()
