import tempfile
import unittest
from pathlib import Path

from query import aggspec
from query.definition import (
    Query,
    TemplateRaise,
    get_environment,
    get_params,
    get_sql,
)
from query.errors import KeywordError, QueryNotFound, TemplateError


FILES = {
    'plain.sql': 'select a\nfrom t\n',
    'undefined.sql': 'select\n  a\nfrom {{ x.y }}\n',
    'syntax.sql': 'select\n{% if %}\n',
    'filter.sql': "select\n{{ 'a' + 1 }}\n",
    'raises.sql': "select\n{{ raise('pass either a or b') }}\n",
    'includes_undefined.sql': "select\n{% include 'sub/inc_undefined.sql' %}\n",
    'includes_syntax.sql': "select\n{% include 'sub/inc_syntax.sql' %}\n",
    'includes_missing.sql': "select\n\n{% include 'sub/nope.sql' %}\n",
    'sub/inc_undefined.sql': 'a,\n{{ z.w }}\n',
    'sub/inc_syntax.sql': 'a,\nb,\n{% for %}\n',
    'matching/eindstatus.sql': 'select 1 from dual\n',
}


class DefinitionErrorTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        for name, text in FILES.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding='utf-8')
        self.env = get_environment(paths=self.root)

    def template_error(self, source: str, **kwargs) -> TemplateError:
        with self.assertRaises(TemplateError) as cm:
            get_sql(source, env=self.env, **kwargs)
        return cm.exception


class QueryNotFoundTest(DefinitionErrorTest):
    def test_raised_with_search_paths_and_suggestions(self) -> None:
        with self.assertRaises(QueryNotFound) as cm:
            Query.from_name('matching/eindstatus_niew', env=self.env)
        err = cm.exception
        self.assertEqual(err.query_name, 'matching/eindstatus_niew')
        self.assertEqual(err.searched[0], str(self.root))
        self.assertIn('matching/eindstatus', err.suggestions)
        self.assertIsNone(err.__cause__)
        self.assertIn('did you mean  matching/eindstatus', str(err))

    def test_from_get_sql_and_get_params(self) -> None:
        with self.assertRaises(QueryNotFound):
            get_sql('does/not/exist', env=self.env)
        with self.assertRaises(QueryNotFound):
            get_params('does/not/exist', env=self.env)

    def test_html(self) -> None:
        with self.assertRaises(QueryNotFound) as cm:
            Query.from_name('matching/eindstatus_niew', env=self.env)
        html = cm.exception._repr_html_()
        self.assertIn('QueryNotFound', html)
        self.assertIn('matching/eindstatus', html)


class TemplateErrorTest(DefinitionErrorTest):
    def test_render_error_in_query(self) -> None:
        err = self.template_error('undefined')
        self.assertEqual((err.query_name, err.template), ('undefined', 'undefined'))
        self.assertEqual(err.lineno, 3)
        self.assertEqual(err.source, FILES['undefined.sql'])
        self.assertEqual(err.path, str(self.root / 'undefined.sql'))
        self.assertIn("'x' is undefined", err.message)

    def test_syntax_error_in_query(self) -> None:
        err = self.template_error('syntax')
        self.assertEqual(err.template, 'syntax')
        self.assertEqual(err.lineno, 2)
        self.assertEqual(err.source, FILES['syntax.sql'])

    def test_python_error_from_template_code(self) -> None:
        err = self.template_error('filter')
        self.assertEqual(err.lineno, 2)
        self.assertTrue(err.message.startswith('TypeError: '))

    def test_render_error_in_include(self) -> None:
        err = self.template_error('includes_undefined')
        self.assertEqual(err.query_name, 'includes_undefined')
        self.assertEqual(err.template, 'sub/inc_undefined.sql')
        self.assertEqual(err.lineno, 2)
        self.assertEqual(err.source, FILES['sub/inc_undefined.sql'])
        self.assertEqual(err.path, str(self.root / 'sub' / 'inc_undefined.sql'))
        self.assertIn('in sub/inc_undefined.sql', str(err))

    def test_syntax_error_in_include(self) -> None:
        err = self.template_error('includes_syntax')
        self.assertEqual(err.template, 'sub/inc_syntax.sql')
        self.assertEqual(err.lineno, 3)
        self.assertEqual(err.source, FILES['sub/inc_syntax.sql'])

    def test_missing_include(self) -> None:
        err = self.template_error('includes_missing')
        self.assertEqual(err.template, 'includes_missing')
        self.assertEqual(err.lineno, 3)
        self.assertEqual(err.message, 'included template not found: sub/nope.sql')

    def test_raise_in_own_template_shows_message_only(self) -> None:
        err = self.template_error('raises')
        self.assertEqual(err.message, 'pass either a or b')
        self.assertIsNone(err.source)
        self.assertIsNone(err.lineno)
        self.assertIsInstance(err.__cause__, TemplateRaise)

    def test_inline(self) -> None:
        err = self.template_error('select\n{{ x.y }}')
        self.assertEqual((err.query_name, err.template), ('<inline>', '<inline>'))
        self.assertEqual(err.lineno, 2)
        self.assertIsNone(err.path)

    def test_chained(self) -> None:
        err = self.template_error('undefined')
        self.assertIsNotNone(err.__cause__)


class KeywordErrorTest(DefinitionErrorTest):
    def keyword_error(self, **kwargs) -> KeywordError:
        with self.assertRaises(KeywordError) as cm:
            get_sql('plain', env=self.env, **kwargs)
        return cm.exception

    def test_aggspec_validation(self) -> None:
        err = self.keyword_error(values=aggspec.count)
        self.assertEqual(err.query_name, 'plain')
        self.assertEqual(err.keyword, 'values')
        self.assertIsInstance(err.__cause__, aggspec.ValidationError)

    def test_raise_in_library_template(self) -> None:
        err = self.keyword_error(values='a')
        self.assertIsNone(err.keyword)
        self.assertTrue(err.message.startswith('AGG: '))
        self.assertIsInstance(err.__cause__, TemplateRaise)

    def test_wrong_type_for_where(self) -> None:
        err = self.keyword_error(where=5)
        self.assertTrue(err.message.startswith('CTE: '))

    def test_render_traceback_hint_without_connection(self) -> None:
        err = self.keyword_error(values='a')
        self.assertEqual(err._render_traceback_()[-1], '(HTML view: sys.last_value)')


class WrapSqlEnvTest(DefinitionErrorTest):
    def test_wrap_sql_uses_passed_env(self) -> None:
        (self.root / 'utils').mkdir()
        (self.root / 'utils' / 'wrapper.sql').write_text('-- own wrapper\n{{ main_statement }}', encoding='utf-8')
        sql = get_sql('plain', env=self.env, where='a > 1')
        self.assertTrue(sql.text.startswith('-- own wrapper'))


if __name__ == '__main__':
    unittest.main()
