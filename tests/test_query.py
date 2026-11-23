import tempfile
import unittest
from pathlib import Path
from unittest import mock

from query.definition import (
    AGG_DEFAULTS,
    DEFAULT_QUERY_NAME,
    SQL,
    Query,
    get_environment,
    get_params,
    get_sql,
    try_path,
)
from query.errors import QueryNotFound


FILES = {
    'plain.sql': 'select a\nfrom t\n',
    'params.sql': 'select a\nfrom t\nwhere b = {{ b }}\n{% if c %}and c = 1{% endif %}\n',
    'labels.sql': "select '{{ label_na }}' as na, '{{ label_totals }}' as tot from t\n",
    'select students.sql': 'select 1 from students\n',
    'matching/eindstatus.sql': 'select 1 from dual\n',
}


class QueryTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        for name, text in FILES.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding='utf-8')
        self.env = get_environment(paths=self.root)
        # A file outside the search paths, for from_path and absolute paths.
        other = tempfile.TemporaryDirectory()
        self.addCleanup(other.cleanup)
        self.outside = Path(other.name).resolve() / 'scratch.sql'
        self.outside.write_text('select 2 from dual\n', encoding='utf-8')


class ConstructorTest(QueryTest):
    def test_from_name(self) -> None:
        query = Query.from_name('plain', env=self.env)
        self.assertEqual(query, Query(FILES['plain.sql'], 'plain', str(self.root / 'plain.sql')))
        self.assertIs(query.env, self.env)

    def test_from_name_with_suffix(self) -> None:
        query = Query.from_name('plain.sql', env=self.env)
        self.assertEqual((query.name, query.path), ('plain.sql', str(self.root / 'plain.sql')))

    def test_from_name_not_found(self) -> None:
        with self.assertRaises(QueryNotFound) as cm:
            Query.from_name('matching/eindstatus_niew', env=self.env)
        self.assertIn('matching/eindstatus', cm.exception.suggestions)

    def test_from_path(self) -> None:
        query = Query.from_path(self.outside, env=self.env)
        self.assertEqual(query, Query('select 2 from dual\n', str(self.outside), str(self.outside)))

    def test_from_path_not_found(self) -> None:
        with self.assertRaises(QueryNotFound) as cm:
            Query.from_path(self.outside.with_name('nope.sql'), env=self.env)
        self.assertEqual((cm.exception.searched, cm.exception.suggestions), ([], []))

    def test_from_text(self) -> None:
        query = Query.from_text('select 1', env=self.env)
        self.assertEqual(query, Query('select 1', DEFAULT_QUERY_NAME, None))
        self.assertEqual(DEFAULT_QUERY_NAME, '<inline>')

    def test_env_not_compared_or_shown(self) -> None:
        other = get_environment(paths=self.root)
        self.assertEqual(Query.from_text('x', env=self.env), Query.from_text('x', env=other))
        self.assertNotIn('env', repr(Query.from_text('x', env=self.env)))


class RenderTest(QueryTest):
    def test_params(self) -> None:
        self.assertEqual(Query.from_name('params', env=self.env).params(), {'b', 'c'})

    def test_render(self) -> None:
        query = Query.from_name('params', env=self.env)
        sql = query.render(b=1)
        self.assertIsInstance(sql, SQL)
        self.assertEqual(sql.text, 'select a\nfrom t\nwhere b = 1\n')
        self.assertIs(sql.query, query)
        self.assertEqual(sql.params, {'b': 1})

    def test_aggregation_defaults_rendered_not_recorded(self) -> None:
        sql = Query.from_name('labels', env=self.env).render()
        self.assertIn(f"'{AGG_DEFAULTS['label_na']}' as na", sql.text)
        self.assertIn(f"'{AGG_DEFAULTS['label_totals']}' as tot", sql.text)
        self.assertEqual(sql.params, {})

    def test_passed_value_overrides_default(self) -> None:
        sql = Query.from_name('labels', env=self.env).render(label_na='-')
        self.assertIn("'-' as na", sql.text)
        self.assertEqual(sql.params, {'label_na': '-'})


class SQLDisplayTest(QueryTest):
    def test_str_and_repr(self) -> None:
        sql = Query.from_text('select 1', env=self.env).render()
        self.assertEqual((str(sql), repr(sql)), ('select 1', 'select 1'))

    def test_html(self) -> None:
        html = Query.from_text("select '<NA>'\nfrom t", env=self.env).render()._repr_html_()
        self.assertTrue(html.startswith('<div class="oq2 sql">\n<style>'))
        self.assertIn('<span class="tok-k">select</span>', html)
        self.assertIn('&lt;NA&gt;', html)
        self.assertNotIn('<NA>', html)
        self.assertEqual(html.count('class="line"'), 2)

    def test_not_a_str(self) -> None:
        self.assertNotIsInstance(Query.from_text('select 1', env=self.env).render(), str)


class GetSqlTest(QueryTest):
    def test_returns_sql(self) -> None:
        sql = get_sql('params', env=self.env, b=1)
        self.assertIsInstance(sql, SQL)
        self.assertEqual((sql.query.name, sql.params), ('params', {'b': 1}))

    def test_sql_passed_through(self) -> None:
        sql = get_sql('params', env=self.env, b=1)
        self.assertIs(get_sql(sql), sql)

    def test_sql_with_parameters_raises(self) -> None:
        sql = get_sql('params', env=self.env, b=1)
        with self.assertRaises(TypeError) as cm:
            get_sql(sql, b=2)
        self.assertIn("['b']", str(cm.exception))

    def test_query_not_read_again(self) -> None:
        query = Query.from_name('params', env=self.env)
        with mock.patch.object(self.env.loader, 'get_source', side_effect=AssertionError('read again')):
            sql = get_sql(query, b=1)
        self.assertIn('b = 1', sql.text)

    def test_wrappers(self) -> None:
        self.assertEqual(get_params('params', env=self.env), {'b', 'c'})
        self.assertEqual(try_path('plain', env=self.env), FILES['plain.sql'])


class DispatchTest(QueryTest):
    def query(self, source) -> Query:
        return get_sql(source, env=self.env).query

    def assert_inline(self, source: str) -> None:
        query = self.query(source)
        self.assertEqual((query.name, query.path, query.text), (DEFAULT_QUERY_NAME, None, source))

    def test_newline_is_sql(self) -> None:
        self.assert_inline('plain\n')                          # even if it is a name

    def test_name_found(self) -> None:
        self.assertEqual(self.query('matching/eindstatus').path, str(self.root / 'matching/eindstatus.sql'))
        self.assertEqual(self.query(Path('plain')).path, str(self.root / 'plain.sql'))

    def test_name_with_spaces_starting_with_select(self) -> None:
        self.assertEqual(self.query('select students').path, str(self.root / 'select students.sql'))

    def test_absolute_path(self) -> None:
        self.assertEqual(self.query(str(self.outside)).path, str(self.outside))
        self.assertEqual(self.query(self.outside).path, str(self.outside))

    def test_one_line_sql(self) -> None:
        for sql in [
            'select 1 from dual',
            'SELECT 1 FROM dual',
            'with x as (select 1 from dual) select * from x',
            '  select 1 from dual',
            'select\t1 from dual',
            'select* from dual',
            '(select 1 from dual) union (select 2 from dual)',
            '( select 1 from dual)',
        ]:
            with self.subTest(sql=sql):
                self.assert_inline(sql)

    def test_not_found(self) -> None:
        for source in ['matching/eindstatus_niew', 'delete from t', Path('select 1 from dual')]:
            with self.subTest(source=source), self.assertRaises(QueryNotFound):
                self.query(source)

    def test_misspelled_name_starting_with_select_is_sql(self) -> None:
        self.assert_inline('select studnets')                  # known limitation

    def test_query_passed_through(self) -> None:
        query = Query.from_name('plain', env=self.env)
        self.assertIs(self.query(query), query)

    def test_other_type(self) -> None:
        with self.assertRaises(TypeError):
            get_sql(object())


if __name__ == '__main__':
    unittest.main()
