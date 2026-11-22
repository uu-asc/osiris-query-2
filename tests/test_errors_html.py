import io
import re
import sys
import unittest
from contextlib import redirect_stderr
from datetime import datetime
from unittest import mock

from query import errors
from query.errors import ConnectError, QueryError, TemplateError, report

AT = datetime(2026, 11, 22, 14, 3, 41)
SQL = "select '<NA>' as a\nfrom t\nwhere colegejaar <> 2024\n  and\tx = 1"


def query_error(sql: str = SQL, code: str = 'ORA-00904', offset_at: str | None = 'colegejaar', **kwargs) -> QueryError:
    raw = None if offset_at is None else len(sql[:sql.index(offset_at)].encode('utf-8'))
    return QueryError(
        query_name='q', connection='osiris', sql=sql, code=code,
        db_message=f'{code}: boom', raw_offset=raw, raised_at=AT, **kwargs,
    )


def marked(html: str) -> list[str]:
    """Text inside the underline spans (tags inside removed)."""
    return [re.sub(r'<[^>]+>', '', m) for m in re.findall(r'<span class="mark">(.*?)</span>', html)]


def body(html: str) -> str:
    """The output without the embedded stylesheet."""
    return html.split('</style>', 1)[1]


def context(html: str) -> str:
    """The context block (between the position line and the full SQL)."""
    return html.split('class="position"', 1)[1].split('<details>', 1)[0]


def error_lines(html: str) -> list[str]:
    return re.findall(r'<span class="line errline"><span class="lineno">\s*(\d+)', html)


class QueryErrorHtmlTest(unittest.TestCase):
    def test_escaping(self) -> None:
        html = body(query_error()._repr_html_())
        self.assertNotIn('<NA>', html)
        self.assertIn('&lt;NA&gt;', html)
        self.assertIn('&lt;&gt;', re.sub(r'<[^>]+>', '', html))   # from <>

    def test_mark_and_error_line(self) -> None:
        html = query_error()._repr_html_()
        self.assertEqual(marked(html), ['colegejaar', 'colegejaar'])   # context and full SQL
        self.assertEqual(error_lines(html), ['3', '3'])
        self.assertIn('line 3, column 7', html)

    def test_mark_after_tab(self) -> None:
        html = query_error(offset_at='x =')._repr_html_()
        self.assertEqual(marked(html)[0], 'x')
        self.assertEqual(error_lines(html)[0], '4')

    def test_mark_on_whitespace_extends_to_next_token(self) -> None:
        sql = 'select a,  b from t'
        html = query_error(sql, offset_at='  b')._repr_html_()
        self.assertEqual(''.join(marked(context(html))), '  b')

    def test_mark_past_end_of_line(self) -> None:
        sql = 'select a,\nfrom t'
        html = query_error(sql, offset_at='\nfrom')._repr_html_()
        self.assertEqual(marked(html)[0], ' ')
        self.assertEqual(error_lines(html)[0], '1')

    def test_no_position(self) -> None:
        html = body(query_error(code='ORA-01476')._repr_html_())
        self.assertNotIn('errline', html)
        self.assertNotIn('class="mark"', html)
        self.assertNotIn('column', html)
        self.assertIn('full SQL (4 lines)', html)

    def test_every_line_rendered(self) -> None:
        sql = "select 1 /* multi\nline comment */ from dual\n\n-- last"
        html = query_error(sql, code='ORA-01476', offset_at=None)._repr_html_()
        full = html.split('<summary>', 1)[1]
        self.assertEqual(full.count('<span class="lineno">'), 4)

    def test_link_and_escaped_message(self) -> None:
        err = query_error()
        err.db_message = 'ORA-00942: <b>x</b>\nHelp: https://docs.oracle.com/error-help/db/ora-00942/.'
        html = err._repr_html_()
        self.assertIn('&lt;b&gt;x&lt;/b&gt;', html)
        self.assertIn('<a href="https://docs.oracle.com/error-help/db/ora-00942/"', html)
        self.assertIn('</a>.', html)         # trailing dot not part of the link

    def test_params(self) -> None:
        html = query_error(passed={'colegejaar': 2024}, not_passed=['collegejaar'], unused=['colegejaar'])._repr_html_()
        self.assertIn('colegejaar=2024', html)
        self.assertIn('<span class="label">not passed</span><span>collegejaar</span>', html)
        self.assertIn('<span class="unused">colegejaar</span>', html)

    def test_no_template(self) -> None:
        html = query_error(not_passed=None, unused=None)._repr_html_()
        self.assertIn('(none)', html)
        self.assertNotIn('not passed', html)
        self.assertNotIn('unused', body(html))

    def test_path(self) -> None:
        err = query_error(path='/srv/queries/q.sql')
        self.assertIn('<div class="path"><code>/srv/queries/q.sql</code></div>', err._repr_html_())
        self.assertEqual(str(err).splitlines()[1], '/srv/queries/q.sql')
        self.assertNotIn('class="path"', query_error()._repr_html_())

    def test_style_and_no_script(self) -> None:
        html = query_error()._repr_html_()
        self.assertTrue(html.startswith('<div class="oq2-error">\n<style>'))
        self.assertEqual(html.count('<style>'), 1)
        self.assertNotIn('<script', html)

    def test_highlighting(self) -> None:
        self.assertIn('<span class="tok-k">select</span>', query_error()._repr_html_())

    def test_without_pygments(self) -> None:
        blocked = {name: None for name in ('pygments', 'pygments.lexers', 'pygments.token')}
        errors._token_classes.cache_clear()
        try:
            with mock.patch.dict(sys.modules, blocked):
                html = query_error()._repr_html_()
        finally:
            errors._token_classes.cache_clear()
        self.assertNotIn('tok-', body(html))
        self.assertIn('&lt;NA&gt;', html)
        self.assertEqual(marked(context(html)), ['colegejaar'])


class OtherErrorsHtmlTest(unittest.TestCase):
    def test_template_error(self) -> None:
        err = TemplateError('q', 'filters/jaar.sql', "unexpected '}'", source='select\n{{ x }\nfrom t', lineno=2, raised_at=AT)
        html = err._repr_html_()
        self.assertIn('· q · in filters/jaar.sql', html)
        self.assertEqual(error_lines(html), ['2'])
        self.assertIn('class="tok-t"', html)                 # Jinja delimiters highlighted
        self.assertNotIn('class="mark"', html)               # no column for template errors

    def test_template_error_crlf_source(self) -> None:
        err = TemplateError('q', 'q', 'boom', source='select\r\n{{ x }\r\nfrom t', lineno=2, raised_at=AT)
        html = err._repr_html_()
        self.assertEqual(error_lines(html), ['2'])
        self.assertNotIn('\r', body(html))

    def test_template_error_message_only(self) -> None:
        html = body(TemplateError('q', 'utils/agg.sql', 'AGG: Cannot use both', raised_at=AT)._repr_html_())
        self.assertIn('AGG: Cannot use both', html)
        self.assertNotIn('<pre', html)

    def test_connect_error(self) -> None:
        html = body(ConnectError('osiris', 'ORA-01017', 'ORA-01017: invalid', raised_at=AT)._repr_html_())
        self.assertIn('<b>ConnectError</b> · osiris', html)
        self.assertNotIn('<pre', html)
        self.assertNotIn('<details', html)


class DisplayHooksTest(unittest.TestCase):
    def test_mimebundle(self) -> None:
        err = query_error()
        bundle = err._repr_mimebundle_()
        self.assertEqual(bundle['text/plain'], str(err))
        self.assertEqual(bundle['text/html'], err._repr_html_())

    def test_mimebundle_survives_broken_html(self) -> None:
        err = query_error()
        with mock.patch.object(QueryError, '_repr_html_', side_effect=RuntimeError):
            self.assertEqual(err._repr_mimebundle_(), {'text/plain': str(err)})

    def test_render_traceback(self) -> None:
        err = query_error()
        lines = err._render_traceback_()
        self.assertEqual(lines[:len(str(err).splitlines())], str(err).splitlines())
        self.assertIn('last_error', lines[-1])

    def test_report_without_ipython_goes_to_stderr(self) -> None:
        err = ConnectError('osiris', None, 'down', raised_at=AT)
        buffer = io.StringIO()
        with mock.patch.dict(sys.modules, {'IPython': None}), redirect_stderr(buffer):
            report(err)
        self.assertEqual(buffer.getvalue(), str(err) + '\n')


if __name__ == '__main__':
    unittest.main()
