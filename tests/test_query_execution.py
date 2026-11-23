import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from query.config import ConnectionSettings
from query.connections.connection import DuckdbSettings, OracleSettings
from query.definition import Query, get_environment, get_sql
from query.errors import ConnectError, QueryCancelled, QueryError, QueryNotFound
from query.execution import Phase, Progress, QueryExecution
from tests.fake_sqlite import SLOW, FakeSettings, rows


def closed(con: sqlite3.Connection) -> bool:
    try:
        con.execute('select 1')
    except sqlite3.ProgrammingError:
        return True
    return False


class FetchTest(unittest.TestCase):
    def test_full_result(self) -> None:
        settings = FakeSettings()
        ex = QueryExecution(rows(25), settings=settings)
        result = ex.fetch()
        self.assertEqual(result.rows, 25)
        self.assertFalse(result.truncated)
        self.assertEqual(list(result.df.columns), ['n', 's'])
        self.assertEqual(str(result.df['n'].dtype), 'Int64')    # numpy_nullable by default
        self.assertGreater(result.nbytes, 0)
        self.assertEqual(ex.progress.phase, Phase.DONE)
        self.assertTrue(closed(settings.connections[0]))

    def test_progress_per_phase_and_batch(self) -> None:
        log: list[Progress] = []
        QueryExecution(rows(25), settings=FakeSettings()).fetch(chunksize=10, on_progress=log.append)
        self.assertEqual(
            [(p.phase.value, p.rows) for p in log],
            [('connecting', 0), ('executing', 0), ('fetching', 0),
             ('fetching', 10), ('fetching', 20), ('fetching', 25), ('done', 25)],
        )

    def test_empty_result_keeps_columns(self) -> None:
        result = QueryExecution('select 1 as a where 0', settings=FakeSettings()).fetch()
        self.assertEqual(result.df.shape, (0, 1))
        self.assertEqual(list(result.df.columns), ['a'])

    def test_column_names_normalized_like_sqlalchemy(self) -> None:
        # Oracle's dialect lowercases case-insensitive uppercase names; the
        # fake stands in with a simplified rule.
        settings = FakeSettings()
        engine = settings.engine()
        engine.dialect.requires_name_normalize = True
        engine.dialect.normalize_name = lambda name: name.lower() if name.isupper() else name
        with mock.patch.object(settings, 'engine', return_value=engine):
            result = QueryExecution('select 1 as "SINH_ID", 2 as "Naam"', settings=settings).fetch()
        self.assertEqual(list(result.df.columns), ['sinh_id', 'Naam'])

    def test_frame_options(self) -> None:
        result = QueryExecution(rows(3), settings=FakeSettings()).fetch(index_col='n', dtype={'s': 'object'})
        self.assertEqual(result.df.index.name, 'n')
        self.assertEqual(result.df['s'].dtype, object)

    def test_elapsed_starts_at_run(self) -> None:
        ex = QueryExecution(rows(3), settings=FakeSettings())
        self.assertEqual(ex.progress, Progress(Phase.PENDING, 0, 0.0))
        result = ex.fetch()
        self.assertGreater(result.elapsed, 0)
        self.assertEqual(result.elapsed, ex.progress.elapsed)   # clock stops at the end


class MaxRowsTest(unittest.TestCase):
    def test_truncated(self) -> None:
        settings = FakeSettings()
        ex = QueryExecution(rows(25), settings=settings)
        result = ex.fetch(max_rows=7, chunksize=5)
        self.assertTrue(result.truncated)
        self.assertEqual(result.rows, 7)
        self.assertEqual(result.df['n'].tolist(), list(range(1, 8)))
        self.assertEqual(ex.progress.phase, Phase.DONE)
        self.assertTrue(closed(settings.connections[0]))

    def test_exactly_max_rows_is_not_truncated(self) -> None:
        result = QueryExecution(rows(7), settings=FakeSettings()).fetch(max_rows=7)
        self.assertFalse(result.truncated)
        self.assertEqual(result.rows, 7)

    def test_zero(self) -> None:
        result = QueryExecution(rows(3), settings=FakeSettings()).fetch(max_rows=0)
        self.assertTrue(result.truncated)
        self.assertEqual(result.df.shape, (0, 2))

    def test_batch_capped_by_max_rows(self) -> None:
        # fetchmany(n) collects n rows over several round trips, so the batch
        # itself must be capped: one batch of max_rows + 1 rows.
        log: list[Progress] = []
        QueryExecution(rows(100), settings=FakeSettings(arraysize=50)).fetch(max_rows=3, on_progress=log.append)
        self.assertEqual(max(p.rows for p in log), 4)


class BatchSizeTest(unittest.TestCase):
    def batch_rows(self, settings: FakeSettings, **kwargs) -> list[int]:
        log: list[Progress] = []
        QueryExecution(rows(25), settings=settings).fetch(on_progress=log.append, **kwargs)
        return sorted({p.rows for p in log if p.phase is Phase.FETCHING and p.rows})

    def test_default_from_dialect_arraysize(self) -> None:
        self.assertEqual(self.batch_rows(FakeSettings(arraysize=10)), [10, 20, 25])

    def test_default_without_arraysize(self) -> None:
        self.assertEqual(self.batch_rows(FakeSettings()), [25])          # DEFAULT_CHUNKSIZE

    def test_chunksize_overrides_arraysize(self) -> None:
        self.assertEqual(self.batch_rows(FakeSettings(arraysize=10), chunksize=20), [20, 25])


class ChunksTest(unittest.TestCase):
    def test_chunks(self) -> None:
        ex = QueryExecution(rows(25), settings=FakeSettings())
        sizes = [len(df) for df in ex.chunks(chunksize=10)]
        self.assertEqual(sizes, [10, 10, 5])
        self.assertEqual(ex.progress.phase, Phase.DONE)
        self.assertEqual(ex.progress.rows, 25)

    def test_stop_early(self) -> None:
        settings = FakeSettings()
        ex = QueryExecution(rows(25), settings=settings)
        chunks = ex.chunks(chunksize=10)
        for df in chunks:
            break
        chunks.close()          # as when the generator is discarded
        self.assertEqual(ex.progress.phase, Phase.DONE)
        self.assertEqual(ex.progress.rows, 10)
        self.assertTrue(closed(settings.connections[0]))


class CancelTest(unittest.TestCase):
    def test_during_execute(self) -> None:
        settings = FakeSettings()
        ex = QueryExecution(SLOW, settings=settings)
        timer = threading.Timer(0.2, ex.cancel)
        timer.start()
        self.addCleanup(timer.cancel)
        with self.assertRaises(QueryCancelled) as cm:
            ex.fetch()
        self.assertIsInstance(cm.exception.__cause__, sqlite3.OperationalError)   # 'interrupted'
        self.assertEqual(cm.exception.connection, 'fake')
        self.assertEqual(ex.progress.phase, Phase.CANCELLED)
        self.assertLess(ex.progress.elapsed, 5)
        self.assertTrue(closed(settings.connections[0]))

    def test_during_fetch(self) -> None:
        ex = QueryExecution(rows(25), settings=FakeSettings())

        def cancel_after_first_batch(p: Progress) -> None:
            if p.phase is Phase.FETCHING and p.rows:
                ex.cancel()

        with self.assertRaises(QueryCancelled) as cm:
            ex.fetch(chunksize=10, on_progress=cancel_after_first_batch)
        self.assertIsNone(cm.exception.__cause__)                  # flag, no driver error
        self.assertEqual(ex.progress, Progress(Phase.CANCELLED, 10, ex.progress.elapsed))

    def test_during_connect(self) -> None:
        settings = FakeSettings()
        ex = QueryExecution(rows(3), settings=settings)
        engine = settings.engine()
        connect = engine.raw_connection

        def cancel_then_connect() -> sqlite3.Connection:
            ex.cancel()
            return connect()

        engine.raw_connection = cancel_then_connect
        with mock.patch.object(settings, 'engine', return_value=engine):
            with self.assertRaises(QueryCancelled):
                ex.fetch()
        self.assertEqual(ex.progress.phase, Phase.CANCELLED)
        self.assertTrue(closed(settings.connections[0]))

    def test_before_run(self) -> None:
        ex = QueryExecution(rows(3), settings=FakeSettings())
        ex.cancel()
        with self.assertRaises(QueryCancelled):
            ex.fetch()

    def test_after_done_is_noop(self) -> None:
        ex = QueryExecution(rows(3), settings=FakeSettings())
        ex.fetch()
        ex.cancel()
        self.assertEqual(ex.progress.phase, Phase.DONE)


class ErrorTest(unittest.TestCase):
    def test_query_error(self) -> None:
        env = get_environment(paths=[])
        query = Query.from_text('select {{ col }} from no_such_table', env)
        ex = QueryExecution(query, settings=FakeSettings(), params={'col': 'n', 'colx': 1})
        with self.assertRaises(QueryError) as cm:
            ex.fetch()
        err = cm.exception
        self.assertEqual(err.query_name, '<inline>')
        self.assertEqual(err.connection, 'fake')
        self.assertEqual(err.sql, 'select n from no_such_table')
        self.assertIn('no such table', err.db_message)
        self.assertIsNone(err.offset)
        self.assertEqual(err.passed, {'col': 'n', 'colx': 1})
        self.assertEqual(err.unused, ['colx'])
        self.assertIsInstance(err.__cause__, sqlite3.OperationalError)
        self.assertEqual(ex.progress.phase, Phase.FAILED)

    def test_connect_error(self) -> None:
        ex = QueryExecution(rows(3), settings=FakeSettings(connect_error=True))
        with self.assertRaises(ConnectError) as cm:
            ex.fetch()
        self.assertEqual(cm.exception.connection, 'fake')
        self.assertIn('unable to open', cm.exception.db_message)
        self.assertEqual(ex.progress.phase, Phase.FAILED)

    def test_single_use(self) -> None:
        ex = QueryExecution(rows(3), settings=FakeSettings())
        ex.fetch()
        with self.assertRaises(RuntimeError):
            ex.fetch()
        with self.assertRaises(RuntimeError):
            next(ex.chunks())


class RenderTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        (root / 'q.sql').write_text('select {{ x }} as x', encoding='utf-8')
        self.env = get_environment(paths=root)

    def test_renders_on_creation(self) -> None:
        ex = QueryExecution('q', settings=FakeSettings(), params={'x': 5}, env=self.env)
        self.assertEqual(ex.sql.text, 'select 5 as x')
        self.assertEqual(ex.sql.query.name, 'q')
        self.assertEqual(ex.progress.phase, Phase.PENDING)

    def test_not_found_on_creation(self) -> None:
        with self.assertRaises(QueryNotFound):
            QueryExecution('nope', settings=FakeSettings(), env=self.env)

    def test_sql_passed_through(self) -> None:
        sql = get_sql('q', env=self.env, x=5)
        ex = QueryExecution(sql, settings=FakeSettings())
        self.assertIs(ex.sql, sql)
        self.assertEqual(ex.fetch().df['x'].tolist(), [5])

    def test_sql_with_params(self) -> None:
        sql = get_sql('q', env=self.env, x=5)
        with self.assertRaises(TypeError):
            QueryExecution(sql, settings=FakeSettings(), params={'x': 6})

    def test_params_reach_template_unchanged(self) -> None:
        # Not through get_sql: its own options would be taken out of the parameters.
        query = Query.from_text('select {{ print_sql }} as p', self.env)
        ex = QueryExecution(query, settings=FakeSettings(), params={'print_sql': 1})
        self.assertEqual(ex.sql.text, 'select 1 as p')


class SettingsCancelTest(unittest.TestCase):
    def test_base_does_nothing(self) -> None:
        con = mock.Mock()
        ConnectionSettings.cancel(mock.sentinel.settings, con)
        self.assertEqual(con.mock_calls, [])

    def test_oracle(self) -> None:
        con = mock.Mock()
        OracleSettings.cancel(mock.sentinel.settings, con)
        con.cancel.assert_called_once_with()

    def test_duckdb(self) -> None:
        con = mock.Mock()
        DuckdbSettings.cancel(mock.sentinel.settings, con)
        con.interrupt.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
