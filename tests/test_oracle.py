"""
Tests against the configured Oracle database (osiris).

Skipped unless the environment variable OQ2_ORACLE_TESTS is set, e.g.:
    OQ2_ORACLE_TESTS=1 python -m unittest tests.test_oracle
"""
import os
import unittest

# Broken statements with the error they cause and where the caret should go.
# (sql, code, marker): expected offset = err.sql.index(marker); marker None
# means no caret (runtime error, or no position reported).
# The database runs with cursor_sharing = force, so literals before the error
# shift Oracle's own position; the cases with literals check that the second
# parse with cursor_sharing = exact (execution._exact_offset) corrects it.
# Comments are not rewritten. Positions verified with dbms_sql on 2026-11-23.
# No '{' (Jinja).
# Single-line statements must start with 'select ' to be read as inline SQL.
OFFSET_CASES = [
    # no literals
    ("select\n  nonexistent_col\nfrom dual",                                 'ORA-00904', 'nonexistent_col'),
    ("select dual.from from dual",                                            'ORA-01747', 'from from'),
    ("select\n  dummy,\n  a b c\nfrom dual\nwhere dummy = 'X'",                'ORA-00923', 'c\nfrom'),
    # literals before the error
    ("select 'é€' a, nonexistent_col from dual",                             'ORA-00904', 'nonexistent_col'),
    ("select 1 from no_such_table",                                           'ORA-00942', 'no_such_table'),
    ("select 1, 2, 3 from no_such_table",                                     'ORA-00942', 'no_such_table'),
    ("select 1 x,\n       'a' y\nfrom no_such_table",                         'ORA-00942', 'no_such_table'),
    ("-- comment 'x'\nselect 1\nfrom no_such_table",                          'ORA-00942', 'no_such_table'),
    ("select 'é€' x, 1 2 from dual",                                          'ORA-00923', '2 from'),
    ("select 1 /* 'é' */\nfrom dual\nwhere dummy = 'X'\n  and and 1 = 1",      'ORA-00936', 'and 1 = 1'),
    ("select (1 from dual",                                                   'ORA-00907', 'from'),
    ("select 1 from dual d extra",                                            'ORA-00933', 'extra'),
    # no caret
    ("select 1 from dual union select 1, 2 from dual",                        'ORA-01789', None),   # offset 0
    ("select 1/0 from dual",                                                  'ORA-01476', None),
    ("select to_number('x') from dual",                                       'ORA-01722', None),
]


# Real database work for the execute-phase cancel. Runs ~44 s on its own
# (2026-11-23), so a cancel that stops working costs under a minute.
# Cancel latency measured then: 0.0 s when cancelled at 5 s, 0.1 s at 20 s.
HEAVY = """select count(*)
from (select level l from dual connect by level <= 100000) a,
     (select level l from dual connect by level <= 1000) b
where mod(a.l + b.l, 7) = 3"""

# Types whose conversion is set by SQLAlchemy's oracledb output type handler
# (CLOB, decimal NUMBER, DATE), NULLs in each, and a quoted mixed-case alias
# that name normalization must leave alone.
PARITY = """select
  level as id,
  case when mod(level, 3) = 0 then null else to_clob(rpad('x', level * 10, 'y')) end as txt_clob,
  case when mod(level, 4) = 0 then null else level / 8 end as num_dec,
  case when mod(level, 5) = 0 then null else date '2024-01-01' + level end as dt,
  'a' as "MixedCase"
from dual connect by level <= 20"""


@unittest.skipUnless(os.environ.get('OQ2_ORACLE_TESTS'), 'set OQ2_ORACLE_TESTS to run Oracle tests')
class TestOracleExecution(unittest.TestCase):
    def test_cancel_during_execute(self) -> None:
        import threading
        from query import errors, execution
        from query.connections import osiris

        ex = execution.QueryExecution(HEAVY, settings=osiris.SETTINGS)
        timer = threading.Timer(2, ex.cancel)
        timer.start()
        self.addCleanup(timer.cancel)
        with self.assertRaises(errors.QueryCancelled) as cm:
            ex.fetch()
        cause = cm.exception.__cause__
        self.assertIsNotNone(cause, 'cancelled before the statement was sent')
        self.assertEqual(execution._driver_error(cause).code, 'ORA-01013')
        self.assertIs(ex.progress.phase, execution.Phase.CANCELLED)

    def test_cancel_during_fetch(self) -> None:
        from query import errors, execution
        from query.connections import osiris

        ex = execution.QueryExecution(
            'select level as x from dual connect by level <= 100000', settings=osiris.SETTINGS,
        )

        def cancel_after_first_batch(p: execution.Progress) -> None:
            if p.phase is execution.Phase.FETCHING and p.rows:
                ex.cancel()

        with self.assertRaises(errors.QueryCancelled) as cm:
            ex.fetch(chunksize=1000, on_progress=cancel_after_first_batch)
        self.assertIsNone(cm.exception.__cause__)                  # flag, no driver error
        self.assertIs(ex.progress.phase, execution.Phase.CANCELLED)
        self.assertEqual(ex.progress.rows, 1000)

    def test_parity_with_read_sql_query(self) -> None:
        """
        pd.read_sql_query (over SQLAlchemy) and QueryExecution (raw cursor +
        _wrap_result) give the same frame. Run before deploying a pandas,
        SQLAlchemy or oracledb upgrade.
        """
        import importlib.util
        import pandas as pd
        from query import execution
        from query.connections import osiris

        for backend in ('numpy_nullable', 'pyarrow'):
            with self.subTest(dtype_backend=backend):
                if backend == 'pyarrow' and importlib.util.find_spec('pyarrow') is None:
                    self.skipTest('pyarrow not installed')
                old = pd.read_sql_query(PARITY, osiris.SETTINGS.engine(), dtype_backend=backend)
                new = execution.QueryExecution(PARITY, settings=osiris.SETTINGS).fetch(
                    chunksize=7, dtype_backend=backend,
                ).df
                self.assertEqual(list(new.columns), ['id', 'txt_clob', 'num_dec', 'dt', 'MixedCase'])
                pd.testing.assert_frame_equal(new, old)


@unittest.skipUnless(os.environ.get('OQ2_ORACLE_TESTS'), 'set OQ2_ORACLE_TESTS to run Oracle tests')
class TestOracleConnection(unittest.TestCase):
    def test_arraysize_reaches_cursor(self) -> None:
        from sqlalchemy import text
        from query.connections import osiris

        engine = osiris.SETTINGS.engine()
        with engine.connect() as con:
            result = con.execute(text('select 1 from dual'))
            self.assertEqual(result.cursor.arraysize, osiris.SETTINGS.arraysize)


@unittest.skipUnless(os.environ.get('OQ2_ORACLE_TESTS'), 'set OQ2_ORACLE_TESTS to run Oracle tests')
class TestOracleOffsets(unittest.TestCase):
    """
    Verifies the compile-time allowlist in errors.py: for each code on it the
    caret lands where the database stopped parsing; runtime codes get none.
    A failure shows the actual offset, to correct the marker or the allowlist.
    """
    def run_broken(self, sql: str):
        from query import errors, execution
        from query.connections import osiris

        with self.assertRaises(errors.QueryError) as cm:
            execution.execute_query(sql, settings=osiris.SETTINGS, raise_errors=True)
        return cm.exception

    @staticmethod
    def at(sql: str, offset: int | None) -> str:
        return 'no caret' if offset is None else f"{offset} ({sql[offset:offset + 15]!r})"

    def test_offsets(self) -> None:
        for sql, code, marker in OFFSET_CASES:
            with self.subTest(sql=sql):
                err = self.run_broken(sql)
                self.assertEqual(err.code, code, err.db_message)
                expected = None if marker is None else err.sql.index(marker)
                self.assertEqual(
                    err.offset, expected,
                    f"{code}: expected {self.at(err.sql, expected)}, "
                    f"got {self.at(err.sql, err.offset)}, raw_offset {err.raw_offset}",
                )

    def test_position(self) -> None:
        err = self.run_broken(OFFSET_CASES[0][0])
        self.assertEqual(err.position, (2, 3))


if __name__ == '__main__':
    unittest.main()
