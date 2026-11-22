"""
Tests against the configured Oracle database (osiris).

Skipped unless the environment variable OQ2_ORACLE_TESTS is set, e.g.:
    OQ2_ORACLE_TESTS=1 python -m unittest tests.test_oracle
"""
import os
import unittest


@unittest.skipUnless(os.environ.get('OQ2_ORACLE_TESTS'), 'set OQ2_ORACLE_TESTS to run Oracle tests')
class TestOracleConnection(unittest.TestCase):
    def test_arraysize_reaches_cursor(self) -> None:
        from sqlalchemy import text
        from query.connections import osiris

        engine = osiris.SETTINGS.engine()
        with engine.connect() as con:
            result = con.execute(text('select 1 from dual'))
            self.assertEqual(result.cursor.arraysize, osiris.SETTINGS.arraysize)


if __name__ == '__main__':
    unittest.main()
