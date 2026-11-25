"""
execute_query's waiting loop (execution._fetch_watched): fetch on a worker
thread, status updates, interrupt handling.

Interrupts are simulated with _thread.interrupt_main(), which raises
KeyboardInterrupt in the main thread the next time it runs Python code; the
loop wakes up at least every 0.5 s, so each interrupt test takes about that
long. Each simulated interrupt is triggered while the main thread is known to
be inside the call, so it can't land outside assertRaises.
"""
import _thread
import sqlite3
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

import pandas as pd

from query import errors, execution
from query.errors import QueryCancelled
from query.execution import FetchResult, Phase, Progress
from query.status import StatusLine
from tests.fake_sqlite import SLOW, FakeSettings, rows


class FakeExecution:
    """
    QueryExecution stand-in: fetch() blocks until `release` is set. With
    `stops_on_cancel`, cancel() ends fetch() with QueryCancelled, as a driver
    with a working cancel; without it, fetch() keeps running (as sap, where
    pymssql has no cancel). `on_cancel` runs inside cancel().
    """

    def __init__(self, *, stops_on_cancel: bool = True, error: BaseException | None = None, on_cancel=None) -> None:
        self.sql = SimpleNamespace(query=SimpleNamespace(name='fake'))
        self.release = threading.Event()
        self.cancelled = threading.Event()
        self.returned = threading.Event()
        self.stops_on_cancel = stops_on_cancel
        self.error = error
        self.on_cancel = on_cancel
        self.phase = Phase.EXECUTING
        self.options: dict | None = None
        self.result = FetchResult(df=pd.DataFrame(), truncated=False, rows=0, nbytes=0, elapsed=1.0)

    @property
    def progress(self) -> Progress:
        return Progress(self.phase, 0, 1.0)

    def fetch(self, **options) -> FetchResult:
        self.options = options
        try:
            while not self.release.wait(0.01):
                if self.stops_on_cancel and self.cancelled.is_set():
                    self.phase = Phase.CANCELLED
                    raise QueryCancelled(query_name='fake', connection='fake')
            if self.error is not None:
                self.phase = Phase.FAILED
                raise self.error
            self.phase = Phase.DONE
            return self.result
        finally:
            self.returned.set()

    def cancel(self) -> None:
        self.cancelled.set()
        if self.on_cancel is not None:
            self.on_cancel()


def interrupt_after(seconds: float) -> threading.Timer:
    timer = threading.Timer(seconds, _thread.interrupt_main)
    timer.start()
    return timer


class FetchWatchedTest(unittest.TestCase):
    def setUp(self) -> None:
        self.status = mock.create_autospec(StatusLine, instance=True)

    def watched(self, ex: FakeExecution, **options):
        self.addCleanup(ex.release.set)       # never leave a worker blocked
        return execution._fetch_watched(ex, self.status, **options)

    def calls(self) -> list[str]:
        return [name for name, *_ in self.status.method_calls]

    def test_returns_result(self) -> None:
        ex = FakeExecution()
        ex.release.set()
        result = self.watched(ex, max_rows=5)
        self.assertIs(result, ex.result)
        self.assertEqual(ex.options, {'max_rows': 5})
        self.status.update.assert_called_with(Progress(Phase.DONE, 0, 1.0))

    def test_status_updated_while_waiting(self) -> None:
        ex = FakeExecution()
        threading.Timer(0.7, ex.release.set).start()
        self.watched(ex)
        self.assertGreaterEqual(self.status.update.call_count, 2)    # at least one tick, plus the end

    def test_worker_error_reraised(self) -> None:
        ex = FakeExecution(error=RuntimeError('boom'))
        ex.release.set()
        with self.assertRaisesRegex(RuntimeError, 'boom'):
            self.watched(ex)
        self.status.update.assert_called_with(Progress(Phase.FAILED, 0, 1.0))

    def test_interrupt_cancels_waits_and_reraises(self) -> None:
        ex = FakeExecution()
        interrupt_after(0.2)
        with self.assertRaises(KeyboardInterrupt):
            self.watched(ex)
        self.assertTrue(ex.cancelled.is_set())
        self.assertTrue(ex.returned.is_set())          # waited for the worker
        self.assertEqual(ex.phase, Phase.CANCELLED)
        self.assertIn('cancel_requested', self.calls())
        self.assertNotIn('stopped_waiting', self.calls())
        self.status.update.assert_called_with(Progress(Phase.CANCELLED, 0, 1.0))

    def test_second_interrupt_stops_waiting(self) -> None:
        # The fetch ignores the cancel; the second interrupt is triggered from
        # cancel(), so it lands while the loop waits for the worker.
        ex = FakeExecution(stops_on_cancel=False, on_cancel=lambda: interrupt_after(0.1))
        interrupt_after(0.2)
        with self.assertRaises(KeyboardInterrupt):
            self.watched(ex)
        self.assertTrue(ex.cancelled.is_set())
        self.assertFalse(ex.returned.is_set())         # still running: not waited for
        self.assertEqual(self.calls()[-1], 'stopped_waiting')
        ex.release.set()
        self.assertTrue(ex.returned.wait(2))           # the worker finishes on its own


class ExecuteQueryTest(unittest.TestCase):
    """The waiting loop and the status line through execute_query, on the sqlite fake."""

    def setUp(self) -> None:
        self.settings = FakeSettings()
        errors._last_errors.clear()
        self.addCleanup(errors._last_errors.clear)

    def test_interrupt_cancels_the_statement(self) -> None:
        interrupt_after(0.3)
        with self.assertRaises(KeyboardInterrupt):
            execution.execute_query(SLOW, settings=self.settings, progress=False)
        con = self.settings.connections[0]
        with self.assertRaisesRegex(sqlite3.ProgrammingError, 'closed'):
            con.execute('select 1')                    # the worker closed it
        self.assertNotIn('fake', errors._last_errors)  # an interrupt is not stored

    def test_progress_keyword_and_done(self) -> None:
        with mock.patch.object(execution, 'StatusLine', autospec=True) as status_line:
            df = execution.execute_query(rows(3), settings=self.settings, progress=False, squeeze=False)
        status_line.assert_called_once_with('fake', '<inline>', live=False)
        result = status_line.return_value.done.call_args.args[0]
        self.assertEqual(result.rows, 3)
        self.assertEqual(len(df), 3)

    def test_progress_defaults_to_true(self) -> None:
        with mock.patch.object(execution, 'StatusLine', autospec=True) as status_line:
            execution.execute_query(rows(3), settings=self.settings)
        self.assertEqual(status_line.call_args.kwargs, {'live': True})

    def test_no_done_on_query_error(self) -> None:
        with (
            mock.patch.object(execution, 'StatusLine', autospec=True) as status_line,
            mock.patch.object(errors, 'report'),
        ):
            self.assertIsNone(execution.execute_query('select * from no_such_table', settings=self.settings))
        status_line.return_value.done.assert_not_called()
        self.assertIn('fake', errors._last_errors)


if __name__ == '__main__':
    unittest.main()
