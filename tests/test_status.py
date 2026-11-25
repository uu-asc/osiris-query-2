import io
import sys
import types
import unittest
from unittest import mock

import pandas as pd

from query import status
from query.execution import FetchResult, Phase, Progress
from query.status import StatusLine


def progress(phase: Phase, elapsed: float, rows: int = 0) -> Progress:
    return Progress(phase, rows, elapsed)


def result(elapsed: float, *, rows: int = 10, truncated: bool = False, nbytes: int = 2048) -> FetchResult:
    return FetchResult(df=pd.DataFrame(), truncated=truncated, rows=rows, nbytes=nbytes, elapsed=elapsed)


class FakeHandle:
    def __init__(self, shown: list) -> None:
        self.shown = shown

    def update(self, bundle: dict, raw: bool) -> None:
        self.shown.append(('update', bundle))


class NotebookCase(unittest.TestCase):
    """StatusLine in a notebook kernel; displays are recorded in self.shown."""

    def setUp(self) -> None:
        self.shown: list[tuple[str, dict]] = []

        def display(bundle: dict, handle):
            if handle is None:
                self.shown.append(('display', bundle))
                return FakeHandle(self.shown)
            handle.update(bundle, raw=True)
            return handle

        for patcher in (
            mock.patch.object(status, '_in_notebook', return_value=True),
            mock.patch.object(status, '_display', side_effect=display),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def line(self, **kwargs) -> StatusLine:
        return StatusLine('osiris', 'matching/eindstatus', **kwargs)

    def texts(self) -> list[str]:
        return [bundle['text/plain'] for _, bundle in self.shown]


class TerminalCase(unittest.TestCase):
    """StatusLine outside a notebook; stderr is captured in self.stderr."""

    def setUp(self) -> None:
        self.stderr = io.StringIO()
        for patcher in (
            mock.patch.object(status, '_in_notebook', return_value=False),
            mock.patch.object(sys, 'stderr', self.stderr),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def line(self, **kwargs) -> StatusLine:
        return StatusLine('osiris', 'matching/eindstatus', **kwargs)

    def lines(self) -> list[str]:
        return self.stderr.getvalue().splitlines()


class FormatTest(unittest.TestCase):
    def test_duration(self) -> None:
        self.assertEqual(status._duration(0.4), '0 s')
        self.assertEqual(status._duration(59.9), '59 s')
        self.assertEqual(status._duration(190), '3 min 10 s')
        self.assertEqual(status._duration(3900), '1 h 5 min')

    def test_size(self) -> None:
        self.assertEqual(status._size(500), '500 B')
        self.assertEqual(status._size(820 * 1024), '820 KB')
        self.assertEqual(status._size(int(4.1 * 1024**2)), '4.1 MB')
        self.assertEqual(status._size(72 * 1024**2), '72 MB')
        self.assertEqual(status._size(3 * 1024**3), '3.0 GB')

    def test_state_per_phase(self) -> None:
        self.assertEqual(status._state(progress(Phase.CONNECTING, 3)), 'connecting… 3 s')
        self.assertEqual(status._state(progress(Phase.EXECUTING, 12)), 'executing… 12 s')
        self.assertEqual(status._state(progress(Phase.FETCHING, 31, 120_000)), 'fetching: 120,000 rows, 31 s')
        self.assertEqual(status._state(progress(Phase.FAILED, 31)), 'failed after 31 s')
        self.assertEqual(status._state(progress(Phase.CANCELLED, 12)), 'cancelled after 12 s')

    def test_state_stalled(self) -> None:
        text = status._state(progress(Phase.FETCHING, 190, 120_000), stalled_for=120)
        self.assertEqual(text, 'fetching: 120,000 rows, 3 min 10 s (no new rows for 2 min 0 s)')

    def test_final(self) -> None:
        text = ''.join(part for part, _ in status._final(result(31, rows=1_204_331, nbytes=72 * 1024**2)))
        self.assertEqual(text, 'done: 1,204,331 rows, 72 MB, 31 s')

    def test_final_truncated_marks_the_notice(self) -> None:
        segments = status._final(result(12, rows=50_000, truncated=True, nbytes=int(4.1 * 1024**2)))
        self.assertEqual(
            ''.join(part for part, _ in segments),
            'done: 50,000 rows, stopped at max_rows (more available), 4.1 MB, 12 s',
        )
        self.assertEqual([part for part, warn in segments if warn], ['stopped at max_rows (more available)'])

    def test_one_row(self) -> None:
        self.assertEqual(status._rows(1), '1 row')
        self.assertEqual(status._rows(0), '0 rows')

    def test_html_escapes_and_highlights(self) -> None:
        out = status._html([('osiris · <inline> · ', False), ('stopped', True)])
        self.assertIn('&lt;inline&gt;', out)
        self.assertIn('<span class="warn">stopped</span>', out)
        self.assertIn('<div class="oq2 status">', out)


class NotebookLiveTest(NotebookCase):
    def test_nothing_before_threshold(self) -> None:
        line = self.line()
        line.update(progress(Phase.CONNECTING, 0.1))
        line.update(progress(Phase.EXECUTING, 1.9))
        self.assertEqual(self.shown, [])

    def test_shown_after_threshold_then_updated_in_place(self) -> None:
        line = self.line()
        line.update(progress(Phase.EXECUTING, 2.0))
        line.update(progress(Phase.FETCHING, 2.5, 10_000))
        self.assertEqual([kind for kind, _ in self.shown], ['display', 'update'])
        self.assertEqual(self.texts(), [
            'osiris · matching/eindstatus · executing… 2 s',
            'osiris · matching/eindstatus · fetching: 10,000 rows, 2 s',
        ])
        self.assertIn('text/html', self.shown[0][1])

    def test_no_new_rows(self) -> None:
        line = self.line()
        line.update(progress(Phase.FETCHING, 5, 10_000))
        line.update(progress(Phase.FETCHING, 14.9, 10_000))
        self.assertNotIn('no new rows', self.texts()[-1])
        line.update(progress(Phase.FETCHING, 15, 10_000))
        self.assertTrue(self.texts()[-1].endswith('(no new rows for 10 s)'))
        line.update(progress(Phase.FETCHING, 16, 20_000))     # rows again: reset
        self.assertNotIn('no new rows', self.texts()[-1])

    def test_live_false_shows_nothing_while_running(self) -> None:
        line = self.line(live=False)
        line.update(progress(Phase.EXECUTING, 30))
        line.update(progress(Phase.FAILED, 31))
        self.assertEqual(self.shown, [])


class NotebookFinalTest(NotebookCase):
    def test_slow_query_final_line_replaces_live_line(self) -> None:
        line = self.line()
        line.update(progress(Phase.FETCHING, 3, 10_000))
        line.done(result(31, rows=1_204_331, nbytes=72 * 1024**2))
        self.assertEqual([kind for kind, _ in self.shown], ['display', 'update'])
        self.assertEqual(self.texts()[-1], 'osiris · matching/eindstatus · done: 1,204,331 rows, 72 MB, 31 s')

    def test_slow_query_finishing_before_first_tick_still_gets_final_line(self) -> None:
        self.line().done(result(2.2))
        self.assertEqual([kind for kind, _ in self.shown], ['display'])

    def test_fast_query_nothing(self) -> None:
        self.line().done(result(0.3))
        self.assertEqual(self.shown, [])

    def test_truncated_fast_query_gets_final_line(self) -> None:
        self.line().done(result(0.3, truncated=True))
        self.assertEqual(len(self.shown), 1)
        self.assertIn('<span class="warn">stopped at max_rows (more available)</span>', self.shown[0][1]['text/html'])

    def test_truncated_with_live_false_gets_final_line(self) -> None:
        self.line(live=False).done(result(30, truncated=True))
        self.assertEqual(len(self.shown), 1)

    def test_live_false_slow_query_nothing(self) -> None:
        self.line(live=False).done(result(30))
        self.assertEqual(self.shown, [])

    def test_failed_after_line_shown(self) -> None:
        line = self.line()
        line.update(progress(Phase.EXECUTING, 5))
        line.update(progress(Phase.FAILED, 31))
        self.assertEqual(self.texts()[-1], 'osiris · matching/eindstatus · failed after 31 s')

    def test_fast_failure_nothing(self) -> None:
        line = self.line()
        line.update(progress(Phase.FAILED, 0.5))
        self.assertEqual(self.shown, [])

    def test_display_failure_is_ignored(self) -> None:
        with mock.patch.object(status, '_display', side_effect=RuntimeError('frontend')):
            self.line().done(result(30))      # no exception


class NotebookCancelTest(NotebookCase):
    def test_cancel_with_line_shown(self) -> None:
        line = self.line()
        line.update(progress(Phase.EXECUTING, 12))
        line.cancel_requested(progress(Phase.EXECUTING, 12))
        line.update(progress(Phase.EXECUTING, 12.5))          # no ordinary updates while cancelling
        line.update(progress(Phase.CANCELLED, 12.6))
        self.assertEqual(self.texts(), [
            'osiris · matching/eindstatus · executing… 12 s',
            'osiris · matching/eindstatus · cancelled after 12 s',
        ])

    def test_interrupt_within_threshold_nothing(self) -> None:
        line = self.line()
        line.cancel_requested(progress(Phase.EXECUTING, 1))
        line.update(progress(Phase.EXECUTING, 1.5))
        line.update(progress(Phase.CANCELLED, 1.6))
        self.assertEqual(self.shown, [])

    def test_hint_when_cancel_is_slow_also_with_live_false(self) -> None:
        line = self.line(live=False)
        line.cancel_requested(progress(Phase.EXECUTING, 1))
        line.update(progress(Phase.EXECUTING, 2.9))
        self.assertEqual(self.shown, [])
        line.update(progress(Phase.EXECUTING, 3.0))
        line.update(progress(Phase.EXECUTING, 3.5))           # hint once
        self.assertEqual(self.texts(), [
            'osiris · matching/eindstatus · cancelling… (interrupt again to stop waiting)',
        ])
        line.update(progress(Phase.CANCELLED, 5))
        self.assertEqual(self.texts()[-1], 'osiris · matching/eindstatus · cancelled after 5 s')

    def test_stopped_waiting(self) -> None:
        line = self.line()
        line.cancel_requested(progress(Phase.EXECUTING, 1))
        line.update(progress(Phase.EXECUTING, 3))
        line.stopped_waiting(progress(Phase.EXECUTING, 4))
        self.assertEqual(
            self.texts()[-1],
            'osiris · matching/eindstatus · stopped waiting after 4 s; '
            'the statement may still be running on the database',
        )


class TerminalTest(TerminalCase):
    def test_no_live_updates(self) -> None:
        line = self.line()
        line.update(progress(Phase.EXECUTING, 5))
        line.update(progress(Phase.FETCHING, 9, 10_000))
        self.assertEqual(self.lines(), [])

    def test_slow_query_final_line(self) -> None:
        line = self.line()
        line.update(progress(Phase.EXECUTING, 5))
        line.done(result(31, rows=1_204_331, nbytes=72 * 1024**2))
        self.assertEqual(self.lines(), ['osiris · matching/eindstatus · done: 1,204,331 rows, 72 MB, 31 s'])

    def test_fast_query_nothing(self) -> None:
        self.line().done(result(0.3))
        self.assertEqual(self.lines(), [])

    def test_truncated_fast_query_and_live_false(self) -> None:
        self.line().done(result(0.3, truncated=True))
        self.line(live=False).done(result(0.3, truncated=True))
        self.assertEqual(len(self.lines()), 2)
        self.assertIn('stopped at max_rows (more available)', self.lines()[0])

    def test_failed_nothing_extra(self) -> None:
        line = self.line()
        line.update(progress(Phase.EXECUTING, 5))
        line.update(progress(Phase.FAILED, 31))
        self.assertEqual(self.lines(), [])

    def test_hint_once(self) -> None:
        line = self.line()
        line.cancel_requested(progress(Phase.EXECUTING, 5))
        line.update(progress(Phase.EXECUTING, 7))
        line.update(progress(Phase.EXECUTING, 8))
        line.update(progress(Phase.CANCELLED, 9))
        self.assertEqual(self.lines(), [
            'osiris · matching/eindstatus · cancelling… (interrupt again to stop waiting)',
        ])


class InNotebookTest(unittest.TestCase):
    def shell(self, class_name: str | None):
        module = types.ModuleType('IPython')
        instance = None if class_name is None else type(class_name, (), {})()
        module.get_ipython = lambda: instance
        return mock.patch.dict(sys.modules, {'IPython': module})

    def test_kernel(self) -> None:
        with self.shell('ZMQInteractiveShell'):
            self.assertTrue(status._in_notebook())

    def test_terminal_ipython(self) -> None:
        with self.shell('TerminalInteractiveShell'):
            self.assertFalse(status._in_notebook())

    def test_plain_python_with_ipython_installed(self) -> None:
        with self.shell(None):
            self.assertFalse(status._in_notebook())

    def test_without_ipython(self) -> None:
        with mock.patch.dict(sys.modules, {'IPython': None}):
            self.assertFalse(status._in_notebook())


if __name__ == '__main__':
    unittest.main()
