"""
Status line for execute_query.

In a notebook kernel (JupyterLab, VS Code) a query that runs longer than
SHOW_AFTER gets one output line that is updated in place: connecting,
executing, fetching, and its final state. Outside a notebook (scripts,
scheduled tasks, terminal IPython) there are no live updates; the final line
is written to stderr.

A truncated result (max_rows) always gets the final line, also with
`live=False` and for fast queries. After an interrupt, a hint to interrupt
again appears if the execution hasn't stopped within CANCEL_HINT_AFTER, also
with `live=False`.

All timing comes from `Progress.elapsed` (the execution's own clock). Showing
the line never raises: a failing display is ignored.
"""
from __future__ import annotations

import html
import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from query.execution import FetchResult, Progress

SHOW_AFTER = 2.0          # s before the live line appears
STALL_AFTER = 10.0        # s without new rows before "no new rows for ..."
CANCEL_HINT_AFTER = 2.0   # s after an interrupt before "interrupt again to stop waiting"

Segments = list[tuple[str, bool]]     # (text, highlighted)

_STYLE = """<style>
.oq2.status { font-size: 0.85em; color: var(--jp-ui-font-color2, #777); }
.oq2.status .warn { color: var(--jp-warn-color0, #b45309); }
</style>"""


class StatusLine:
    """
    The status line of one execute_query call. The waiting loop calls
    `update` with the execution's progress (and once more after the run has
    ended), `cancel_requested` at an interrupt, `stopped_waiting` at a second
    interrupt, and `done` with the result.

    Phases are compared as strings (`Phase` is a StrEnum), so this module
    doesn't import `execution` at runtime.
    """

    def __init__(self, connection: str, query_name: str, *, live: bool = True) -> None:
        self.connection = connection
        self.query_name = query_name
        self.live = live
        self._notebook = _in_notebook()
        self._handle: Any = None                # notebook output, once shown
        self._fetching = False
        self._rows = 0                          # for the stall check
        self._rows_since = 0.0                  # elapsed when the row count last changed
        self._cancel_at: float | None = None    # elapsed at the interrupt
        self._hint_shown = False

    def update(self, progress: Progress) -> None:
        phase = progress.phase
        if phase in ('pending', 'done'):        # done: see done()
            return
        if phase in ('failed', 'cancelled'):
            if self._notebook and (self._handle is not None or self._live_after(progress.elapsed)):
                self._show([(self._prefix() + _state(progress), False)])
            return
        if self._cancel_at is not None:
            if not self._hint_shown and progress.elapsed - self._cancel_at >= CANCEL_HINT_AFTER:
                self._hint_shown = True
                self._show([(self._prefix() + 'cancelling… (interrupt again to stop waiting)', False)])
            return
        stalled_for = self._stalled_for(progress)
        if self._notebook and self._live_after(progress.elapsed):
            self._show([(self._prefix() + _state(progress, stalled_for=stalled_for), False)])

    def cancel_requested(self, progress: Progress) -> None:
        self._cancel_at = progress.elapsed

    def stopped_waiting(self, progress: Progress) -> None:
        self._show([(
            self._prefix()
            + f'stopped waiting after {_duration(progress.elapsed)}; '
            + 'the statement may still be running on the database',
            False,
        )])

    def done(self, result: FetchResult) -> None:
        if result.truncated or self._live_after(result.elapsed):
            self._show([(self._prefix(), False), *_final(result)])

    def _live_after(self, elapsed: float) -> bool:
        return self.live and elapsed >= SHOW_AFTER

    def _stalled_for(self, progress: Progress) -> float | None:
        """Seconds without new rows in the fetch phase, once over STALL_AFTER."""
        if progress.phase != 'fetching':
            return None
        if not self._fetching or progress.rows != self._rows:
            self._fetching = True
            self._rows = progress.rows
            self._rows_since = progress.elapsed
            return None
        stalled = progress.elapsed - self._rows_since
        return stalled if stalled >= STALL_AFTER else None

    def _prefix(self) -> str:
        return f'{self.connection} · {self.query_name} · '

    def _show(self, segments: Segments) -> None:
        text = ''.join(part for part, _ in segments)
        try:
            if self._notebook:
                bundle = {'text/html': _html(segments), 'text/plain': text}
                self._handle = _display(bundle, self._handle)
            else:
                print(text, file=sys.stderr)
        except Exception:
            pass                                # a status line must never break the query


def _state(progress: Progress, *, stalled_for: float | None = None) -> str:
    elapsed = _duration(progress.elapsed)
    match progress.phase:
        case 'connecting':
            return f'connecting… {elapsed}'
        case 'executing':
            return f'executing… {elapsed}'
        case 'fetching':
            text = f'fetching: {_rows(progress.rows)}, {elapsed}'
            if stalled_for is not None:
                text += f' (no new rows for {_duration(stalled_for)})'
            return text
        case 'failed':
            return f'failed after {elapsed}'
        case 'cancelled':
            return f'cancelled after {elapsed}'
        case _:
            return str(progress.phase)


def _final(result: FetchResult) -> Segments:
    segments: Segments = [(f'done: {_rows(result.rows)}, ', False)]
    if result.truncated:
        segments += [('stopped at max_rows (more available)', True), (', ', False)]
    segments.append((f'{_size(result.nbytes)}, {_duration(result.elapsed)}', False))
    return segments


def _rows(n: int) -> str:
    return f'{n:,} row' if n == 1 else f'{n:,} rows'


def _duration(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f'{s} s'
    m, s = divmod(s, 60)
    if m < 60:
        return f'{m} min {s} s'
    h, m = divmod(m, 60)
    return f'{h} h {m} min'


def _size(nbytes: int) -> str:
    """Binary units, as `DataFrame.info()`."""
    for unit, factor in (('GB', 1024**3), ('MB', 1024**2), ('KB', 1024)):
        if nbytes >= factor:
            value = nbytes / factor
            return f'{value:.1f} {unit}' if value < 10 else f'{value:.0f} {unit}'
    return f'{nbytes} B'


def _html(segments: Segments) -> str:
    body = ''.join(
        f'<span class="warn">{html.escape(part)}</span>' if warn else html.escape(part)
        for part, warn in segments
    )
    return f'{_STYLE}<div class="oq2 status">{body}</div>'


def _in_notebook() -> bool:
    """A notebook kernel (JupyterLab, VS Code); terminal IPython can't update output in place."""
    try:
        from IPython import get_ipython
    except ImportError:
        return False
    shell = get_ipython()
    return shell is not None and type(shell).__name__ == 'ZMQInteractiveShell'


def _display(bundle: dict[str, str], handle: Any) -> Any:
    """Show `bundle` as a new output, or update `handle`; returns the handle."""
    from IPython.display import display
    if handle is None:
        return display(bundle, raw=True, display_id=True)
    handle.update(bundle, raw=True)
    return handle
