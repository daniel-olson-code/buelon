"""Status history for `boo web`: a background sampler, a bounded store, and the config.

The dashboard only ever shows *now*. An operator's real question is comparative --
"was it like this an hour ago?", "is the backlog draining or growing?", "when did the
errors start?" -- so the web process keeps a memory of its own.

The sampling happens **server-side**, in the `boo web` process, not in the browser:
history has to accumulate while nobody has the tab open, survive a reload, and be the
same series for every browser pointed at that server.

Retention story, stated plainly because a dashboard must not be able to fill a disk:
**at most `MAX_SAMPLES` (2000) samples, and nothing older than `MAX_AGE_DAYS` (7) days,
whichever bites first.** At the 10-minute default that is the 7 days; only intervals
under 5 minutes ever reach the sample cap. A sample is ~15 integers, so the file is a
few hundred kilobytes at worst -- this is deliberately not a database.
"""

from __future__ import annotations

import os
import json
import time
import asyncio
import collections
from typing import Any, Awaitable, Callable, Iterable

import buelon.settings

# The fixed set the web UI may pick from. A free-text box has no upside here: there is
# no good reason to sample every 7 seconds, and a hand-typed `0` must not be reachable.
ALLOWED_INTERVALS = (1, 5, 10, 15, 30, 60)
DEFAULT_INTERVAL_MINUTES = 10

MAX_SAMPLES = 2000
MAX_AGE_DAYS = 7
MAX_AGE_SECONDS = MAX_AGE_DAYS * 24 * 60 * 60

# The append-only file is rewritten (trimmed) once it holds this many more lines than
# the store keeps. Rewriting on every sample would be silly; never rewriting would grow
# without bound.
TRIM_SLACK = 250

# How long the sampler sleeps between checks of "is a sample due yet". Shorter than any
# allowed interval, so an interval *change* is picked up within seconds instead of
# waiting out the old (possibly hour-long) cadence -- without restarting the process.
TICK_SECONDS = 5.0


def default_interval_minutes() -> int:
    """The default cadence, overridable by env like `BOO_WEB_PORT` / `BOO_WEB_HOST`.

    An explicit setting from the web UI wins over this -- see `HistoryStore.config`.
    """
    for var in ('BOO_WEB_HISTORY_INTERVAL', 'BUE_WEB_HISTORY_INTERVAL'):
        raw = os.environ.get(var)
        if raw is None:
            continue
        try:
            value = int(float(raw))
        except (TypeError, ValueError):
            continue
        if value in ALLOWED_INTERVALS:
            return value
    return DEFAULT_INTERVAL_MINUTES


def web_dir() -> str:
    """`.boo/web` (`.bue/web` pre-rename). Never hardcode either -- BUGS.md #55."""
    return os.path.join(buelon.settings.DIR_PATH, 'web')


class HistoryStore:
    """A bounded ring buffer of samples, backed by an append-only JSONL file.

    Reads come out of memory; the file exists so history survives a `boo web` restart.
    Nothing here is async: writes are small appends done from the sampler task, and the
    endpoints only read the deque.
    """

    def __init__(self, directory: str | None = None):
        self.directory = directory if directory is not None else web_dir()
        self.history_path = os.path.join(self.directory, 'history.jsonl')
        # Deliberately *not* settings.yaml: that file is hand-authored, comments and
        # all, and a web UI rewriting it would clobber the operator's work.
        self.config_path = os.path.join(self.directory, 'history-config.json')

        self.samples: collections.deque[dict] = collections.deque(maxlen=MAX_SAMPLES)
        self._lines_on_disk = 0
        self._interval_override: int | None = None

    # -- config ---------------------------------------------------------------

    @property
    def interval_minutes(self) -> int:
        return self._interval_override or default_interval_minutes()

    def config(self) -> dict:
        return {
            'interval_minutes': self.interval_minutes,
            'allowed_intervals': list(ALLOWED_INTERVALS),
            'default_interval_minutes': default_interval_minutes(),
            'explicit': self._interval_override is not None,
            'retention': {
                'max_samples': MAX_SAMPLES,
                'max_age_days': MAX_AGE_DAYS,
                'samples': len(self.samples),
            },
        }

    def load_config(self) -> None:
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (OSError, ValueError):
            return
        value = data.get('interval_minutes') if isinstance(data, dict) else None
        if value in ALLOWED_INTERVALS:
            self._interval_override = value

    def set_interval(self, minutes: Any) -> dict:
        """Persist a new cadence. Raises `ValueError` on anything outside the set."""
        try:
            value = int(minutes)
        except (TypeError, ValueError):
            raise ValueError(
                f'interval_minutes must be one of {list(ALLOWED_INTERVALS)}, got {minutes!r}'
            )
        if value not in ALLOWED_INTERVALS:
            raise ValueError(
                f'interval_minutes must be one of {list(ALLOWED_INTERVALS)}, got {value}'
            )

        self._interval_override = value
        os.makedirs(self.directory, exist_ok=True)
        tmp = self.config_path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump({'interval_minutes': value}, f)
        os.replace(tmp, self.config_path)
        return self.config()

    # -- samples --------------------------------------------------------------

    def load(self) -> None:
        """Read the file back on startup. A corrupt line is skipped, not fatal."""
        self.load_config()
        rows: list[dict] = []
        lines = 0
        try:
            with open(self.history_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    lines += 1
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(row, dict) and isinstance(row.get('ts'), (int, float)):
                        rows.append(row)
        except OSError:
            self._lines_on_disk = 0
            return

        rows.sort(key=lambda r: r['ts'])
        self._lines_on_disk = lines
        self.samples = collections.deque(self._retained(rows), maxlen=MAX_SAMPLES)
        # An oversized or stale file is compacted right away rather than waiting for
        # the first append -- a server restarted after two idle weeks should not carry
        # a file full of expired samples around until it happens to sample again.
        if self._lines_on_disk > len(self.samples) + TRIM_SLACK:
            self._rewrite()

    def _retained(self, rows: Iterable[dict]) -> list[dict]:
        rows = list(rows)[-MAX_SAMPLES:]
        cutoff = time.time() - MAX_AGE_SECONDS
        return [r for r in rows if r.get('ts', 0) >= cutoff]

    def add(self, sample: dict) -> dict:
        """Append one sample to memory and to disk. Never raises on an I/O problem."""
        self.samples.append(sample)
        self._expire()
        try:
            os.makedirs(self.directory, exist_ok=True)
            with open(self.history_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(sample) + '\n')
            self._lines_on_disk += 1
            if self._lines_on_disk > len(self.samples) + TRIM_SLACK:
                self._rewrite()
        except OSError as e:
            print(f'boo web: could not persist history sample: {e}', flush=True)
        return sample

    def _expire(self) -> None:
        cutoff = time.time() - MAX_AGE_SECONDS
        while self.samples and self.samples[0].get('ts', 0) < cutoff:
            self.samples.popleft()

    def _rewrite(self) -> None:
        """Trim the file to what the store keeps, by rewriting it."""
        try:
            os.makedirs(self.directory, exist_ok=True)
            tmp = self.history_path + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                for row in self.samples:
                    f.write(json.dumps(row) + '\n')
            os.replace(tmp, self.history_path)
            self._lines_on_disk = len(self.samples)
        except OSError as e:
            print(f'boo web: could not trim history file: {e}', flush=True)

    def window(self, limit: int | None = None, since: float | None = None) -> list[dict]:
        """Oldest-first samples, optionally only those after `since`, at most `limit`."""
        rows = list(self.samples)
        if since is not None:
            try:
                floor = float(since)
            except (TypeError, ValueError):
                floor = None
            if floor is not None:
                rows = [r for r in rows if r.get('ts', 0) > floor]
        if limit is not None:
            try:
                count = int(limit)
            except (TypeError, ValueError):
                count = 0
            if count > 0:
                rows = rows[-count:]
        return rows

    def payload(self, limit: int | None = None, since: float | None = None) -> dict:
        return {
            'samples': self.window(limit=limit, since=since),
            'config': self.config(),
            'server_time': time.time(),
            'next_sample_at': self.next_sample_at(),
        }

    def next_sample_at(self) -> float:
        if not self.samples:
            return time.time()
        return self.samples[-1]['ts'] + self.interval_minutes * 60

    # -- sampling -------------------------------------------------------------

    async def sample_once(self, fetch: Callable[[], Awaitable[dict]]) -> dict:
        """Take one sample. A failure is recorded as a sample carrying `error`.

        Recording the failure is the point: a gap in the record is information, and it
        should be *visible* as a gap rather than silently absent.
        """
        try:
            info = await fetch()
            counts = dict((info or {}).get('counts') or {})
            workers = (info or {}).get('workers')
            sample: dict = {'ts': time.time(), 'counts': counts}
            if isinstance(workers, dict):
                sample['workers'] = len(workers)
            return self.add(sample)
        except asyncio.CancelledError:
            raise
        except BaseException as e:  # the sampler must never take the server down
            return self.add({'ts': time.time(), 'error': f'{type(e).__name__}: {e}'})

    async def run(self, fetch: Callable[[], Awaitable[dict]]) -> None:
        """The sampler loop. Cancel it to stop; it swallows everything else.

        The cadence is re-read every tick, so an interval change from the web UI takes
        effect on the next tick instead of waiting out the old interval -- and existing
        history is kept when it changes. Mixed intervals in one series are fine: every
        sample carries its own `ts`, and no reader may assume even spacing.
        """
        await self.sample_once(fetch)  # a fresh server should not start out empty
        while True:
            try:
                await asyncio.sleep(TICK_SECONDS)
                if time.time() >= self.next_sample_at():
                    await self.sample_once(fetch)
            except asyncio.CancelledError:
                raise
            except BaseException as e:
                print(f'boo web: history sampler error: {e}', flush=True)
                await asyncio.sleep(TICK_SECONDS)
