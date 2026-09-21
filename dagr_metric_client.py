"""A MetricClient that writes measurements to a Dagr stream instead of a multiprocessing.Queue.

Each producing process (API server, model worker) owns one log file, `meas-<pid>.dagr` in the
directory given by MAX_SERVE_MEASUREMENT_LOG. Instrument names and attribute sets are interned
on first use, so a measurement on the wire is (ts_ns, name_id, attrs_id, value).

Measurements are buffered and handed to Mojo in batches: one extension call per flush instead
of one per measurement. A flush happens when the buffer reaches MAX_SERVE_MEASUREMENT_BATCH
(default 64), when a transaction closes, or when MAX_SERVE_MEASUREMENT_MAX_DELAY_S has passed
(default 0.05). A background flusher thread enforces that delay even when nothing else is
appended: without it a slowly-sampled instrument's last measurement waits for the next append,
which is how two once-per-second API samplers lost their final sample from a scrape taken while
the server was idle. Appends and flushes share a lock so a flush cannot swap the buffer out
from under an append.
"""

from __future__ import annotations

import functools
import os
import sys
import threading
import time
from contextlib import asynccontextmanager, contextmanager

from max.serve.telemetry.metrics import MaxMeasurement, MetricClient


class DagrMetricClient(MetricClient):
    def __init__(self, directory: str, module_dir: str | None = None) -> None:
        self.directory = directory
        self.module_dir = module_dir or os.path.dirname(os.path.abspath(__file__))
        self.batch_size = int(os.environ.get("MAX_SERVE_MEASUREMENT_BATCH", "64"))
        self.max_delay_s = float(os.environ.get("MAX_SERVE_MEASUREMENT_MAX_DELAY_S", "0.05"))
        # Writing rows into the mapping is not the same as msyncing it: batching keeps the
        # telemetry side within 50 ms, while an msync (only when DAGR_LOG_MSYNC=1) is worth
        # far less often.
        self.msync_s = float(os.environ.get("MAX_SERVE_MEASUREMENT_MSYNC_S", "1.0"))
        self._last_msync = time.monotonic()
        self._log = None
        self._rows: list[tuple] = []
        self._names: dict[str, int] = {}
        self._attrs: dict[tuple, int] = {}
        self._attrs_by_id: dict[int, tuple[dict, int]] = {}   # identity fast path
        self._depth = 0
        self._last_flush = time.monotonic()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._flusher = threading.Thread(target=self._flush_loop, daemon=True,
                                         name="dagr-metric-flush")
        self._flusher.start()

    # -- lifecycle ---------------------------------------------------------------
    def _open(self):
        if self._log is None:
            sys.path.insert(0, self.module_dir)
            import importlib

            metrics_log = importlib.import_module("metrics_log")
            os.makedirs(self.directory, exist_ok=True)
            pid = os.getpid()
            self._log = metrics_log.MeasurementLog(
                os.path.join(self.directory, f"meas-{pid}.dagr"), f"max-serve pid={pid}", pid
            )
        return self._log

    def close(self) -> int:
        self._stop.set()
        if self._log is None:
            return 0
        with self._lock:
            self._flush()
        n = self._log.close()
        self._log = None
        return n

    # -- interning ---------------------------------------------------------------
    def _name_id(self, name: str) -> int:
        nid = self._names.get(name)
        if nid is None:
            nid = len(self._names)
            self._names[name] = nid
            self._open().define_name(nid, name)
        return nid

    def _attrs_id(self, attrs) -> int:
        if not attrs:
            return self._attr_set_id(())
        cached = self._attrs_by_id.get(id(attrs))
        if cached is not None and cached[0] is attrs:
            return cached[1]                    # the same dict object as last time
        key = tuple(sorted((str(k), str(v)) for k, v in attrs.items()))
        aid = self._attr_set_id(key)
        self._attrs_by_id[id(attrs)] = (attrs, aid)
        return aid

    def _attr_set_id(self, key: tuple) -> int:
        aid = self._attrs.get(key)
        if aid is None:
            aid = len(self._attrs)
            self._attrs[key] = aid
            flat = [s for pair in key for s in pair]
            self._open().define_attrs(aid, flat)
        return aid

    # -- MetricClient ------------------------------------------------------------
    def send_measurement(self, m: MaxMeasurement) -> None:
        with self._lock:
            self._rows.append(
                (m.time_unix_nano, self._name_id(m.instrument_name),
                 self._attrs_id(m.attributes), float(m.value))
            )
            if self._depth > 0:
                return
            if (len(self._rows) >= self.batch_size
                    or time.monotonic() - self._last_flush >= self.max_delay_s):
                self._flush()

    def _flush_loop(self) -> None:
        """Flush on a clock, so an idle producer's last rows still reach the stream."""
        while not self._stop.wait(self.max_delay_s):
            with self._lock:
                if self._rows and time.monotonic() - self._last_flush >= self.max_delay_s:
                    self._flush()

    @contextmanager
    def transaction(self):
        with self._lock:
            self._depth += 1
        try:
            yield
        finally:
            with self._lock:
                self._depth -= 1
                if self._depth == 0 and self._rows:
                    self._flush()

    def _flush(self) -> None:
        """Caller holds the lock."""
        if not self._rows:
            return
        rows, self._rows = self._rows, []
        log = self._open()
        log.append_rows(rows)
        now = time.monotonic()
        if now - self._last_msync >= self.msync_s:
            log.flush()      # a no-op unless DAGR_LOG_MSYNC=1; then an msync
            self._last_msync = now
        self._last_flush = now

    def cross_process_factory(self, settings):
        # Each process writes its own file; the child builds a fresh client.
        return functools.partial(_child_client, self.directory, self.module_dir)

    def __getstate__(self):
        return {"directory": self.directory, "module_dir": self.module_dir}

    def __setstate__(self, state):
        self.__init__(state["directory"], state["module_dir"])


@asynccontextmanager
async def _child_client(directory: str, module_dir: str):
    client = DagrMetricClient(directory, module_dir)
    try:
        yield client
    finally:
        client.close()
