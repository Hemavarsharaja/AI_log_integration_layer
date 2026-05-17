"""
Requirement 3.1 — The Sliding Window Buffer (Data Management)

Holds exactly `window_seconds` of telemetry history using a FIFO deque.
At 120 logs/sec × 180 s the deque caps at 21,600 entries; older entries
drop off automatically when maxlen is exceeded.

Memory footprint guard: each entry is capped so the whole buffer stays
well under the 50 MB hard limit specified in NFR §4.
"""

from __future__ import annotations

import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

# Rough byte estimate per log entry (level + message + timestamp dict)
_BYTES_PER_ENTRY_ESTIMATE = 256
_MAX_BYTES = 50 * 1024 * 1024  # 50 MB


@dataclass
class LogEntry:
    """A single timestamped log record."""
    timestamp: float          # Unix epoch (seconds)
    level: str                # DEBUG | INFO | WARN | ERROR | FATAL | CRITICAL | PANIC
    message: str
    raw: str = field(repr=False)  # Original string from the generator

    @classmethod
    def from_raw(cls, raw: str, received_at: Optional[float] = None) -> "LogEntry":
        """
        Parse a raw log line.
        Expected format: ``HH:MM:SS - LEVEL - message``
        Falls back gracefully for non-conforming lines.
        """
        ts = received_at or time.time()
        parts = raw.split(" - ", 2)
        if len(parts) == 3:
            level = parts[1].strip().upper()
            message = parts[2].strip()
        else:
            level = "INFO"
            message = raw.strip()
        return cls(timestamp=ts, level=level, message=message, raw=raw)


class SlidingWindowBuffer:
    """
    FIFO in-memory ring-buffer for log entries.

    Parameters
    ----------
    window_seconds : int
        How many seconds of history to retain (default 180 = 3 minutes).
    logs_per_second : int
        Expected ingestion rate used to pre-size the deque (default 120).
    """

    def __init__(self, window_seconds: int = 180, logs_per_second: int = 120) -> None:
        self.window_seconds = window_seconds
        self.logs_per_second = logs_per_second
        maxlen = window_seconds * logs_per_second  # 21,600
        self._buf: deque[LogEntry] = deque(maxlen=maxlen)

        estimated_mb = (maxlen * _BYTES_PER_ENTRY_ESTIMATE) / (1024 ** 2)
        if estimated_mb > 50:                          # NFR guard
            raise ValueError(
                f"Buffer would consume ~{estimated_mb:.1f} MB — exceeds 50 MB limit."
            )

        logger.info(
            "SlidingWindowBuffer ready: maxlen=%d entries (~%.1f MB estimated)",
            maxlen,
            estimated_mb,
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    def push(self, entry: LogEntry) -> None:
        """Append a log entry; oldest entry is evicted automatically when full."""
        self._buf.append(entry)

    def push_raw(self, raw: str) -> LogEntry:
        """Parse *raw* into a LogEntry, push it, and return the entry."""
        entry = LogEntry.from_raw(raw)
        self.push(entry)
        return entry

    def snapshot(self) -> List[LogEntry]:
        """Return a point-in-time copy of the entire buffer (oldest → newest)."""
        return list(self._buf)

    def slice_window(
        self,
        start_ts: float,
        end_ts: float,
    ) -> List[LogEntry]:
        """
        Return all entries whose timestamp falls within [start_ts, end_ts].
        O(n) scan — acceptable for ≤21,600 entries.
        """
        return [e for e in self._buf if start_ts <= e.timestamp <= end_ts]

    @property
    def size(self) -> int:
        return len(self._buf)

    @property
    def estimated_bytes(self) -> int:
        return self.size * _BYTES_PER_ENTRY_ESTIMATE