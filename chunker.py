"""
Requirement 3.3 — The "Timeline Sandwich" Chunking Strategy

When a tripwire fires at time T the chunker:

  1. Extracts the window  [T − pre_seconds, T + post_seconds]
     (default: −60 s … +30 s = 90-second slice)
  2. Filters out all INFO and DEBUG entries.
  3. Keeps only WARN | ERROR | FATAL | CRITICAL | PANIC lines,
     targeting < 50 critical lines per incident.

The 90-second raw window at 120 logs/s = 10,800 raw entries.
After noise filtration this should reduce to a dense packet of
under 50 lines in the typical case.
"""

from __future__ import annotations

from typing import List
import logging

from log_buffer import LogEntry

logger = logging.getLogger(__name__)

# Only these levels survive noise filtration
PERMITTED_LEVELS = frozenset({"WARN", "WARNING", "ERROR", "FATAL", "CRITICAL", "PANIC"})

# Absolute cap to stay inside the 1,000-token LLM budget (NFR §4)
MAX_CRITICAL_LINES = 50


class TimelineSandwichChunker:
    """
    Slices a specific portion of the sliding-window snapshot around
    the trigger point and strips low-severity noise.

    Parameters
    ----------
    pre_seconds : int
        Seconds *before* T to include (captures the cause). Default 60.
    post_seconds : int
        Seconds *after* T to include (captures the fallout). Default 30.
    """

    def __init__(self, pre_seconds: int = 60, post_seconds: int = 30) -> None:
        self.pre_seconds  = pre_seconds
        self.post_seconds = post_seconds

    # ── Public API ─────────────────────────────────────────────────────────────

    def slice(
        self,
        snapshot: List[LogEntry],
        trigger_time: float,
    ) -> List[LogEntry]:
        """
        Extract entries within [T − pre, T + post] from a buffer snapshot.

        Parameters
        ----------
        snapshot : list[LogEntry]
            Full ordered copy of the sliding window (oldest → newest).
        trigger_time : float
            Unix epoch of the tripwire event (T).

        Returns
        -------
        list[LogEntry]
            Raw (un-filtered) entries in the 90-second window.
        """
        start = trigger_time - self.pre_seconds
        end   = trigger_time + self.post_seconds

        raw_slice = [e for e in snapshot if start <= e.timestamp <= end]

        logger.debug(
            "Timeline slice [T-%.0fs … T+%.0fs]: %d raw entries extracted",
            self.pre_seconds,
            self.post_seconds,
            len(raw_slice),
        )
        return raw_slice

    def filter_noise(self, raw_slice: List[LogEntry]) -> List[LogEntry]:
        """
        Remove INFO and DEBUG entries; keep only WARN/ERROR/FATAL/CRITICAL/PANIC.
        Also enforces the MAX_CRITICAL_LINES cap (most-recent lines preferred).

        Returns
        -------
        list[LogEntry]
            Dense packet of ≤ 50 critical log lines.
        """
        filtered = [
            e for e in raw_slice
            if e.level.upper() in PERMITTED_LEVELS
        ]

        if len(filtered) > MAX_CRITICAL_LINES:
            logger.debug(
                "Filtered slice has %d entries — truncating to last %d (most recent).",
                len(filtered),
                MAX_CRITICAL_LINES,
            )
            # Prefer the most-recent lines (most relevant to the incident)
            filtered = filtered[-MAX_CRITICAL_LINES:]

        logger.info(
            "Noise filtration: %d raw → %d critical lines",
            len(raw_slice),
            len(filtered),
        )
        return filtered

    def process(
        self,
        snapshot: List[LogEntry],
        trigger_time: float,
    ) -> List[LogEntry]:
        """Convenience: slice + filter in one call."""
        return self.filter_noise(self.slice(snapshot, trigger_time))