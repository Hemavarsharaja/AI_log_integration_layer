"""
Requirement 3.2 — The Tripwire Logic (Decision Layer)

Stays passive until one of two condition classes fires:

  Metric Thresholds
  ─────────────────
  • CPU  > 90 %
  • RAM  > 95 %
  • Disk I/O wait spikes (configurable baseline)

  Log Keywords
  ────────────
  • FATAL | CRITICAL | PANIC | OOMKilled | CrashLoopBackOff

Non-Functional: 30-second cooldown (dampening / de-duplication).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# ── Thresholds ─────────────────────────────────────────────────────────────────
CPU_THRESHOLD_PCT  = 90.0
RAM_THRESHOLD_PCT  = 95.0
DISK_IO_WAIT_MS    = 200.0   # ms — spike baseline (configurable)

HIGH_SEVERITY_KEYWORDS = frozenset(
    {"FATAL", "CRITICAL", "PANIC", "OOMKILLED", "CRASHLOOPBACKOFF"}
)


@dataclass
class TriggerEvent:
    trigger_time: float
    reason: str         # human-readable explanation
    source: str         # "metric" | "log_keyword"


class TripwireEngine:
    """
    Evaluates each incoming log entry + current Prometheus metrics and
    fires when any threshold is breached.

    Parameters
    ----------
    cooldown_seconds : int
        Minimum gap between consecutive tripwire fires (NFR dampening).
    cpu_threshold : float
        CPU utilisation % above which the tripwire fires.
    ram_threshold : float
        RAM utilisation % above which the tripwire fires.
    disk_io_wait_ms : float
        Disk I/O wait time (ms) above which the tripwire fires.
    """

    def __init__(
        self,
        cooldown_seconds: int = 30,
        cpu_threshold: float = CPU_THRESHOLD_PCT,
        ram_threshold: float = RAM_THRESHOLD_PCT,
        disk_io_wait_ms: float = DISK_IO_WAIT_MS,
    ) -> None:
        self.cooldown_seconds  = cooldown_seconds
        self.cpu_threshold     = cpu_threshold
        self.ram_threshold     = ram_threshold
        self.disk_io_wait_ms   = disk_io_wait_ms

        self._last_fired_at: Optional[float] = None
        self._total_fires: int = 0

    # ── Public API ─────────────────────────────────────────────────────────────

    def evaluate(
        self,
        log_entry,                # log_buffer.LogEntry
        metrics: Dict[str, float],
    ) -> Tuple[bool, Optional[float]]:
        """
        Check *log_entry* and *metrics* for tripwire conditions.

        Returns
        -------
        (fired: bool, trigger_time: Optional[float])
            fired=True only if a condition was met AND we are outside cooldown.
        """
        now = time.time()

        # ── Cooldown guard ─────────────────────────────────────────────────────
        if self._in_cooldown(now):
            return False, None

        # ── Condition evaluation ───────────────────────────────────────────────
        event = self._check_metrics(metrics, now) or self._check_log(log_entry, now)
        if event is None:
            return False, None

        # ── Fire ───────────────────────────────────────────────────────────────
        self._last_fired_at = now
        self._total_fires  += 1
        logger.warning(
            "TRIPWIRE fired #%d | source=%s | reason=%s",
            self._total_fires,
            event.source,
            event.reason,
        )
        return True, event.trigger_time

    @property
    def in_cooldown(self) -> bool:
        return self._in_cooldown(time.time())

    @property
    def cooldown_remaining(self) -> float:
        if self._last_fired_at is None:
            return 0.0
        elapsed = time.time() - self._last_fired_at
        return max(0.0, self.cooldown_seconds - elapsed)

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _in_cooldown(self, now: float) -> bool:
        if self._last_fired_at is None:
            return False
        return (now - self._last_fired_at) < self.cooldown_seconds

    def _check_metrics(
        self, metrics: Dict[str, float], now: float
    ) -> Optional[TriggerEvent]:
        cpu  = metrics.get("cpu_utilization_pct", 0.0)
        ram  = metrics.get("memory_utilization_pct", 0.0)
        disk_wait = metrics.get("disk_io_wait_ms", 0.0)

        if cpu > self.cpu_threshold:
            return TriggerEvent(
                trigger_time=now,
                reason=f"CPU utilisation {cpu:.1f}% > {self.cpu_threshold}%",
                source="metric",
            )
        if ram > self.ram_threshold:
            return TriggerEvent(
                trigger_time=now,
                reason=f"RAM utilisation {ram:.1f}% > {self.ram_threshold}%",
                source="metric",
            )
        if disk_wait > self.disk_io_wait_ms:
            return TriggerEvent(
                trigger_time=now,
                reason=f"Disk I/O wait {disk_wait:.1f} ms > {self.disk_io_wait_ms} ms",
                source="metric",
            )
        return None

    def _check_log(self, log_entry, now: float) -> Optional[TriggerEvent]:
        """Check level field AND message body for high-severity keywords."""
        level = (log_entry.level or "").upper()
        message_upper = (log_entry.message or "").upper()

        # Direct level match
        if level in HIGH_SEVERITY_KEYWORDS:
            return TriggerEvent(
                trigger_time=now,
                reason=f"High-severity log level detected: {level}",
                source="log_keyword",
            )

        # Keyword scan in message body (e.g. CrashLoopBackOff inside a WARN line)
        for kw in HIGH_SEVERITY_KEYWORDS:
            if kw in message_upper:
                return TriggerEvent(
                    trigger_time=now,
                    reason=f"High-severity keyword '{kw}' found in log message",
                    source="log_keyword",
                )

        return None