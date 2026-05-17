"""
Requirement 3.4 — Data Serialization (The Context Pack)

Merges filtered log lines and live Prometheus metrics into the
minified JSON structure required by the PRD:

{
    "incident_id":            "UUID-string",
    "timestamp_of_incident":  "ISO-8601",
    "prometheus_metrics": {
        "cpu_utilization_pct":    <float>,
        "memory_utilization_pct": <float>,
        "disk_pvc_usage_pct":     <float>
    },
    "filtered_error_logs": [ "<HH:MM:SS - LEVEL - message>", … ]
}
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List
import logging

from log_buffer import LogEntry

logger = logging.getLogger(__name__)

# Token budget guard — serialized JSON must stay under 1,000 tokens.
# Rough heuristic: 1 token ≈ 4 chars.
MAX_PAYLOAD_CHARS = 4_000


class ContextPackSerializer:
    """Builds and validates the Context Pack JSON payload."""

    # ── Public API ─────────────────────────────────────────────────────────────

    def build(
        self,
        logs: List[LogEntry],
        metrics: Dict[str, float],
        trigger_time: float,
    ) -> Dict:
        """
        Construct the Context Pack dictionary.

        Parameters
        ----------
        logs : list[LogEntry]
            Noise-filtered critical log entries (≤ 50 lines).
        metrics : dict
            Live Prometheus snapshot with keys:
            cpu_utilization_pct, memory_utilization_pct,
            disk_pvc_usage_pct  (and optionally disk_io_wait_ms).
        trigger_time : float
            Unix epoch of the tripwire event.

        Returns
        -------
        dict
            Ready-to-serialise Context Pack.
        """
        iso_ts = datetime.fromtimestamp(trigger_time, tz=timezone.utc).isoformat()

        pack = {
            "incident_id":           str(uuid.uuid4()),
            "timestamp_of_incident": iso_ts,
            "prometheus_metrics": {
                "cpu_utilization_pct":    round(metrics.get("cpu_utilization_pct", 0.0), 2),
                "memory_utilization_pct": round(metrics.get("memory_utilization_pct", 0.0), 2),
                "disk_pvc_usage_pct":     round(metrics.get("disk_pvc_usage_pct", 0.0), 2),
            },
            "filtered_error_logs": self._format_logs(logs),
        }

        self._validate_token_budget(pack)
        return pack

    def to_json(self, pack: Dict, pretty: bool = False) -> str:
        """Serialise a Context Pack to a JSON string."""
        if pretty:
            return json.dumps(pack, indent=2)
        return json.dumps(pack, separators=(",", ":"))   # minified

    # ── Internals ──────────────────────────────────────────────────────────────

    @staticmethod
    def _format_logs(logs: List[LogEntry]) -> List[str]:
        """
        Convert LogEntry objects into the PRD-specified string format:
        ``HH:MM:SS - LEVEL - message``
        """
        formatted = []
        for e in logs:
            # Re-format timestamp from Unix epoch → HH:MM:SS
            ts_str = datetime.fromtimestamp(
                e.timestamp, tz=timezone.utc
            ).strftime("%H:%M:%S")
            formatted.append(f"{ts_str} - {e.level} - {e.message}")
        return formatted

    def _validate_token_budget(self, pack: Dict) -> None:
        """
        Warn (and truncate log list if needed) to stay under 1,000 tokens.
        """
        serialized = self.to_json(pack)
        char_count  = len(serialized)

        if char_count <= MAX_PAYLOAD_CHARS:
            logger.debug("Context Pack size: %d chars — within token budget.", char_count)
            return

        # Trim log lines one-by-one from the oldest end until we fit
        logs = pack["filtered_error_logs"]
        while len(serialized) > MAX_PAYLOAD_CHARS and logs:
            logs.pop(0)
            serialized = self.to_json(pack)

        logger.warning(
            "Payload exceeded token budget — trimmed to %d log lines (%d chars).",
            len(logs),
            len(serialized),
        )