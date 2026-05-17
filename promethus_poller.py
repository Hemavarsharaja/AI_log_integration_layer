"""
Prometheus Metrics Poller — Container Aligned Configuration
Complete Production-Ready Implementation
"""

from __future__ import annotations

import os
import time
from typing import Dict, Optional
import logging

import httpx

logger = logging.getLogger(__name__)

_PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090")

# Aligned PromQL expressions mapping container level metrics populated by cAdvisor
_QUERIES: Dict[str, str] = {
    "cpu_utilization_pct": os.getenv(
        "PROM_CPU_QUERY",
        "sum(rate(container_cpu_usage_seconds_total{container_name!=''}[1m])) * 100",
    ),
    "memory_utilization_pct": os.getenv(
        "PROM_RAM_QUERY",
        "(sum(container_memory_usage_bytes{container_name!=''}) / sum(machine_memory_bytes)) * 100",
    ),
    "disk_pvc_usage_pct": os.getenv(
        "PROM_DISK_QUERY",
        "sum(container_fs_usage_bytes{container_name!=''}) / sum(container_fs_limit_bytes{container_name!=''}) * 100",
    ),
    "disk_io_wait_ms": os.getenv(
        "PROM_IO_QUERY",
        "sum(rate(container_fs_reads_total[1m]) + rate(container_fs_writes_total[1m]))",
    ),
}

_CACHE_TTL = 1.0


class PrometheusPoller:
    """
    Async Prometheus API client adapter. Extracts container cluster utilization 
    gauges with built-in localized fallback capabilities for safe standalone development runs.
    """

    def __init__(
        self,
        prometheus_url: str = _PROMETHEUS_URL,
        cache_ttl: float = _CACHE_TTL,
    ) -> None:
        self._url       = prometheus_url.rstrip("/")
        self._cache_ttl = cache_ttl
        self._cache: Optional[Dict[str, float]] = None
        self._cache_ts: float = 0.0

    async def current_metrics(self) -> Dict[str, float]:
        """
        Returns latest container environment metrics, wrapping calls inside a 
        high-efficiency local 1-second interval timestamp cache.
        """
        now = time.monotonic()
        if self._cache and (now - self._cache_ts) < self._cache_ttl:
            return self._cache

        metrics = await self._fetch_all()
        self._cache    = metrics
        self._cache_ts = now
        return metrics

    # ── In-Process Network Query Implementations ────────────────────────────────

    async def _fetch_all(self) -> Dict[str, float]:
        """Sequentially triggers network fetches for all operational metric expressions."""
        results: Dict[str, float] = {}
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                for key, query in _QUERIES.items():
                    value = await self._instant_query(client, query)
                    results[key] = value
        except Exception as exc:
            logger.warning("Prometheus endpoint unreachable (%s) — using static fallback arrays.", exc)
            results = self._fallback_metrics()
        return results

    async def _instant_query(self, client: httpx.AsyncClient, query: str) -> float:
        """Executes instant-query operations against target Prometheus HTTP APIs."""
        resp = await client.get(
            f"{self._url}/api/v1/query",
            params={"query": query},
        )
        resp.raise_for_status()
        data = resp.json()
        result = data.get("data", {}).get("result", [])
        if not result:
            return 0.0
        
        # Unpack standard instant-query response coordinates: [timestamp, string_value]
        return float(result[0]["value"][1])

    @staticmethod
    def _fallback_metrics() -> Dict[str, float]:
        """
        Synthetic non-tripping baseline limits preventing false positives when 
        running components outside a live cluster ecosystem.
        """
        return {
            "cpu_utilization_pct":    0.0,
            "memory_utilization_pct": 0.0,
            "disk_pvc_usage_pct":     0.0,
            "disk_io_wait_ms":        0.0,
        }