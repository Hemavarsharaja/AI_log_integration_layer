"""
Integration & Async Pipeline Test Suite for the Guardian System
Covers: main.py background tasks, Prometheus polling fallbacks, 
and log streaming adapter execution modes.

Run with: pytest integration_tests.py -v
"""

import asyncio
import json
import os
import sys
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from log_buffer import LogEntry, SlidingWindowBuffer
from prometheus_poller import PrometheusPoller
from log_generator_adapter import LogGeneratorAdapter
import main  # Imports our modified main entry point with background metrics task

# Tell pytest-asyncio to treat the scope of async fixtures as a module
pytestmark = pytest.mark.asyncio


# ─────────────────────────────────────────────────────────────────────────────
# 1. Testing prometheus_poller.py (Fallback & Cache Behaviors)
# ─────────────────────────────────────────────────────────────────────────────

async def test_prometheus_poller_fallback_on_connection_error():
    """Verify that if Prometheus is offline, the poller returns non-tripping fallback numbers."""
    # Initialize with a dummy or broken URL
    poller = PrometheusPoller(prometheus_url="http://localhost:9999_broken_url")
    
    # Force a network call failure inside the fetch operation
    metrics = await poller.current_metrics()
    
    assert "cpu_utilization_pct" in metrics
    assert "memory_utilization_pct" in metrics
    assert metrics["cpu_utilization_pct"] == 0.0
    assert metrics["memory_utilization_pct"] == 0.0


async def test_prometheus_poller_cache_ttl():
    """Verify that the poller respects the 1-second TTL cache to prevent hammering the network."""
    poller = PrometheusPoller(prometheus_url="http://localhost:9090", cache_ttl=2.0)
    
    # Mock the internal instant query to see how many times it's executed
    with patch.object(poller, "_fetch_all", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = {"cpu_utilization_pct": 10.0}
        
        # Call it twice rapidly within the 2-second TTL window
        res1 = await poller.current_metrics()
        res2 = await poller.current_metrics()
        
        # It should only fetch from the network ONCE
        assert mock_fetch.call_count == 1
        assert res1 == res2


# ─────────────────────────────────────────────────────────────────────────────
# 2. Testing log_generator_adapter.py (Ingestion Modes)
# ─────────────────────────────────────────────────────────────────────────────

async def test_log_adapter_file_mode(tmp_path):
    """Verify file-tail mode correctly tracks lines as they append (simulating 'tail -f')."""
    # Setup a temporary test log file
    test_log_file = tmp_path / "test_app.log"
    test_log_file.write_text("12:00:00 - INFO - Initial background log\n")
    
    # Override environment settings using patch
    with patch("log_generator_adapter._LOG_SOURCE", "file"), \
         patch("log_generator_adapter._LOG_FILE_PATH", str(test_log_file)):
         
        adapter = LogGeneratorAdapter()
        stream_iterator = adapter.stream()
        
        # Grab the first line that already exists
        first_entry = await stream_iterator.__anext__()
        assert first_entry.message == "Initial background log"
        
        # Asynchronously append a new line while the generator is active
        test_log_file.write_text(test_log_file.read_text() + "12:00:01 - ERROR - Live crash event\n")
        
        # Give the 10ms file system sleep loop a moment to read
        second_entry = await stream_iterator.__anext__()
        assert second_entry.level == "ERROR"
        assert second_entry.message == "Live crash event"


# ─────────────────────────────────────────────────────────────────────────────
# 3. Testing main.py Async Orchestration & Background Tasks
# ─────────────────────────────────────────────────────────────────────────────

async def test_metrics_updater_background_task():
    """Verify that the metrics updater running task modifies the global shared metrics object."""
    mock_poller = MagicMock(spec=PrometheusPoller)
    mock_poller.current_metrics = AsyncMock(return_value={
        "cpu_utilization_pct": 45.0,
        "memory_utilization_pct": 55.0,
        "disk_pvc_usage_pct": 65.0,
        "disk_io_wait_ms": 5.0
    })
    
    stop_future = asyncio.get_running_loop().create_future()
    
    # Launch background updater task
    updater_task = asyncio.create_task(main.metrics_updater_task(mock_poller, stop_future))
    
    # Wait a tiny moment for the async loop iteration to trigger the first poll update
    await asyncio.sleep(0.1)
    
    try:
        # Check if global values were synchronized successfully
        assert main.shared_metrics["cpu_utilization_pct"] == 45.0
        assert main.shared_metrics["memory_utilization_pct"] == 55.0
    finally:
        # Gracefully kill the running task loop
        stop_future.set_result(None)
        await updater_task


async def test_delayed_incident_processing_timeline_sandwich():
    """Verify that when an incident is thrown, processing yields to allow fallout log collection."""
    # Setup mocks for all components wired inside main.py
    main.buffer = SlidingWindowBuffer(window_seconds=10, logs_per_second=10)
    main.chunker = MagicMock()
    main.chunker.post_seconds = 1  # 1-second short delay for fast test speed
    main.chunker.slice.return_value = []
    main.chunker.filter_noise.return_value = []
    
    main.serializer = MagicMock()
    main.serializer.build.return_value = {}
    
    main.reasoner = MagicMock()
    main.reasoner.analyse = AsyncMock(return_value={"root_cause": "Test Pass", "remediation": "No action"})
    
    start_time = time.monotonic()
    
    # Execute the delayed task that handles the fallout window delay
    await main.process_delayed_incident(time.time(), {})
    
    elapsed = time.monotonic() - start_time
    
    # Verify the timeline sandwich mechanism forced the loop to yield for the post_seconds fallout delay
    assert elapsed >= 1.0
    assert main.reasoner.analyse.call_count == 1