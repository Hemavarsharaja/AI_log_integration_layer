"""
Event-Driven Log Chunking & AI Reasoning Layer
Core Entry Point — Wires all components together, manages high-speed log loops,
and orchestrates decoupled asynchronous background tasks.
"""

import asyncio
import signal
import sys
import time
from log_buffer import SlidingWindowBuffer
from tripwire import TripwireEngine
from chunker import TimelineSandwichChunker
from serializer import ContextPackSerializer
from reasoning import ReasoningLayer
from promethus_poller import PrometheusPoller
from log_generator_adapter import LogGeneratorAdapter
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("main")

# Global thread-safe dictionary to share live metrics across async workers
shared_metrics = {
    "cpu_utilization_pct": 0.0,
    "memory_utilization_pct": 0.0,
    "disk_pvc_usage_pct": 0.0,
    "disk_io_wait_ms": 0.0
}


async def metrics_updater_task(poller: PrometheusPoller, stop_event: asyncio.Future):
    """
    Independent background worker task. Polls Prometheus periodically to avoid 
    clogging or lagging the main sequential log processing loop.
    """
    logger.info("Background Prometheus metrics updater task successfully initialized.")
    global shared_metrics
    while not stop_event.done():
        try:
            shared_metrics = await poller.current_metrics()
        except Exception as exc:
            logger.error("Error executing background Prometheus poll: %s", exc)
        
        # Maintain a steady 1-second resolution cache heartbeat
        await asyncio.sleep(1.0)


async def process_delayed_incident(
    trigger_time: float, 
    metrics_snapshot: dict,
    buffer: SlidingWindowBuffer,
    chunker: TimelineSandwichChunker,
    serializer: ContextPackSerializer,
    reasoner: ReasoningLayer
):
    """
    Asynchronous delayed worker. Yields control for post_seconds to allow the 
    sliding buffer to safely accumulate the incident's fallout logs before 
    generating the final Context Pack for the AI.
    """
    wait_duration = chunker.post_seconds
    logger.info("Holding execution for %.0fs to capture full fallout log timeline...", wait_duration)
    
    # Asynchronously sleep without freezing the main log stream ingestion
    await asyncio.sleep(wait_duration)
    
    # Freeze a snapshot of the buffer containing the complete history and fallout
    raw_slice = chunker.slice(buffer.snapshot(), trigger_time)
    filtered  = chunker.filter_noise(raw_slice)

    logger.info(
        "Context Sandwich Assembled: %d raw logs filtered down to %d critical lines",
        len(raw_slice),
        len(filtered),
    )

    # Serialize into the strict 1,000-token budget compliant Context Pack
    pack = serializer.build(
        logs=filtered,
        metrics=metrics_snapshot,
        trigger_time=trigger_time,
    )

    # Route through the Tiered Brain (Local Cache first, then Cloud/Local AI)
    result = await reasoner.analyse(pack)

    logger.info(
        "\n══════════════════════════════════════\n"
        "  Root Cause : %s\n"
        "  Remediation: %s\n"
        "══════════════════════════════════════",
        result["root_cause"],
        result["remediation"],
    )


async def main():
    logger.info("🚀 Starting Event-Driven Log Chunking & AI Reasoning Layer")
    global shared_metrics

    # ── Component initialization ──────────────────────────────────────────────
    buffer        = SlidingWindowBuffer(window_seconds=180, logs_per_second=120)
    tripwire      = TripwireEngine(cooldown_seconds=30)
    chunker       = TimelineSandwichChunker(pre_seconds=60, post_seconds=30)
    serializer    = ContextPackSerializer()
    reasoner      = ReasoningLayer()
    prom_poller   = PrometheusPoller()
    log_adapter   = LogGeneratorAdapter()

    # ── Safe Multi-Platform Shutdown Handler ──────────────────────────────────
    loop = asyncio.get_running_loop()
    stop = loop.create_future()

    def _handle_signal():
        logger.info("Shutdown signal received — stopping pipeline cleanly.")
        if not stop.done():
            stop.set_result(None)

    # Windows alternative fallback workaround vs. POSIX signal listeners
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_signal)
    else:
        logger.info("Running on Windows host: Overriding loop signals with Ctrl+C interception handler.")

    # ── Start Background Tasks ────────────────────────────────────────────────
    updater_task = asyncio.create_task(metrics_updater_task(prom_poller, stop))

    # ── Main Ingestion Pipeline Coroutine ─────────────────────────────────────
    async def pipeline():
        global shared_metrics
        try:
            async for log_entry in log_adapter.stream():
                # 1. Store every incoming log at native speed (120 logs/sec)
                buffer.push(log_entry)

                # 2. Evaluate tripwire rules instantaneously using cached background metrics
                fired, trigger_time = tripwire.evaluate(log_entry, shared_metrics)

                if not fired:
                    continue

                logger.warning(
                    "⚡ Tripwire fired at %s — scheduling event-driven contextual analysis job.", trigger_time
                )

                # 3. Fire-and-forget the incident processing asynchronously to avoid stalling ingestion
                asyncio.create_task(
                    process_delayed_incident(
                        trigger_time, 
                        shared_metrics.copy(), # Deep copy snapshot to preserve exact metrics state at time T
                        buffer, 
                        chunker, 
                        serializer, 
                        reasoner
                    )
                )
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.critical("Fatal crash inside the log pipeline worker stream: %s", exc)

    pipeline_task = asyncio.create_task(pipeline())

    # ── Process Lifespan Resolution ───────────────────────────────────────────
    await stop
    
    # Graceful teardown of running asynchronous components
    pipeline_task.cancel()
    updater_task.cancel()
    
    try:
        await asyncio.gather(pipeline_task, updater_task, return_exceptions=True)
    except asyncio.CancelledError:
        pass

    logger.info("All pipeline loops and background tasks shut down cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Console interrupted — execution stopped.")
        sys.exit(0)