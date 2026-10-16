"""
scripts/benchmark_resources.py: Resource usage profiler for Criterion 8.
Runs the complete MayaCardio data pipeline in online mode for a given duration,
accurately measuring:
- Average CPU utilization (%)
- Peak memory usage (RSS in MB)
- Total disk data written (MB)
- Total network data sent (MB)
"""

import argparse
import asyncio
import json
import os
import platform
import subprocess
import sys
import threading
import time
import psutil

# Ensure workspace root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from biosignal_mock.mock_service import BiosignalMockServer
from cloud.server import HTTPServer, CloudRequestHandler, CloudDatabase
from pipeline.storage import StorageEngine
from pipeline.loss_tracker import LossTracker
from pipeline.collector import BiosignalCollector
from pipeline.state_machine import PipelineStateMachine
from pipeline.sync_client import CloudSyncClient


def get_hardware_info():
    uname = platform.uname()
    mem = psutil.virtual_memory()
    return {
        "system": uname.system,
        "machine": uname.machine,
        "processor": uname.processor or uname.machine,
        "cpu_cores_logical": psutil.cpu_count(logical=True),
        "cpu_cores_physical": psutil.cpu_count(logical=False),
        "total_ram_gb": round(mem.total / (1024**3), 2),
        "python_version": platform.python_version(),
    }


async def run_benchmark(duration_sec: int, db_path: str, mock_port: int, cloud_port: int):
    print(f"Starting Resource Benchmark for {duration_sec} seconds...")
    hw_info = get_hardware_info()
    print("Environment:", json.dumps(hw_info, indent=2))

    # 1. Start Cloud Server in thread
    cloud_db_path = db_path + ".cloud.db"
    cloud_db = CloudDatabase(cloud_db_path)
    CloudRequestHandler.db = cloud_db
    CloudRequestHandler.start_time = time.time()
    cloud_server = HTTPServer(("127.0.0.1", cloud_port), CloudRequestHandler)
    cloud_thread = threading.Thread(target=cloud_server.serve_forever, daemon=True)
    cloud_thread.start()

    # 2. Start Mock Server in task
    mock_server = BiosignalMockServer(
        mode="signal",
        seed=7,
        duration=duration_sec + 60,
        port=mock_port,
        inject_faults=False,
    )
    mock_task = asyncio.create_task(mock_server.run())
    await asyncio.sleep(0.2)

    # 3. Setup Pipeline Components
    storage = StorageEngine(db_path)
    loss_tracker = LossTracker(storage)
    collector = BiosignalCollector(storage, loss_tracker, host="127.0.0.1", port=mock_port)
    state_machine = PipelineStateMachine(cloud_url=f"http://127.0.0.1:{cloud_port}")
    sync_client = CloudSyncClient(storage, state_machine, cloud_url=f"http://127.0.0.1:{cloud_port}")

    # Set state machine to online immediately
    await state_machine.probe_once()
    await state_machine.update_state(True)

    state_machine.start()
    sync_client.start()
    collector_task = asyncio.create_task(collector.run_loop())

    # Measurement tracking
    process = psutil.Process(os.getpid())
    cpu_measurements = []
    peak_rss_bytes = 0

    io_start = process.io_counters() if hasattr(process, "io_counters") else None
    net_start = psutil.net_io_counters()

    t_start = time.time()
    next_tick = t_start + 1.0

    while time.time() - t_start < duration_sec:
        await asyncio.sleep(max(0.1, next_tick - time.time()))
        next_tick += 1.0

        # Sample metrics
        cpu_pct = process.cpu_percent(interval=None)
        mem_info = process.memory_info()
        rss = mem_info.rss

        cpu_measurements.append(cpu_pct)
        if rss > peak_rss_bytes:
            peak_rss_bytes = rss

        elapsed = int(time.time() - t_start)
        if elapsed % 10 == 0:
            saved = storage.count_samples()
            synced = storage.count_synced_samples()
            print(f"[{elapsed}s / {duration_sec}s] CPU: {cpu_pct:.1f}% | RAM: {rss / (1024**2):.1f} MB | Samples Saved: {saved} | Synced: {synced}")

    t_end = time.time()
    actual_duration = t_end - t_start

    # Final measurements
    io_end = process.io_counters() if hasattr(process, "io_counters") else None
    net_end = psutil.net_io_counters()

    disk_write_mb = 0.0
    if io_start and io_end:
        disk_write_bytes = io_end.write_bytes - io_start.write_bytes
        disk_write_mb = disk_write_bytes / (1024**2)

    net_sent_mb = 0.0
    if net_start and net_end:
        # Loopback / process net approximation
        net_sent_bytes = net_end.bytes_sent - net_start.bytes_sent
        net_sent_mb = net_sent_bytes / (1024**2)

    avg_cpu = sum(cpu_measurements) / len(cpu_measurements) if cpu_measurements else 0.0
    peak_ram_mb = peak_rss_bytes / (1024**2)

    # Clean shutdown
    collector.stop()
    sync_client.stop()
    state_machine.stop()
    collector_task.cancel()
    mock_task.cancel()
    cloud_server.shutdown()
    cloud_server.server_close()
    storage.close()
    cloud_db.close()

    result = {
        "duration_seconds": round(actual_duration, 1),
        "hardware": hw_info,
        "metrics": {
            "average_cpu_percent": round(avg_cpu, 2),
            "peak_memory_rss_mb": round(peak_ram_mb, 2),
            "disk_written_mb": round(disk_write_mb, 2),
            "network_sent_mb": round(net_sent_mb, 2),
        },
        "extrapolated_1_hour": {
            "disk_written_1hr_mb": round(disk_write_mb * (3600 / max(1, actual_duration)), 2),
            "network_sent_1hr_mb": round(net_sent_mb * (3600 / max(1, actual_duration)), 2),
        },
    }

    print("\n" + "=" * 50)
    print("MAYA CARDIO BENCHMARK RESULTS")
    print("=" * 50)
    print(json.dumps(result, indent=2))

    with open("benchmark_results.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    return result


def main():
    parser = argparse.ArgumentParser(description="MayaCardio Resource Profiler")
    parser.add_argument("--duration", type=int, default=30, help="Benchmark run duration in seconds")
    parser.add_argument("--db", type=str, default="benchmark_pipeline.db", help="Path to DB")
    parser.add_argument("--mock-port", type=int, default=7155, help="Mock port")
    parser.add_argument("--cloud-port", type=int, default=8099, help="Cloud port")
    args = parser.parse_args()

    asyncio.run(run_benchmark(args.duration, args.db, args.mock_port, args.cloud_port))


if __name__ == "__main__":
    main()
