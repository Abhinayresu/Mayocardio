"""
pipeline/cli.py: Main CLI entrypoint for MayaCardio pipeline.
Implements required commands:
- pipeline status
- pipeline export --session ID --from device|cloud
- pipeline results --session ID
- pipeline run (starts collector, state machine, and sync client)
"""

import argparse
import asyncio
import json
import os
import signal
import sys
import urllib.request
from typing import Optional
from pipeline.storage import StorageEngine
from pipeline.loss_tracker import LossTracker
from pipeline.collector import BiosignalCollector
from pipeline.state_machine import PipelineStateMachine
from pipeline.sync_client import CloudSyncClient

DEFAULT_DB_PATH = os.environ.get("PIPELINE_DB_PATH", "pipeline_data.db")
DEFAULT_MOCK_HOST = os.environ.get("MOCK_HOST", "127.0.0.1")
DEFAULT_MOCK_PORT = int(os.environ.get("MOCK_PORT", "7100"))
DEFAULT_CLOUD_URL = os.environ.get("CLOUD_URL", "http://127.0.0.1:8080")


def cmd_status(args):
    storage = StorageEngine(args.db)
    saved_count = storage.count_samples()
    synced_count = storage.count_synced_samples()
    backlog = max(0, saved_count - synced_count)

    # Check cloud status
    current_mode = "offline"
    samples_in_cloud = synced_count
    try:
        req = urllib.request.Request(f"{args.cloud_url.rstrip('/')}/api/v1/status", method="GET")
        with urllib.request.urlopen(req, timeout=1.5) as resp:
            if resp.status == 200:
                current_mode = "online"
                cloud_data = json.loads(resp.read().decode("utf-8"))
                samples_in_cloud = cloud_data.get("total_samples", synced_count)
    except Exception:
        current_mode = "offline"

    storage.close()

    status_obj = {
        "mode": current_mode,
        "samples_saved": saved_count,
        "samples_in_cloud": samples_in_cloud,
        "backlog": backlog,
    }
    # Exactly one JSON line
    print(json.dumps(status_obj))


def cmd_export(args):
    session_id = args.session
    source = args.source.lower()

    if source == "device":
        storage = StorageEngine(args.db)
        lines = storage.export_session_ndjson(session_id)
        storage.close()
        for line in lines:
            print(line)

    elif source == "cloud":
        try:
            req = urllib.request.Request(
                f"{args.cloud_url.rstrip('/')}/api/v1/export?session_id={session_id}", method="GET"
            )
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                content = resp.read().decode("utf-8")
                for line in content.splitlines():
                    if line.strip():
                        print(line.strip())
        except Exception as e:
            sys.stderr.write(f"Error querying cloud export: {e}\n")
            sys.exit(1)
    else:
        sys.stderr.write(f"Unknown source '{args.source}'. Must be 'device' or 'cloud'.\n")
        sys.exit(1)


def cmd_results(args):
    session_id = args.session
    # Try cloud first if reachable
    cloud_results = []
    try:
        req = urllib.request.Request(
            f"{args.cloud_url.rstrip('/')}/api/v1/results?session_id={session_id}", method="GET"
        )
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            if resp.status == 200:
                content = resp.read().decode("utf-8")
                cloud_results = [l.strip() for l in content.splitlines() if l.strip()]
    except Exception:
        pass

    if cloud_results:
        for line in cloud_results:
            print(line)
        return

    # Fallback to device offline results
    storage = StorageEngine(args.db)
    lines = storage.get_window_results(session_id)
    storage.close()
    for line in lines:
        print(line)


def cmd_reconcile(args):
    storage = StorageEngine(args.db)
    state_machine = PipelineStateMachine(cloud_url=args.cloud_url)
    sync_client = CloudSyncClient(storage=storage, state_machine=state_machine, cloud_url=args.cloud_url)
    report = sync_client.reconcile_with_cloud(args.session)
    storage.close()
    print(json.dumps(report))
    if not report.get("reconciled"):
        sys.exit(1)


async def async_run_pipeline(args):
    storage = StorageEngine(args.db)
    loss_tracker = LossTracker(storage)

    collector = BiosignalCollector(
        storage=storage,
        loss_tracker=loss_tracker,
        host=args.mock_host,
        port=args.mock_port,
    )

    state_machine = PipelineStateMachine(
        cloud_url=args.cloud_url,
        probe_interval_sec=2.0,
        probe_timeout_sec=1.5,
    )

    sync_client = CloudSyncClient(
        storage=storage,
        state_machine=state_machine,
        cloud_url=args.cloud_url,
    )

    state_machine.start()
    sync_client.start()

    collector_task = asyncio.create_task(collector.run_loop())

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _sig_handler():
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig_handler)
        except (NotImplementedError, AttributeError):
            # Windows signal handling fallback
            pass

    print(f"MayaCardio Pipeline running [DB={args.db}, Mock={args.mock_host}:{args.mock_port}, Cloud={args.cloud_url}]")
    try:
        await stop_event.wait()
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        print("\nShutting down MayaCardio Pipeline...")
        collector.stop()
        sync_client.stop()
        state_machine.stop()
        collector_task.cancel()
        storage.close()


def cmd_run(args):
    try:
        asyncio.run(async_run_pipeline(args))
    except KeyboardInterrupt:
        pass


def main():
    parser = argparse.ArgumentParser(description="MayaCardio Edge Pipeline CLI", prog="pipeline")
    subparsers = parser.add_subparsers(dest="subcommand", help="Sub-commands")

    # pipeline status
    p_status = subparsers.add_parser("status", help="Print pipeline status as one JSON line")
    p_status.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to SQLite database")
    p_status.add_argument("--cloud-url", default=DEFAULT_CLOUD_URL, help="Cloud service URL")

    # pipeline export
    p_export = subparsers.add_parser("export", help="Export session data and losses as NDJSON")
    p_export.add_argument("--session", required=True, help="Session ID to export")
    p_export.add_argument("--from", dest="source", choices=["device", "cloud"], required=True, help="Source to export from")
    p_export.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to SQLite database")
    p_export.add_argument("--cloud-url", default=DEFAULT_CLOUD_URL, help="Cloud service URL")

    # pipeline results
    p_results = subparsers.add_parser("results", help="Print Section 3 results as NDJSON")
    p_results.add_argument("--session", required=True, help="Session ID to get results for")
    p_results.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to SQLite database")
    p_results.add_argument("--cloud-url", default=DEFAULT_CLOUD_URL, help="Cloud service URL")

    # pipeline reconcile
    p_reconcile = subparsers.add_parser("reconcile", help="Verify device and cloud session parity")
    p_reconcile.add_argument("--session", required=True, help="Session ID to reconcile")
    p_reconcile.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to SQLite database")
    p_reconcile.add_argument("--cloud-url", default=DEFAULT_CLOUD_URL, help="Cloud service URL")

    # pipeline run
    p_run = subparsers.add_parser("run", help="Run the continuous pipeline service")
    p_run.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to SQLite database")
    p_run.add_argument("--mock-host", default=DEFAULT_MOCK_HOST, help="Biosignal mock host")
    p_run.add_argument("--mock-port", type=int, default=DEFAULT_MOCK_PORT, help="Biosignal mock port")
    p_run.add_argument("--cloud-url", default=DEFAULT_CLOUD_URL, help="Cloud service URL")

    args = parser.parse_args()
    if not args.subcommand:
        parser.print_help()
        sys.exit(1)

    if args.subcommand == "status":
        cmd_status(args)
    elif args.subcommand == "export":
        cmd_export(args)
    elif args.subcommand == "results":
        cmd_results(args)
    elif args.subcommand == "reconcile":
        cmd_reconcile(args)
    elif args.subcommand == "run":
        cmd_run(args)


if __name__ == "__main__":
    main()
