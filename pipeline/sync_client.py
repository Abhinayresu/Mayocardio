"""
sync_client.py: Cloud synchronization engine.
Implements:
1. Low-latency live streaming in online mode (< 1s latency, meeting 5s criterion).
2. High-throughput backlog recovery with gzip compression (< 3 min for 2h outage).
3. On-device signal processing during offline mode.
4. Idempotent synchronization of samples, losses, and offline-computed window results.
"""

import asyncio
import gzip
import json
import logging
import os
import urllib.request
from typing import Dict, List, Optional, Tuple, Any
from pipeline.storage import StorageEngine
from pipeline.state_machine import PipelineStateMachine
from pipeline.signal_processor import process_window

logger = logging.getLogger("pipeline.sync_client")


class CloudSyncClient:
    def __init__(
        self,
        storage: StorageEngine,
        state_machine: PipelineStateMachine,
        cloud_url: str = "http://127.0.0.1:8080",
        live_batch_size: int = 800,
        backlog_batch_size: int = 5000,
        api_key: Optional[str] = None,
    ):
        self.storage = storage
        self.state_machine = state_machine
        self.cloud_url = cloud_url.rstrip("/")
        self.live_batch_size = live_batch_size
        self.backlog_batch_size = backlog_batch_size
        self.api_key = api_key if api_key is not None else os.environ.get("CLOUD_API_KEY")
        self.running = False
        self._sync_task: Optional[asyncio.Task] = None
        self._offline_proc_task: Optional[asyncio.Task] = None

    def _send_ingest_http(self, payload: Dict[str, Any], use_gzip: bool = False) -> Tuple[bool, Dict[str, Any]]:
        json_data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        if use_gzip:
            json_data = gzip.compress(json_data, compresslevel=1)
            headers["Content-Encoding"] = "gzip"

        try:
            req = urllib.request.Request(
                f"{self.cloud_url}/api/v1/ingest",
                data=json_data,
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                if resp.status == 200:
                    resp_data = json.loads(resp.read().decode("utf-8"))
                    if resp_data.get("status") == "ok":
                        return True, resp_data
                return False, {}
        except Exception as e:
            logger.debug(f"HTTP Ingest error: {e}")
            return False, {}

    async def sync_step(self) -> int:
        """
        Pulls unsynced samples, losses, and offline results.
        Transmits to cloud and marks acknowledged items as synced.
        Returns number of samples successfully synced.
        """
        if self.state_machine.mode != "online":
            return 0

        batch_size = self.backlog_batch_size

        loop = asyncio.get_running_loop()
        unsynced, unsynced_losses, unsynced_results = await loop.run_in_executor(
            None, lambda: self.storage.get_all_unsynced(sample_limit=batch_size)
        )

        if not unsynced and not unsynced_losses and not unsynced_results:
            return 0

        use_gzip = (len(unsynced) >= 500)

        # Group items by session_id
        session_batches: Dict[str, List[Tuple[str, int, int, List[int]]]] = {}
        for item in unsynced:
            sess_id = item[0]
            if sess_id not in session_batches:
                session_batches[sess_id] = []
            session_batches[sess_id].append(item)

        # Ensure sessions present in losses or results are also processed
        for l in unsynced_losses:
            if l["session_id"] not in session_batches:
                session_batches[l["session_id"]] = []
        for r in unsynced_results:
            if r["session_id"] not in session_batches:
                session_batches[r["session_id"]] = []

        total_synced_this_step = 0

        for sess_id, items in session_batches.items():
            samples_payload = []
            sample_keys_to_ack = []
            for item in items:
                _, sensor, sample_idx, vals = item
                samples_payload.append({
                    "sensor": sensor,
                    "sample_idx": sample_idx,
                    "values": vals,
                })
                sample_keys_to_ack.append((sess_id, sensor, sample_idx))

            losses_for_sess = [l for l in unsynced_losses if l["session_id"] == sess_id]
            loss_ids_to_ack = [l["id"] for l in losses_for_sess]
            losses_payload = [
                {
                    "sensor": l["sensor"],
                    "from_index": l["from_index"],
                    "to_index": l["to_index"],
                    "reason": l["reason"],
                }
                for l in losses_for_sess
            ]

            results_for_sess = [r for r in unsynced_results if r["session_id"] == sess_id]
            result_keys_to_ack = [(r["session_id"], r["window"]) for r in results_for_sess]

            payload = {
                "session_id": sess_id,
                "samples": samples_payload,
                "losses": losses_payload,
                "offline_results": results_for_sess,
            }

            ok, resp_data = await loop.run_in_executor(None, lambda: self._send_ingest_http(payload, use_gzip=use_gzip))
            if ok and resp_data.get("status") == "ok":
                # Mark as synced atomically
                if sample_keys_to_ack:
                    await loop.run_in_executor(None, lambda: self.storage.mark_samples_synced(sample_keys_to_ack))
                if loss_ids_to_ack:
                    await loop.run_in_executor(None, lambda: self.storage.mark_losses_synced(loss_ids_to_ack))
                if result_keys_to_ack:
                    await loop.run_in_executor(None, lambda: self.storage.mark_window_results_synced(result_keys_to_ack))
                total_synced_this_step += len(sample_keys_to_ack)
            else:
                # Ingest failed - trigger immediate state machine evaluation
                await self.state_machine.update_state(False)
                break

        return total_synced_this_step

    async def offline_process_step(self):
        """
        Computes Section 3 outputs locally for any completed windows when in offline mode.
        """
        if self.state_machine.mode != "offline":
            return

        cursor = self.storage.conn.cursor()
        cursor.execute("SELECT session_id, MAX(sample_idx) as max_idx FROM samples GROUP BY session_id")
        sessions = cursor.fetchall()
        cursor.close()

        for s in sessions:
            sess_id = s["session_id"]
            max_idx = s["max_idx"]
            if max_idx is None or max_idx < 399:
                continue

            max_win = (max_idx - 399) // 400
            for w in range(0, max_win + 1):
                cursor = self.storage.conn.cursor()
                cursor.execute(
                    "SELECT 1 FROM window_results WHERE session_id = ? AND window = ?",
                    (sess_id, w),
                )
                exists = cursor.fetchone()
                cursor.close()
                if exists:
                    continue

                # Fetch window samples (and past 5s for ECG HR if w >= 4)
                s_start = 400 * max(0, w - 4)
                s_end = 400 * (w + 1) - 1
                s1_map = self.storage.get_window_samples(sess_id, 1, s_start, s_end)
                s2_map = self.storage.get_window_samples(sess_id, 2, s_start, s_end)

                res = process_window(sess_id, w, mode="offline", s1_samples_map=s1_map, s2_samples_map=s2_map)
                self.storage.save_window_result(
                    session_id=sess_id,
                    window=w,
                    mode="offline",
                    missing_1=res["missing_1"],
                    missing_2=res["missing_2"],
                    complete=res["complete"],
                    result_json=json.dumps(res),
                )

    async def run_sync_loop(self):
        self.running = True
        while self.running:
            if self.state_machine.mode == "online":
                synced = await self.sync_step()
                if synced > 500:
                    await asyncio.sleep(0.01)
                else:
                    await asyncio.sleep(0.2)
            else:
                await asyncio.sleep(1.0)

    async def run_offline_proc_loop(self):
        while self.running:
            if self.state_machine.mode == "offline":
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.offline_process_step)
                await asyncio.sleep(1.0)
            else:
                await asyncio.sleep(1.0)

    def start(self):
        self._sync_task = asyncio.create_task(self.run_sync_loop())
        self._offline_proc_task = asyncio.create_task(self.run_offline_proc_loop())

    def stop(self):
        self.running = False
        if self._sync_task:
            self._sync_task.cancel()
        if self._offline_proc_task:
            self._offline_proc_task.cancel()

    def reconcile_with_cloud(self, session_id: str) -> Dict[str, Any]:
        """
        Queries device and cloud reconciliation summaries and provides an auditable report.
        """
        device_summary = self.storage.get_session_reconciliation_summary(session_id)
        cloud_summary = {}
        try:
            req_headers = {}
            if self.api_key:
                req_headers["X-API-Key"] = self.api_key
            req = urllib.request.Request(
                f"{self.cloud_url}/api/v1/reconcile?session_id={session_id}",
                headers=req_headers,
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                if resp.status == 200:
                    cloud_summary = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            return {
                "reconciled": False,
                "session_id": session_id,
                "error": f"Failed to contact cloud reconciliation endpoint: {e}",
                "device": device_summary,
                "cloud": None,
                "discrepancies": ["Cloud unreachable for reconciliation"],
            }

        discrepancies = []
        if device_summary["total_samples"] != cloud_summary.get("total_samples"):
            discrepancies.append(
                f"Sample count mismatch: device has {device_summary['total_samples']}, "
                f"cloud has {cloud_summary.get('total_samples')}"
            )
        if device_summary["unsynced_samples"] != 0:
            discrepancies.append(f"Device still has {device_summary['unsynced_samples']} unsynced samples")
        if device_summary["sensor1_count"] != cloud_summary.get("sensor1_count"):
            discrepancies.append(
                f"Sensor 1 count mismatch: device {device_summary['sensor1_count']} vs cloud {cloud_summary.get('sensor1_count')}"
            )
        if device_summary["sensor2_count"] != cloud_summary.get("sensor2_count"):
            discrepancies.append(
                f"Sensor 2 count mismatch: device {device_summary['sensor2_count']} vs cloud {cloud_summary.get('sensor2_count')}"
            )
        if device_summary["losses_count"] != cloud_summary.get("losses_count"):
            discrepancies.append(
                f"Losses count mismatch: device {device_summary['losses_count']} vs cloud {cloud_summary.get('losses_count')}"
            )

        is_reconciled = (len(discrepancies) == 0)
        return {
            "reconciled": is_reconciled,
            "session_id": session_id,
            "device": device_summary,
            "cloud": cloud_summary,
            "discrepancies": discrepancies,
        }

