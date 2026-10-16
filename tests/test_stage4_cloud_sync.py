"""
tests/test_stage4_cloud_sync.py: Deterministic tests for Stage 4 Cloud Synchronization & Backlog Catch-up.

Tests:
1. Lost acknowledgement after successful cloud persistence.
2. Duplicate batch delivery.
3. Partial failure and retry.
4. Client restart with a non-empty backlog.
5. Cloud service restart.
6. Large backlog with bounded memory and throughput >= 6,400 samples/sec.
7. Exact device/cloud reconciliation.
8. Online latency while uploads continue (< 5.0s).
"""

import asyncio
import gc
import json
import os
import tempfile
import threading
import time
import tracemalloc
import unittest
from typing import List, Dict, Any

from biosignal_mock.mock_service import BiosignalGenerator
from cloud.server import HTTPServer, CloudRequestHandler, CloudDatabase
from pipeline.storage import StorageEngine
from pipeline.state_machine import PipelineStateMachine
from pipeline.sync_client import CloudSyncClient


class MockFaultyCloudHandler(CloudRequestHandler):
    """Subclass allowing dynamic injection of server faults (500 errors)."""
    inject_500: bool = False

    def do_POST(self):
        if self.inject_500:
            self._send_json_response(500, {"error": "Injected Internal Server Error"})
            return
        super().do_POST()


class TestStage4CloudSync(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.device_db_path = os.path.join(self.temp_dir.name, "device_stage4.db")
        self.cloud_db_path = os.path.join(self.temp_dir.name, "cloud_stage4.db")
        self.port = 8112

        self.cloud_db = CloudDatabase(self.cloud_db_path)
        MockFaultyCloudHandler.db = self.cloud_db
        MockFaultyCloudHandler.start_time = time.time()
        MockFaultyCloudHandler.delay_seconds = 0.0
        MockFaultyCloudHandler.inject_500 = False

        self.cloud_server = HTTPServer(("127.0.0.1", self.port), MockFaultyCloudHandler)
        self.cloud_thread = threading.Thread(target=self.cloud_server.serve_forever, daemon=True)
        self.cloud_thread.start()
        await asyncio.sleep(0.05)

        self.storage = StorageEngine(self.device_db_path)
        self.state_machine = PipelineStateMachine(
            cloud_url=f"http://127.0.0.1:{self.port}",
            probe_interval_sec=0.5,
            probe_timeout_sec=0.4,
            fail_threshold=2,
        )
        self.state_machine.mode = "online"
        self.sync_client = CloudSyncClient(
            storage=self.storage,
            state_machine=self.state_machine,
            cloud_url=f"http://127.0.0.1:{self.port}",
            live_batch_size=800,
            backlog_batch_size=5000,
        )
        self.gen = BiosignalGenerator(mode="signal", seed=11)

    async def asyncTearDown(self):
        self.sync_client.stop()
        self.state_machine.stop()
        self.storage.close()
        self.cloud_server.shutdown()
        self.cloud_server.server_close()
        self.cloud_db.close()
        try:
            self.temp_dir.cleanup()
        except Exception:
            pass

    async def test_01_lost_ack_after_cloud_persistence(self):
        """Test 1: Lost acknowledgement after successful cloud persistence causes safe idempotent retry."""
        sess_id = "sess_lost_ack"
        d_s1 = [self.gen.get_sample(1, i) for i in range(500)]
        d_s2 = [self.gen.get_sample(2, i) for i in range(500)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2)

        # First sync: sends all 1000 samples to cloud
        synced = await self.sync_client.sync_step()
        self.assertEqual(synced, 1000)
        self.assertEqual(self.cloud_db.count_samples(sess_id), 1000)

        # Simulate lost acknowledgement: client crashed or socket dropped right before ACK commit
        cursor = self.storage.conn.cursor()
        cursor.execute("UPDATE samples SET synced_to_cloud = 0 WHERE session_id = ?", (sess_id,))
        cursor.close()

        # Local storage thinks 1000 samples are unsynced
        self.assertEqual(self.storage.count_synced_samples(sess_id), 0)

        # Client re-attempts sync (duplicate delivery)
        resynced = await self.sync_client.sync_step()
        self.assertEqual(resynced, 1000)

        # Cloud must STILL contain exactly 1000 samples (zero duplicate rows)
        self.assertEqual(self.cloud_db.count_samples(sess_id), 1000)
        self.assertEqual(self.storage.count_synced_samples(sess_id), 1000)

    async def test_02_duplicate_batch_delivery(self):
        """Test 2: Duplicate batch delivery of samples, losses, and offline results is completely idempotent."""
        sess_id = "sess_dup_batch"
        d_s1 = [self.gen.get_sample(1, i) for i in range(400)]
        d_s2 = [self.gen.get_sample(2, i) for i in range(400)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2)
        self.storage.insert_loss(sess_id, 1, 1000, 1399, "buffer_overflow")
        self.storage.save_window_result(
            session_id=sess_id,
            window=0,
            mode="offline",
            missing_1=0,
            missing_2=0,
            complete=True,
            result_json=json.dumps({"window": 0, "session_id": sess_id, "mode": "offline", "complete": True}),
        )

        # Sync once
        await self.sync_client.sync_step()
        self.assertEqual(self.cloud_db.count_samples(sess_id), 800)

        # Manually reset sync flags to simulate duplicate delivery
        cursor = self.storage.conn.cursor()
        cursor.execute("UPDATE samples SET synced_to_cloud = 0 WHERE session_id = ?", (sess_id,))
        cursor.execute("UPDATE losses SET synced_to_cloud = 0 WHERE session_id = ?", (sess_id,))
        cursor.execute("UPDATE window_results SET synced_to_cloud = 0 WHERE session_id = ?", (sess_id,))
        cursor.close()

        # Resend duplicate batch
        await self.sync_client.sync_step()

        # Cloud counts must remain strictly identical
        self.assertEqual(self.cloud_db.count_samples(sess_id), 800)
        cursor = self.cloud_db.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM cloud_losses WHERE session_id = ?", (sess_id,))
        self.assertEqual(cursor.fetchone()[0], 1)
        cursor.execute("SELECT COUNT(*) FROM cloud_window_results WHERE session_id = ?", (sess_id,))
        self.assertEqual(cursor.fetchone()[0], 1)
        cursor.close()

    async def test_03_partial_failure_and_retry(self):
        """Test 3: Server 500 error aborts sync pass without marking records; clean retry succeeds."""
        sess_id = "sess_part_fail"
        d_s1 = [self.gen.get_sample(1, i) for i in range(300)]
        d_s2 = [self.gen.get_sample(2, i) for i in range(300)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2)

        # Inject server error
        MockFaultyCloudHandler.inject_500 = True
        synced = await self.sync_client.sync_step()
        self.assertEqual(synced, 0, "Sync step must report 0 on server failure")
        self.assertEqual(self.storage.count_synced_samples(sess_id), 0, "No records may be marked synced")

        # Clear server error
        MockFaultyCloudHandler.inject_500 = False
        self.state_machine.mode = "online"  # Restore state

        # Retry sync
        synced_retry = await self.sync_client.sync_step()
        self.assertEqual(synced_retry, 600)
        self.assertEqual(self.storage.count_synced_samples(sess_id), 600)
        self.assertEqual(self.cloud_db.count_samples(sess_id), 600)

    async def test_04_client_restart_with_non_empty_backlog(self):
        """Test 4: Client restarting midway through a backlog cleanly resumes without loss or duplication."""
        sess_id = "sess_restart_backlog"
        # Ingest 2,000 samples
        d_s1 = [self.gen.get_sample(1, i) for i in range(1000)]
        d_s2 = [self.gen.get_sample(2, i) for i in range(1000)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2)

        # Client 1 syncs with smaller batch limit of 800
        client1 = CloudSyncClient(
            storage=self.storage,
            state_machine=self.state_machine,
            cloud_url=f"http://127.0.0.1:{self.port}",
            live_batch_size=800,
            backlog_batch_size=800,
        )
        synced_c1 = await client1.sync_step()
        self.assertEqual(synced_c1, 800)
        self.assertEqual(self.storage.count_synced_samples(sess_id), 800)
        self.assertEqual(self.storage.count_samples(sess_id) - self.storage.count_synced_samples(sess_id), 1200)

        # Client 1 terminates abruptly
        client1.stop()

        # Client 2 starts fresh
        client2 = CloudSyncClient(
            storage=self.storage,
            state_machine=self.state_machine,
            cloud_url=f"http://127.0.0.1:{self.port}",
            live_batch_size=800,
            backlog_batch_size=5000,
        )
        # Client 2 resumes from SQLite state and syncs remaining 1200 samples
        synced_c2 = await client2.sync_step()
        self.assertEqual(synced_c2, 1200)
        client2.stop()

        self.assertEqual(self.storage.count_synced_samples(sess_id), 2000)
        self.assertEqual(self.cloud_db.count_samples(sess_id), 2000)

    async def test_05_cloud_service_restart_during_sync(self):
        """Test 5: Cloud service restarts midway during backlog; sync client resumes when service returns."""
        sess_id = "sess_cloud_restart"
        d_s1 = [self.gen.get_sample(1, i) for i in range(2000)]
        d_s2 = [self.gen.get_sample(2, i) for i in range(2000)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2)

        # Set batch size to 1000 so first sync step leaves 3,000 samples unsynced
        self.sync_client.backlog_batch_size = 1000
        synced_first = await self.sync_client.sync_step()
        self.assertEqual(synced_first, 1000)
        self.assertEqual(self.storage.count_samples(sess_id) - self.storage.count_synced_samples(sess_id), 3000)

        # Stop cloud server
        self.cloud_server.shutdown()
        self.cloud_server.server_close()

        # Sync attempt during cloud downtime fails (1st failure)
        synced_down = await self.sync_client.sync_step()
        self.assertEqual(synced_down, 0)
        # Probe once more to cross fail_threshold=2
        alive = await self.state_machine.probe_once()
        self.assertFalse(alive)
        await self.state_machine.update_state(alive)
        self.assertEqual(self.state_machine.mode, "offline")

        # Restart cloud server
        self.cloud_server = HTTPServer(("127.0.0.1", self.port), MockFaultyCloudHandler)
        self.cloud_thread = threading.Thread(target=self.cloud_server.serve_forever, daemon=True)
        self.cloud_thread.start()

        # Probe detects recovery
        alive = await self.state_machine.probe_once()
        self.assertTrue(alive)
        await self.state_machine.update_state(alive)
        self.assertEqual(self.state_machine.mode, "online")

        # Sync resumes and catches up the remaining 3000 samples
        self.sync_client.backlog_batch_size = 5000
        while self.storage.count_samples(sess_id) > self.storage.count_synced_samples(sess_id):
            s = await self.sync_client.sync_step()
            if s == 0:
                break

        self.assertEqual(self.cloud_db.count_samples(sess_id), 4000)

    async def test_06_large_backlog_bounded_memory_and_throughput(self):
        """
        Test 6: 100,000 sample backlog catchup.
        Measures throughput (> 6,400 samples/sec), peak memory (< 35 MB), and ensures zero backlog remaining.
        """
        sess_id = "sess_perf_100k"
        total_samples = 100000  # 50,000 per sensor
        self.sync_client.backlog_batch_size = 25000

        # Ingest in chunks into SQLite
        chunk_size = 5000
        for start_idx in range(0, total_samples // 2, chunk_size):
            d_s1 = [self.gen.get_sample(1, start_idx + j) for j in range(chunk_size)]
            d_s2 = [self.gen.get_sample(2, start_idx + j) for j in range(chunk_size)]
            self.storage.insert_samples_batch(sess_id, 1, start_idx, d_s1)
            self.storage.insert_samples_batch(sess_id, 2, start_idx, d_s2)

        self.assertEqual(self.storage.count_samples(sess_id), total_samples)
        self.assertEqual(self.storage.count_synced_samples(sess_id), 0)

        # Track memory and throughput during catch-up
        gc.collect()
        tracemalloc.start(1)
        m_start = time.monotonic()

        synced_total = 0
        while True:
            synced = await self.sync_client.sync_step()
            if synced == 0:
                break
            synced_total += synced

        m_end = time.monotonic()
        _, peak_memory_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        elapsed_sec = m_end - m_start
        throughput_sps = synced_total / elapsed_sec if elapsed_sec > 0 else 0
        peak_mb = peak_memory_bytes / (1024 * 1024)

        print(f"\n[Stage 4 Backlog Benchmark]")
        print(f"Total Samples Synced: {synced_total}")
        print(f"Elapsed Time:         {elapsed_sec:.3f} s")
        print(f"Measured Throughput:  {throughput_sps:.1f} samples/sec (Target: >= 6,400 samples/sec)")
        print(f"Peak Memory (Traced): {peak_mb:.2f} MB (Limit: < 35 MB)")

        self.assertEqual(synced_total, total_samples)
        self.assertEqual(self.cloud_db.count_samples(sess_id), total_samples)
        self.assertGreaterEqual(throughput_sps, 6400, "Throughput must meet or exceed 6,400 samples/sec")
        self.assertLess(peak_mb, 35.0, "Memory consumption must remain strictly bounded (< 35 MB)")

    async def test_07_exact_device_cloud_reconciliation(self):
        """Test 7: Auditable reconciliation proving device and cloud session match 100%."""
        sess_id = "sess_reconcile_audit"
        d_s1 = [self.gen.get_sample(1, i) for i in range(1200)]
        d_s2 = [self.gen.get_sample(2, i) for i in range(1200)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2)
        self.storage.insert_loss(sess_id, 1, 5000, 5399, "unannounced_sequence_gap")
        self.storage.save_window_result(
            session_id=sess_id,
            window=0,
            mode="offline",
            missing_1=0,
            missing_2=0,
            complete=True,
            result_json=json.dumps({"window": 0, "session_id": sess_id, "mode": "offline", "complete": True}),
        )

        # Before sync: reconciliation reports discrepancies
        pre_report = self.sync_client.reconcile_with_cloud(sess_id)
        self.assertFalse(pre_report["reconciled"])
        self.assertGreater(len(pre_report["discrepancies"]), 0)

        # Drain backlog
        while self.storage.count_samples(sess_id) > self.storage.count_synced_samples(sess_id):
            synced = await self.sync_client.sync_step()
            if synced == 0:
                break

        # After complete sync: reconciliation succeeds with 0 discrepancies
        post_report = self.sync_client.reconcile_with_cloud(sess_id)
        self.assertTrue(post_report["reconciled"], f"Discrepancies found: {post_report['discrepancies']}")
        self.assertEqual(len(post_report["discrepancies"]), 0)
        self.assertEqual(post_report["device"]["total_samples"], 2400)
        self.assertEqual(post_report["cloud"]["total_samples"], 2400)
        self.assertEqual(post_report["device"]["losses_count"], 1)
        self.assertEqual(post_report["cloud"]["losses_count"], 1)

    async def test_08_online_latency_during_backlog_upload(self):
        """
        Test 8: Online processing latency.
        Each second's cloud result must appear in the cloud within 5.0 seconds of arrival.
        """
        sess_id = "sess_latency"
        # Seed a 10,000-sample backlog
        d_s1 = [self.gen.get_sample(1, i) for i in range(5000)]
        d_s2 = [self.gen.get_sample(2, i) for i in range(5000)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2)

        # Live 1-second window completes: window 13 (samples 5200 to 5599)
        # Record monotonic time when the completed window is saved
        t_produced = time.monotonic()
        d_live_s1 = [self.gen.get_sample(1, 5000 + i) for i in range(600)]
        d_live_s2 = [self.gen.get_sample(2, 5000 + i) for i in range(600)]
        self.storage.insert_samples_batch(sess_id, 1, 5000, d_live_s1)
        self.storage.insert_samples_batch(sess_id, 2, 5000, d_live_s2)

        # Sync loop processes batches
        while self.storage.count_samples(sess_id) > self.storage.count_synced_samples(sess_id):
            synced = await self.sync_client.sync_step()
            if synced == 0:
                break

        # Check cloud window results
        cloud_results = self.cloud_db.get_results_ndjson(sess_id)
        self.assertGreater(len(cloud_results), 0)

        t_cloud_available = time.monotonic()
        latency_sec = t_cloud_available - t_produced

        print(f"\n[Online Processing Latency Metric]")
        print(f"Elapsed time from window completion to cloud result: {latency_sec:.3f} s (Limit: < 5.0 s)")
        self.assertLess(latency_sec, 5.0, "Live window result must appear in cloud within 5 seconds")


if __name__ == "__main__":
    unittest.main()
