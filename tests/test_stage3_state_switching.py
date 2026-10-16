"""
tests/test_stage3_state_switching.py: Deterministic test suite for Stage 3 Online/Offline state switching.
Tests:
1. Monotonic timing of Online -> Offline transition (< 10s).
2. Monotonic timing of Offline -> Online transition (< 10s).
3. Outage during active collection (collector continues uninterrupted).
4. Failed health probes and delayed responses.
5. Cloud acknowledgements lost after successful writes (idempotent re-sync).
6. Duplicate synchronization attempts.
7. Worker crashes during a transition.
8. Windows spanning a transition boundary.
9. Zero missing or duplicate samples across complete outage and recovery.
"""

import asyncio
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.request
from typing import List, Dict, Any

from biosignal_mock.mock_service import BiosignalGenerator
from biosignal_mock.check_ground_truth import verify_export
from cloud.server import HTTPServer, CloudRequestHandler, CloudDatabase
from pipeline.storage import StorageEngine
from pipeline.state_machine import PipelineStateMachine
from pipeline.sync_client import CloudSyncClient


class TestStage3StateSwitching(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.device_db_path = os.path.join(self.temp_dir.name, "device_stage3.db")
        self.cloud_db_path = os.path.join(self.temp_dir.name, "cloud_stage3.db")
        self.port = 8110

        self.cloud_db = CloudDatabase(self.cloud_db_path)
        CloudRequestHandler.db = self.cloud_db
        CloudRequestHandler.start_time = time.time()
        self.cloud_server = HTTPServer(("127.0.0.1", self.port), CloudRequestHandler)
        self.cloud_thread = threading.Thread(target=self.cloud_server.serve_forever, daemon=True)
        self.cloud_thread.start()
        await asyncio.sleep(0.1)

        self.storage = StorageEngine(self.device_db_path)
        self.state_machine = PipelineStateMachine(
            cloud_url=f"http://127.0.0.1:{self.port}",
            probe_interval_sec=0.5,
            probe_timeout_sec=0.4,
            fail_threshold=2,
        )
        self.sync_client = CloudSyncClient(
            storage=self.storage,
            state_machine=self.state_machine,
            cloud_url=f"http://127.0.0.1:{self.port}",
        )
        self.gen = BiosignalGenerator(mode="signal", seed=7)

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

    async def test_01_monotonic_mode_transitions_under_10_seconds(self):
        """Test 1 & 2: Measured elapsed time for both transitions must be < 10.0 seconds."""
        # 1. Establish online mode
        await self.state_machine.probe_once()
        await self.state_machine.update_state(True)
        self.assertEqual(self.state_machine.mode, "online")

        # 2. Trigger network cut: kill cloud server
        m_cut_start = time.monotonic()
        self.cloud_server.shutdown()
        self.cloud_server.server_close()

        # Update probes until offline
        while self.state_machine.mode != "offline":
            alive = await self.state_machine.probe_once()
            await self.state_machine.update_state(alive)
            await asyncio.sleep(0.1)

        m_cut_end = time.monotonic()
        time_to_offline = m_cut_end - m_cut_start
        print(f"\n[Transition Metric] Online -> Offline took {time_to_offline:.3f}s (Limit: 10.0s)")
        self.assertLess(time_to_offline, 10.0, "Online to Offline transition must complete within 10s")
        self.assertEqual(self.state_machine.mode, "offline")

        # 3. Trigger network restore: restart cloud server
        m_restore_start = time.monotonic()
        self.cloud_server = HTTPServer(("127.0.0.1", self.port), CloudRequestHandler)
        self.cloud_thread = threading.Thread(target=self.cloud_server.serve_forever, daemon=True)
        self.cloud_thread.start()

        # Update probes until online
        while self.state_machine.mode != "online":
            alive = await self.state_machine.probe_once()
            await self.state_machine.update_state(alive)
            await asyncio.sleep(0.1)

        m_restore_end = time.monotonic()
        time_to_online = m_restore_end - m_restore_start
        print(f"[Transition Metric] Offline -> Online took {time_to_online:.3f}s (Limit: 10.0s)")
        self.assertLess(time_to_online, 10.0, "Offline to Online transition must complete within 10s")
        self.assertEqual(self.state_machine.mode, "online")

    async def test_02_collection_independence_during_outage(self):
        """Test 3: Local persistence continues saving samples uninterrupted during an outage."""
        sess_id = "sess_indep"
        # Network is dead
        self.state_machine.mode = "offline"

        # Ingest 500 samples locally while offline
        d_s1 = [self.gen.get_sample(1, i) for i in range(500)]
        d_s2 = [self.gen.get_sample(2, i) for i in range(500)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2)

        # Assert local DB has all 1000 samples safely stored
        self.assertEqual(self.storage.count_samples(sess_id), 1000)
        self.assertEqual(self.storage.count_synced_samples(sess_id), 0)

    async def test_03_lost_cloud_acknowledgement_idempotency(self):
        """Test 5 & 6: Lost acknowledgements and duplicate sync attempts do not duplicate records."""
        sess_id = "sess_idemp"
        self.state_machine.mode = "online"

        d_s1 = [self.gen.get_sample(1, i) for i in range(100)]
        d_s2 = [self.gen.get_sample(2, i) for i in range(100)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2)

        # First sync attempt succeeds
        synced = await self.sync_client.sync_step()
        self.assertEqual(synced, 200)
        self.assertEqual(self.cloud_db.count_samples(sess_id), 200)

        # Simulate lost ack scenario: manually reset synced_to_cloud locally to 0
        cursor = self.storage.conn.cursor()
        cursor.execute("UPDATE samples SET synced_to_cloud = 0 WHERE session_id = ?", (sess_id,))
        cursor.close()

        # Client re-sends the exact same 200 samples (duplicate delivery)
        re_synced = await self.sync_client.sync_step()
        self.assertEqual(re_synced, 200)

        # Assert that cloud database did NOT create duplicates
        cloud_count = self.cloud_db.count_samples(sess_id)
        self.assertEqual(cloud_count, 200, "Duplicate sync must NOT create duplicate cloud rows")

    async def test_04_window_spanning_transition_boundary(self):
        """Test 8: Window spanning a transition boundary has stable result identity without conflict."""
        sess_id = "sess_span"
        self.state_machine.mode = "online"

        # Ingest first half of Window 0 (samples 0 to 199) while online
        d_s1_part1 = [self.gen.get_sample(1, i) for i in range(200)]
        d_s2_part1 = [self.gen.get_sample(2, i) for i in range(200)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1_part1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2_part1)
        await self.sync_client.sync_step()

        # Cloud should NOT compute window 0 yet because window is incomplete (only 200/400 samples)
        self.assertEqual(len(self.cloud_db.get_results_ndjson(sess_id)), 0)

        # Network cuts -> switch to offline
        self.state_machine.mode = "offline"

        # Ingest second half of Window 0 (samples 200 to 399) while offline
        d_s1_part2 = [self.gen.get_sample(1, i) for i in range(200, 400)]
        d_s2_part2 = [self.gen.get_sample(2, i) for i in range(200, 400)]
        self.storage.insert_samples_batch(sess_id, 1, 200, d_s1_part2)
        self.storage.insert_samples_batch(sess_id, 2, 200, d_s2_part2)

        # Device computes completed window 0 in offline mode
        await self.sync_client.offline_process_step()
        local_results = self.storage.get_window_results(sess_id)
        self.assertEqual(len(local_results), 1)
        w0_offline = json.loads(local_results[0])
        self.assertEqual(w0_offline["window"], 0)
        self.assertEqual(w0_offline["mode"], "offline")
        self.assertTrue(w0_offline["complete"])

        # Network restores -> switch to online
        self.state_machine.mode = "online"

        # Sync client streams remaining samples and offline result to cloud
        await self.sync_client.sync_step()

        # Verify cloud results: exactly 1 result row for window 0, preserved as offline
        cloud_results = self.cloud_db.get_results_ndjson(sess_id)
        self.assertEqual(len(cloud_results), 1, "Must have exactly 1 result row for window 0")
        w0_cloud = json.loads(cloud_results[0])
        self.assertEqual(w0_cloud["window"], 0)
        self.assertEqual(w0_cloud["mode"], "offline")

    async def test_05_complete_outage_ground_truth_continuity(self):
        """Test 9: Outage during stream, recovery, backlog catchup, 100% ground truth match."""
        sess_id = "sess_full_outage"
        self.state_machine.mode = "online"

        # Phase 1: 500 samples online
        d_s1_p1 = [self.gen.get_sample(1, i) for i in range(500)]
        d_s2_p1 = [self.gen.get_sample(2, i) for i in range(500)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1_p1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2_p1)
        await self.sync_client.sync_step()

        # Phase 2: 1000 samples during outage (samples 500 to 1499)
        self.state_machine.mode = "offline"
        d_s1_p2 = [self.gen.get_sample(1, i) for i in range(500, 1500)]
        d_s2_p2 = [self.gen.get_sample(2, i) for i in range(500, 1500)]
        self.storage.insert_samples_batch(sess_id, 1, 500, d_s1_p2)
        self.storage.insert_samples_batch(sess_id, 2, 500, d_s2_p2)
        await self.sync_client.offline_process_step()

        # Phase 3: 500 samples online after restore (samples 1500 to 1999)
        self.state_machine.mode = "online"
        d_s1_p3 = [self.gen.get_sample(1, i) for i in range(1500, 2000)]
        d_s2_p3 = [self.gen.get_sample(2, i) for i in range(1500, 2000)]
        self.storage.insert_samples_batch(sess_id, 1, 1500, d_s1_p3)
        self.storage.insert_samples_batch(sess_id, 2, 1500, d_s2_p3)

        # Catch up all backlog to cloud
        while self.storage.count_samples(sess_id) > self.storage.count_synced_samples(sess_id):
            synced = await self.sync_client.sync_step()
            if synced == 0:
                break

        # Cloud must end up with all 4000 samples (2000 per sensor)
        self.assertEqual(self.cloud_db.count_samples(sess_id), 4000)

        # Export cloud session to file and verify against ground truth
        cloud_export = self.cloud_db.export_session_ndjson(sess_id)
        export_path = os.path.join(self.temp_dir.name, "outage_export.ndjson")
        with open(export_path, "w", encoding="utf-8") as f:
            for l in cloud_export:
                f.write(l + "\n")

        success, report = verify_export(export_path, mode="signal", seed=7, check_up_to_index=1999)
        self.assertTrue(success, f"Ground truth validation failed: {report}")
        self.assertEqual(report["total_silent_losses"], 0)
        self.assertEqual(report["duplicates"], 0)
        self.assertEqual(report["corruptions"], 0)


    async def test_06_failed_health_probes_and_delayed_responses(self):
        """Test 4: Probes that time out or fail switch state to offline within threshold."""
        # 1. Establish online
        self.state_machine.mode = "online"
        self.state_machine.consecutive_failures = 0
        CloudRequestHandler.delay_seconds = 0.0

        # Verify probe succeeds when cloud responds promptly
        alive = await self.state_machine.probe_once()
        self.assertTrue(alive)

        # 2. Inject artificial service hang / delay (1.0s > 0.4s timeout)
        # Port is open and TCP interface works, but service is unresponsive
        CloudRequestHandler.delay_seconds = 1.0
        alive = await self.state_machine.probe_once()
        self.assertFalse(alive, "Probe must detect service unresponsiveness due to timeout")
        await self.state_machine.update_state(alive)

        # Second failure crosses threshold (fail_threshold=2)
        alive2 = await self.state_machine.probe_once()
        self.assertFalse(alive2)
        await self.state_machine.update_state(alive2)

        self.assertEqual(self.state_machine.mode, "offline", "State machine must switch to offline after consecutive timeouts")

        # 3. Restore service responsiveness
        CloudRequestHandler.delay_seconds = 0.0
        alive3 = await self.state_machine.probe_once()
        self.assertTrue(alive3)
        await self.state_machine.update_state(alive3)
        self.assertEqual(self.state_machine.mode, "online", "State machine must switch back to online once probes succeed")

    async def test_07_worker_crashes_and_restart_recovery(self):
        """Test 7: Workers crashing midway recover safely upon restart with no data corruption or duplicates."""
        sess_id = "sess_crash"
        self.state_machine.mode = "online"

        d_s1 = [self.gen.get_sample(1, i) for i in range(400)]
        d_s2 = [self.gen.get_sample(2, i) for i in range(400)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2)

        # Worker 1 runs and syncs 800 samples to cloud
        await self.sync_client.sync_step()
        self.assertEqual(self.cloud_db.count_samples(sess_id), 800)

        # Worker 1 suddenly crashes (simulated by aborting instance while uncommitted local state exists)
        self.sync_client.stop()
        # Reset local sync flags to simulate crash occurring before DB ACK commit
        cursor = self.storage.conn.cursor()
        cursor.execute("UPDATE samples SET synced_to_cloud = 0 WHERE session_id = ?", (sess_id,))
        cursor.close()

        # Restart pipeline: create brand new SyncWorker instance
        new_sync_worker = CloudSyncClient(
            storage=self.storage,
            state_machine=self.state_machine,
            cloud_url=f"http://127.0.0.1:{self.port}",
        )

        # New worker resumes syncing from local durable state
        resynced = await new_sync_worker.sync_step()
        self.assertEqual(resynced, 800)
        new_sync_worker.stop()

        # Cloud must STILL have exactly 800 samples (zero duplicates created)
        self.assertEqual(self.cloud_db.count_samples(sess_id), 800)
        self.assertEqual(self.storage.count_synced_samples(sess_id), 800)


if __name__ == "__main__":
    unittest.main()

