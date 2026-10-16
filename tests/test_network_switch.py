"""
Integration test for Criterion 5: Automatic network switching within 10 seconds without loss or duplication.
"""

import asyncio
import os
import tempfile
import threading
import time
import unittest
from biosignal_mock.mock_service import BiosignalGenerator
from biosignal_mock.check_ground_truth import verify_export
from cloud.server import HTTPServer, CloudRequestHandler, CloudDatabase
from pipeline.storage import StorageEngine
from pipeline.state_machine import PipelineStateMachine
from pipeline.sync_client import CloudSyncClient


class TestNetworkSwitch(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.device_db_path = os.path.join(self.temp_dir.name, "device.db")
        self.cloud_db_path = os.path.join(self.temp_dir.name, "cloud.db")
        self.port = 8092

        # Start Cloud Server
        self.cloud_db = CloudDatabase(self.cloud_db_path)
        CloudRequestHandler.db = self.cloud_db
        CloudRequestHandler.start_time = time.time()
        self.cloud_server = HTTPServer(("127.0.0.1", self.port), CloudRequestHandler)
        self.cloud_thread = threading.Thread(target=self.cloud_server.serve_forever, daemon=True)
        self.cloud_thread.start()
        await asyncio.sleep(0.1)

        # Device Storage & Components
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

    async def test_network_cut_and_restore_switch(self):
        sess_id = "switch_sess"

        # 1. Initial State: Cloud online
        await self.state_machine.probe_once()
        await self.state_machine.update_state(True)
        self.assertEqual(self.state_machine.mode, "online")

        # Ingest first 1000 samples while online
        d_s1 = [self.gen.get_sample(1, i) for i in range(1000)]
        d_s2 = [self.gen.get_sample(2, i) for i in range(1000)]
        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2)

        # Sync step
        synced = await self.sync_client.sync_step()
        self.assertGreater(synced, 0)

        # 2. Simulate Network Cut (kill cloud server)
        t_cut = time.time()
        self.cloud_server.shutdown()
        self.cloud_server.server_close()

        # Update probes until offline
        while self.state_machine.mode != "offline":
            alive = await self.state_machine.probe_once()
            await self.state_machine.update_state(alive)
            await asyncio.sleep(0.2)

        t_offline = time.time()
        switch_to_offline_duration = t_offline - t_cut
        print(f"Switched to OFFLINE in {switch_to_offline_duration:.2f} seconds (< 10s)")
        self.assertLess(switch_to_offline_duration, 10.0)
        self.assertEqual(self.state_machine.mode, "offline")

        # Ingest 1000 more samples while offline
        d_s1_off = [self.gen.get_sample(1, i) for i in range(1000, 2000)]
        d_s2_off = [self.gen.get_sample(2, i) for i in range(1000, 2000)]
        self.storage.insert_samples_batch(sess_id, 1, 1000, d_s1_off)
        self.storage.insert_samples_batch(sess_id, 2, 1000, d_s2_off)

        # Offline processing step
        await self.sync_client.offline_process_step()
        offline_results = self.storage.get_window_results(sess_id)
        self.assertGreater(len(offline_results), 0)

        # 3. Restore Network (restart cloud server)
        t_restore = time.time()
        self.cloud_server = HTTPServer(("127.0.0.1", self.port), CloudRequestHandler)
        self.cloud_thread = threading.Thread(target=self.cloud_server.serve_forever, daemon=True)
        self.cloud_thread.start()

        # Probe until online
        while self.state_machine.mode != "online":
            alive = await self.state_machine.probe_once()
            await self.state_machine.update_state(alive)
            await asyncio.sleep(0.2)

        t_online = time.time()
        switch_to_online_duration = t_online - t_restore
        print(f"Switched to ONLINE in {switch_to_online_duration:.2f} seconds (< 10s)")
        self.assertLess(switch_to_online_duration, 10.0)
        self.assertEqual(self.state_machine.mode, "online")

        # Catchup backlog
        while self.storage.count_samples() > self.storage.count_synced_samples():
            await self.sync_client.sync_step()

        # Verify cloud has all 4000 samples (2000 per sensor)
        self.assertEqual(self.cloud_db.count_samples(sess_id), 4000)

        # Export cloud session and verify with ground truth
        cloud_export = self.cloud_db.export_session_ndjson(sess_id)
        export_file = os.path.join(self.temp_dir.name, "cloud_switch_export.ndjson")
        with open(export_file, "w", encoding="utf-8") as f:
            for l in cloud_export:
                f.write(l + "\n")

        success, report = verify_export(export_file, mode="signal", seed=7, check_up_to_index=1999)
        self.assertTrue(success)
        self.assertEqual(report["total_silent_losses"], 0)
        self.assertEqual(report["duplicates"], 0)


if __name__ == "__main__":
    unittest.main()
