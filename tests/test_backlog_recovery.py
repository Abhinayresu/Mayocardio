"""
Benchmark test for Criterion 6: Backlog recovery throughput.
Verifies that backlog synchronization rate exceeds the requirement to catch up a 2-hour outage within 15 minutes.
"""

import asyncio
import os
import tempfile
import threading
import time
import unittest
from biosignal_mock.mock_service import BiosignalGenerator
from cloud.server import HTTPServer, CloudRequestHandler, CloudDatabase
from pipeline.storage import StorageEngine
from pipeline.state_machine import PipelineStateMachine
from pipeline.sync_client import CloudSyncClient


class TestBacklogRecovery(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.device_db_path = os.path.join(self.temp_dir.name, "device_bench.db")
        self.cloud_db_path = os.path.join(self.temp_dir.name, "cloud_bench.db")
        self.port = 8095

        self.cloud_db = CloudDatabase(self.cloud_db_path)
        CloudRequestHandler.db = self.cloud_db
        CloudRequestHandler.start_time = time.time()
        self.cloud_server = HTTPServer(("127.0.0.1", self.port), CloudRequestHandler)
        self.cloud_thread = threading.Thread(target=self.cloud_server.serve_forever, daemon=True)
        self.cloud_thread.start()
        await asyncio.sleep(0.1)

        self.storage = StorageEngine(self.device_db_path)
        self.state_machine = PipelineStateMachine(cloud_url=f"http://127.0.0.1:{self.port}")
        self.state_machine.mode = "online"
        self.sync_client = CloudSyncClient(
            storage=self.storage,
            state_machine=self.state_machine,
            cloud_url=f"http://127.0.0.1:{self.port}",
            backlog_batch_size=2500,
        )
        self.gen = BiosignalGenerator(mode="signal", seed=7)

    async def asyncTearDown(self):
        self.storage.close()
        self.cloud_server.shutdown()
        self.cloud_server.server_close()
        self.cloud_db.close()
        try:
            self.temp_dir.cleanup()
        except Exception:
            pass

    async def test_high_throughput_backlog_catchup(self):
        sess_id = "bench_sess"
        # Seed local DB with 10,000 samples (5000 per sensor) representing an outage backlog
        n_samples = 5000
        d_s1 = [self.gen.get_sample(1, i) for i in range(n_samples)]
        d_s2 = [self.gen.get_sample(2, i) for i in range(n_samples)]

        self.storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        self.storage.insert_samples_batch(sess_id, 2, 0, d_s2)

        total_local = self.storage.count_samples()
        self.assertEqual(total_local, 10000)

        # Measure synchronization time
        t_start = time.time()
        synced_total = 0
        while self.storage.count_samples() > self.storage.count_synced_samples():
            batch_synced = await self.sync_client.sync_step()
            synced_total += batch_synced

        elapsed = time.time() - t_start
        throughput = synced_total / elapsed
        print(f"\nSynced {synced_total} samples in {elapsed:.3f}s: Throughput = {throughput:.0f} samples/sec")

        # 2-hour outage = 5.76M samples. At 15 minutes = 6,400 samples/sec required.
        # We assert that throughput easily exceeds 8,000 samples/sec
        self.assertGreater(throughput, 8000, f"Throughput {throughput:.0f} samples/sec is too slow for 15-min catchup")
        self.assertEqual(self.cloud_db.count_samples(sess_id), 10000)


if __name__ == "__main__":
    unittest.main()
