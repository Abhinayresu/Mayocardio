"""
Integration tests for collector engine under fault conditions (CRC corruption, duplicate replies, mock restarts).
"""

import asyncio
import os
import tempfile
import unittest
from biosignal_mock.mock_service import BiosignalMockServer
from biosignal_mock.check_ground_truth import verify_export
from pipeline.storage import StorageEngine
from pipeline.loss_tracker import LossTracker
from pipeline.collector import BiosignalCollector


class TestCollectorFaults(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "collector_test.db")
        self.storage = StorageEngine(self.db_path)
        self.tracker = LossTracker(self.storage)
        self.port = 7123

        # Start mock server with fault injection enabled
        self.mock_server = BiosignalMockServer(
            mode="signal",
            seed=7,
            duration=30,
            port=self.port,
            inject_faults=True,
        )
        self.server_task = asyncio.create_task(self.mock_server.run())
        await asyncio.sleep(0.1)  # Allow server to bind

        self.collector = BiosignalCollector(
            storage=self.storage,
            loss_tracker=self.tracker,
            host="127.0.0.1",
            port=self.port,
        )

    async def asyncTearDown(self):
        self.collector.stop()
        if self.mock_server._server:
            self.mock_server._server.close()
            await self.mock_server._server.wait_closed()
        self.server_task.cancel()
        try:
            await self.server_task
        except asyncio.CancelledError:
            pass
        self.storage.close()
        try:
            self.temp_dir.cleanup()
        except Exception:
            pass

    async def test_collector_under_faults_and_ground_truth(self):
        connected = await self.collector.connect()
        self.assertTrue(connected)

        # Collect data for ~2 seconds (enough for multiple batches and injected faults)
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < 2.5:
            await self.collector.step_read_sensor(1)
            await self.collector.step_read_sensor(2)
            await asyncio.sleep(0.05)

        sess_id = self.collector.current_session_id
        self.assertIsNotNone(sess_id)

        # Verify that samples were saved
        s1_count = self.storage.get_highest_sample_index(sess_id, 1) + 1
        s2_count = self.storage.get_highest_sample_index(sess_id, 2) + 1
        self.assertGreater(s1_count, 100)
        self.assertGreater(s2_count, 100)

        # Export session
        export_lines = self.storage.export_session_ndjson(sess_id)
        export_file = os.path.join(self.temp_dir.name, "faults_export.ndjson")
        with open(export_file, "w", encoding="utf-8") as f:
            for line in export_lines:
                f.write(line + "\n")

        # Run ground truth validator
        min_collected = min(s1_count, s2_count) - 1
        success, report = verify_export(
            export_filepath=export_file,
            mode="signal",
            seed=7,
            check_up_to_index=min_collected,
        )
        self.assertTrue(success, f"Ground truth validation failed: {report}")
        self.assertEqual(report["total_silent_losses"], 0)
        self.assertEqual(report["duplicates"], 0)
        self.assertEqual(report["corruptions"], 0)


if __name__ == "__main__":
    unittest.main()
