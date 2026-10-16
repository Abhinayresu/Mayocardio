"""
Unit tests for storage engine, durability guarantees, loss tracking, and exports.
"""

import os
import tempfile
import unittest
from pipeline.storage import StorageEngine
from pipeline.loss_tracker import LossTracker
from biosignal_mock.mock_service import BiosignalGenerator
from biosignal_mock.check_ground_truth import verify_export


class TestStorageAndDurability(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_pipeline.db")
        self.storage = StorageEngine(self.db_path)
        self.tracker = LossTracker(self.storage)
        self.gen = BiosignalGenerator(mode="signal", seed=7)

    def tearDown(self):
        self.storage.close()
        try:
            self.temp_dir.cleanup()
        except Exception:
            pass

    def test_duplicate_rejection_and_durability(self):
        data_s1 = [self.gen.get_sample(1, i) for i in range(100)]
        self.storage.insert_samples_batch("sess1", 1, 0, data_s1)
        self.assertEqual(self.storage.count_samples("sess1"), 100)

        # Attempt to insert same 100 samples again (duplicate injection)
        self.storage.insert_samples_batch("sess1", 1, 0, data_s1)
        self.assertEqual(self.storage.count_samples("sess1"), 100)

        data_s2 = [self.gen.get_sample(2, i) for i in range(100)]
        self.storage.insert_samples_batch("sess1", 2, 0, data_s2)
        self.assertEqual(self.storage.count_samples("sess1"), 200)

    def test_loss_tracking_buffer_overflow(self):
        s_id = "sess_overflow"
        self.tracker.initialize_session(s_id)

        # Batch 1: Both sensors 0-99
        d1_s1 = [self.gen.get_sample(1, i) for i in range(100)]
        d1_s2 = [self.gen.get_sample(2, i) for i in range(100)]
        self.tracker.process_incoming_batch(s_id, 1, 0, 100, [])
        self.storage.insert_samples_batch(s_id, 1, 0, d1_s1)
        self.tracker.process_incoming_batch(s_id, 2, 0, 100, [])
        self.storage.insert_samples_batch(s_id, 2, 0, d1_s2)

        # Batch 2: Sensor 1 has buffer overflow 100-199, then delivers 200-299
        lost_ranges = [{"from_index": 100, "to_index": 199, "reason": "buffer_overflow"}]
        d2_s1 = [self.gen.get_sample(1, i) for i in range(200, 300)]
        self.tracker.process_incoming_batch(s_id, 1, 200, 100, lost_ranges)
        self.storage.insert_samples_batch(s_id, 1, 200, d2_s1)

        # Sensor 2 delivers all 100-299 without loss
        d2_s2 = [self.gen.get_sample(2, i) for i in range(100, 300)]
        self.tracker.process_incoming_batch(s_id, 2, 100, 200, [])
        self.storage.insert_samples_batch(s_id, 2, 100, d2_s2)

        # Export session and verify with ground truth checker
        export_lines = self.storage.export_session_ndjson(s_id)
        export_path = os.path.join(self.temp_dir.name, "export_overflow.ndjson")
        with open(export_path, "w", encoding="utf-8") as f:
            for l in export_lines:
                f.write(l + "\n")

        success, report = verify_export(export_path, mode="signal", seed=7, check_up_to_index=299)
        self.assertTrue(success)
        self.assertEqual(report["total_silent_losses"], 0)
        self.assertEqual(report["reported_losses"], 1)

    def test_mock_restart_handling(self):
        old_sess = "sess_old"
        new_sess = "sess_new"
        self.tracker.initialize_session(old_sess)

        d1 = [self.gen.get_sample(1, i) for i in range(50)]
        self.tracker.process_incoming_batch(old_sess, 1, 0, 50, [])
        self.storage.insert_samples_batch(old_sess, 1, 0, d1)

        # Mock restarts
        self.tracker.handle_mock_restart(old_sess, new_sess)

        cursor = self.storage.conn.cursor()
        cursor.execute("SELECT from_index, to_index, reason FROM losses WHERE session_id = ?", (old_sess,))
        losses = cursor.fetchall()
        self.assertEqual(len(losses), 2)  # Sensor 1 and Sensor 2
        s1_loss = [l for l in losses if l["from_index"] == 50][0]
        self.assertIsNone(s1_loss["to_index"])
        self.assertEqual(s1_loss["reason"], "mock_restart")


if __name__ == "__main__":
    unittest.main()
