"""
tests/test_vertical_slice_1.py: Verification of Vertical Slice 1.
Validates:
1. Reading data from unmodified biosignal-mock over TCP.
2. CRC-32 response validation.
3. Durable persistence in SQLite WAL.
4. Export in the required NDJSON format matching check_ground_truth.
5. Duplicate rejection: identical responses do NOT create duplicate records.
6. Pipeline restart: restarting the pipeline preserves previously committed samples.
"""

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import sqlite3

from biosignal_mock.mock_service import BiosignalMockServer
from biosignal_mock.check_ground_truth import verify_export
from pipeline.storage import StorageEngine
from pipeline.loss_tracker import LossTracker
from pipeline.collector import BiosignalCollector


class TestVerticalSlice1(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 7160
        cls.mock_server = BiosignalMockServer(
            mode="signal",
            seed=7,
            duration=300,
            port=cls.port,
            inject_faults=False,
        )
        cls.server_thread = threading.Thread(
            target=lambda: asyncio.run(cls.mock_server.run()), daemon=True
        )
        cls.server_thread.start()
        time.sleep(0.3)  # Wait for TCP server to bind

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "vertical_slice.db")

    def tearDown(self):
        try:
            self.temp_dir.cleanup()
        except Exception:
            pass

    def test_01_end_to_end_ingest_and_export(self):
        """Test: Ingest from mock, persist to SQLite, export via CLI, verify with ground truth."""
        storage = StorageEngine(self.db_path)
        tracker = LossTracker(storage)
        collector = BiosignalCollector(storage, tracker, host="127.0.0.1", port=self.port)

        async def run_collect():
            connected = await collector.connect()
            self.assertTrue(connected, "Must connect to biosignal-mock over TCP")
            # Collect 3 batches per sensor
            for _ in range(3):
                await collector.step_read_sensor(1)
                await collector.step_read_sensor(2)
                await asyncio.sleep(0.02)
            collector.stop()

        asyncio.run(run_collect())

        session_id = collector.current_session_id
        self.assertIsNotNone(session_id, "Collector must capture active session_id")

        s1_count = storage.get_highest_sample_index(session_id, 1) + 1
        s2_count = storage.get_highest_sample_index(session_id, 2) + 1
        self.assertGreater(s1_count, 0)
        self.assertGreater(s2_count, 0)
        storage.close()

        # Run pipeline export via CLI subprocess
        export_proc = subprocess.run(
            [sys.executable, "-m", "pipeline.cli", "export", "--session", session_id, "--from", "device", "--db", self.db_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

        export_lines = [l.strip() for l in export_proc.stdout.splitlines() if l.strip()]
        self.assertGreater(len(export_lines), 0)

        # Inspect format: every sample line must match {"sensor": ..., "index": ..., "values": [...]}
        first_line = json.loads(export_lines[0])
        self.assertIn("sensor", first_line)
        self.assertIn("index", first_line)
        self.assertIn("values", first_line)
        self.assertIsInstance(first_line["values"], list)

        # Save to file and run check_ground_truth
        export_file = os.path.join(self.temp_dir.name, "export_v1.ndjson")
        with open(export_file, "w", encoding="utf-8") as f:
            f.write(export_proc.stdout)

        min_idx = min(s1_count, s2_count) - 1
        success, report = verify_export(
            export_filepath=export_file,
            mode="signal",
            seed=7,
            check_up_to_index=min_idx,
        )
        self.assertTrue(success, f"Ground truth validation failed: {report}")
        self.assertEqual(report["total_silent_losses"], 0)
        self.assertEqual(report["duplicates"], 0)
        self.assertEqual(report["corruptions"], 0)

    def test_02_duplicate_response_immunity(self):
        """Demonstrate that duplicate responses do not create duplicate records."""
        storage = StorageEngine(self.db_path)
        sess_id = "test_dup_sess"

        # Batch 1: Sample indices 0 to 49
        sample_data = [[100 + i, 200 + i, 300 + i] for i in range(50)]
        storage.insert_samples_batch(sess_id, 1, 0, sample_data)
        count_after_first = storage.count_samples(sess_id)
        self.assertEqual(count_after_first, 50)

        # Duplicate delivery: Send exact same 50 samples again
        storage.insert_samples_batch(sess_id, 1, 0, sample_data)
        count_after_dup = storage.count_samples(sess_id)
        self.assertEqual(count_after_dup, 50, "Duplicate insertion must NOT increase sample count")

        # Overlapping delivery: Send samples 25 to 74 (25 overlap, 25 new)
        overlap_data = [[100 + i, 200 + i, 300 + i] for i in range(25, 75)]
        storage.insert_samples_batch(sess_id, 1, 25, overlap_data)
        count_after_overlap = storage.count_samples(sess_id)
        self.assertEqual(count_after_overlap, 75, "Overlapping insertion must add only new unique samples")

        # Verify export has exactly 75 records, uniquely indexed from 0 to 74
        lines = storage.export_session_ndjson(sess_id)
        storage.close()

        indices = [json.loads(l)["index"] for l in lines if "index" in json.loads(l)]
        self.assertEqual(len(indices), 75)
        self.assertEqual(len(set(indices)), 75, "All indices must be strictly unique")
        self.assertEqual(indices, list(range(75)))

    def test_03_restart_preserves_previously_committed_samples(self):
        """Demonstrate that stopping and restarting the pipeline preserves committed samples."""
        sess_id = "test_restart_sess"

        # Phase 1: Pipeline Instance 1 runs and commits 100 samples
        storage1 = StorageEngine(self.db_path)
        data_s1_part1 = [[700 + i, 1200 + i, 1500 + i] for i in range(100)]
        data_s2_part1 = [[400000 + i, 150000 + i] for i in range(100)]
        storage1.insert_samples_batch(sess_id, 1, 0, data_s1_part1)
        storage1.insert_samples_batch(sess_id, 2, 0, data_s2_part1)

        self.assertEqual(storage1.count_samples(sess_id), 200)
        # Abruptly close / simulate process termination
        storage1.close()

        # Phase 2: Pipeline Instance 2 starts from the existing database
        storage2 = StorageEngine(self.db_path)
        tracker2 = LossTracker(storage2)
        tracker2.initialize_session(sess_id)

        # Verify existing samples are still present and intact
        self.assertEqual(storage2.count_samples(sess_id), 200)
        self.assertEqual(storage2.get_highest_sample_index(sess_id, 1), 99)
        self.assertEqual(storage2.get_highest_sample_index(sess_id, 2), 99)

        # Commit next 100 samples (indices 100 to 199)
        data_s1_part2 = [[700 + i, 1200 + i, 1500 + i] for i in range(100, 200)]
        data_s2_part2 = [[400000 + i, 150000 + i] for i in range(100, 200)]
        storage2.insert_samples_batch(sess_id, 1, 100, data_s1_part2)
        storage2.insert_samples_batch(sess_id, 2, 100, data_s2_part2)

        # Total count must be exactly 400 (200 per sensor)
        self.assertEqual(storage2.count_samples(sess_id), 400)
        self.assertEqual(storage2.get_highest_sample_index(sess_id, 1), 199)
        self.assertEqual(storage2.get_highest_sample_index(sess_id, 2), 199)

        # Verify continuous integrity across restart boundary
        s1_samples = storage2.get_window_samples(sess_id, 1, 0, 199)
        self.assertEqual(len(s1_samples), 200)
        for i in range(200):
            self.assertIn(i, s1_samples)
            self.assertEqual(s1_samples[i], [700 + i, 1200 + i, 1500 + i])

        storage2.close()

    def test_04_abrupt_process_kill_and_reopen_durability(self):
        """Simulate abrupt process kill (SIGKILL) under synchronous=FULL, verify WAL recovery & integrity."""
        sess_id = "test_kill_sess"
        cwd = os.getcwd()

        worker_code = f"""
import sys, os, time
sys.path.insert(0, r'{cwd}')
from pipeline.storage import StorageEngine

db = StorageEngine(r'{self.db_path}')
for batch in range(20):
    data_s1 = [[1000 + i, 2000 + i, 3000 + i] for i in range(20)]
    db.insert_samples_batch('{sess_id}', 1, batch * 20, data_s1)
    time.sleep(0.05)
"""
        worker_file = os.path.join(self.temp_dir.name, "kill_worker.py")
        with open(worker_file, "w", encoding="utf-8") as f:
            f.write(worker_code)

        proc = subprocess.Popen([sys.executable, worker_file])
        time.sleep(0.3)  # Allow a few batches to commit
        proc.kill()     # Abruptly kill process
        proc.wait()

        # Reopen database in parent process
        reopened_storage = StorageEngine(self.db_path)
        cursor = reopened_storage.conn.cursor()
        cursor.execute("PRAGMA integrity_check")
        integrity = cursor.fetchone()[0]
        self.assertEqual(integrity, "ok", "Database must be completely uncorrupted after abrupt kill")

        # Verify committed samples are intact
        cursor.execute("SELECT COUNT(*) FROM samples WHERE session_id = ?", (sess_id,))
        count = cursor.fetchone()[0]
        self.assertGreater(count, 0, "Committed samples prior to kill must be preserved")
        reopened_storage.close()


if __name__ == "__main__":
    unittest.main()
