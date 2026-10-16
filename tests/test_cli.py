"""
Integration tests for pipeline CLI commands:
- pipeline status
- pipeline export --session ID --from device
- pipeline export --session ID --from cloud
- pipeline results --session ID
"""

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from biosignal_mock.mock_service import BiosignalGenerator
from cloud.server import HTTPServer, CloudRequestHandler, CloudDatabase
from pipeline.storage import StorageEngine


class TestPipelineCLI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.cloud_db_path = os.path.join(cls.temp_dir.name, "cli_cloud.db")
        cls.device_db_path = os.path.join(cls.temp_dir.name, "cli_device.db")
        cls.port = 8097

        # Seed device DB
        cls.storage = StorageEngine(cls.device_db_path)
        cls.gen = BiosignalGenerator(mode="signal", seed=7)
        cls.sess_id = "cli_sess"

        d_s1 = [cls.gen.get_sample(1, i) for i in range(400)]
        d_s2 = [cls.gen.get_sample(2, i) for i in range(400)]
        cls.storage.insert_samples_batch(cls.sess_id, 1, 0, d_s1)
        cls.storage.insert_samples_batch(cls.sess_id, 2, 0, d_s2)
        cls.storage.insert_loss(cls.sess_id, 1, 4000, 4399, "buffer_overflow")

        # Start cloud server
        cls.cloud_db = CloudDatabase(cls.cloud_db_path)
        CloudRequestHandler.db = cls.cloud_db
        CloudRequestHandler.start_time = time.time()
        cls.cloud_server = HTTPServer(("127.0.0.1", cls.port), CloudRequestHandler)
        cls.cloud_thread = threading.Thread(target=cls.cloud_server.serve_forever, daemon=True)
        cls.cloud_thread.start()
        time.sleep(0.1)

        # Ingest to cloud as well
        cls.cloud_db.ingest_payload(
            cls.sess_id,
            [{"sensor": 1, "sample_idx": 0, "values": d_s1[0]}],
            [{"sensor": 1, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}],
        )

    @classmethod
    def tearDownClass(cls):
        cls.storage.close()
        cls.cloud_server.shutdown()
        cls.cloud_server.server_close()
        cls.cloud_db.close()
        try:
            cls.temp_dir.cleanup()
        except Exception:
            pass

    def run_cli(self, args_list):
        cmd = [sys.executable, "-m", "pipeline.cli"] + args_list
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return res

    def test_pipeline_status_json_line(self):
        res = self.run_cli(["status", "--db", self.device_db_path, "--cloud-url", f"http://127.0.0.1:{self.port}"])
        self.assertEqual(res.returncode, 0)
        lines = [l for l in res.stdout.strip().splitlines() if l.strip()]
        self.assertEqual(len(lines), 1, "pipeline status must print exactly one line")

        obj = json.loads(lines[0])
        self.assertIn("mode", obj)
        self.assertIn("samples_saved", obj)
        self.assertIn("samples_in_cloud", obj)
        self.assertIn("backlog", obj)
        self.assertEqual(obj["mode"], "online")
        self.assertEqual(obj["samples_saved"], 800)

    def test_pipeline_export_device(self):
        res = self.run_cli(["export", "--session", self.sess_id, "--from", "device", "--db", self.device_db_path])
        self.assertEqual(res.returncode, 0)
        lines = [l for l in res.stdout.strip().splitlines() if l.strip()]
        # 800 samples + 1 loss = 801 lines
        self.assertEqual(len(lines), 801)
        first_line = json.loads(lines[0])
        self.assertEqual(first_line["sensor"], 1)
        self.assertEqual(first_line["index"], 0)
        last_line = json.loads(lines[-1])
        self.assertEqual(last_line["reason"], "buffer_overflow")

    def test_pipeline_export_cloud(self):
        res = self.run_cli(["export", "--session", self.sess_id, "--from", "cloud", "--cloud-url", f"http://127.0.0.1:{self.port}"])
        self.assertEqual(res.returncode, 0)
        lines = [l for l in res.stdout.strip().splitlines() if l.strip()]
        self.assertEqual(len(lines), 2)  # 1 sample, 1 loss

    def test_pipeline_results_cloud(self):
        res = self.run_cli(["results", "--session", self.sess_id, "--cloud-url", f"http://127.0.0.1:{self.port}"])
        self.assertEqual(res.returncode, 0)


if __name__ == "__main__":
    unittest.main()
