"""
Integration tests for Cloud Ingest API, cloud window processing, and export matching.
"""

import gzip
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.request
from biosignal_mock.mock_service import BiosignalGenerator
from biosignal_mock.check_ground_truth import verify_export
from cloud.server import HTTPServer, CloudRequestHandler, CloudDatabase


class TestCloudService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = os.path.join(cls.temp_dir.name, "test_cloud.db")
        cls.port = 8089
        cls.db = CloudDatabase(cls.db_path)
        CloudRequestHandler.db = cls.db
        CloudRequestHandler.start_time = time.time()

        cls.server = HTTPServer(("127.0.0.1", cls.port), CloudRequestHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.db.close()
        try:
            cls.temp_dir.cleanup()
        except Exception:
            pass

    def test_cloud_ingest_and_results(self):
        gen = BiosignalGenerator(mode="signal", seed=7)
        sess_id = "cloud_sess_test"

        # Generate 2000 samples (5 seconds = windows 0 to 4)
        samples = []
        for i in range(2000):
            samples.append({"sensor": 1, "sample_idx": i, "values": gen.get_sample(1, i)})
            samples.append({"sensor": 2, "sample_idx": i, "values": gen.get_sample(2, i)})

        payload = {
            "session_id": sess_id,
            "samples": samples,
            "losses": [],
        }

        # Send POST /api/v1/ingest
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/api/v1/ingest",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            self.assertEqual(resp.status, 200)
            res_body = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(res_body["status"], "ok")
            self.assertEqual(res_body["ingested_samples"], 4000)

        # Verify GET /api/v1/status
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/api/v1/status") as resp:
            status_data = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(status_data["status"], "online")
            self.assertEqual(status_data["total_samples"], 4000)

        # Verify GET /api/v1/results
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/api/v1/results?session_id={sess_id}") as resp:
            lines = [l.strip() for l in resp.read().decode("utf-8").splitlines() if l.strip()]
            self.assertGreaterEqual(len(lines), 5)
            # Inspect window 4 result
            w4_res = json.loads(lines[4])
            self.assertEqual(w4_res["window"], 4)
            self.assertEqual(w4_res["mode"], "online")
            self.assertEqual(w4_res["heart_rate"], 60.0)

        # Verify GET /api/v1/export
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/api/v1/export?session_id={sess_id}") as resp:
            export_content = resp.read().decode("utf-8")
            export_path = os.path.join(self.temp_dir.name, "cloud_export.ndjson")
            with open(export_path, "w", encoding="utf-8") as f:
                f.write(export_content)

        success, report = verify_export(export_path, mode="signal", seed=7, check_up_to_index=1999)
        self.assertTrue(success)
        self.assertEqual(report["total_silent_losses"], 0)


if __name__ == "__main__":
    unittest.main()
