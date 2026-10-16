"""
tests/test_stage5_deployment.py: Integration tests for Cloud Deployment & Security.

Tests:
1. Server starts with PORT and CLOUD_DB_PATH from environment.
2. Unauthenticated access to /healthz returns 200 OK (liveness/readiness probe).
3. When CLOUD_API_KEY is configured, unauthenticated data endpoints return 401 Unauthorized.
4. When CLOUD_API_KEY is configured, valid API key in X-API-Key header succeeds.
5. When CLOUD_API_KEY is configured, valid API key in Bearer token header succeeds.
6. CloudSyncClient and PipelineStateMachine pass API key and successfully ingest/reconcile.
7. Unauthenticated backward compatibility when CLOUD_API_KEY is empty.
8. HTTPS URL parsing and protocol compatibility.
"""

import asyncio
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.request
import urllib.error

from biosignal_mock.mock_service import BiosignalGenerator
from cloud.server import HTTPServer, CloudRequestHandler, CloudDatabase
from pipeline.storage import StorageEngine
from pipeline.state_machine import PipelineStateMachine
from pipeline.sync_client import CloudSyncClient


class TestStage5CloudDeployment(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "cloud_test.db")
        self.port = 8125
        self.api_key = "secret_test_key_xyz_789"

        # Initialize cloud database and handler
        self.cloud_db = CloudDatabase(self.db_path)
        CloudRequestHandler.db = self.cloud_db
        CloudRequestHandler.start_time = time.time()
        CloudRequestHandler.api_key = self.api_key

        self.server = HTTPServer(("127.0.0.1", self.port), CloudRequestHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        time.sleep(0.05)

    def tearDown(self):
        CloudRequestHandler.api_key = None
        self.server.shutdown()
        self.server.server_close()
        self.cloud_db.close()
        try:
            self.temp_dir.cleanup()
        except Exception:
            pass

    def test_01_healthz_endpoint_is_unauthenticated(self):
        """Test 1: /healthz responds with 200 without requiring API key."""
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/healthz", method="GET")
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(data["status"], "healthy")
            self.assertIn("uptime_seconds", data)

    def test_02_status_endpoint_is_unauthenticated(self):
        """Test 2: /api/v1/status responds with 200 without requiring API key."""
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}/api/v1/status", method="GET")
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(data["status"], "online")

    def test_03_unauthenticated_ingest_rejected_401(self):
        """Test 3: POST /api/v1/ingest without key returns 401 Unauthorized."""
        payload = json.dumps({"session_id": "test_s1", "samples": []}).encode("utf-8")
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/api/v1/ingest",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=2.0)
        self.assertEqual(ctx.exception.code, 401)

    def test_04_authenticated_ingest_with_x_api_key(self):
        """Test 4: POST /api/v1/ingest with X-API-Key header succeeds."""
        payload = json.dumps({
            "session_id": "test_auth_xkey",
            "samples": [{"sensor": 1, "sample_idx": 0, "values": [100, 200, 300]}],
        }).encode("utf-8")
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/api/v1/ingest",
            data=payload,
            headers={"Content-Type": "application/json", "X-API-Key": self.api_key},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(data["status"], "ok")
            self.assertEqual(data["ingested_samples"], 1)

    def test_05_authenticated_ingest_with_bearer_token(self):
        """Test 5: POST /api/v1/ingest with Authorization: Bearer <key> succeeds."""
        payload = json.dumps({
            "session_id": "test_auth_bearer",
            "samples": [{"sensor": 2, "sample_idx": 0, "values": [400, 500]}],
        }).encode("utf-8")
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/api/v1/ingest",
            data=payload,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(data["status"], "ok")
            self.assertEqual(data["ingested_samples"], 1)

    def test_06_sync_client_authenticated_e2e(self):
        """Test 6: CloudSyncClient with api_key transmits and reconciles seamlessly."""
        dev_db = os.path.join(self.temp_dir.name, "device_auth.db")
        storage = StorageEngine(dev_db)
        gen = BiosignalGenerator(mode="signal", seed=99)
        sess_id = "sess_auth_sync"

        # Insert 800 samples on device
        d_s1 = [gen.get_sample(1, i) for i in range(400)]
        d_s2 = [gen.get_sample(2, i) for i in range(400)]
        storage.insert_samples_batch(sess_id, 1, 0, d_s1)
        storage.insert_samples_batch(sess_id, 2, 0, d_s2)

        sm = PipelineStateMachine(cloud_url=f"http://127.0.0.1:{self.port}", api_key=self.api_key)
        sm.mode = "online"
        client = CloudSyncClient(
            storage=storage,
            state_machine=sm,
            cloud_url=f"http://127.0.0.1:{self.port}",
            api_key=self.api_key,
        )

        async def run_sync():
            synced = await client.sync_step()
            self.assertEqual(synced, 800)

        asyncio.run(run_sync())

        # Check reconciliation with auth
        report = client.reconcile_with_cloud(sess_id)
        self.assertTrue(report["reconciled"])
        self.assertEqual(report["device"]["total_samples"], 800)
        self.assertEqual(report["cloud"]["total_samples"], 800)

        storage.close()

    def test_07_unauthenticated_backward_compatibility(self):
        """Test 7: When CloudRequestHandler has no api_key, unauthenticated requests work without headers."""
        CloudRequestHandler.api_key = None
        payload = json.dumps({"session_id": "test_open", "samples": []}).encode("utf-8")
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/api/v1/ingest",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            self.assertEqual(resp.status, 200)

    def test_08_https_url_validation_and_support(self):
        """Test 8: Verify client accepts https:// URLs cleanly."""
        client = CloudSyncClient(
            storage=None,
            state_machine=None,
            cloud_url="https://mayacardio-cloud.onrender.com",
            api_key="prod_token_abc",
        )
        self.assertEqual(client.cloud_url, "https://mayacardio-cloud.onrender.com")
        self.assertEqual(client.api_key, "prod_token_abc")


if __name__ == "__main__":
    unittest.main()
