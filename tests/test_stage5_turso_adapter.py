"""
tests/test_stage5_turso_adapter.py: Unit and integration tests for Turso (libSQL) external database adapter.

IMPORTANT TRANSPARENCY NOTICE:
All tests in this module run against a high-fidelity local Mock Turso libSQL HTTP server
implementing the /v2/pipeline wire protocol.
These tests validate protocol compliance, typed argument serialization, pipeline atomicity,
transaction rollback, duplicate idempotency, transient error retry loops, and session reconciliation.
THEY ARE MOCKED TESTS AND DO NOT CLAIM TO PROVE LIVE PUBLIC CLOUD DURABILITY WITHOUT LIVE CREDENTIALS.
"""

import asyncio
import base64
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
import os
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest
import urllib.request
from typing import Any, Dict, List, Optional

from cloud.database import TursoCloudDatabase, create_cloud_database, _format_turso_arg, _unpack_turso_cell
from cloud.server import CloudRequestHandler
from pipeline.storage import StorageEngine
from pipeline.state_machine import PipelineStateMachine
from pipeline.sync_client import CloudSyncClient


def _unpack_client_cell(cell: Dict[str, Any]) -> Any:
    t = cell.get("type")
    if t == "null":
        return None
    elif t == "integer":
        return int(cell["value"])
    elif t == "float":
        return float(cell["value"])
    elif t == "text":
        return str(cell["value"])
    elif t == "blob":
        return base64.b64decode(cell.get("base64", ""))
    return cell.get("value")


def _pack_mock_cell(val: Any) -> Dict[str, Any]:
    if val is None:
        return {"type": "null"}
    elif isinstance(val, bool):
        return {"type": "integer", "value": "1" if val else "0"}
    elif isinstance(val, int):
        return {"type": "integer", "value": str(val)}
    elif isinstance(val, float):
        return {"type": "float", "value": val}
    elif isinstance(val, (bytes, bytearray)):
        return {"type": "blob", "base64": base64.b64encode(val).decode("ascii")}
    else:
        return {"type": "text", "value": str(val)}


class MockTursoHandler(BaseHTTPRequestHandler):
    """
    Mock libSQL /v2/pipeline handler for deterministic protocol testing.
    Executes statements against an in-memory SQLite database.
    """
    conn: Optional[sqlite3.Connection] = None
    fail_next_count: int = 0
    fail_on_commit: bool = False
    in_transaction: bool = False
    request_log: List[Dict[str, Any]] = []

    def log_message(self, format, *args):
        pass

    def do_POST(self):
        if self.path != "/v2/pipeline":
            self.send_response(404)
            self.end_headers()
            return

        # Check for simulated network / server failure
        if MockTursoHandler.fail_next_count > 0:
            MockTursoHandler.fail_next_count -= 1
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Simulated Turso cluster 500 error"}).encode("utf-8"))
            return

        length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(length)
        payload = json.loads(raw_body.decode("utf-8"))
        MockTursoHandler.request_log.append(payload)

        requests = payload.get("requests", [])
        results = []
        aborted = False

        conn = MockTursoHandler.conn
        cursor = conn.cursor()

        for req in requests:
            req_type = req.get("type")
            if req_type == "close":
                results.append({"type": "ok", "response": {"type": "close"}})
                continue

            if req_type == "execute":
                stmt_obj = req.get("stmt", {})
                sql = stmt_obj.get("sql", "").strip()
                raw_args = stmt_obj.get("args", [])
                params = [_unpack_client_cell(a) for a in raw_args]

                # Check simulated commit failure
                if sql == "COMMIT" and MockTursoHandler.fail_on_commit:
                    if MockTursoHandler.in_transaction:
                        try:
                            cursor.execute("ROLLBACK")
                        except Exception:
                            pass
                        MockTursoHandler.in_transaction = False
                    results.append({
                        "type": "error",
                        "error": {"message": "Simulated disk write failure on COMMIT"}
                    })
                    aborted = True
                    break

                try:
                    if sql == "BEGIN":
                        cursor.execute("BEGIN")
                        MockTursoHandler.in_transaction = True
                        results.append({
                            "type": "ok",
                            "response": {"type": "execute", "result": {"cols": [], "rows": [], "affected_row_count": 0}}
                        })
                    elif sql == "COMMIT":
                        cursor.execute("COMMIT")
                        MockTursoHandler.in_transaction = False
                        results.append({
                            "type": "ok",
                            "response": {"type": "execute", "result": {"cols": [], "rows": [], "affected_row_count": 0}}
                        })
                    elif sql == "ROLLBACK":
                        cursor.execute("ROLLBACK")
                        MockTursoHandler.in_transaction = False
                        results.append({
                            "type": "ok",
                            "response": {"type": "execute", "result": {"cols": [], "rows": [], "affected_row_count": 0}}
                        })
                    else:
                        cursor.execute(sql, params)
                        if cursor.description is not None:
                            cols = [{"name": d[0]} for d in cursor.description]
                            rows = [[_pack_mock_cell(val) for val in r] for r in cursor.fetchall()]
                            results.append({
                                "type": "ok",
                                "response": {
                                    "type": "execute",
                                    "result": {
                                        "cols": cols,
                                        "rows": rows,
                                        "affected_row_count": 0
                                    }
                                }
                            })
                        else:
                            if not MockTursoHandler.in_transaction:
                                conn.commit()
                            results.append({
                                "type": "ok",
                                "response": {
                                    "type": "execute",
                                    "result": {
                                        "cols": [],
                                        "rows": [],
                                        "affected_row_count": cursor.rowcount
                                    }
                                }
                            })
                except Exception as e:
                    if MockTursoHandler.in_transaction:
                        try:
                            cursor.execute("ROLLBACK")
                        except Exception:
                            pass
                        MockTursoHandler.in_transaction = False
                    results.append({
                        "type": "error",
                        "error": {"message": str(e)}
                    })
                    aborted = True
                    break

        cursor.close()

        resp_body = json.dumps({"results": results}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(resp_body)))
        self.end_headers()
        self.wfile.write(resp_body)


class TestStage5TursoAdapter(unittest.TestCase):
    """
    Test suite verifying the Turso external database adapter and cloud integration.
    All tests interact with MockTursoServer via HTTP /v2/pipeline.
    """

    @classmethod
    def setUpClass(cls):
        # Start Mock Turso HTTP Server on ephemeral port
        cls.mock_turso_db = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
        MockTursoHandler.conn = cls.mock_turso_db
        MockTursoHandler.fail_next_count = 0
        MockTursoHandler.fail_on_commit = False
        MockTursoHandler.in_transaction = False
        MockTursoHandler.request_log = []

        cls.mock_turso_server = HTTPServer(("127.0.0.1", 0), MockTursoHandler)
        cls.turso_port = cls.mock_turso_server.server_port
        cls.turso_url = f"http://127.0.0.1:{cls.turso_port}"

        cls.turso_thread = threading.Thread(target=cls.mock_turso_server.serve_forever, daemon=True)
        cls.turso_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.mock_turso_server.shutdown()
        cls.mock_turso_server.server_close()
        cls.mock_turso_db.close()

    def setUp(self):
        # Reset tables in mock Turso
        cursor = self.mock_turso_db.cursor()
        cursor.execute("DROP TABLE IF EXISTS cloud_samples")
        cursor.execute("DROP TABLE IF EXISTS cloud_losses")
        cursor.execute("DROP TABLE IF EXISTS cloud_window_results")
        cursor.close()

        MockTursoHandler.fail_next_count = 0
        MockTursoHandler.fail_on_commit = False
        MockTursoHandler.in_transaction = False
        MockTursoHandler.request_log.clear()

        self.temp_dir = tempfile.mkdtemp(prefix="turso_test_")
        self.client_db_path = os.path.join(self.temp_dir, "client.db")
        self.storage = StorageEngine(self.client_db_path)
        self.state_machine = PipelineStateMachine()
        self.state_machine.mode = "online"

    def tearDown(self):
        self.storage.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_01_mocked_turso_schema_init_and_single_batch(self):
        """
        [MOCKED TEST: Validates Turso adapter schema initialization and atomic single batch write]
        Verifies TursoCloudDatabase initializes tables over /v2/pipeline and successfully commits a batch.
        """
        turso_db = TursoCloudDatabase(turso_url=self.turso_url, turso_token="mock_token_123")

        samples = [
            {"sensor": 1, "sample_idx": 0, "values": [100, 200, 300]},
            {"sensor": 1, "sample_idx": 1, "values": [105, 205, 305]},
            {"sensor": 2, "sample_idx": 0, "values": [500, 600]},
            {"sensor": 2, "sample_idx": 1, "values": [510, 610]},
        ]
        losses = [
            {"sensor": 1, "from_index": 2, "to_index": 2, "reason": "crc_error"}
        ]

        s_c, l_c, r_c = turso_db.ingest_payload("sess_01", samples, losses)
        self.assertEqual(s_c, 4)
        self.assertEqual(l_c, 1)
        self.assertEqual(r_c, 0)

        # Confirm count and export via Turso adapter
        self.assertEqual(turso_db.count_samples("sess_01"), 4)
        lines = turso_db.export_session_ndjson("sess_01")
        self.assertEqual(len(lines), 5)  # 4 samples + 1 loss

    def test_02_mocked_turso_duplicate_batch_idempotence(self):
        """
        [MOCKED TEST: Validates idempotent deduplication on duplicate batch delivery over Turso]
        Simulates network retransmissions where the same batch is delivered twice.
        Ensures INSERT OR IGNORE prevents duplicate records and keeps sample count exact.
        """
        turso_db = TursoCloudDatabase(turso_url=self.turso_url, turso_token="mock_token_123")

        samples = [
            {"sensor": 1, "sample_idx": 0, "values": [10, 20, 30]},
            {"sensor": 2, "sample_idx": 0, "values": [40, 50]},
        ]
        losses = [
            {"sensor": 2, "from_index": 1, "to_index": 1, "reason": "seq_gap"}
        ]

        # First delivery
        s1, l1, _ = turso_db.ingest_payload("sess_dup", samples, losses)
        self.assertEqual(s1, 2)
        self.assertEqual(l1, 1)
        self.assertEqual(turso_db.count_samples("sess_dup"), 2)

        # Duplicate delivery
        s2, l2, _ = turso_db.ingest_payload("sess_dup", samples, losses)
        self.assertEqual(s2, 2)
        self.assertEqual(l2, 1)

        # Sample count MUST remain strictly 2, losses strictly 1
        self.assertEqual(turso_db.count_samples("sess_dup"), 2)
        reconcile = turso_db.get_session_reconciliation_summary("sess_dup")
        self.assertEqual(reconcile["total_samples"], 2)
        self.assertEqual(reconcile["losses_count"], 1)

    def test_03_mocked_turso_interrupted_write_rollback_no_durable_ack(self):
        """
        [MOCKED TEST: Validates interrupted write abort and rollback guarantees on Turso]
        Simulates an interrupted write or disk error during COMMIT in the Turso pipeline.
        Ensures transaction aborts, no partial rows are committed, and Cloud Server returns 500 durable:False.
        """
        turso_db = TursoCloudDatabase(turso_url=self.turso_url, turso_token="mock_token_123")

        # Ingest baseline 2 samples
        turso_db.ingest_payload("sess_abort", [
            {"sensor": 1, "sample_idx": 0, "values": [1, 2, 3]},
            {"sensor": 2, "sample_idx": 0, "values": [4, 5]},
        ], [])
        self.assertEqual(turso_db.count_samples("sess_abort"), 2)

        # Set mock to simulate commit failure
        MockTursoHandler.fail_on_commit = True

        new_samples = [
            {"sensor": 1, "sample_idx": 1, "values": [10, 20, 30]},
            {"sensor": 2, "sample_idx": 1, "values": [40, 50]},
        ]

        # Ingestion must raise RuntimeError and NOT return success
        with self.assertRaises(RuntimeError) as ctx:
            turso_db.ingest_payload("sess_abort", new_samples, [])
        self.assertIn("Simulated disk write failure on COMMIT", str(ctx.exception))

        # Reset flag
        MockTursoHandler.fail_on_commit = False

        # Database state MUST remain strictly at 2 (rolled back)
        self.assertEqual(turso_db.count_samples("sess_abort"), 2)

    def test_04_mocked_turso_transient_error_and_sync_client_retry(self):
        """
        [MOCKED TEST: Validates transient Turso 500 error handling, durable ack requirement, and retry loop]
        Connects CloudServer backed by TursoCloudDatabase.
        Injects a transient failure on the first ingest request.
        Verifies CloudSyncClient does NOT mark local records synced on failure,
        and safely synchronizes and marks records synced on retry.
        """
        # Start CloudServer pointing to TursoCloudDatabase
        turso_db = create_cloud_database(turso_url=self.turso_url, turso_token="mock_token")
        CloudRequestHandler.db = turso_db
        CloudRequestHandler.api_key = "test_secret_key"
        CloudRequestHandler.start_time = time.time()
        cloud_server = HTTPServer(("127.0.0.1", 0), CloudRequestHandler)
        cloud_port = cloud_server.server_port
        cloud_url = f"http://127.0.0.1:{cloud_port}"

        cloud_thread = threading.Thread(target=cloud_server.serve_forever, daemon=True)
        cloud_thread.start()

        try:
            # Seed client storage with 10 samples
            data = [[100 + i, 200 + i, 300 + i] for i in range(10)]
            self.storage.insert_samples_batch("sess_retry", 1, 0, data)
            self.assertEqual(self.storage.count_samples(), 10)

            sync_client = CloudSyncClient(
                storage=self.storage,
                state_machine=self.state_machine,
                cloud_url=cloud_url,
                api_key="test_secret_key",
            )

            # Inject transient failure on Mock Turso (1 failure)
            MockTursoHandler.fail_next_count = 1

            # Step 1: Attempt sync - should fail because Turso returned 500
            synced_count_1 = asyncio.run(sync_client.sync_step())
            self.assertEqual(synced_count_1, 0)

            # Local records MUST remain unsynced
            unsynced, _, _ = self.storage.get_all_unsynced()
            self.assertEqual(len(unsynced), 10)
            self.assertEqual(turso_db.count_samples("sess_retry"), 0)

            # Reset state machine to online for retry
            self.state_machine.mode = "online"

            # Step 2: Retry sync - should succeed now that transient error has cleared
            synced_count_2 = asyncio.run(sync_client.sync_step())
            self.assertEqual(synced_count_2, 10)

            # Local records MUST now be marked synced
            unsynced_after, _, _ = self.storage.get_all_unsynced()
            self.assertEqual(len(unsynced_after), 0)

            # External Turso database MUST confirm exact 10 samples committed
            self.assertEqual(turso_db.count_samples("sess_retry"), 10)

        finally:
            cloud_server.shutdown()
            cloud_server.server_close()
            turso_db.close()

    def test_05_mocked_turso_session_reconciliation_and_export(self):
        """
        [MOCKED TEST: Validates session reconciliation and NDJSON export parity over Turso]
        Verifies that /api/v1/reconcile and /api/v1/export produce identical data
        between local device storage and the Turso cloud database adapter.
        """
        turso_db = create_cloud_database(turso_url=self.turso_url, turso_token="mock_token")
        CloudRequestHandler.db = turso_db
        CloudRequestHandler.api_key = None
        CloudRequestHandler.start_time = time.time()
        cloud_server = HTTPServer(("127.0.0.1", 0), CloudRequestHandler)
        cloud_port = cloud_server.server_port
        cloud_url = f"http://127.0.0.1:{cloud_port}"

        cloud_thread = threading.Thread(target=cloud_server.serve_forever, daemon=True)
        cloud_thread.start()

        try:
            # Insert 40 samples on device
            d1 = [[10 + i, 20 + i, 30 + i] for i in range(20)]
            d2 = [[40 + i, 50 + i] for i in range(20)]
            self.storage.insert_samples_batch("sess_rec", 1, 0, d1)
            self.storage.insert_samples_batch("sess_rec", 2, 0, d2)
            self.storage.insert_loss("sess_rec", 1, 20, 20, "seq_gap")

            sync_client = CloudSyncClient(
                storage=self.storage,
                state_machine=self.state_machine,
                cloud_url=cloud_url,
            )

            synced = asyncio.run(sync_client.sync_step())
            self.assertEqual(synced, 40)

            # Call /api/v1/reconcile
            req = urllib.request.Request(f"{cloud_url}/api/v1/reconcile?session_id=sess_rec")
            with urllib.request.urlopen(req) as resp:
                self.assertEqual(resp.status, 200)
                summary = json.loads(resp.read().decode("utf-8"))

            self.assertEqual(summary["total_samples"], 40)
            self.assertEqual(summary["sensor1_count"], 20)
            self.assertEqual(summary["sensor2_count"], 20)
            self.assertEqual(summary["sensor1_min_idx"], 0)
            self.assertEqual(summary["sensor1_max_idx"], 19)
            self.assertEqual(summary["sensor2_min_idx"], 0)
            self.assertEqual(summary["sensor2_max_idx"], 19)
            self.assertEqual(summary["losses_count"], 1)

            # Call /api/v1/export
            req_exp = urllib.request.Request(f"{cloud_url}/api/v1/export?session_id=sess_rec")
            with urllib.request.urlopen(req_exp) as resp:
                self.assertEqual(resp.status, 200)
                cloud_lines = [l for l in resp.read().decode("utf-8").strip().split("\n") if l]

            local_lines = self.storage.export_session_ndjson("sess_rec")
            self.assertEqual(len(cloud_lines), len(local_lines))
            self.assertEqual(cloud_lines, local_lines)

        finally:
            cloud_server.shutdown()
            cloud_server.server_close()
            turso_db.close()

    def test_06_mocked_turso_offline_results_and_cloud_processing(self):
        """
        [MOCKED TEST: Validates offline results ingestion and online window processing on Turso]
        Transmits offline-computed window results and raw samples to Turso.
        Verifies that cloud Section 3 window processing runs, results are persisted with idempotent keys,
        and results NDJSON is returned accurately.
        """
        turso_db = create_cloud_database(turso_url=self.turso_url, turso_token="mock_token")
        CloudRequestHandler.db = turso_db
        CloudRequestHandler.api_key = None
        CloudRequestHandler.start_time = time.time()
        cloud_server = HTTPServer(("127.0.0.1", 0), CloudRequestHandler)
        cloud_port = cloud_server.server_port
        cloud_url = f"http://127.0.0.1:{cloud_port}"

        cloud_thread = threading.Thread(target=cloud_server.serve_forever, daemon=True)
        cloud_thread.start()

        try:
            # Construct 400 samples (window 0)
            now = time.time()
            samples = []
            for i in range(400):
                samples.append({"sensor": 1, "sample_idx": i, "values": [100, 200, 300]})
                samples.append({"sensor": 2, "sample_idx": i, "values": [400, 500]})

            # Simulate offline result for window 0
            offline_results = [{
                "session_id": "sess_win",
                "window": 0,
                "mode": "offline",
                "missing_1": 0,
                "missing_2": 0,
                "complete": True,
                "result_json": json.dumps({
                    "session_id": "sess_win",
                    "window": 0,
                    "mode": "offline",
                    "missing_1": 0,
                    "missing_2": 0,
                    "complete": True,
                    "mean_1_1": 100.0,
                    "mean_1_2": 200.0,
                    "mean_1_3": 300.0,
                    "mean_2_1": 400.0,
                    "mean_2_2": 500.0,
                    "mean_magnitude_2": 640.3,
                    "peaks_count": 0,
                    "heart_rate_bpm": None
                }),
            }]

            payload = {
                "session_id": "sess_win",
                "samples": samples,
                "losses": [],
                "offline_results": offline_results,
            }

            req = urllib.request.Request(
                f"{cloud_url}/api/v1/ingest",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req) as resp:
                self.assertEqual(resp.status, 200)
                data = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(data.get("durable"))
                self.assertEqual(data.get("ingested_samples"), 800)
                self.assertEqual(data.get("ingested_results"), 1)

            # Query results endpoint
            req_res = urllib.request.Request(f"{cloud_url}/api/v1/results?session_id=sess_win")
            with urllib.request.urlopen(req_res) as resp:
                self.assertEqual(resp.status, 200)
                lines = [l for l in resp.read().decode("utf-8").strip().split("\n") if l]

            self.assertEqual(len(lines), 1)
            parsed_res = json.loads(lines[0])
            self.assertEqual(parsed_res["window"], 0)
            self.assertEqual(parsed_res["mode"], "offline")
            self.assertTrue(parsed_res["complete"])

        finally:
            cloud_server.shutdown()
            cloud_server.server_close()
            turso_db.close()

    def test_07_mocked_turso_pipeline_error_propagation(self):
        """
        [MOCKED TEST: Validates explicit error propagation from Turso pipeline failures]
        Verifies that when Turso returns a statement-level or database error,
        _execute_pipeline raises a RuntimeError rather than swallowing errors.
        """
        turso_db = TursoCloudDatabase(turso_url=self.turso_url, turso_token="mock_token")

        # Execute an intentionally invalid SQL statement
        with self.assertRaises(RuntimeError) as ctx:
            turso_db._execute_pipeline([("SELECT non_existent_column FROM non_existent_table", [])])

        self.assertIn("error", str(ctx.exception).lower())

    def test_08_factory_adapter_selection_sqlite_vs_turso(self):
        """
        [MOCKED TEST: Validates factory adapter selection between SQLite and Turso]
        Verifies that create_cloud_database returns SQLiteCloudDatabase when no Turso URL is configured,
        and returns TursoCloudDatabase when TURSO_DATABASE_URL is provided.
        """
        # Case 1: No Turso credentials -> SQLiteCloudDatabase
        local_db_path = os.path.join(self.temp_dir, "local_test.db")
        sqlite_db = create_cloud_database(db_path=local_db_path)
        from cloud.database import SQLiteCloudDatabase
        self.assertIsInstance(sqlite_db, SQLiteCloudDatabase)
        sqlite_db.close()

        # Case 2: Explicit turso_url -> TursoCloudDatabase
        turso_adapter = create_cloud_database(turso_url=self.turso_url, turso_token="tok")
        self.assertIsInstance(turso_adapter, TursoCloudDatabase)
        turso_adapter.close()

        # Case 3: Environment variable TURSO_DATABASE_URL -> TursoCloudDatabase
        old_env = os.environ.get("TURSO_DATABASE_URL")
        try:
            os.environ["TURSO_DATABASE_URL"] = self.turso_url
            env_adapter = create_cloud_database()
            self.assertIsInstance(env_adapter, TursoCloudDatabase)
            env_adapter.close()
        finally:
            if old_env is not None:
                os.environ["TURSO_DATABASE_URL"] = old_env
            else:
                os.environ.pop("TURSO_DATABASE_URL", None)


if __name__ == "__main__":
    unittest.main()
