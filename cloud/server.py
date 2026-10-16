"""
cloud/server.py: Lightweight, dependency-free Cloud Service HTTP Server.
Implements the MayaCardio Cloud API:
- POST /api/v1/ingest
- GET /api/v1/status
- GET /api/v1/export
- GET /api/v1/results
"""

import argparse
import gzip
import json
import logging
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
import sys
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs
from cloud.database import CloudDatabase

logger = logging.getLogger("cloud.server")


class CloudRequestHandler(BaseHTTPRequestHandler):
    db: CloudDatabase = None
    start_time: float = time.time()
    delay_seconds: float = 0.0
    api_key: Optional[str] = None

    def log_message(self, format, *args):
        # Suppress verbose default request logging unless debugging
        pass

    def _send_json_response(self, status_code: int, data: Any):
        try:
            body = json.dumps(data).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass

    def _send_ndjson_response(self, status_code: int, lines: list):
        try:
            body = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/x-ndjson")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass

    def _is_authorized(self) -> bool:
        if not self.api_key:
            return True
        key = self.headers.get("X-API-Key")
        if not key:
            auth_header = self.headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                key = auth_header[7:].strip()
        return key == self.api_key

    def do_GET(self):
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        # Readiness and liveness probes (unauthenticated for load balancers and orchestrators)
        if path == "/healthz":
            uptime = time.time() - self.start_time
            self._send_json_response(200, {
                "status": "healthy",
                "service": "MayaCardio Cloud Ingest API",
                "uptime_seconds": round(uptime, 1)
            })
            return

        if path == "/api/v1/status" or path == "/":
            total_samples = self.db.count_samples()
            uptime = time.time() - self.start_time
            self._send_json_response(200, {
                "status": "online",
                "service": "MayaCardio Cloud Ingest API",
                "total_samples": total_samples,
                "uptime_seconds": round(uptime, 1)
            })
            return

        # Data retrieval endpoints require authorization if configured
        if not self._is_authorized():
            self._send_json_response(401, {"error": "Unauthorized: valid API key required"})
            return

        if path == "/api/v1/export":
            session_id = query.get("session_id", [""])[0]
            if not session_id:
                self._send_json_response(400, {"error": "Missing session_id parameter"})
                return
            lines = self.db.export_session_ndjson(session_id)
            self._send_ndjson_response(200, lines)

        elif path == "/api/v1/results":
            session_id = query.get("session_id", [""])[0]
            if not session_id:
                self._send_json_response(400, {"error": "Missing session_id parameter"})
                return
            lines = self.db.get_results_ndjson(session_id)
            self._send_ndjson_response(200, lines)

        elif path == "/api/v1/reconcile":
            session_id = query.get("session_id", [""])[0]
            if not session_id:
                self._send_json_response(400, {"error": "Missing session_id parameter"})
                return
            summary = self.db.get_session_reconciliation_summary(session_id)
            self._send_json_response(200, summary)

        else:
            self._send_json_response(404, {"error": "Not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/api/v1/ingest":
            self._send_json_response(404, {"error": "Not found"})
            return

        if not self._is_authorized():
            self._send_json_response(401, {"error": "Unauthorized: valid API key required"})
            return

        content_len = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_len)

        # Check for gzip compression
        if self.headers.get("Content-Encoding") == "gzip":
            try:
                raw_body = gzip.decompress(raw_body)
            except Exception as e:
                self._send_json_response(400, {"error": f"Failed to decompress gzip body: {e}"})
                return

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception as e:
            self._send_json_response(400, {"error": f"Invalid JSON payload: {e}"})
            return

        session_id = payload.get("session_id")
        if not session_id:
            self._send_json_response(400, {"error": "Missing session_id in payload"})
            return

        samples = payload.get("samples", [])
        losses = payload.get("losses", [])
        offline_results = payload.get("offline_results", [])

        # Ingest into cloud DB
        try:
            s_count, l_count, r_count = self.db.ingest_payload(session_id, samples, losses, offline_results=offline_results)
            # Trigger cloud Section 3 window processing
            new_windows = self.db.process_pending_windows(session_id)
        except Exception as e:
            logger.error(f"Cloud DB ingest failure: {e}")
            self._send_json_response(500, {
                "error": f"Database transaction commit failed: {e}",
                "durable": False
            })
            return

        self._send_json_response(200, {
            "status": "ok",
            "durable": True,
            "ingested_samples": s_count,
            "ingested_losses": l_count,
            "ingested_results": r_count,
            "computed_windows": new_windows,
        })


def run_cloud_server(
    port: Optional[int] = None,
    db_path: Optional[str] = None,
    api_key: Optional[str] = None,
    turso_url: Optional[str] = None,
    turso_token: Optional[str] = None,
):
    effective_port = port if port is not None else int(os.environ.get("PORT", "8080"))
    effective_db = db_path if db_path is not None else os.environ.get("CLOUD_DB_PATH", "cloud_data.db")
    effective_api_key = api_key if api_key is not None else os.environ.get("CLOUD_API_KEY", "")
    effective_turso_url = turso_url if turso_url is not None else os.environ.get("TURSO_DATABASE_URL")
    effective_turso_token = turso_token if turso_token is not None else os.environ.get("TURSO_AUTH_TOKEN")

    db = CloudDatabase(
        db_path=effective_db,
        turso_url=effective_turso_url,
        turso_token=effective_turso_token,
    )
    CloudRequestHandler.db = db
    CloudRequestHandler.start_time = time.time()
    CloudRequestHandler.api_key = effective_api_key or None

    server = HTTPServer(("0.0.0.0", effective_port), CloudRequestHandler)
    auth_status = "enabled" if effective_api_key else "disabled"
    backend_info = f"Turso at {effective_turso_url}" if effective_turso_url else f"SQLite at {effective_db}"
    print(f"MayaCardio Cloud Server listening on http://0.0.0.0:{effective_port} (db: {backend_info}, auth: {auth_status})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down cloud server.")
    finally:
        server.server_close()
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MayaCardio Cloud Service")
    parser.add_argument("--port", type=int, default=None, help="HTTP port to listen on (default: $PORT or 8080)")
    parser.add_argument("--db", type=str, default=None, help="Cloud SQLite database file (default: $CLOUD_DB_PATH or cloud_data.db)")
    parser.add_argument("--api-key", type=str, default=None, help="Optional API key for authorization (default: $CLOUD_API_KEY)")
    parser.add_argument("--turso-url", type=str, default=None, help="Turso / libSQL database URL (default: $TURSO_DATABASE_URL)")
    parser.add_argument("--turso-token", type=str, default=None, help="Turso auth token (default: $TURSO_AUTH_TOKEN)")
    args = parser.parse_args()

    run_cloud_server(
        port=args.port,
        db_path=args.db,
        api_key=args.api_key,
        turso_url=args.turso_url,
        turso_token=args.turso_token,
    )
