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

    def log_message(self, format, *args):
        # Suppress verbose default request logging unless debugging
        pass

    def _send_json_response(self, status_code: int, data: Any):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_ndjson_response(self, status_code: int, lines: list):
        body = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/api/v1/status" or path == "/":
            total_samples = self.db.count_samples()
            uptime = time.time() - self.start_time
            self._send_json_response(200, {
                "status": "online",
                "service": "MayaCardio Cloud Ingest API",
                "total_samples": total_samples,
                "uptime_seconds": round(uptime, 1)
            })

        elif path == "/api/v1/export":
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

        else:
            self._send_json_response(404, {"error": "Not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/api/v1/ingest":
            self._send_json_response(404, {"error": "Not found"})
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

        # Ingest into cloud DB
        s_count, l_count = self.db.ingest_payload(session_id, samples, losses)

        # Trigger cloud Section 3 window processing
        new_windows = self.db.process_pending_windows(session_id)

        total_cloud = self.db.count_samples(session_id)
        self._send_json_response(200, {
            "status": "ok",
            "ingested_samples": s_count,
            "ingested_losses": l_count,
            "computed_windows": new_windows,
            "session_total_samples": total_cloud,
        })


def run_cloud_server(port: int = 8080, db_path: str = "cloud_data.db"):
    db = CloudDatabase(db_path)
    CloudRequestHandler.db = db
    CloudRequestHandler.start_time = time.time()

    server = HTTPServer(("0.0.0.0", port), CloudRequestHandler)
    print(f"MayaCardio Cloud Server listening on http://0.0.0.0:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down cloud server.")
    finally:
        server.server_close()
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MayaCardio Cloud Service")
    parser.add_argument("--port", type=int, default=8080, help="HTTP port to listen on")
    parser.add_argument("--db", type=str, default="cloud_data.db", help="Cloud SQLite database file")
    args = parser.parse_args()

    run_cloud_server(port=args.port, db_path=args.db)
