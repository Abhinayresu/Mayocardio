"""
cloud/database.py: Cloud storage and window processing database.
Stores ingested samples and losses idempotently, and computes online window results.
"""

import json
import os
import sqlite3
import time
from typing import Dict, List, Optional, Tuple, Any
from pipeline.signal_processor import process_window

CLOUD_SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS cloud_samples (
    session_id TEXT NOT NULL,
    sensor INTEGER NOT NULL,
    sample_idx INTEGER NOT NULL,
    ch1 INTEGER NOT NULL,
    ch2 INTEGER NOT NULL,
    ch3 INTEGER,
    received_at REAL NOT NULL,
    PRIMARY KEY (session_id, sensor, sample_idx)
);

CREATE INDEX IF NOT EXISTS idx_cloud_samples_win 
    ON cloud_samples (session_id, sensor, sample_idx);

CREATE TABLE IF NOT EXISTS cloud_losses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    sensor INTEGER NOT NULL,
    from_index INTEGER NOT NULL,
    to_index INTEGER,
    reason TEXT NOT NULL,
    received_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS cloud_window_results (
    session_id TEXT NOT NULL,
    window INTEGER NOT NULL,
    mode TEXT NOT NULL,
    missing_1 INTEGER NOT NULL,
    missing_2 INTEGER NOT NULL,
    complete INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    processed_at REAL NOT NULL,
    PRIMARY KEY (session_id, window)
);
"""


class CloudDatabase:
    def __init__(self, db_path: str = "cloud_data.db"):
        self.db_path = db_path
        d = os.path.dirname(self.db_path)
        if d:
            os.makedirs(d, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        cursor = self.conn.cursor()
        cursor.executescript(CLOUD_SCHEMA_SQL)
        cursor.close()

    def ingest_payload(
        self, session_id: str, samples: List[Dict[str, Any]], losses: List[Dict[str, Any]]
    ) -> Tuple[int, int]:
        now = time.time()
        cursor = self.conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            sample_rows = []
            for s in samples:
                sensor = s["sensor"]
                idx = s["sample_idx"]
                vals = s["values"]
                ch1 = vals[0]
                ch2 = vals[1]
                ch3 = vals[2] if sensor == 1 and len(vals) > 2 else None
                sample_rows.append((session_id, sensor, idx, ch1, ch2, ch3, now))

            if sample_rows:
                cursor.executemany(
                    """
                    INSERT OR IGNORE INTO cloud_samples 
                    (session_id, sensor, sample_idx, ch1, ch2, ch3, received_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    sample_rows,
                )

            loss_rows = []
            for l in losses:
                loss_rows.append((
                    session_id,
                    l["sensor"],
                    l["from_index"],
                    l.get("to_index"),
                    l.get("reason", "unknown"),
                    now,
                ))

            if loss_rows:
                cursor.executemany(
                    """
                    INSERT INTO cloud_losses 
                    (session_id, sensor, from_index, to_index, reason, received_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    loss_rows,
                )

            cursor.execute("COMMIT")
            return len(sample_rows), len(loss_rows)
        except Exception as e:
            cursor.execute("ROLLBACK")
            raise e
        finally:
            cursor.close()

    def count_samples(self, session_id: Optional[str] = None) -> int:
        cursor = self.conn.cursor()
        if session_id:
            cursor.execute("SELECT COUNT(*) FROM cloud_samples WHERE session_id = ?", (session_id,))
        else:
            cursor.execute("SELECT COUNT(*) FROM cloud_samples")
        res = cursor.fetchone()[0]
        cursor.close()
        return res

    def process_pending_windows(self, session_id: str) -> int:
        """
        Check for uncomputed windows in the cloud and compute Section 3 outputs (online mode).
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT MAX(sample_idx) as max_idx FROM cloud_samples WHERE session_id = ?
            """,
            (session_id,),
        )
        row = cursor.fetchone()
        if not row or row["max_idx"] is None:
            cursor.close()
            return 0

        max_idx = row["max_idx"]
        max_window = max_idx // 400

        cursor.execute(
            "SELECT COALESCE(MAX(window), -1) as last_win FROM cloud_window_results WHERE session_id = ?",
            (session_id,),
        )
        last_win = cursor.fetchone()["last_win"]
        cursor.close()

        computed_count = 0
        for w in range(last_win + 1, max_window + 1):
            # Fetch samples for window w
            # For ECG peak and HR, need window w-4 through w (if w >= 4)
            s_start = 400 * max(0, w - 4)
            s_end = 400 * (w + 1) - 1

            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT sensor, sample_idx, ch1, ch2, ch3 
                FROM cloud_samples 
                WHERE session_id = ? AND sample_idx >= ? AND sample_idx <= ?
                """,
                (session_id, s_start, s_end),
            )
            rows = cursor.fetchall()
            cursor.close()

            s1_map = {}
            s2_map = {}
            for r in rows:
                if r["sensor"] == 1:
                    s1_map[r["sample_idx"]] = [r["ch1"], r["ch2"], r["ch3"]]
                else:
                    s2_map[r["sample_idx"]] = [r["ch1"], r["ch2"]]

            res = process_window(session_id, w, mode="online", s1_samples_map=s1_map, s2_samples_map=s2_map)
            res_json = json.dumps(res)

            cursor = self.conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            try:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO cloud_window_results 
                    (session_id, window, mode, missing_1, missing_2, complete, result_json, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (session_id, w, "online", res["missing_1"], res["missing_2"], 1 if res["complete"] else 0, res_json, time.time()),
                )
                cursor.execute("COMMIT")
                computed_count += 1
            except Exception:
                cursor.execute("ROLLBACK")
            finally:
                cursor.close()

        return computed_count

    def export_session_ndjson(self, session_id: str) -> List[str]:
        lines = []
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT sensor, sample_idx, ch1, ch2, ch3 
            FROM cloud_samples 
            WHERE session_id = ? 
            ORDER BY sample_idx ASC, sensor ASC
            """,
            (session_id,),
        )
        for r in cursor:
            vals = [r["ch1"], r["ch2"]] if r["sensor"] == 2 else [r["ch1"], r["ch2"], r["ch3"]]
            lines.append(json.dumps({"sensor": r["sensor"], "index": r["sample_idx"], "values": vals}))

        cursor.execute(
            """
            SELECT sensor, from_index, to_index, reason 
            FROM cloud_losses 
            WHERE session_id = ? 
            ORDER BY from_index ASC, sensor ASC
            """,
            (session_id,),
        )
        for r in cursor:
            lines.append(json.dumps({
                "sensor": r["sensor"],
                "from_index": r["from_index"],
                "to_index": r["to_index"],
                "reason": r["reason"],
            }))

        cursor.close()
        return lines

    def get_results_ndjson(self, session_id: str) -> List[str]:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT result_json 
            FROM cloud_window_results 
            WHERE session_id = ? 
            ORDER BY window ASC
            """,
            (session_id,),
        )
        rows = cursor.fetchall()
        cursor.close()
        return [r["result_json"] for r in rows]

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
