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

CREATE INDEX IF NOT EXISTS idx_cloud_samples_sess_idx 
    ON cloud_samples (session_id, sample_idx);

CREATE TABLE IF NOT EXISTS cloud_losses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    sensor INTEGER NOT NULL,
    from_index INTEGER NOT NULL,
    to_index INTEGER,
    reason TEXT NOT NULL,
    received_at REAL NOT NULL,
    UNIQUE (session_id, sensor, from_index)
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
        self,
        session_id: str,
        samples: List[Dict[str, Any]],
        losses: List[Dict[str, Any]],
        offline_results: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[int, int, int]:
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
                    INSERT OR IGNORE INTO cloud_losses 
                    (session_id, sensor, from_index, to_index, reason, received_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    loss_rows,
                )

            r_count = 0
            if offline_results:
                result_rows = []
                for res in offline_results:
                    result_rows.append((
                        session_id,
                        res["window"],
                        res.get("mode", "offline"),
                        res["missing_1"],
                        res["missing_2"],
                        1 if res["complete"] else 0,
                        res["result_json"],
                        now,
                    ))
                cursor.executemany(
                    """
                    INSERT OR IGNORE INTO cloud_window_results 
                    (session_id, window, mode, missing_1, missing_2, complete, result_json, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    result_rows,
                )
                r_count = len(result_rows)

            cursor.execute("COMMIT")
            return len(sample_rows), len(loss_rows), r_count
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
        cursor.close()

        # A window w contains samples 400w through 400w + 399.
        # A window is only completed once stream has reached or passed index 400w + 399.
        if max_idx < 399:
            return 0

        max_window = (max_idx - 399) // 400

        # Query existing computed windows in range once to avoid repeated queries
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT window FROM cloud_window_results WHERE session_id = ? AND window <= ?",
            (session_id, max_window),
        )
        existing_windows = set(r[0] for r in cursor.fetchall())

        uncomputed_windows = [w for w in range(0, max_window + 1) if w not in existing_windows]
        if not uncomputed_windows:
            cursor.close()
            return 0

        first_w = uncomputed_windows[0]
        last_w = uncomputed_windows[-1]

        # Single range query fetching all necessary samples
        s_start = 400 * max(0, first_w - 4)
        s_end = 400 * (last_w + 1) - 1

        cursor.execute(
            """
            SELECT sensor, sample_idx, ch1, ch2, ch3 
            FROM cloud_samples 
            WHERE session_id = ? AND sample_idx >= ? AND sample_idx <= ?
            """,
            (session_id, s_start, s_end),
        )
        rows = cursor.fetchall()

        all_s1: Dict[int, List[int]] = {}
        all_s2: Dict[int, List[int]] = {}
        for r in rows:
            if r[0] == 1:
                all_s1[r[1]] = [r[2], r[3], r[4]]
            else:
                all_s2[r[1]] = [r[2], r[3]]

        new_window_rows = []
        now = time.time()
        for w in uncomputed_windows:
            win_start = 400 * max(0, w - 4)
            win_end = 400 * (w + 1) - 1

            s1_map = {idx: all_s1[idx] for idx in range(win_start, win_end + 1) if idx in all_s1}
            s2_map = {idx: all_s2[idx] for idx in range(win_start, win_end + 1) if idx in all_s2}

            res = process_window(session_id, w, mode="online", s1_samples_map=s1_map, s2_samples_map=s2_map)
            res_json = json.dumps(res)
            new_window_rows.append((
                session_id,
                w,
                "online",
                res["missing_1"],
                res["missing_2"],
                1 if res["complete"] else 0,
                res_json,
                now,
            ))

        if new_window_rows:
            cursor.execute("BEGIN IMMEDIATE")
            try:
                cursor.executemany(
                    """
                    INSERT OR IGNORE INTO cloud_window_results 
                    (session_id, window, mode, missing_1, missing_2, complete, result_json, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    new_window_rows,
                )
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")

        cursor.close()
        return len(new_window_rows)

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

    def get_session_reconciliation_summary(self, session_id: str) -> Dict[str, Any]:
        """Provides an auditable reconciliation summary of session data in the cloud."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM cloud_samples WHERE session_id = ?", (session_id,))
        total = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(*), MIN(sample_idx), MAX(sample_idx) FROM cloud_samples WHERE session_id = ? AND sensor = 1",
            (session_id,),
        )
        s1_row = cursor.fetchone()
        cursor.execute(
            "SELECT COUNT(*), MIN(sample_idx), MAX(sample_idx) FROM cloud_samples WHERE session_id = ? AND sensor = 2",
            (session_id,),
        )
        s2_row = cursor.fetchone()
        cursor.execute("SELECT COUNT(*) FROM cloud_losses WHERE session_id = ?", (session_id,))
        losses_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM cloud_window_results WHERE session_id = ?", (session_id,))
        results_count = cursor.fetchone()[0]
        cursor.close()

        return {
            "session_id": session_id,
            "total_samples": total,
            "sensor1_count": s1_row[0] if s1_row else 0,
            "sensor1_min_idx": s1_row[1] if s1_row else None,
            "sensor1_max_idx": s1_row[2] if s1_row else None,
            "sensor2_count": s2_row[0] if s2_row else 0,
            "sensor2_min_idx": s2_row[1] if s2_row else None,
            "sensor2_max_idx": s2_row[2] if s2_row else None,
            "losses_count": losses_count,
            "window_results_count": results_count,
        }

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
