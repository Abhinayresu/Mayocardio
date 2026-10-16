"""
storage.py: Durable, transactional SQLite storage engine for MayaCardio pipeline.
Implements WAL mode, atomic batch inserts, duplicate rejection, and sync tracking.
"""

import json
import os
import sqlite3
import time
from typing import Dict, List, Optional, Tuple, Any

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS samples (
    session_id TEXT NOT NULL,
    sensor INTEGER NOT NULL,
    sample_idx INTEGER NOT NULL,
    ch1 INTEGER NOT NULL,
    ch2 INTEGER NOT NULL,
    ch3 INTEGER,
    synced_to_cloud INTEGER DEFAULT 0,
    created_at REAL NOT NULL,
    PRIMARY KEY (session_id, sensor, sample_idx)
);

CREATE INDEX IF NOT EXISTS idx_samples_sync 
    ON samples (synced_to_cloud, session_id, sensor, sample_idx);

CREATE INDEX IF NOT EXISTS idx_samples_window 
    ON samples (session_id, sensor, sample_idx);

CREATE TABLE IF NOT EXISTS losses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    sensor INTEGER NOT NULL,
    from_index INTEGER NOT NULL,
    to_index INTEGER,
    reason TEXT NOT NULL,
    synced_to_cloud INTEGER DEFAULT 0,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_losses_session 
    ON losses (session_id, sensor);

CREATE TABLE IF NOT EXISTS session_state (
    session_id TEXT PRIMARY KEY,
    sensor1_last_index INTEGER DEFAULT -1,
    sensor2_last_index INTEGER DEFAULT -1,
    is_ended INTEGER DEFAULT 0,
    cloud_acked_s1_index INTEGER DEFAULT -1,
    cloud_acked_s2_index INTEGER DEFAULT -1,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS window_results (
    session_id TEXT NOT NULL,
    window INTEGER NOT NULL,
    mode TEXT NOT NULL,
    missing_1 INTEGER NOT NULL,
    missing_2 INTEGER NOT NULL,
    complete INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    synced_to_cloud INTEGER DEFAULT 0,
    created_at REAL NOT NULL,
    PRIMARY KEY (session_id, window)
);
"""


class StorageEngine:
    """Manages transactional durability, querying, and exports."""

    def __init__(self, db_path: str = "pipeline_data.db"):
        self.db_path = db_path
        self._ensure_dir()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _ensure_dir(self):
        d = os.path.dirname(self.db_path)
        if d:
            os.makedirs(d, exist_ok=True)

    def _init_db(self):
        cursor = self.conn.cursor()
        cursor.executescript(SCHEMA_SQL)
        cursor.close()

    def insert_samples_batch(
        self, session_id: str, sensor: int, first_index: int, data: List[List[int]]
    ) -> int:
        """Insert a batch of samples atomically. Skips duplicates with INSERT OR IGNORE."""
        if not data:
            return 0

        now = time.time()
        rows = []
        for i, sample in enumerate(data):
            idx = first_index + i
            ch1 = sample[0]
            ch2 = sample[1]
            ch3 = sample[2] if sensor == 1 and len(sample) > 2 else None
            rows.append((session_id, sensor, idx, ch1, ch2, ch3, 0, now))

        cursor = self.conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            cursor.executemany(
                """
                INSERT OR IGNORE INTO samples 
                (session_id, sensor, sample_idx, ch1, ch2, ch3, synced_to_cloud, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            # Update session state
            last_idx = first_index + len(data) - 1
            col_name = "sensor1_last_index" if sensor == 1 else "sensor2_last_index"
            cursor.execute(
                f"""
                INSERT INTO session_state (session_id, {col_name}, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    {col_name} = MAX({col_name}, excluded.{col_name}),
                    updated_at = excluded.updated_at
                """,
                (session_id, last_idx, now, now),
            )
            cursor.execute("COMMIT")
            return len(rows)
        except Exception as e:
            cursor.execute("ROLLBACK")
            raise e
        finally:
            cursor.close()

    def insert_loss(
        self, session_id: str, sensor: int, from_index: int, to_index: Optional[int], reason: str
    ):
        """Record an explicit loss range."""
        cursor = self.conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            cursor.execute(
                """
                INSERT INTO losses (session_id, sensor, from_index, to_index, reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (session_id, sensor, from_index, to_index, reason, time.time()),
            )
            cursor.execute("COMMIT")
        except Exception as e:
            cursor.execute("ROLLBACK")
            raise e
        finally:
            cursor.close()

    def get_highest_sample_index(self, session_id: str, sensor: int) -> int:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT MAX(sample_idx) FROM samples WHERE session_id = ? AND sensor = ?",
            (session_id, sensor),
        )
        row = cursor.fetchone()
        cursor.close()
        return row[0] if (row and row[0] is not None) else -1

    def count_samples(self, session_id: Optional[str] = None) -> int:
        cursor = self.conn.cursor()
        if session_id:
            cursor.execute("SELECT COUNT(*) FROM samples WHERE session_id = ?", (session_id,))
        else:
            cursor.execute("SELECT COUNT(*) FROM samples")
        res = cursor.fetchone()[0]
        cursor.close()
        return res

    def count_synced_samples(self, session_id: Optional[str] = None) -> int:
        cursor = self.conn.cursor()
        if session_id:
            cursor.execute(
                "SELECT COUNT(*) FROM samples WHERE session_id = ? AND synced_to_cloud = 1",
                (session_id,),
            )
        else:
            cursor.execute("SELECT COUNT(*) FROM samples WHERE synced_to_cloud = 1")
        res = cursor.fetchone()[0]
        cursor.close()
        return res

    def get_unsynced_samples(
        self, limit: int = 1000
    ) -> List[Tuple[str, int, int, List[int]]]:
        """Returns list of (session_id, sensor, sample_idx, values)."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT session_id, sensor, sample_idx, ch1, ch2, ch3 
            FROM samples 
            WHERE synced_to_cloud = 0 
            ORDER BY session_id, sensor, sample_idx 
            LIMIT ?
            """,
            (limit,),
        )
        res = []
        for r in cursor:
            vals = [r[3], r[4]] if r[1] == 2 else [r[3], r[4], r[5]]
            res.append((r[0], r[1], r[2], vals))
        cursor.close()
        return res

    def get_all_unsynced(
        self, sample_limit: int = 10000, loss_limit: int = 200, result_limit: int = 100
    ) -> Tuple[List, List, List]:
        """Fetches unsynced samples, losses, and results in a single method call."""
        samples = self.get_unsynced_samples(limit=sample_limit)
        losses = self.get_unsynced_losses(limit=loss_limit)
        results = self.get_unsynced_window_results(limit=result_limit)
        return samples, losses, results

    def mark_samples_synced(self, sample_keys: List[Tuple[str, int, int]]):
        """Mark given list of (session_id, sensor, sample_idx) as synced to cloud."""
        if not sample_keys:
            return

        # Group by (session_id, sensor) and find contiguous ranges for high throughput
        grouped: Dict[Tuple[str, int], List[int]] = {}
        for sess_id, sensor, idx in sample_keys:
            key = (sess_id, sensor)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(idx)

        ranges: List[Tuple[str, int, int, int]] = []
        for (sess_id, sensor), indices in grouped.items():
            indices.sort()
            start = indices[0]
            prev = indices[0]
            for curr in indices[1:]:
                if curr == prev + 1:
                    prev = curr
                else:
                    ranges.append((sess_id, sensor, start, prev))
                    start = curr
                    prev = curr
            ranges.append((sess_id, sensor, start, prev))

        cursor = self.conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            cursor.executemany(
                """
                UPDATE samples 
                SET synced_to_cloud = 1 
                WHERE session_id = ? AND sensor = ? AND sample_idx >= ? AND sample_idx <= ? AND synced_to_cloud = 0
                """,
                ranges,
            )
            cursor.execute("COMMIT")
        except Exception as e:
            cursor.execute("ROLLBACK")
            raise e
        finally:
            cursor.close()

    def get_unsynced_losses(self, limit: int = 500) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT id, session_id, sensor, from_index, to_index, reason 
            FROM losses 
            WHERE synced_to_cloud = 0 
            ORDER BY id ASC 
            LIMIT ?
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        cursor.close()
        return [
            {
                "id": r["id"],
                "session_id": r["session_id"],
                "sensor": r["sensor"],
                "from_index": r["from_index"],
                "to_index": r["to_index"],
                "reason": r["reason"],
            }
            for r in rows
        ]

    def mark_losses_synced(self, loss_ids: List[int]):
        if not loss_ids:
            return
        cursor = self.conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            cursor.executemany(
                "UPDATE losses SET synced_to_cloud = 1 WHERE id = ?",
                [(lid,) for lid in loss_ids],
            )
            cursor.execute("COMMIT")
        except Exception as e:
            cursor.execute("ROLLBACK")
            raise e
        finally:
            cursor.close()

    def get_unsynced_window_results(self, limit: int = 500) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT session_id, window, mode, missing_1, missing_2, complete, result_json 
            FROM window_results 
            WHERE synced_to_cloud = 0 
            ORDER BY window ASC 
            LIMIT ?
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        cursor.close()
        return [
            {
                "session_id": r["session_id"],
                "window": r["window"],
                "mode": r["mode"],
                "missing_1": r["missing_1"],
                "missing_2": r["missing_2"],
                "complete": bool(r["complete"]),
                "result_json": r["result_json"],
            }
            for r in rows
        ]

    def mark_window_results_synced(self, keys: List[Tuple[str, int]]):
        if not keys:
            return
        cursor = self.conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            cursor.executemany(
                "UPDATE window_results SET synced_to_cloud = 1 WHERE session_id = ? AND window = ?",
                keys,
            )
            cursor.execute("COMMIT")
        except Exception as e:
            cursor.execute("ROLLBACK")
            raise e
        finally:
            cursor.close()


    def get_window_samples(
        self, session_id: str, sensor: int, start_idx: int, end_idx: int
    ) -> Dict[int, List[int]]:
        """Returns dict of {sample_idx: values} for given window."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT sample_idx, ch1, ch2, ch3 
            FROM samples 
            WHERE session_id = ? AND sensor = ? AND sample_idx >= ? AND sample_idx <= ?
            ORDER BY sample_idx ASC
            """,
            (session_id, sensor, start_idx, end_idx),
        )
        rows = cursor.fetchall()
        cursor.close()
        res = {}
        for r in rows:
            vals = [r["ch1"], r["ch2"]] if sensor == 2 else [r["ch1"], r["ch2"], r["ch3"]]
            res[r["sample_idx"]] = vals
        return res

    def save_window_result(
        self, session_id: str, window: int, mode: str, missing_1: int, missing_2: int, complete: bool, result_json: str
    ):
        cursor = self.conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO window_results
                (session_id, window, mode, missing_1, missing_2, complete, result_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (session_id, window, mode, missing_1, missing_2, 1 if complete else 0, result_json, time.time()),
            )
            cursor.execute("COMMIT")
        except Exception as e:
            cursor.execute("ROLLBACK")
            raise e
        finally:
            cursor.close()

    def get_window_results(self, session_id: str) -> List[str]:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT result_json 
            FROM window_results 
            WHERE session_id = ? 
            ORDER BY window ASC, mode ASC
            """,
            (session_id,),
        )
        rows = cursor.fetchall()
        cursor.close()
        return [r["result_json"] for r in rows]

    def export_session_ndjson(self, session_id: str) -> List[str]:
        """
        Produce exported data lines:
        {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
        followed by loss lines:
        {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}
        """
        lines = []
        cursor = self.conn.cursor()

        # Samples
        cursor.execute(
            """
            SELECT sensor, sample_idx, ch1, ch2, ch3 
            FROM samples 
            WHERE session_id = ? 
            ORDER BY sample_idx ASC, sensor ASC
            """,
            (session_id,),
        )
        for r in cursor:
            vals = [r["ch1"], r["ch2"]] if r["sensor"] == 2 else [r["ch1"], r["ch2"], r["ch3"]]
            lines.append(json.dumps({"sensor": r["sensor"], "index": r["sample_idx"], "values": vals}))

        # Losses
        cursor.execute(
            """
            SELECT sensor, from_index, to_index, reason 
            FROM losses 
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

    def get_session_reconciliation_summary(self, session_id: str) -> Dict[str, Any]:
        """Provides an auditable reconciliation summary of session data on device."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM samples WHERE session_id = ?", (session_id,))
        total = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM samples WHERE session_id = ? AND synced_to_cloud = 1", (session_id,))
        synced = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(*), MIN(sample_idx), MAX(sample_idx) FROM samples WHERE session_id = ? AND sensor = 1",
            (session_id,),
        )
        s1_row = cursor.fetchone()
        cursor.execute(
            "SELECT COUNT(*), MIN(sample_idx), MAX(sample_idx) FROM samples WHERE session_id = ? AND sensor = 2",
            (session_id,),
        )
        s2_row = cursor.fetchone()
        cursor.execute("SELECT COUNT(*) FROM losses WHERE session_id = ?", (session_id,))
        losses_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM losses WHERE session_id = ? AND synced_to_cloud = 1", (session_id,))
        losses_synced = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM window_results WHERE session_id = ?", (session_id,))
        results_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM window_results WHERE session_id = ? AND synced_to_cloud = 1", (session_id,))
        results_synced = cursor.fetchone()[0]
        cursor.close()

        return {
            "session_id": session_id,
            "total_samples": total,
            "synced_samples": synced,
            "unsynced_samples": total - synced,
            "sensor1_count": s1_row[0] if s1_row else 0,
            "sensor1_min_idx": s1_row[1] if s1_row else None,
            "sensor1_max_idx": s1_row[2] if s1_row else None,
            "sensor2_count": s2_row[0] if s2_row else 0,
            "sensor2_min_idx": s2_row[1] if s2_row else None,
            "sensor2_max_idx": s2_row[2] if s2_row else None,
            "losses_count": losses_count,
            "unsynced_losses": losses_count - losses_synced,
            "window_results_count": results_count,
            "unsynced_window_results": results_count - results_synced,
        }

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
