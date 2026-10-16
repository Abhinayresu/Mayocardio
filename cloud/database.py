"""
cloud/database.py: Cloud storage and window processing database.
Provides dual adapters:
1. SQLiteCloudDatabase: High-performance local SQLite engine with WAL mode.
2. TursoCloudDatabase: Durable cloud SQLite (libSQL) engine over HTTP (/v2/pipeline).

Factory function create_cloud_database and CloudDatabase automatically select
the appropriate adapter based on environment variables or explicit parameters.
"""

from abc import ABC, abstractmethod
import base64
import json
import logging
import os
import sqlite3
import time
import urllib.error
import urllib.request
from typing import Dict, List, Optional, Tuple, Any

from pipeline.signal_processor import process_window

logger = logging.getLogger("cloud.database")

SQLITE_SCHEMA_SQL = """
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

TURSO_SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS cloud_samples (
        session_id TEXT NOT NULL,
        sensor INTEGER NOT NULL,
        sample_idx INTEGER NOT NULL,
        ch1 INTEGER NOT NULL,
        ch2 INTEGER NOT NULL,
        ch3 INTEGER,
        received_at REAL NOT NULL,
        PRIMARY KEY (session_id, sensor, sample_idx)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_cloud_samples_win ON cloud_samples (session_id, sensor, sample_idx)",
    "CREATE INDEX IF NOT EXISTS idx_cloud_samples_sess_idx ON cloud_samples (session_id, sample_idx)",
    """
    CREATE TABLE IF NOT EXISTS cloud_losses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        sensor INTEGER NOT NULL,
        from_index INTEGER NOT NULL,
        to_index INTEGER,
        reason TEXT NOT NULL,
        received_at REAL NOT NULL,
        UNIQUE (session_id, sensor, from_index)
    )
    """,
    """
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
    )
    """,
]


def _format_turso_arg(val: Any) -> Dict[str, Any]:
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


def _unpack_turso_cell(cell: Dict[str, Any]) -> Any:
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


class BaseCloudDatabase(ABC):
    """Abstract interface defining the cloud database contract."""

    @abstractmethod
    def ingest_payload(
        self,
        session_id: str,
        samples: List[Dict[str, Any]],
        losses: List[Dict[str, Any]],
        offline_results: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[int, int, int]:
        pass

    @abstractmethod
    def count_samples(self, session_id: Optional[str] = None) -> int:
        pass

    @abstractmethod
    def process_pending_windows(self, session_id: str) -> int:
        pass

    @abstractmethod
    def export_session_ndjson(self, session_id: str) -> List[str]:
        pass

    @abstractmethod
    def get_results_ndjson(self, session_id: str) -> List[str]:
        pass

    @abstractmethod
    def get_session_reconciliation_summary(self, session_id: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def close(self):
        pass


class SQLiteCloudDatabase(BaseCloudDatabase):
    """
    Local SQLite cloud database implementation using WAL mode.
    Preserves verified, deterministic performance for local development and unit tests.
    """

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
        cursor.executescript(SQLITE_SCHEMA_SQL)
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
        if max_idx < 399:
            cursor.close()
            return 0

        max_window = (max_idx - 399) // 400

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


class TursoCloudDatabase(BaseCloudDatabase):
    """
    Durable external cloud database adapter for Turso (libSQL) over HTTP (/v2/pipeline).
    Solves Render Free tier ephemeral disk persistence by storing all records externally in Turso.
    """

    def __init__(self, turso_url: str, turso_token: str, timeout_sec: float = 10.0):
        # Normalize protocol to https://
        clean_url = turso_url.rstrip("/")
        if clean_url.startswith("libsql://"):
            clean_url = "https://" + clean_url[9:]
        elif clean_url.startswith("turso://"):
            clean_url = "https://" + clean_url[8:]
        elif not clean_url.startswith("http://") and not clean_url.startswith("https://"):
            clean_url = "https://" + clean_url

        self.turso_url = clean_url
        self.turso_token = turso_token
        self.timeout_sec = timeout_sec
        self._init_db()

    def _execute_pipeline(self, statements: List[Tuple[str, List[Any]]]) -> List[Dict[str, Any]]:
        """
        Executes a sequence of SQL statements within a single HTTP pipeline request.
        Raises an exception if any statement errors or connection fails.
        """
        requests = []
        for sql, params in statements:
            stmt_obj: Dict[str, Any] = {"sql": sql}
            if params:
                stmt_obj["args"] = [_format_turso_arg(p) for p in params]
            requests.append({"type": "execute", "stmt": stmt_obj})
        requests.append({"type": "close"})

        body = json.dumps({"requests": requests}).encode("utf-8")
        req = urllib.request.Request(
            f"{self.turso_url}/v2/pipeline",
            data=body,
            headers={
                "Authorization": f"Bearer {self.turso_token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Turso HTTP error: status {resp.status}")
                resp_data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8") if e.fp else ""
            raise RuntimeError(f"Turso HTTP Error {e.code}: {err_body or e.reason}")
        except Exception as e:
            raise RuntimeError(f"Turso connection error: {e}")

        results = resp_data.get("results", [])
        for idx, res in enumerate(results):
            if res.get("type") == "error":
                err = res.get("error", {})
                raise RuntimeError(f"Turso statement {idx} error: {err.get('message', 'Unknown error')}")

        return results

    def _init_db(self):
        stmts = [(sql.strip(), []) for sql in TURSO_SCHEMA_SQL]
        try:
            self._execute_pipeline(stmts)
        except Exception as e:
            logger.warning(f"Turso schema initialization notice: {e}")

    def ingest_payload(
        self,
        session_id: str,
        samples: List[Dict[str, Any]],
        losses: List[Dict[str, Any]],
        offline_results: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[int, int, int]:
        now = time.time()
        stmts = [("BEGIN", [])]

        for s in samples:
            sensor = s["sensor"]
            idx = s["sample_idx"]
            vals = s["values"]
            ch1 = vals[0]
            ch2 = vals[1]
            ch3 = vals[2] if sensor == 1 and len(vals) > 2 else None
            stmts.append((
                "INSERT OR IGNORE INTO cloud_samples (session_id, sensor, sample_idx, ch1, ch2, ch3, received_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [session_id, sensor, idx, ch1, ch2, ch3, now]
            ))

        for l in losses:
            stmts.append((
                "INSERT OR IGNORE INTO cloud_losses (session_id, sensor, from_index, to_index, reason, received_at) VALUES (?, ?, ?, ?, ?, ?)",
                [session_id, l["sensor"], l["from_index"], l.get("to_index"), l.get("reason", "unknown"), now]
            ))

        r_count = 0
        if offline_results:
            for res in offline_results:
                stmts.append((
                    "INSERT OR IGNORE INTO cloud_window_results (session_id, window, mode, missing_1, missing_2, complete, result_json, processed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [session_id, res["window"], res.get("mode", "offline"), res["missing_1"], res["missing_2"], 1 if res["complete"] else 0, res["result_json"], now]
                ))
            r_count = len(offline_results)

        stmts.append(("COMMIT", []))

        # Atomic commit to Turso
        self._execute_pipeline(stmts)
        return len(samples), len(losses), r_count

    def count_samples(self, session_id: Optional[str] = None) -> int:
        if session_id:
            stmts = [("SELECT COUNT(*) FROM cloud_samples WHERE session_id = ?", [session_id])]
        else:
            stmts = [("SELECT COUNT(*) FROM cloud_samples", [])]
        results = self._execute_pipeline(stmts)
        res_obj = results[0].get("response", {}).get("result", {})
        rows = res_obj.get("rows", [])
        if rows:
            return _unpack_turso_cell(rows[0][0])
        return 0

    def process_pending_windows(self, session_id: str) -> int:
        stmts = [
            ("SELECT MAX(sample_idx) FROM cloud_samples WHERE session_id = ?", [session_id]),
        ]
        results = self._execute_pipeline(stmts)
        max_idx_rows = results[0].get("response", {}).get("result", {}).get("rows", [])
        if not max_idx_rows or not max_idx_rows[0] or max_idx_rows[0][0].get("type") == "null":
            return 0
        max_idx = _unpack_turso_cell(max_idx_rows[0][0])
        if max_idx is None or max_idx < 399:
            return 0

        max_window = (max_idx - 399) // 400

        stmts = [
            ("SELECT window FROM cloud_window_results WHERE session_id = ? AND window <= ?", [session_id, max_window]),
        ]
        res_win = self._execute_pipeline(stmts)
        existing_rows = res_win[0].get("response", {}).get("result", {}).get("rows", [])
        existing_windows = set(_unpack_turso_cell(r[0]) for r in existing_rows)

        uncomputed_windows = [w for w in range(0, max_window + 1) if w not in existing_windows]
        if not uncomputed_windows:
            return 0

        first_w = uncomputed_windows[0]
        last_w = uncomputed_windows[-1]
        s_start = 400 * max(0, first_w - 4)
        s_end = 400 * (last_w + 1) - 1

        stmts = [
            ("SELECT sensor, sample_idx, ch1, ch2, ch3 FROM cloud_samples WHERE session_id = ? AND sample_idx >= ? AND sample_idx <= ?", [session_id, s_start, s_end]),
        ]
        res_samples = self._execute_pipeline(stmts)
        sample_rows = res_samples[0].get("response", {}).get("result", {}).get("rows", [])

        all_s1: Dict[int, List[int]] = {}
        all_s2: Dict[int, List[int]] = {}
        for r in sample_rows:
            sensor = _unpack_turso_cell(r[0])
            s_idx = _unpack_turso_cell(r[1])
            ch1 = _unpack_turso_cell(r[2])
            ch2 = _unpack_turso_cell(r[3])
            if sensor == 1:
                ch3 = _unpack_turso_cell(r[4])
                all_s1[s_idx] = [ch1, ch2, ch3]
            else:
                all_s2[s_idx] = [ch1, ch2]

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
            stmts = [("BEGIN", [])]
            for row in new_window_rows:
                stmts.append((
                    "INSERT OR IGNORE INTO cloud_window_results (session_id, window, mode, missing_1, missing_2, complete, result_json, processed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    list(row),
                ))
            stmts.append(("COMMIT", []))
            self._execute_pipeline(stmts)

        return len(new_window_rows)

    def export_session_ndjson(self, session_id: str) -> List[str]:
        stmts = [
            ("SELECT sensor, sample_idx, ch1, ch2, ch3 FROM cloud_samples WHERE session_id = ? ORDER BY sample_idx ASC, sensor ASC", [session_id]),
            ("SELECT sensor, from_index, to_index, reason FROM cloud_losses WHERE session_id = ? ORDER BY from_index ASC, sensor ASC", [session_id]),
        ]
        results = self._execute_pipeline(stmts)
        sample_rows = results[0].get("response", {}).get("result", {}).get("rows", [])
        loss_rows = results[1].get("response", {}).get("result", {}).get("rows", [])

        lines = []
        for r in sample_rows:
            sensor = _unpack_turso_cell(r[0])
            s_idx = _unpack_turso_cell(r[1])
            ch1 = _unpack_turso_cell(r[2])
            ch2 = _unpack_turso_cell(r[3])
            vals = [ch1, ch2, _unpack_turso_cell(r[4])] if sensor == 1 else [ch1, ch2]
            lines.append(json.dumps({"sensor": sensor, "index": s_idx, "values": vals}))

        for r in loss_rows:
            sensor = _unpack_turso_cell(r[0])
            from_idx = _unpack_turso_cell(r[1])
            to_idx = _unpack_turso_cell(r[2])
            reason = _unpack_turso_cell(r[3])
            lines.append(json.dumps({"sensor": sensor, "from_index": from_idx, "to_index": to_idx, "reason": reason}))

        return lines

    def get_results_ndjson(self, session_id: str) -> List[str]:
        stmts = [
            ("SELECT result_json FROM cloud_window_results WHERE session_id = ? ORDER BY window ASC", [session_id]),
        ]
        results = self._execute_pipeline(stmts)
        rows = results[0].get("response", {}).get("result", {}).get("rows", [])
        return [_unpack_turso_cell(r[0]) for r in rows]

    def get_session_reconciliation_summary(self, session_id: str) -> Dict[str, Any]:
        stmts = [
            ("SELECT COUNT(*) FROM cloud_samples WHERE session_id = ?", [session_id]),
            ("SELECT COUNT(*), MIN(sample_idx), MAX(sample_idx) FROM cloud_samples WHERE session_id = ? AND sensor = 1", [session_id]),
            ("SELECT COUNT(*), MIN(sample_idx), MAX(sample_idx) FROM cloud_samples WHERE session_id = ? AND sensor = 2", [session_id]),
            ("SELECT COUNT(*) FROM cloud_losses WHERE session_id = ?", [session_id]),
            ("SELECT COUNT(*) FROM cloud_window_results WHERE session_id = ?", [session_id]),
        ]
        results = self._execute_pipeline(stmts)
        total = _unpack_turso_cell(results[0]["response"]["result"]["rows"][0][0])
        s1_rows = results[1]["response"]["result"]["rows"]
        s2_rows = results[2]["response"]["result"]["rows"]
        losses_count = _unpack_turso_cell(results[3]["response"]["result"]["rows"][0][0])
        results_count = _unpack_turso_cell(results[4]["response"]["result"]["rows"][0][0])

        s1_c = _unpack_turso_cell(s1_rows[0][0]) if s1_rows else 0
        s1_min = _unpack_turso_cell(s1_rows[0][1]) if s1_rows else None
        s1_max = _unpack_turso_cell(s1_rows[0][2]) if s1_rows else None

        s2_c = _unpack_turso_cell(s2_rows[0][0]) if s2_rows else 0
        s2_min = _unpack_turso_cell(s2_rows[0][1]) if s2_rows else None
        s2_max = _unpack_turso_cell(s2_rows[0][2]) if s2_rows else None

        return {
            "session_id": session_id,
            "total_samples": total,
            "sensor1_count": s1_c,
            "sensor1_min_idx": s1_min,
            "sensor1_max_idx": s1_max,
            "sensor2_count": s2_c,
            "sensor2_min_idx": s2_min,
            "sensor2_max_idx": s2_max,
            "losses_count": losses_count,
            "window_results_count": results_count,
        }

    def close(self):
        pass


def create_cloud_database(
    db_path: Optional[str] = None,
    turso_url: Optional[str] = None,
    turso_token: Optional[str] = None,
) -> BaseCloudDatabase:
    """
    Factory function returning TursoCloudDatabase if turso_url / TURSO_DATABASE_URL is provided,
    otherwise returning SQLiteCloudDatabase.
    """
    effective_turso_url = turso_url or os.environ.get("TURSO_DATABASE_URL")
    effective_turso_token = turso_token or os.environ.get("TURSO_AUTH_TOKEN")

    if effective_turso_url:
        return TursoCloudDatabase(
            turso_url=effective_turso_url,
            turso_token=effective_turso_token or "",
        )
    return SQLiteCloudDatabase(db_path=db_path or os.environ.get("CLOUD_DB_PATH", "cloud_data.db"))


class CloudDatabase:
    """
    Backward-compatible factory/proxy class.
    Preserves exact constructor signature CloudDatabase(db_path=...) for existing tests,
    while automatically instantiating TursoCloudDatabase if turso_url is supplied.
    """

    def __new__(
        cls,
        db_path: str = "cloud_data.db",
        turso_url: Optional[str] = None,
        turso_token: Optional[str] = None,
    ):
        return create_cloud_database(
            db_path=db_path,
            turso_url=turso_url,
            turso_token=turso_token,
        )
