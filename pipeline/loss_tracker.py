"""
loss_tracker.py: Explicit loss accounting engine.
Ensures zero silent data loss by tracking expected sequence indices, recording
buffer overflows reported by the mock, capturing unannounced gaps, and recording
session restart discontinuities with to_index=None.
"""

from typing import Dict, List, Optional, Any
from pipeline.storage import StorageEngine


class LossTracker:
    def __init__(self, storage: StorageEngine):
        self.storage = storage
        # In-memory tracking of last known sample index per session & sensor
        # (session_id, sensor) -> last_index
        self._last_index_seen: Dict[str, Dict[int, int]] = {}
        self._current_session_id: Optional[str] = None

    def initialize_session(self, session_id: str):
        if session_id not in self._last_index_seen:
            # Query storage for existing highest index in case of process restart
            s1_max = self.storage.get_highest_sample_index(session_id, 1)
            s2_max = self.storage.get_highest_sample_index(session_id, 2)
            self._last_index_seen[session_id] = {1: s1_max, 2: s2_max}

    def handle_mock_restart(self, old_session_id: str, new_session_id: str):
        """
        Per Section 6.3:
        'restart, which starts a new session. Unread data of the old session is then lost
        and must be listed, with "to_index": null when its end is unknown.'
        """
        if old_session_id and old_session_id in self._last_index_seen:
            for sensor in (1, 2):
                last_idx = self._last_index_seen[old_session_id].get(sensor, -1)
                from_idx = last_idx + 1
                self.storage.insert_loss(
                    session_id=old_session_id,
                    sensor=sensor,
                    from_index=from_idx,
                    to_index=None,
                    reason="mock_restart",
                )
        self.initialize_session(new_session_id)
        self._current_session_id = new_session_id

    def process_incoming_batch(
        self,
        session_id: str,
        sensor: int,
        first_index: int,
        count: int,
        explicit_lost: List[Dict[str, Any]],
    ):
        """
        Audit batch against expected index and record explicit + implicit losses.
        """
        if session_id != self._current_session_id:
            if self._current_session_id is not None:
                self.handle_mock_restart(self._current_session_id, session_id)
            else:
                self.initialize_session(session_id)
                self._current_session_id = session_id

        expected_idx = self._last_index_seen[session_id].get(sensor, -1) + 1

        # 1. Process explicit losses reported by mock
        reported_covered_until = expected_idx
        for loss in explicit_lost:
            f_idx = loss["from_index"]
            t_idx = loss.get("to_index")
            reason = loss.get("reason", "mock_reported_loss")
            self.storage.insert_loss(
                session_id=session_id,
                sensor=sensor,
                from_index=f_idx,
                to_index=t_idx,
                reason=reason,
            )
            if t_idx is not None and t_idx >= reported_covered_until:
                reported_covered_until = t_idx + 1

        # 2. Check for unannounced sequence gap
        effective_start = max(expected_idx, reported_covered_until)
        if first_index > effective_start:
            # An unaccounted gap occurred
            self.storage.insert_loss(
                session_id=session_id,
                sensor=sensor,
                from_index=effective_start,
                to_index=first_index - 1,
                reason="unannounced_sequence_gap",
            )

        # 3. Update last known sample index
        if count > 0:
            new_last = first_index + count - 1
            if new_last > self._last_index_seen[session_id].get(sensor, -1):
                self._last_index_seen[session_id][sensor] = new_last
