"""
collector.py: Real-time biosignal collector engine.
Connects to biosignal-mock over TCP NDJSON, performs bit-exact CRC-32 validation,
handles corruption with automatic re-read, handles duplicate responses cleanly,
tracks sessions, and persists samples with sub-100ms transactional durability.
"""

import asyncio
import json
import logging
import time
import zlib
from typing import Optional, Dict, Any, Tuple
from pipeline.storage import StorageEngine
from pipeline.loss_tracker import LossTracker

logger = logging.getLogger("pipeline.collector")


def extract_data_and_verify_crc(raw_line: bytes, expected_crc_hex: str) -> Tuple[bool, Optional[Any]]:
    try:
        line_str = raw_line.decode("utf-8").strip()
        data_key_idx = line_str.find('"data":')
        if data_key_idx == -1:
            return False, None

        bracket_start = line_str.find("[", data_key_idx)
        bracket_end = line_str.rfind("]")
        if bracket_start == -1 or bracket_end == -1 or bracket_end <= bracket_start:
            return False, None

        data_substring = line_str[bracket_start : bracket_end + 1]
        calc_crc = zlib.crc32(data_substring.encode("utf-8"))
        calc_crc_hex = f"{calc_crc:08x}"

        if calc_crc_hex.lower() != expected_crc_hex.lower():
            return False, None

        data_obj = json.loads(data_substring)
        return True, data_obj
    except Exception as e:
        logger.warning(f"Error parsing raw line for CRC verification: {e}")
        return False, None


class BiosignalCollector:
    def __init__(
        self,
        storage: StorageEngine,
        loss_tracker: LossTracker,
        host: str = "127.0.0.1",
        port: int = 7100,
        read_timeout_sec: float = 3.0,
    ):
        self.storage = storage
        self.loss_tracker = loss_tracker
        self.host = host
        self.port = port
        self.read_timeout_sec = read_timeout_sec
        self.running = False
        self.current_session_id: Optional[str] = None
        self.next_expected_index: Dict[int, int] = {1: 0, 2: 0}
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()

    async def connect(self) -> bool:
        try:
            self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
            logger.info(f"Connected to biosignal-mock at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.debug(f"Connection to biosignal-mock failed: {e}")
            return False

    def _process_valid_reply(self, reply: Dict[str, Any]):
        sensor = reply["sensor"]
        session_id = reply["session_id"]
        first_index = reply["first_index"]
        count = reply["count"]
        lost_ranges = reply.get("lost", [])
        data = reply["data"]

        # Detect session restart
        if self.current_session_id is None:
            self.current_session_id = session_id
            self.loss_tracker.initialize_session(session_id)
        elif self.current_session_id != session_id:
            logger.info(f"Mock restarted! Session changed from {self.current_session_id} to {session_id}")
            self.loss_tracker.handle_mock_restart(self.current_session_id, session_id)
            self.current_session_id = session_id
            self.next_expected_index = {1: 0, 2: 0}

        # Audit losses and sequence gaps
        self.loss_tracker.process_incoming_batch(
            session_id=session_id,
            sensor=sensor,
            first_index=first_index,
            count=count,
            explicit_lost=lost_ranges,
        )

        # Persist samples atomically
        if count > 0:
            self.storage.insert_samples_batch(
                session_id=session_id,
                sensor=sensor,
                first_index=first_index,
                data=data,
            )
            new_highest = first_index + count
            if new_highest > self.next_expected_index.get(sensor, 0):
                self.next_expected_index[sensor] = new_highest

    async def _read_one_reply(self) -> Tuple[Optional[Dict[str, Any]], bool]:
        """
        Reads one line from socket, validates CRC.
        Returns (reply_dict, is_corrupt).
        """
        if not self._reader:
            return None, False
        try:
            raw_line = await asyncio.wait_for(self._reader.readline(), timeout=self.read_timeout_sec)
            if not raw_line:
                return None, False

            full_obj = json.loads(raw_line.decode("utf-8"))
            expected_crc = full_obj.get("crc32", "")

            crc_valid, parsed_data = extract_data_and_verify_crc(raw_line, expected_crc)
            if not crc_valid:
                logger.warning(
                    f"CORRUPT reply detected for sensor {full_obj.get('sensor')} at index {full_obj.get('first_index')} (CRC mismatch). Discarding!"
                )
                return full_obj, True

            full_obj["data"] = parsed_data
            return full_obj, False
        except asyncio.TimeoutError:
            logger.warning("Timeout reading from socket")
            return None, False
        except Exception as e:
            logger.warning(f"Error reading socket reply: {e}")
            return None, False

    async def step_read_sensor(self, sensor: int) -> bool:
        async with self._lock:
            if not self._writer or self._writer.is_closing():
                return False

            cmd_dict = {"cmd": "read", "sensor": sensor}
            cmd_bytes = json.dumps(cmd_dict).encode("utf-8") + b"\n"
            try:
                self._writer.write(cmd_bytes)
                await self._writer.drain()
            except Exception as e:
                logger.warning(f"Failed to send read command: {e}")
                return False

            # We may receive multiple lines if previous command was duplicated
            # Read until we receive the response for our requested sensor or run out of lines
            max_attempts = 4
            for _ in range(max_attempts):
                reply, is_corrupt = await self._read_one_reply()
                if reply is None:
                    return False

                if is_corrupt:
                    # Issue immediate re-read with from_index
                    corrupt_sensor = reply.get("sensor", sensor)
                    corrupt_first_idx = reply.get("first_index", self.next_expected_index[corrupt_sensor])
                    re_cmd = {"cmd": "read", "sensor": corrupt_sensor, "from_index": corrupt_first_idx}
                    self._writer.write(json.dumps(re_cmd).encode("utf-8") + b"\n")
                    await self._writer.drain()

                    re_reply, re_corrupt = await self._read_one_reply()
                    if re_reply is not None and not re_corrupt:
                        self._process_valid_reply(re_reply)
                        if re_reply["sensor"] == sensor:
                            return True
                    continue

                # Process valid reply
                self._process_valid_reply(reply)

                # If this reply matched our requested sensor, we are done with this step
                if reply["sensor"] == sensor:
                    return True

            return True

    async def run_loop(self):
        self.running = True
        while self.running:
            if not self._writer or self._writer.is_closing():
                connected = await self.connect()
                if not connected:
                    await asyncio.sleep(1.0)
                    continue

            ok1 = await self.step_read_sensor(1)
            ok2 = await self.step_read_sensor(2)

            if not ok1 and not ok2:
                if self._writer:
                    try:
                        self._writer.close()
                    except Exception:
                        pass
                await asyncio.sleep(0.5)
            else:
                await asyncio.sleep(0.01)

    def stop(self):
        self.running = False
        if self._writer:
            try:
                self._writer.close()
            except Exception:
                pass
