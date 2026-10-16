"""
biosignal-mock: Reference implementation of MayaCardio's sensor mock service.
Implements the exact TCP protocol, data generation (signal and ramp modes),
buffer overflow tracking, CRC32 generation, and configurable fault injection.
"""

import argparse
import asyncio
import json
import math
import random
import sys
import time
import uuid
import zlib
from typing import Dict, List, Optional, Tuple, Any

SAMPLE_RATE = 400  # 400 samples per second
BUFFER_SECONDS = 10  # 10 second rolling buffer
BUFFER_SAMPLES = BUFFER_SECONDS * SAMPLE_RATE  # 4000 samples


def generate_ramp_sensor1(index: int) -> List[int]:
    """Sensor 1 channel c at sample i = (3i + c) mod 8,388,608."""
    MOD = 8_388_608
    return [(3 * index + c) % MOD for c in (1, 2, 3)]


def generate_ramp_sensor2(index: int) -> List[int]:
    """Sensor 2 channel c = (2i + c) mod 8,388,608."""
    MOD = 8_388_608
    return [(2 * index + c) % MOD for c in (1, 2)]


class BiosignalGenerator:
    """Generates synthetic signal data according to Section 6.1."""

    def __init__(self, mode: str = "signal", seed: int = 7):
        self.mode = mode
        self.seed = seed
        self.rng = random.Random(seed)
        self._s2_phase = self.rng.random() * 2 * math.pi

    def _get_heart_rate_bpm(self, sample_idx: int) -> float:
        sec = sample_idx / SAMPLE_RATE
        cycle_sec = sec % 360  # 6-minute cycle
        if cycle_sec < 120:
            return 60.0
        elif cycle_sec < 240:
            return 90.0
        else:
            return 72.0

    def get_sample(self, sensor: int, index: int) -> List[int]:
        if self.mode == "ramp":
            return generate_ramp_sensor1(index) if sensor == 1 else generate_ramp_sensor2(index)

        # --mode signal
        if sensor == 1:
            # S1 Ch2 is ECG with sharp peak per heartbeat
            t = index / SAMPLE_RATE
            bpm = self._get_heart_rate_bpm(index)
            beat_period = 60.0 / bpm
            phase_in_beat = (t % beat_period) / beat_period  # [0, 1)

            # Baseline noise
            baseline = int(30.0 * math.sin(2 * math.pi * 0.15 * t) + 10.0 * math.cos(2 * math.pi * 0.5 * t))

            # QRS peak centered around phase 0.2
            dp = phase_in_beat - 0.2
            if abs(dp) < 0.05:
                peak = int(2500.0 * math.exp(-((dp / 0.012) ** 2)))
            elif -0.1 < dp < 0.0:
                peak = -int(200.0 * math.exp(-(((dp + 0.03) / 0.01) ** 2)))
            elif 0.05 < dp < 0.2:
                peak = int(350.0 * math.exp(-(((dp - 0.12) / 0.035) ** 2)))
            else:
                peak = 0

            ch2 = baseline + peak
            ch1 = int(round(ch2 * 0.6))
            ch3 = int(round(ch2 * 1.3))
            return [ch1, ch2, ch3]

        else:
            # Sensor 2: channel 1 near 400,000 and channel 2 near 150,000, varying slowly
            t = index / SAMPLE_RATE
            ch1 = int(400_000 + 5000 * math.sin(0.05 * t + self._s2_phase) + 1200 * math.cos(0.2 * t))
            ch2 = int(150_000 + 3000 * math.cos(0.04 * t + self._s2_phase) + 800 * math.sin(0.15 * t))
            return [ch1, ch2]


class MockSession:
    """Maintains state for an active mock session with 10s rolling buffer."""

    def __init__(self, session_id: str, generator: BiosignalGenerator, duration_sec: int, start_time: float):
        self.session_id = session_id
        self.generator = generator
        self.duration_sec = duration_sec
        self.start_time = start_time
        self.total_samples = duration_sec * SAMPLE_RATE
        self.next_index = {1: 0, 2: 0}

    def current_sample_clock(self) -> int:
        elapsed = time.time() - self.start_time
        current_sample = int(elapsed * SAMPLE_RATE)
        return min(current_sample, self.total_samples)

    def read_sensor(
        self, sensor: int, from_index: Optional[int] = None, max_samples: int = SAMPLE_RATE
    ) -> Tuple[Dict[str, Any], str]:
        curr = self.current_sample_clock()
        buf_start = max(0, curr - BUFFER_SAMPLES)

        lost_ranges = []
        if from_index is not None:
            req_index = from_index
        else:
            req_index = self.next_index[sensor]

        if req_index < buf_start:
            lost_ranges.append({
                "from_index": req_index,
                "to_index": buf_start - 1,
                "reason": "buffer_overflow"
            })
            req_index = buf_start

        avail_count = max(0, curr - req_index)
        deliver_count = min(avail_count, max_samples)

        data = []
        for i in range(req_index, req_index + deliver_count):
            data.append(self.generator.get_sample(sensor, i))

        self.next_index[sensor] = req_index + deliver_count
        session_end = (self.next_index[sensor] >= self.total_samples)

        data_json = json.dumps(data, separators=(", ", ", "))
        crc = zlib.crc32(data_json.encode("utf-8"))
        crc_hex = f"{crc:08x}"

        payload = {
            "session_id": self.session_id,
            "sensor": sensor,
            "first_index": req_index,
            "count": deliver_count,
            "lost": lost_ranges,
            "session_end": session_end,
            "crc32": crc_hex,
            "data": data,
        }
        return payload, data_json


class BiosignalMockServer:
    """TCP Server for biosignal-mock with fault injection capabilities."""

    def __init__(
        self,
        mode: str = "signal",
        seed: int = 7,
        duration: int = 3600,
        port: int = 7100,
        inject_faults: bool = False,
    ):
        self.mode = mode
        self.seed = seed
        self.duration = duration
        self.port = port
        self.inject_faults = inject_faults
        self.generator = BiosignalGenerator(mode=mode, seed=seed)
        self.current_session: Optional[MockSession] = None
        self._server = None
        self.fault_counter = 0

    def start_new_session(self) -> MockSession:
        new_id = uuid.uuid4().hex[:8]
        self.current_session = MockSession(
            session_id=new_id,
            generator=self.generator,
            duration_sec=self.duration,
            start_time=time.time(),
        )
        return self.current_session

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        if self.current_session is None:
            self.start_new_session()

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                line_str = line.decode("utf-8").strip()
                if not line_str:
                    continue

                try:
                    cmd_obj = json.loads(line_str)
                except Exception:
                    continue

                if cmd_obj.get("cmd") == "read":
                    sensor = int(cmd_obj.get("sensor", 1))
                    from_index = cmd_obj.get("from_index")
                    if from_index is not None:
                        from_index = int(from_index)

                    reply_dict, raw_data_str = self.current_session.read_sensor(sensor, from_index=from_index)

                    if self.inject_faults:
                        self.fault_counter += 1
                        # 1. Corrupt CRC every 25 requests
                        if self.fault_counter % 25 == 7:
                            reply_dict["crc32"] = "deadbeef"
                        # 2. Duplicate reply every 35 requests
                        elif self.fault_counter % 35 == 12:
                            resp_line = json.dumps(reply_dict) + "\n"
                            writer.write(resp_line.encode("utf-8"))
                            await writer.drain()
                            writer.write(resp_line.encode("utf-8"))
                            await writer.drain()
                            continue
                        # 3. Simulated slow response / pause
                        elif self.fault_counter % 50 == 20:
                            await asyncio.sleep(1.5)

                    resp_line = json.dumps(reply_dict) + "\n"
                    writer.write(resp_line.encode("utf-8"))
                    await writer.drain()

                elif cmd_obj.get("cmd") == "restart":
                    self.start_new_session()
                    writer.write(json.dumps({"status": "restarted", "new_session_id": self.current_session.session_id}).encode("utf-8") + b"\n")
                    await writer.drain()

        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def run(self):
        self.start_new_session()
        self._server = await asyncio.start_server(self.handle_client, "127.0.0.1", self.port)
        async with self._server:
            await self._server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="biosignal-mock service")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal", help="Data mode (signal or ramp)")
    parser.add_argument("--seed", type=int, default=7, help="RNG seed")
    parser.add_argument("--duration", type=int, default=3600, help="Duration in seconds")
    parser.add_argument("--port", type=int, default=7100, help="TCP port to listen on")
    parser.add_argument("--inject-faults", action="store_true", help="Inject simulated faults for testing")
    args = parser.parse_args()

    server = BiosignalMockServer(
        mode=args.mode,
        seed=args.seed,
        duration=args.duration,
        port=args.port,
        inject_faults=args.inject_faults,
    )
    print(f"Starting biosignal-mock on port {args.port} [mode={args.mode}, seed={args.seed}, duration={args.duration}s]")
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("\nShutting down biosignal-mock.")


if __name__ == "__main__":
    main()
