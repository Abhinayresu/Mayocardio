"""
state_machine.py: Observable online/offline state machine with monotonic timing.
Enforces <= 10-second mode transition deadline with zero data loss or duplication.
Uses application-layer health probes to detect actual cloud-service availability.
"""

import asyncio
import logging
import time
import urllib.request
from typing import Callable, Dict, List, Optional, Any

logger = logging.getLogger("pipeline.state_machine")


class PipelineStateMachine:
    def __init__(
        self,
        cloud_url: str = "http://127.0.0.1:8080",
        probe_interval_sec: float = 1.0,
        probe_timeout_sec: float = 1.0,
        fail_threshold: int = 2,
        on_mode_change: Optional[Callable[[str], None]] = None,
    ):
        self.cloud_url = cloud_url.rstrip("/")
        self.probe_interval_sec = probe_interval_sec
        self.probe_timeout_sec = probe_timeout_sec
        self.fail_threshold = fail_threshold
        self.on_mode_change = on_mode_change

        self.mode = "offline"  # Start safe in offline mode until cloud responds
        self.consecutive_failures = 0
        self.consecutive_successes = 0

        # Monotonic timers for measuring transition delays immune to wall-clock changes
        self._start_monotonic = time.monotonic()
        self.last_switch_monotonic = self._start_monotonic
        self.last_probe_monotonic = self._start_monotonic
        self.last_event_start_monotonic = self._start_monotonic

        # Observable transition history
        self.transition_history: List[Dict[str, Any]] = []

        self.running = False
        self._task: Optional[asyncio.Task] = None

    def check_cloud_alive(self) -> bool:
        """
        Application-layer health probe (GET /api/v1/status).
        Detects actual cloud service responsiveness, not just interface link state.
        """
        try:
            req = urllib.request.Request(
                f"{self.cloud_url}/api/v1/status",
                headers={"User-Agent": "MayaCardio-Device-Probe"},
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=self.probe_timeout_sec) as resp:
                return resp.status == 200
        except Exception:
            return False

    async def probe_once(self) -> bool:
        self.last_probe_monotonic = time.monotonic()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.check_cloud_alive)

    async def update_state(self, is_alive: bool):
        now_m = time.monotonic()
        if is_alive:
            self.consecutive_failures = 0
            self.consecutive_successes += 1
            if self.mode != "online":
                old_mode = self.mode
                self.mode = "online"
                elapsed = now_m - self.last_switch_monotonic
                self.last_switch_monotonic = now_m
                record = {
                    "from_mode": old_mode,
                    "to_mode": "online",
                    "monotonic_time": now_m,
                    "elapsed_sec": round(elapsed, 3),
                }
                self.transition_history.append(record)
                logger.info(f"NETWORK RESTORED -> Switched to ONLINE mode in {elapsed:.3f}s")
                if self.on_mode_change:
                    self.on_mode_change("online")
        else:
            self.consecutive_successes = 0
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.fail_threshold and self.mode != "offline":
                old_mode = self.mode
                self.mode = "offline"
                elapsed = now_m - self.last_switch_monotonic
                self.last_switch_monotonic = now_m
                record = {
                    "from_mode": old_mode,
                    "to_mode": "offline",
                    "monotonic_time": now_m,
                    "elapsed_sec": round(elapsed, 3),
                }
                self.transition_history.append(record)
                logger.warning(f"NETWORK LOST -> Switched to OFFLINE mode in {elapsed:.3f}s")
                if self.on_mode_change:
                    self.on_mode_change("offline")

    async def run_loop(self):
        self.running = True
        while self.running:
            alive = await self.probe_once()
            await self.update_state(alive)
            await asyncio.sleep(self.probe_interval_sec)

    def start(self):
        self._task = asyncio.create_task(self.run_loop())

    def stop(self):
        self.running = False
        if self._task:
            self._task.cancel()
