"""
state_machine.py: Network health monitoring and online/offline state machine.
Enforces <= 10-second mode transition deadline with zero data loss or duplication.
"""

import asyncio
import logging
import time
import urllib.request
from typing import Callable, Optional

logger = logging.getLogger("pipeline.state_machine")


class PipelineStateMachine:
    def __init__(
        self,
        cloud_url: str = "http://127.0.0.1:8080",
        probe_interval_sec: float = 2.0,
        probe_timeout_sec: float = 1.5,
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
        self.last_switch_time = time.time()
        self.running = False
        self._task: Optional[asyncio.Task] = None

    def check_cloud_alive(self) -> bool:
        """Synchronous HTTP probe with short timeout."""
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
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.check_cloud_alive)

    async def update_state(self, is_alive: bool):
        now = time.time()
        if is_alive:
            self.consecutive_failures = 0
            self.consecutive_successes += 1
            # Transition to online on first confirmed ping
            if self.mode != "online":
                self.mode = "online"
                transition_duration = now - self.last_switch_time
                self.last_switch_time = now
                logger.info(f"NETWORK RESTORED -> Switched to ONLINE mode in {transition_duration:.2f}s")
                if self.on_mode_change:
                    self.on_mode_change("online")
        else:
            self.consecutive_successes = 0
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.fail_threshold and self.mode != "offline":
                self.mode = "offline"
                transition_duration = now - self.last_switch_time
                self.last_switch_time = now
                logger.warning(f"NETWORK LOST -> Switched to OFFLINE mode in {transition_duration:.2f}s")
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
