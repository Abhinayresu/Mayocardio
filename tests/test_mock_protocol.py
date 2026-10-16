"""
Unit tests for biosignal-mock protocol and check_ground_truth validator.
"""

import asyncio
import json
import os
import tempfile
import time
import unittest
import zlib
from biosignal_mock.mock_service import (
    BiosignalGenerator,
    BiosignalMockServer,
    generate_ramp_sensor1,
    generate_ramp_sensor2,
    SAMPLE_RATE,
)
from biosignal_mock.check_ground_truth import verify_export


class TestBiosignalMock(unittest.TestCase):
    def test_ramp_values(self):
        # Sensor 1 channel c at sample i = (3i + c) mod 8,388,608
        s1_0 = generate_ramp_sensor1(0)
        self.assertEqual(s1_0, [1, 2, 3])
        s1_10 = generate_ramp_sensor1(10)
        self.assertEqual(s1_10, [31, 32, 33])

        # Sensor 2 channel c at sample i = (2i + c) mod 8,388,608
        s2_0 = generate_ramp_sensor2(0)
        self.assertEqual(s2_0, [1, 2])
        s2_10 = generate_ramp_sensor2(10)
        self.assertEqual(s2_10, [21, 22])

    def test_signal_values_ecg_and_ratios(self):
        gen = BiosignalGenerator(mode="signal", seed=7)
        # S1 Ch1 = round(Ch2 * 0.6), Ch3 = round(Ch2 * 1.3)
        for i in range(200):
            sample = gen.get_sample(1, i)
            ch1, ch2, ch3 = sample
            self.assertEqual(ch1, int(round(ch2 * 0.6)))
            self.assertEqual(ch3, int(round(ch2 * 1.3)))

        # S2: Ch1 near 400,000 and Ch2 near 150,000
        for i in range(200):
            sample_s2 = gen.get_sample(2, i)
            self.assertEqual(len(sample_s2), 2)
            self.assertTrue(390_000 <= sample_s2[0] <= 410_000)
            self.assertTrue(140_000 <= sample_s2[1] <= 160_000)

    def test_crc32_example_from_spec(self):
        # From spec page 5:
        data_text = "[[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]"
        crc = zlib.crc32(data_text.encode("utf-8"))
        self.assertEqual(f"{crc:08x}", "f9a5bf62")

    def test_ground_truth_checker_with_synthetic_export(self):
        # Create a small valid export and test checker
        gen = BiosignalGenerator(mode="signal", seed=7)
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".ndjson", encoding="utf-8") as tf:
            path = tf.name
            for i in range(100):
                tf.write(json.dumps({"sensor": 1, "index": i, "values": gen.get_sample(1, i)}) + "\n")
                tf.write(json.dumps({"sensor": 2, "index": i, "values": gen.get_sample(2, i)}) + "\n")

        try:
            success, report = verify_export(path, mode="signal", seed=7, check_up_to_index=99)
            self.assertTrue(success)
            self.assertEqual(report["total_silent_losses"], 0)
            self.assertEqual(report["duplicates"], 0)
            self.assertEqual(report["corruptions"], 0)
        finally:
            if os.path.exists(path):
                os.remove(path)


if __name__ == "__main__":
    unittest.main()
