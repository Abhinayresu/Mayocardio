"""
Unit tests for Section 3 signal processing rules.
"""

import math
import unittest
from biosignal_mock.mock_service import BiosignalGenerator, SAMPLE_RATE
from pipeline.signal_processor import process_window, compute_magnitude_s2, compute_channel_stats


class TestSignalProcessor(unittest.TestCase):
    def setUp(self):
        self.gen = BiosignalGenerator(mode="signal", seed=7)

    def test_window_completeness_and_stats(self):
        # Build samples for window 0 (samples 0 to 399)
        s1_map = {i: self.gen.get_sample(1, i) for i in range(400)}
        s2_map = {i: self.gen.get_sample(2, i) for i in range(400)}

        res = process_window("sess_test", w=0, mode="offline", s1_samples_map=s1_map, s2_samples_map=s2_map)

        self.assertEqual(res["window"], 0)
        self.assertEqual(res["mode"], "offline")
        self.assertEqual(res["missing_1"], 0)
        self.assertEqual(res["missing_2"], 0)
        self.assertTrue(res["complete"])
        self.assertIsNotNone(res["mean"])
        self.assertEqual(len(res["mean"]["sensor_1"]), 3)
        self.assertEqual(len(res["mean"]["sensor_2"]), 2)
        # Heart rate must be null for w < 4
        self.assertIsNone(res["heart_rate"])

    def test_missing_samples_flag(self):
        # 10 samples missing from sensor 1, 5 from sensor 2
        s1_map = {i: self.gen.get_sample(1, i) for i in range(390)}
        s2_map = {i: self.gen.get_sample(2, i) for i in range(395)}

        res = process_window("sess_test", w=0, mode="online", s1_samples_map=s1_map, s2_samples_map=s2_map)
        self.assertEqual(res["missing_1"], 10)
        self.assertEqual(res["missing_2"], 5)
        self.assertFalse(res["complete"])

    def test_magnitude_sensor_2(self):
        # Sample with known values
        samples = [[3, 4], [30, 40]]  # sqrt(3^2+4^2) = 5, sqrt(30^2+40^2) = 50 -> mean = 27.5
        mag = compute_magnitude_s2(samples)
        self.assertEqual(mag, 27.5)

    def test_heart_rate_bpm_window_4_and_above(self):
        # For window 4, samples needed: 400*(4-4) to 400*(4+1)-1 = 0 to 1999
        # And peak detection needs 800 before, so we populate 0 to 2000
        s1_map = {i: self.gen.get_sample(1, i) for i in range(2500)}
        s2_map = {i: self.gen.get_sample(2, i) for i in range(400 * 4, 400 * 5)}

        res = process_window("sess_test", w=4, mode="online", s1_samples_map=s1_map, s2_samples_map=s2_map)
        self.assertIsNotNone(res["heart_rate"])
        self.assertEqual(res["heart_rate"], 60.0)

    def test_heart_rate_null_when_sample_missing(self):
        # Remove sample 1500 (within 5s window of w=4: 0 to 1999)
        s1_map = {i: self.gen.get_sample(1, i) for i in range(2500) if i != 1500}
        s2_map = {i: self.gen.get_sample(2, i) for i in range(400 * 4, 400 * 5)}

        res = process_window("sess_test", w=4, mode="online", s1_samples_map=s1_map, s2_samples_map=s2_map)
        # Heart rate must be null if any sample in 5s window is missing
        self.assertIsNone(res["heart_rate"])


if __name__ == "__main__":
    unittest.main()
