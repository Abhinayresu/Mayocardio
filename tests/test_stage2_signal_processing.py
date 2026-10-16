"""
tests/test_stage2_signal_processing.py: Strict deterministic test suite for Section 3 signal processing.
Every expected value is calculated independently by hand/first principles.
Tests all rules, boundary conditions, edge cases, and null requirements.
"""

import math
import unittest
from pipeline.signal_processor import (
    compute_channel_stats,
    compute_magnitude_s2,
    is_sample_a_peak,
    find_ecg_peaks_in_window,
    compute_heart_rate,
    process_window,
)


class TestSignalProcessingStage2(unittest.TestCase):
    def test_01_window_boundaries_and_missing_counts(self):
        """Rule 1 & 2: Samples 400w to 400w+399, missing counts, completeness."""
        # For window w=2: samples 800 to 1199 (400 slots)
        # Create map where sensor 1 has 385 samples (15 missing)
        # and sensor 2 has 400 samples (0 missing)
        s1_map = {i: [10, 20, 30] for i in range(800, 1185)}
        s2_map = {i: [400000, 150000] for i in range(800, 1200)}

        res = process_window(
            session_id="test_win_bnd",
            w=2,
            mode="offline",
            s1_samples_map=s1_map,
            s2_samples_map=s2_map,
        )

        self.assertEqual(res["window"], 2)
        self.assertEqual(res["missing_1"], 15)  # 400 - 385 = 15
        self.assertEqual(res["missing_2"], 0)   # 400 - 400 = 0
        self.assertFalse(res["complete"], "Complete must be False when missing_1 > 0")

        # Test complete == True when both missing counts are 0
        s1_map_full = {i: [10, 20, 30] for i in range(800, 1200)}
        res_full = process_window(
            session_id="test_win_bnd",
            w=2,
            mode="online",
            s1_samples_map=s1_map_full,
            s2_samples_map=s2_map,
        )
        self.assertEqual(res_full["missing_1"], 0)
        self.assertEqual(res_full["missing_2"], 0)
        self.assertTrue(res_full["complete"], "Complete must be True when both missing counts are 0")

    def test_02_per_channel_stats_hand_calculated(self):
        """Rule 3: mean, min, max per channel over available samples (null if none)."""
        # Sensor 1: 3 channels, 3 known samples
        # Sample 0: [10, 50, 100]
        # Sample 1: [25, 40, 200]
        # Sample 2: [40, 60, 150]
        # Independent hand calculations:
        # Ch 1: values [10, 25, 40] -> min=10, max=40, sum=75, mean=75/3 = 25.0
        # Ch 2: values [50, 40, 60] -> min=40, max=60, sum=150, mean=150/3 = 50.0
        # Ch 3: values [100, 200, 150] -> min=100, max=200, sum=450, mean=450/3 = 150.0
        s1_samples = [[10, 50, 100], [25, 40, 200], [40, 60, 150]]
        means, mins, maxs = compute_channel_stats(s1_samples, num_channels=3)

        self.assertEqual(mins, [10, 40, 100])
        self.assertEqual(maxs, [40, 60, 200])
        self.assertEqual(means, [25.0, 50.0, 150.0])

        # Test empty samples -> returns None, None, None
        empty_means, empty_mins, empty_maxs = compute_channel_stats([], num_channels=3)
        self.assertIsNone(empty_means)
        self.assertIsNone(empty_mins)
        self.assertIsNone(empty_maxs)

    def test_03_sensor_2_magnitude_hand_calculated(self):
        """Rule 4: Mean of sqrt(ch1^2 + ch2^2) over available sensor 2 samples."""
        # Using exact Pythagorean triples to hand-calculate:
        # Sample 1: [3, 4]      -> sqrt(9 + 16) = 5.0
        # Sample 2: [6, 8]      -> sqrt(36 + 64) = 10.0
        # Sample 3: [5, 12]     -> sqrt(25 + 144) = 13.0
        # Sample 4: [9, 12]     -> sqrt(81 + 144) = 15.0
        # Sum of magnitudes = 5.0 + 10.0 + 13.0 + 15.0 = 43.0
        # Mean = 43.0 / 4 = 10.75
        # Rounded to 1 decimal place = 10.8
        s2_samples = [[3, 4], [6, 8], [5, 12], [9, 12]]
        mag = compute_magnitude_s2(s2_samples)
        self.assertEqual(mag, 10.8)

        # Null if no sensor 2 samples
        self.assertIsNone(compute_magnitude_s2([]))

    def test_04_peak_rule_1_preceding_800_and_next_sample_availability(self):
        """Peak Rule 1: 800 samples before it and sample after it must be available."""
        target_idx = 850
        # Build map with 802 samples from index 50 (850 - 800) to 851 (850 + 1)
        base_map = {}
        for j in range(50, 852):
            base_map[j] = [0, 50, 0]  # baseline
        base_map[target_idx] = [0, 500, 0]  # candidate peak (500 > 0.5 * 50 = 25)

        # Baseline check: is a peak when all 802 samples present
        self.assertTrue(is_sample_a_peak(target_idx, base_map, prev_peak_idx=None))

        # Failure Case 1: Drop one sample in the 800 preceding range (e.g. index 200)
        missing_before_map = dict(base_map)
        del missing_before_map[200]
        self.assertFalse(
            is_sample_a_peak(target_idx, missing_before_map, prev_peak_idx=None),
            "Cannot be a peak if any of the 800 samples before it is missing",
        )

        # Failure Case 2: Drop the sample immediately after it (index 851)
        missing_after_map = dict(base_map)
        del missing_after_map[851]
        self.assertFalse(
            is_sample_a_peak(target_idx, missing_after_map, prev_peak_idx=None),
            "Cannot be a peak if the sample immediately after it is missing",
        )

    def test_05_peak_rule_2_strictly_larger_than_neighbours(self):
        """Peak Rule 2: Sample i must be strictly larger than both neighbours."""
        target_idx = 850
        base_map = {j: [0, 50, 0] for j in range(50, 852)}

        # Case A: Flat peak (equal to left neighbour)
        flat_left = dict(base_map)
        flat_left[target_idx - 1] = [0, 500, 0]
        flat_left[target_idx] = [0, 500, 0]
        flat_left[target_idx + 1] = [0, 50, 0]
        self.assertFalse(is_sample_a_peak(target_idx, flat_left, None), "Flat top left is NOT a peak")

        # Case B: Flat peak (equal to right neighbour)
        flat_right = dict(base_map)
        flat_right[target_idx - 1] = [0, 50, 0]
        flat_right[target_idx] = [0, 500, 0]
        flat_right[target_idx + 1] = [0, 500, 0]
        self.assertFalse(is_sample_a_peak(target_idx, flat_right, None), "Flat top right is NOT a peak")

        # Case C: Downward slope (left > center)
        down_slope = dict(base_map)
        down_slope[target_idx - 1] = [0, 600, 0]
        down_slope[target_idx] = [0, 500, 0]
        down_slope[target_idx + 1] = [0, 400, 0]
        self.assertFalse(is_sample_a_peak(target_idx, down_slope, None), "Downward slope is NOT a peak")

        # Case D: Upward slope (center < right)
        up_slope = dict(base_map)
        up_slope[target_idx - 1] = [0, 400, 0]
        up_slope[target_idx] = [0, 500, 0]
        up_slope[target_idx + 1] = [0, 600, 0]
        self.assertFalse(is_sample_a_peak(target_idx, up_slope, None), "Upward slope is NOT a peak")

        # Case E: Strictly larger than both neighbours
        strict_peak = dict(base_map)
        strict_peak[target_idx - 1] = [0, 499, 0]
        strict_peak[target_idx] = [0, 500, 0]
        strict_peak[target_idx + 1] = [0, 499, 0]
        self.assertTrue(is_sample_a_peak(target_idx, strict_peak, None), "Strict local max is a peak")

    def test_06_peak_rule_3_threshold_half_max_of_800_preceding(self):
        """Peak Rule 3: Must be strictly larger than half of the maximum of 800 samples before it."""
        target_idx = 850
        base_map = {j: [0, 50, 0] for j in range(50, 852)}
        # Set a single large spike at index 400 in the preceding 800 samples
        base_map[400] = [0, 2000, 0]  # max of preceding 800 is 2000 -> half is 1000

        # Subcase A: Value = 999 (less than 1000)
        base_map[target_idx - 1] = [0, 100, 0]
        base_map[target_idx] = [0, 999, 0]
        base_map[target_idx + 1] = [0, 100, 0]
        self.assertFalse(is_sample_a_peak(target_idx, base_map, None), "999 <= 1000: NOT a peak")

        # Subcase B: Value = 1000 (exactly equal to half max)
        base_map[target_idx] = [0, 1000, 0]
        self.assertFalse(is_sample_a_peak(target_idx, base_map, None), "1000 is NOT > 1000: NOT a peak")

        # Subcase C: Value = 1001 (strictly greater than half max)
        base_map[target_idx] = [0, 1001, 0]
        self.assertTrue(is_sample_a_peak(target_idx, base_map, None), "1001 > 1000: IS a peak")

    def test_07_peak_rule_4_minimum_spacing_100_samples(self):
        """Peak Rule 4: At least 100 samples after previous peak."""
        target_idx = 850
        base_map = {j: [0, 50, 0] for j in range(50, 852)}
        base_map[target_idx - 1] = [0, 100, 0]
        base_map[target_idx] = [0, 2000, 0]
        base_map[target_idx + 1] = [0, 100, 0]

        # Previous peak at 751 (850 - 751 = 99 < 100)
        self.assertFalse(is_sample_a_peak(target_idx, base_map, prev_peak_idx=751), "Gap of 99 samples: NOT a peak")

        # Previous peak at 750 (850 - 750 = 100 >= 100)
        self.assertTrue(is_sample_a_peak(target_idx, base_map, prev_peak_idx=750), "Gap of 100 samples: IS a peak")

        # Previous peak at 700 (850 - 700 = 150 >= 100)
        self.assertTrue(is_sample_a_peak(target_idx, base_map, prev_peak_idx=700), "Gap of 150 samples: IS a peak")

    def test_08_heart_rate_null_conditions(self):
        """Heart Rate Null Rules: null if w < 4, if any sample missing, or if fewer than 2 peaks."""
        # 1. w < 4
        s1_map = {i: [0, 50, 0] for i in range(2500)}
        for w in (0, 1, 2, 3):
            self.assertIsNone(compute_heart_rate(w, s1_map), f"w={w} must yield null heart_rate")

        # 2. Any sample in 5s window missing
        # For w=4, 5s window is samples 400(4-4) to 400(4+1)-1 = 0 to 1999
        s1_map_complete = {i: [0, 50, 0] for i in range(2500)}
        # Add 2 peaks at 900 and 1300
        s1_map_complete[900] = [0, 1000, 0]
        s1_map_complete[1300] = [0, 1000, 0]

        # Baseline: Valid with complete data
        hr_valid = compute_heart_rate(4, s1_map_complete)
        self.assertIsNotNone(hr_valid)

        # Remove single sample at index 1500 (inside 5s window)
        s1_missing_inside = dict(s1_map_complete)
        del s1_missing_inside[1500]
        self.assertIsNone(compute_heart_rate(4, s1_missing_inside), "Must be null if sample in 5s window is missing")

        # 3. Fewer than 2 peaks in 5-second window
        s1_one_peak = {i: [0, 50, 0] for i in range(2500)}
        s1_one_peak[1000] = [0, 1000, 0]  # only 1 peak
        self.assertIsNone(compute_heart_rate(4, s1_one_peak), "Must be null if fewer than 2 peaks")

        s1_zero_peaks = {i: [0, 50, 0] for i in range(2500)}
        self.assertIsNone(compute_heart_rate(4, s1_zero_peaks), "Must be null if 0 peaks")

    def test_09_heart_rate_exact_math_and_rounding(self):
        """Heart Rate Formula: 60 / mean_gap_seconds with hand-calculated ground truth."""
        # Setup window w=4 (5s window is 0 to 1999). Populate 0 to 2001.
        s1_map = {i: [0, 50, 0] for i in range(2100)}

        # Hand Calculation 1:
        # Place peaks at indices 850, 1150, 1450, 1750 (4 peaks)
        # Gaps in samples:
        # gap1 = 1150 - 850 = 300
        # gap2 = 1450 - 1150 = 300
        # gap3 = 1750 - 1450 = 300
        # Mean gap in samples = 300.0
        # Mean gap in seconds = 300.0 / 400.0 = 0.75 seconds
        # Heart rate = 60 / 0.75 = 80.0 BPM
        for p in (850, 1150, 1450, 1750):
            s1_map[p] = [0, 2000, 0]

        hr1 = compute_heart_rate(4, s1_map)
        self.assertEqual(hr1, 80.0)

        # Hand Calculation 2 (testing non-integer decimal rounding):
        # Reset map
        s1_map2 = {i: [0, 50, 0] for i in range(2100)}
        # Place peaks at indices 900, 1270, 1640 (3 peaks)
        # gap1 = 1270 - 900 = 370
        # gap2 = 1640 - 1270 = 370
        # Mean gap in samples = 370.0
        # Mean gap in seconds = 370 / 400.0 = 0.925 seconds
        # Heart rate = 60 / 0.925 = 64.86486486...
        # Rounded to 1 decimal place: 64.9 BPM
        for p in (900, 1270, 1640):
            s1_map2[p] = [0, 2000, 0]

        hr2 = compute_heart_rate(4, s1_map2)
        self.assertEqual(hr2, 64.9)


if __name__ == "__main__":
    unittest.main()
