"""
signal_processor.py: Strict implementation of MayaCardio's Section 3 Signal Processing rules.
Reusable for both edge device (offline mode) and cloud service (online mode).
Computes 1-second window stats, sensor 2 magnitude, ECG peak detection, and 5-second heart rate BPM.
"""

import math
from typing import Dict, List, Optional, Tuple, Any

SAMPLE_RATE = 400
WINDOW_SIZE = 400  # 1 second of data per window


def compute_channel_stats(samples_list: List[List[int]], num_channels: int) -> Tuple[Optional[List[float]], Optional[List[int]], Optional[List[int]]]:
    """Compute mean, min, max per channel over available samples (null if none)."""
    if not samples_list:
        return None, None, None

    means = []
    mins = []
    maxs = []
    n = len(samples_list)

    for ch_idx in range(num_channels):
        channel_vals = [s[ch_idx] for s in samples_list]
        mins.append(min(channel_vals))
        maxs.append(max(channel_vals))
        means.append(round(sum(channel_vals) / n, 1))

    return means, mins, maxs


def compute_magnitude_s2(s2_samples: List[List[int]]) -> Optional[float]:
    """Mean of sqrt(ch1^2 + ch2^2) over the available sensor 2 samples."""
    if not s2_samples:
        return None
    magnitudes = [math.sqrt(s[0] ** 2 + s[1] ** 2) for s in s2_samples]
    return round(sum(magnitudes) / len(magnitudes), 1)


def find_ecg_peaks(s1_samples_map: Dict[int, List[int]], start_idx: int, end_idx: int) -> List[int]:
    """
    Peak: sample i is a peak when all four of these hold:
    1. The 800 samples before it, and the sample after it, are available (all 802 samples present);
    2. It is larger than both neighbours;
    3. It is larger than half of the maximum of the 800 samples before it;
    4. It is at least 100 samples after the previous peak.
    """
    peaks = []
    prev_peak = None

    for i in range(start_idx, end_idx + 1):
        # 1. Check availability of 800 samples before, and sample after
        # Need i - 800 to i + 1 all present
        # First check fast: sample i, i-1, i+1
        if i not in s1_samples_map or (i - 1) not in s1_samples_map or (i + 1) not in s1_samples_map:
            continue

        val = s1_samples_map[i][1]  # Channel 2 of sensor 1 is ECG
        val_prev = s1_samples_map[i - 1][1]
        val_next = s1_samples_map[i + 1][1]

        # 2. Larger than both neighbours
        if not (val > val_prev and val > val_next):
            continue

        # Check full 800 samples before
        samples_before = []
        all_before_available = True
        for j in range(i - 800, i):
            if j not in s1_samples_map:
                all_before_available = False
                break
            samples_before.append(s1_samples_map[j][1])

        if not all_before_available:
            continue

        # 3. Larger than half of the maximum of the 800 samples before it
        max_before = max(samples_before)
        if val <= 0.5 * max_before:
            continue

        # 4. At least 100 samples after previous peak
        if prev_peak is not None and (i - prev_peak < 100):
            continue

        peaks.append(i)
        prev_peak = i

    return peaks


def compute_heart_rate(
    w: int, s1_samples_map: Dict[int, List[int]]
) -> Optional[float]:
    """
    Rate: for window w, take the peaks in the last 5 seconds (samples 400(w - 4) to 400(w + 1) - 1).
    heart_rate = 60 / (mean gap between consecutive peaks, in seconds).
    No rate: heart_rate is null if w < 4, if any sample in those 5 seconds is missing,
    or if there are fewer than 2 peaks.
    """
    if w < 4:
        return None

    start_idx = 400 * (w - 4)
    end_idx = 400 * (w + 1) - 1

    # Check if ANY sample in those 5 seconds is missing (2000 samples)
    for idx in range(start_idx, end_idx + 1):
        if idx not in s1_samples_map:
            return None

    peaks = find_ecg_peaks(s1_samples_map, start_idx, end_idx)
    if len(peaks) < 2:
        return None

    gaps_samples = [peaks[k + 1] - peaks[k] for k in range(len(peaks) - 1)]
    mean_gap_sec = (sum(gaps_samples) / len(gaps_samples)) / float(SAMPLE_RATE)
    if mean_gap_sec <= 0:
        return None

    heart_rate = 60.0 / mean_gap_sec
    return round(heart_rate, 1)


def process_window(
    session_id: str,
    w: int,
    mode: str,
    s1_samples_map: Dict[int, List[int]],
    s2_samples_map: Dict[int, List[int]],
) -> Dict[str, Any]:
    """
    Process window w (samples 400w to 400w + 399) and produce Section 3 dictionary.
    """
    win_start = 400 * w
    win_end = 400 * w + 399

    s1_win_samples = [s1_samples_map[i] for i in range(win_start, win_end + 1) if i in s1_samples_map]
    s2_win_samples = [s2_samples_map[i] for i in range(win_start, win_end + 1) if i in s2_samples_map]

    missing_1 = WINDOW_SIZE - len(s1_win_samples)
    missing_2 = WINDOW_SIZE - len(s2_win_samples)
    complete = (missing_1 == 0 and missing_2 == 0)

    mean_s1, min_s1, max_s1 = compute_channel_stats(s1_win_samples, num_channels=3)
    mean_s2, min_s2, max_s2 = compute_channel_stats(s2_win_samples, num_channels=2)

    magnitude = compute_magnitude_s2(s2_win_samples)
    heart_rate = compute_heart_rate(w, s1_samples_map)

    mean_dict = {"sensor_1": mean_s1, "sensor_2": mean_s2} if (mean_s1 or mean_s2) else None
    min_dict = {"sensor_1": min_s1, "sensor_2": min_s2} if (min_s1 or min_s2) else None
    max_dict = {"sensor_1": max_s1, "sensor_2": max_s2} if (max_s1 or max_s2) else None

    return {
        "session_id": session_id,
        "window": w,
        "mode": mode,
        "missing_1": missing_1,
        "missing_2": missing_2,
        "complete": complete,
        "mean": mean_dict,
        "min": min_dict,
        "max": max_dict,
        "magnitude": magnitude,
        "heart_rate": heart_rate,
    }
