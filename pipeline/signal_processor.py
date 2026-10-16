"""
signal_processor.py: Strict, deterministic implementation of MayaCardio's Section 3 Signal Processing rules.
Shared library for both edge device (offline mode) and cloud service (online mode).

Implements:
1. 1-second windowing: samples 400w to 400w + 399.
2. missing_1, missing_2 count and complete boolean flag.
3. Per-channel min, max, mean (rounded to 1 decimal place, null if no samples).
4. Sensor 2 magnitude: mean of sqrt(ch1^2 + ch2^2) (rounded to 1 decimal place).
5. Sensor 1 channel 2 ECG peak detection:
   - 800 preceding samples and 1 next sample available
   - Strictly larger than both neighbours
   - Strictly larger than 0.5 * max(preceding 800 samples)
   - At least 100 samples after previous peak
6. 5-second Heart Rate BPM:
   - Peaks within samples 400(w - 4) to 400(w + 1) - 1
   - heart_rate = 60 / (mean gap between consecutive peaks, in seconds)
   - Null if w < 4, if any sample in 5s window is missing, or if fewer than 2 peaks
   - Rounded to 1 decimal place
"""

import math
from typing import Dict, List, Optional, Tuple, Any

SAMPLE_RATE = 400
WINDOW_SIZE = 400  # 400 samples per second


def compute_channel_stats(
    samples_list: List[List[int]], num_channels: int
) -> Tuple[Optional[List[float]], Optional[List[int]], Optional[List[int]]]:
    """
    Compute mean, min, max per channel over available samples (null if none).
    Means are rounded to 1 decimal place.
    """
    if not samples_list:
        return None, None, None

    means: List[float] = []
    mins: List[int] = []
    maxs: List[int] = []
    n = len(samples_list)

    for ch_idx in range(num_channels):
        channel_vals = [s[ch_idx] for s in samples_list]
        mins.append(min(channel_vals))
        maxs.append(max(channel_vals))
        means.append(round(sum(channel_vals) / float(n), 1))

    return means, mins, maxs


def compute_magnitude_s2(s2_samples: List[List[int]]) -> Optional[float]:
    """
    Mean of sqrt(ch1^2 + ch2^2) over the available sensor 2 samples.
    Rounded to 1 decimal place (null if no sensor 2 samples).
    """
    if not s2_samples:
        return None
    magnitudes = [math.sqrt(float(s[0]) ** 2 + float(s[1]) ** 2) for s in s2_samples]
    return round(sum(magnitudes) / float(len(magnitudes)), 1)


def is_sample_a_peak(
    i: int,
    s1_samples_map: Dict[int, List[int]],
    prev_peak_idx: Optional[int],
) -> bool:
    """
    Evaluates whether sample i of sensor 1 channel 2 satisfies all 4 peak conditions:
    1. Preceding 800 samples (i-800 to i-1) and next sample (i+1) are available.
    2. Strictly larger than both neighbours: val[i] > val[i-1] and val[i] > val[i+1].
    3. Strictly larger than half of the maximum of the 800 samples before it:
       val[i] > 0.5 * max(val[i-800 : i]).
    4. At least 100 samples after the previous peak:
       prev_peak_idx is None or (i - prev_peak_idx >= 100).
    """
    # Quick check: i, i-1, i+1 must exist
    if i not in s1_samples_map or (i - 1) not in s1_samples_map or (i + 1) not in s1_samples_map:
        return False

    val = s1_samples_map[i][1]  # Sensor 1 Channel 2
    val_prev = s1_samples_map[i - 1][1]
    val_next = s1_samples_map[i + 1][1]

    # Condition 2: Strictly larger than both neighbours
    if not (val > val_prev and val > val_next):
        return False

    # Condition 4: At least 100 samples after previous peak
    if prev_peak_idx is not None and (i - prev_peak_idx < 100):
        return False

    # Condition 1: Check full 800 samples before it
    # Need indices i-800 through i-1
    max_before = -float("inf")
    for j in range(i - 800, i):
        if j not in s1_samples_map:
            return False
        v_before = s1_samples_map[j][1]
        if v_before > max_before:
            max_before = v_before

    # Condition 3: Strictly larger than half of the maximum of the 800 samples before it
    if val <= 0.5 * max_before:
        return False

    return True


def find_ecg_peaks_in_window(
    s1_samples_map: Dict[int, List[int]],
    start_idx: int,
    end_idx: int,
) -> List[int]:
    """
    Finds all peaks whose index falls within [start_idx, end_idx].
    To maintain accurate minimum peak spacing (>= 100 samples) across window boundaries,
    the search begins from max(800, start_idx - 100) to detect any peak immediately preceding
    the window boundary.
    """
    peaks_in_range: List[int] = []
    prev_peak: Optional[int] = None

    # Earliest possible peak is sample index 800 (needs 800 samples before it: 0 to 799)
    search_start = max(800, start_idx - 100)

    for i in range(search_start, end_idx + 1):
        if is_sample_a_peak(i, s1_samples_map, prev_peak):
            prev_peak = i
            if i >= start_idx:
                peaks_in_range.append(i)

    return peaks_in_range


def compute_heart_rate(
    w: int, s1_samples_map: Dict[int, List[int]]
) -> Optional[float]:
    """
    Rate: for window w, take the peaks in the last 5 seconds (samples 400(w - 4) to 400(w + 1) - 1).
    heart_rate = 60 / (mean gap between consecutive peaks, in seconds).

    No rate: heart_rate is null if:
    1. w < 4
    2. Any sample in those 5 seconds (samples 400(w - 4) to 400(w + 1) - 1) is missing
    3. Fewer than 2 peaks in those 5 seconds

    Rounded to 1 decimal place.
    """
    # Condition 1: w < 4
    if w < 4:
        return None

    start_idx = 400 * (w - 4)
    end_idx = 400 * (w + 1) - 1  # Exactly 2000 samples

    # Condition 2: If ANY sample in those 5 seconds is missing -> null
    for idx in range(start_idx, end_idx + 1):
        if idx not in s1_samples_map:
            return None

    # Find peaks in [start_idx, end_idx]
    peaks = find_ecg_peaks_in_window(s1_samples_map, start_idx, end_idx)

    # Condition 3: If fewer than 2 peaks -> null
    if len(peaks) < 2:
        return None

    # Consecutive gaps between peaks in samples
    gaps_samples = [peaks[k + 1] - peaks[k] for k in range(len(peaks) - 1)]
    mean_gap_sec = (sum(gaps_samples) / float(len(gaps_samples))) / float(SAMPLE_RATE)
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
    Reused identically for both online and offline processing.
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

    mean_dict = {"sensor_1": mean_s1, "sensor_2": mean_s2} if (mean_s1 is not None or mean_s2 is not None) else None
    min_dict = {"sensor_1": min_s1, "sensor_2": min_s2} if (min_s1 is not None or min_s2 is not None) else None
    max_dict = {"sensor_1": max_s1, "sensor_2": max_s2} if (max_s1 is not None or max_s2 is not None) else None

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
