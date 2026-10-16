"""
check_ground_truth: REFERENCE VALIDATOR
-----------------------------------------
NOTE: Standalone ground truth validator implementing the verification logic
from Section 6.3 and Section 4 (Criteria 1 & 2). 

For --mode ramp, comparison is against the pure closed-form mathematical formula:
  Sensor 1 Ch c: (3i + c) mod 8,388,608
  Sensor 2 Ch c: (2i + c) mod 8,388,608
For --mode signal, comparison tests consistency against the reference synthetic generator.
"""

import argparse
import json
import sys
from typing import Dict, List, Set, Tuple, Any
from biosignal_mock.mock_service import BiosignalGenerator, SAMPLE_RATE


def verify_export(
    export_filepath: str,
    mode: str = "signal",
    seed: int = 7,
    expected_duration: int = 60,
    check_up_to_index: int = None,
) -> Tuple[bool, Dict[str, Any]]:
    generator = BiosignalGenerator(mode=mode, seed=seed)

    samples_found: Dict[int, Dict[int, List[int]]] = {1: {}, 2: {}}
    duplicate_count = 0
    corrupt_count = 0
    losses_reported: List[Dict[str, Any]] = []

    with open(export_filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception as e:
                print(f"[Line {line_num}] JSON parse error: {e}")
                return False, {"error": f"JSON parse error at line {line_num}"}

            if "values" in record:
                # Sample record: {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}
                sensor = record["sensor"]
                idx = record["index"]
                vals = record["values"]

                if idx in samples_found[sensor]:
                    duplicate_count += 1
                    print(f"ERROR: Duplicate sample for sensor {sensor} at index {idx}")
                else:
                    samples_found[sensor][idx] = vals

                # Check values against ground truth generator
                expected_vals = generator.get_sample(sensor, idx)
                if vals != expected_vals:
                    corrupt_count += 1
                    print(f"ERROR: Value mismatch at sensor {sensor}, index {idx}: expected {expected_vals}, got {vals}")

            elif "from_index" in record:
                # Loss record: {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}
                losses_reported.append(record)

    # Determine evaluation ceiling
    max_idx_1 = max(samples_found[1].keys()) if samples_found[1] else -1
    max_idx_2 = max(samples_found[2].keys()) if samples_found[2] else -1
    highest_idx = max(max_idx_1, max_idx_2)

    eval_limit = check_up_to_index if check_up_to_index is not None else highest_idx

    # Build lost index sets per sensor
    reported_lost_indices: Dict[int, Set[int]] = {1: set(), 2: set()}
    for loss in losses_reported:
        sensor = loss["sensor"]
        from_idx = loss["from_index"]
        to_idx = loss["to_index"]
        if to_idx is not None:
            for i in range(from_idx, to_idx + 1):
                reported_lost_indices[sensor].add(i)
        else:
            # Unbounded loss (e.g. mock restart where end is unknown)
            if eval_limit is not None and eval_limit >= from_idx:
                for i in range(from_idx, eval_limit + 1):
                    reported_lost_indices[sensor].add(i)

    # Check for silent losses: Any index in [0, eval_limit] not in samples and not in reported_lost_indices
    silent_losses: Dict[int, List[int]] = {1: [], 2: []}
    for sensor in (1, 2):
        if eval_limit >= 0:
            for i in range(0, eval_limit + 1):
                if i not in samples_found[sensor] and i not in reported_lost_indices[sensor]:
                    silent_losses[sensor].append(i)

    total_silent = len(silent_losses[1]) + len(silent_losses[2])
    success = (total_silent == 0 and duplicate_count == 0 and corrupt_count == 0)

    report = {
        "success": success,
        "eval_limit": eval_limit,
        "samples_sensor_1": len(samples_found[1]),
        "samples_sensor_2": len(samples_found[2]),
        "duplicates": duplicate_count,
        "corruptions": corrupt_count,
        "reported_losses": len(losses_reported),
        "silent_losses_sensor_1": len(silent_losses[1]),
        "silent_losses_sensor_2": len(silent_losses[2]),
        "total_silent_losses": total_silent,
    }

    return success, report


def main():
    parser = argparse.ArgumentParser(description="Ground Truth Checker for MayaCardio pipeline exports")
    parser.add_argument("export_file", help="Path to exported NDJSON file")
    parser.add_argument("--mode", choices=["signal", "ramp"], default="signal", help="Data mode (signal or ramp)")
    parser.add_argument("--seed", type=int, default=7, help="RNG seed")
    parser.add_argument("--limit", type=int, default=None, help="Check up to index (optional)")
    args = parser.parse_args()

    success, report = verify_export(
        export_filepath=args.export_file,
        mode=args.mode,
        seed=args.seed,
        check_up_to_index=args.limit,
    )

    print(json.dumps(report, indent=2))
    if not success:
        print("\nFAILURE: Ground truth comparison failed! See deductions/errors above.")
        sys.exit(1)
    else:
        print("\nSUCCESS: 100% match with ground truth! No silent loss, no duplicates, no corruptions.")
        sys.exit(0)


if __name__ == "__main__":
    main()
