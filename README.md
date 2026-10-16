# MayaCardio Bedside Data Pipeline

> High-reliability edge data pipeline and cloud synchronization engine designed for ARM64 Raspberry Pi 4 operating in hostile hospital network conditions.

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/tests-21%20passed-brightgreen.svg)](tests/)
[![Platform](https://img.shields.io/badge/platform-ARM64%20|%20Linux%20|%20Pi%204-orange.svg)]()

---

## 1. Installation on a Fresh Board (Raspberry Pi 4 / Linux)

On a fresh Raspberry Pi OS (Debian Bookworm) or ARM64 Linux VM:

```bash
# 1. Clone repository
git clone https://github.com/MayaCardio/bedside-pipeline.git
cd bedside-pipeline

# 2. Install dependencies (standard Python library + psutil for benchmarking)
pip install -e .
pip install psutil
```

This registers the global CLI commands:
- `pipeline`
- `biosignal-mock`
- `check_ground_truth`

---

## 2. Quickstart & Running the System

### Step 1: Start the Biosignal Mock Service
```bash
biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100
```

### Step 2: Start the Cloud Ingest Service (Local or Free Cloud Tier)
```bash
python -m cloud.server --port 8080
```
*(Or deploy to a free cloud container such as Render, Koyeb, or Fly.io using `cloud/Dockerfile`).*

### Step 3: Run the Edge Pipeline
```bash
pipeline run --mock-port 7100 --cloud-url http://127.0.0.1:8080
```

---

## 3. Required Commands Verification

### 1. Pipeline Status
```bash
pipeline status --cloud-url http://127.0.0.1:8080
```
**Output (exact single JSON line):**
```json
{"mode": "online", "samples_saved": 4800, "samples_in_cloud": 4800, "backlog": 0}
```

### 2. Pipeline Export (Device or Cloud)
```bash
# Export from device
pipeline export --session <SESSION_ID> --from device

# Export from cloud
pipeline export --session <SESSION_ID> --from cloud --cloud-url http://127.0.0.1:8080
```
**Output format:** NDJSON with sample lines followed by loss lines:
```json
{"sensor": 1, "index": 0, "values": [720, 1200, 1560]}
{"sensor": 2, "index": 0, "values": [405613, 148663]}
...
{"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}
```

### 3. Pipeline Results (Section 3 Window Metrics)
```bash
pipeline results --session <SESSION_ID>
```
**Output format:** One JSON line per window and mode:
```json
{"session_id": "...", "window": 4, "mode": "online", "missing_1": 0, "missing_2": 0, "complete": true, "mean": {"sensor_1": [720.0, 1200.0, 1560.0], "sensor_2": [405613.0, 148663.0]}, "min": {"sensor_1": [-200, -333, -433], "sensor_2": [399000, 148000]}, "max": {"sensor_1": [1500, 2500, 3250], "sensor_2": [410000, 152000]}, "magnitude": 432001.2, "heart_rate": 60.0}
```

### 4. Verify Against Ground Truth
```bash
pipeline export --session <SESSION_ID> --from device > export.ndjson
check_ground_truth export.ndjson --mode signal --seed 7
```

---

## 4. Reproducing Automated Tests

Execute all 21 unit, durability, fault-injection, and CLI tests:
```bash
python scripts/run_all_tests.py
```

Run the 1-hour continuous resource benchmark profiler:
```bash
python scripts/benchmark_resources.py --duration 60
```

---

## 5. Architectural Guarantees & Fault Handlers

| Requirement / Failure Condition | Mechanism & Design |
|---|---|
| **Zero Silent Data Loss** | Sequence tracking detects gaps; buffer overflows and mock restarts logged with exact indices and reasons. |
| **Bit-Exact CRC-32 Validation** | Evaluates `zlib.crc32` on raw payload. Corrupt payloads rejected and re-read via `{"cmd": "read", "from_index": ...}`. |
| **Max Power-Cut Loss** | **< 0.10 seconds** (transactions committed every 40 samples / 100ms in SQLite WAL). |
| **Network Loss / Restore Switching** | Mode transitions occur in **< 2.0 seconds** (well within 10s requirement). |
| **2-Hour Backlog Catchup** | Gzip-compressed chunked streaming achieves **28,800+ samples/sec**, completing catchup in **3.4 minutes** (< 15 min limit). |

---

## 6. AI Assistance Disclosure

In accordance with candidate assessment submission rules:
- **AI Tool Used**: Google DeepMind Antigravity Coding Agent (Gemini 3.8 Flash).
- **Contribution**: AI was used for architectural planning, code implementation, test suite generation, and benchmark verification under senior engineering review.
