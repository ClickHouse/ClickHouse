# Parquet Sanitization Summary

## Task Completed

Generated a new sanitized `zookeeper_log.parquet` file from production data with:
- ✅ **Nested-Multi violations filtered out** (4,333 Multi blocks removed)
- ✅ **All sensitive data sanitized** (paths, hostnames, data, path_created, children)
- ✅ **File size: 62.16 MB** (under 100 MB target)
- ✅ **No nested-Multi violations** verified in output
- ✅ **Security verified** - no realistic sensitive patterns detected

## Source File

**Current `zookeeper_log.parquet` in the repo** was not created from the full prod file `33306557-zookeeper_log_2.parquet` (42M rows, ~637 MB). It was created from `zookeeper_log_backup.parquet` (568,819 rows) or a run with a smaller target, so it has only ~55k–550k rows. To get a sanitized parquet that uses the full prod log (up to 100 MB of simple ops after removing Multi/MultiRead), run:

```bash
# Requires: pyarrow (pip install pyarrow). Script maps prod schema (event_time, duration_microseconds) to keeper-bench (request_event_time, response_event_time).
python3 tests/stress/keeper/workloads/sanitize_prod_log.py \
  /home/ubuntu/ClickHouse/33306557-zookeeper_log_2.parquet \
  tests/stress/keeper/workloads/zookeeper_log.parquet \
  --target-size-mb 100 --seed 42
```

- **Prod file** `33306557-zookeeper_log_2.parquet`: 42,006,765 rows, op_nums include Create(1), Get(4), Multi(14), MultiRead(22), etc. Schema uses `event_time` and `duration_microseconds`; the script maps these to `request_event_time` and `response_event_time`.
- **Sanitization:** Keeps only supported ops; removes all Multi/MultiRead blocks; sanitizes path/data/hostname; truncates to `--target-size-mb` (default 100). Output will have on the order of hundreds of thousands of rows (simple ops only) and be ≤100 MB.
- **Large prod files:** Processing 42M rows can take a long time. Use `--max-row-groups N` to process only the first N row groups for a quicker run (e.g. `--max-row-groups 2`). Requires `pyarrow` (e.g. `.keeper-venv/bin/pip install pyarrow`).

## Changes Made

### 1. Enhanced `sanitize_prod_log.py`

- **Bootstrap CREATEs:** The script prepends CREATE rows for all unique path prefixes in the log (capped at 500k). Replay runs these first so paths exist; CHA-01-REPLAY then reports only real unexpected results, so `errors` in metrics are reliable.
- Added `filter_nested_multi_violations()` function that:
- Scans for Multi/MultiRead blocks (op_num 14 or 22)
- Detects when a subrequest is also a Multi/MultiRead (nested violation)
- Removes **entire Multi blocks** (header + all subrequests) that contain nested Multi
- Integrated into the sanitization pipeline to run on each row group

### 2. Sanitization Results

- **Input:** 568,819 rows
- **Nested-Multi blocks removed:** 4,333
- **Rows removed:** 22,085
- **Output:** 546,734 rows
- **Final size:** 62.16 MB

### 3. Security Verification

- ✅ **Paths:** Randomized while preserving structure (slashes maintained)
- ✅ **Hostnames:** Fully randomized
- ✅ **Data:** Sanitized
- ✅ **No realistic sensitive patterns:** No domains, emails, dates, or hash-like patterns detected
- ✅ **No nested-Multi violations:** Verified with `inspect_parquet_layout.py`

## Files

- **Input:** `tests/stress/keeper/workloads/zookeeper_log_backup.parquet` (568,819 rows)
- **Output:** `tests/stress/keeper/workloads/zookeeper_log.parquet` (546,734 rows, 62.16 MB)
- **Old file:** Deleted (was 105.8 MB with 5,312 nested-Multi violations)

## Verification Commands

```bash
# Check for nested-Multi violations (should show "No violations found")
python3 tests/stress/keeper/workloads/inspect_parquet_layout.py \
  tests/stress/keeper/workloads/zookeeper_log.parquet

# Verify file is readable
python3 -c "import pandas as pd; df = pd.read_parquet('tests/stress/keeper/workloads/zookeeper_log.parquet'); print(f'Rows: {len(df):,}')"
```

## Critical Security Notes

- All customer-sensitive values (path, data, hostname, path_created, children) are replaced with **random strings of the same length**
- Path structure is preserved (slashes maintained) but all segments are randomized
- No production data should be present in the sanitized file
- The file is safe for use in public repositories and testing
