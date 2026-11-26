#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every row of a `ProfileTraces` batch repeats the query ID. With a long query ID, a batch is bounded
# by its expanded size (about 1 MiB), not only by its row count, so it is split into small batches.
python3 - "$CLICKHOUSE_URL" "$CLICKHOUSE_CURL" <<'PY'
import json
import shlex
import subprocess
import sys
import time
import uuid

base_url, curl_command = sys.argv[1:]
curl = shlex.split(curl_command)
query_id = "long_query_id_" + uuid.uuid4().hex + "_" + "x" * 100000
url = base_url + (
    "&send_profile_events=0&send_logs_level=none&framing_output_format=JSONEachPacketString"
    "&send_profile_traces=1&max_threads=1&max_block_size=1"
    "&query_profiler_cpu_time_period_ns=0&query_profiler_real_time_period_ns=0"
    "&memory_profiler_step=0&memory_profiler_sample_probability=1"
    "&memory_profiler_sample_min_allocation_size=64000&max_untracked_memory=0"
    "&interactive_delay=3600000000"
    f"&query_id={query_id}"
)
query = "SELECT length(range(number + 100000)) FROM numbers(64) FORMAT Null"

# The shared profiler pipe can discard every sample of a query under load.
for attempt in range(4):
    response = subprocess.run(curl + ["-sS", url, "--data-binary", query], check=True, stdout=subprocess.PIPE).stdout
    items = [json.loads(line) for line in response.decode().splitlines() if line]
    assert items and items[-1]["packet"] == "progress", items[-1:]
    batches = [item["profile_traces"] for item in items if item["packet"] == "profile_traces"]
    samples = 0
    for batch in batches:
        rows = [sample for sample in batch if sample["trace_type"] not in ("Dropped", "Incomplete")]
        assert all(sample["query_id"] == query_id for sample in rows)
        # About 1 MiB of repeated query IDs per batch, at least one row.
        assert len(rows) <= 11, len(rows)
        samples += len(rows)
    if samples:
        break
    time.sleep(0.1)

assert samples, "no samples in four query observations"
print("long query ID: bounded batches")
PY
