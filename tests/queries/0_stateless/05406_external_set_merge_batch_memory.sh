#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-merge-batch-memory.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Each case runs one query in a fresh process. Its set of IN spills from its first keys, so every column of
# keys becomes a run in a temporary file, and the final merge passes the keys of the runs on in batches of up
# to `max_block_size` keys. A batch of 100 million keys would take 800 MB, while the set has at most 2,000
# keys and the query needs less than 2 MB, so the query fits in its limit of 64 MiB only when the merge
# reserves room just for the keys of its runs. A case prints the result of the query, or its error, then the
# sets that spilled, the merges of runs in files, and the chains of runs that the merge read.
run()
{
    local name="$1"
    local subquery="$2"
    local log="${LOCAL_DIR}/${name}.log"
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${name}" --send_logs_level trace --ignore-error --multiquery \
        > "${LOCAL_DIR}/${name}.out" 2> "${log}" <<SQL
SELECT count() FROM numbers(10) WHERE number IN (${subquery})
SETTINGS max_threads = 1, max_block_size = 100000000, max_memory_usage = 67108864,
    max_bytes_before_external_set = 1, max_bytes_ratio_before_external_set = 0,
    max_bytes_for_external_set_bloom_filter = 0, max_bytes_for_external_set_cache = 0,
    temporary_files_buffer_size = 65536;
SELECT sum(value) FROM system.events WHERE event = 'SetsSpilledToDisk';
SELECT sum(value) FROM system.events WHERE event = 'ExternalSetMerge';
SQL

    local result error chains
    result=$(paste -s "${LOCAL_DIR}/${name}.out")
    error=$(grep -m1 'Code: [0-9]*\. DB::Exception' "${log}" \
        | grep -o '([A-Z_]*)' | head -1 | tr -d '()' || true)
    chains=$(grep -oE 'in [0-9]+ chains of runs that do not overlap' "${log}" \
        | grep -oE '[0-9]+' | paste -s || true)
    printf '%s\t%s\t%s\tchains %s\n' "${name}" "${error:-ok}" "${result}" "${chains}"
}

# One run makes one chain, whose keys the merge passes on as a span.
run single_chain "SELECT number FROM numbers(1000)"

# The two columns of keys make two runs that overlap, and so two chains, whose tournament passes the keys on
# one at a time.
run two_chains "SELECT number FROM numbers(1000) UNION ALL SELECT number + 500 FROM numbers(1000)"
