#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not built in the fast test.

# The Parquet reader caps `input_format_parquet_prefer_block_bytes` by a 256th of the memory budget
# (90% of the server-wide hard memory limit). Check that a small `max_server_memory_usage` shrinks
# the blocks produced by the reader accordingly, while a large one leaves the setting as is.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 100000 rows of about 1 KB each in a single row group.
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT number AS n, repeat(toString(number % 10), 1000) AS s FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000000, output_format_parquet_row_group_size_bytes = 1000000000, engine_file_truncate_on_insert = 1"

# Make the hard limit exactly the requested one, independent of the host:
# - `max_server_memory_usage_to_ram_ratio` is raised, so that the requested limit is not lowered at startup on a host with little RAM;
# - `memory_worker_use_cgroup = 0`, so that the memory of other processes in the same cgroup (the test server, concurrent tests)
#   is not accounted as the memory of this process;
# - `memory_worker_dynamic_hard_limit = 0`, so that the limit is not adjusted at runtime.
function max_block_rows()
{
    local limit=$1
    $CLICKHOUSE_LOCAL -q "
        SELECT max(blockSize()), count() FROM file('${FILE}') WHERE NOT ignore(s)
        SETTINGS input_format_parquet_prefer_block_bytes = 16777216, input_format_parquet_max_block_size = 65409, max_threads = 1
        FORMAT TSV" -- \
        --max_server_memory_usage="$limit" \
        --max_server_memory_usage_to_ram_ratio=1000 \
        --memory_worker_use_cgroup=0 \
        --memory_worker_dynamic_hard_limit=0
}

# 1 GiB limit: the budget is 0.9 GiB, so the cap is about 3.6 MiB, which is about 3700 rows per block.
# The previous cap (a 64th of the budget, about 14.4 MiB) would give about 15000 rows.
max_block_rows 1073741824 | awk '{ print ($1 >= 2000 && $1 <= 6000) ? "1 GiB: ok" : "1 GiB: unexpected block size " $1, $2 }'

# 8 GiB limit: the cap is about 28.8 MiB and does not bind, the default 16 MiB gives about 16000 rows.
max_block_rows 8589934592 | awk '{ print ($1 >= 12000 && $1 <= 20000) ? "8 GiB: ok" : "8 GiB: unexpected block size " $1, $2 }'

rm -f "${FILE}"
