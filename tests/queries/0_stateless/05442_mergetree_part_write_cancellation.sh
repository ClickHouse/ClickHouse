#!/usr/bin/env bash
#
# A KILLed INSERT must stop while the MergeTree writer is still writing the columns of a large block,
# for both Compact and Wide parts. The INSERT writes one block of many 1-row granules, which takes
# minutes without a per-granule cancellation check, so `KILL QUERY ... SYNC` must return within the bound.
#
# Every setting the timing depends on is pinned in the query or table SETTINGS:
# * async_insert = 0: an async INSERT is written by a background flush, not by the query we KILL.
# * auto_statistics_types = '': the per-block statistics pass runs before the column write, with no cancellation point.
# * compute_exact_num_defaults_for_sparse_columns = 0: the exact count of default values is another such pass.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

num_columns=64
num_rows=1500000

columns="c1 UInt8"
for i in $(seq 2 $num_columns); do
    columns="$columns, c$i UInt8 DEFAULT c1"
done

insert_err="${CLICKHOUSE_TMP}/05442_insert_err.txt"

for part_type in Compact Wide; do
    if [ "$part_type" = "Compact" ]; then
        part_settings="min_bytes_for_wide_part = 1000000000000, min_rows_for_wide_part = 1000000000000"
    else
        part_settings="min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
    fi

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_write_cancel"
    ${CLICKHOUSE_CLIENT} -q "
        CREATE TABLE t_write_cancel ($columns)
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS index_granularity = 1, auto_statistics_types = '', compute_exact_num_defaults_for_sparse_columns = 0, $part_settings"

    query_id="write_cancel_${CLICKHOUSE_DATABASE}_${part_type}_$$"

    ${CLICKHOUSE_CLIENT} --query_id "$query_id" -q "
        INSERT INTO t_write_cancel (c1)
        SETTINGS max_block_size = $num_rows, max_insert_block_size = $num_rows,
            min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0,
            max_insert_threads = 1, async_insert = 0
        SELECT toUInt8(number % 256) FROM numbers($num_rows)" >/dev/null 2>"$insert_err" &
    insert_pid=$!

    # Wait until the INSERT is writing the part (past source generation), so the cancellation
    # has to be observed inside the writer rather than between blocks.
    writing=0
    for _ in $(seq 1 600); do
        writing=$(${CLICKHOUSE_CLIENT} -q "
            SELECT ProfileEvents['MergeTreeDataWriterBlocks'] > 0
            FROM system.processes WHERE query_id = '$query_id'")
        [ "$writing" = "1" ] && break
        kill -0 "$insert_pid" 2>/dev/null || break
        sleep 0.1
    done

    if [ "$writing" != "1" ]; then
        echo "$part_type: did not observe the part write"
        echo "--- INSERT client output ---"
        cat "$insert_err"
    # KILL over HTTP: the bound then measures the server-side cancellation, not a client startup.
    elif timeout 10 ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
    then
        echo "$part_type: killed promptly"
    else
        echo "$part_type: KILL QUERY SYNC did not return in time"
    fi

    wait "$insert_pid" 2>/dev/null

    # The killed INSERT committed nothing, and the table is still writable with the intended part type.
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_write_cancel"
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO t_write_cancel (c1) SETTINGS async_insert = 0 VALUES (1)"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT part_type FROM system.parts
        WHERE database = currentDatabase() AND table = 't_write_cancel' AND active"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE t_write_cancel"
done

rm -f "$insert_err"
