#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs an S3 disk

# A merge reads a Compact part with many columns with one buffer if the part fits into the worst-case memory of
# the per-column buffers: the configured buffer size (at most 1 MiB in tests) per column, twice with prefetch.
# `compact_parts_merge_max_bytes_to_prefetch_part = 1` makes sure the size threshold does not choose one buffer.
# Each read buffer adds one to `RemoteFSBuffers`: one buffer per column gives at least 128 per source part.
# Statistics are off, because a merge reads the statistics of each column with a separate buffer.
# `t_sparse` has 300 columns and small parts. `t_dense` has a stripe per granule, so its per-column buffers can be
# smaller than the configured size, and one buffer can take more memory. The merge memory must stay within the bound
# plus 64 MiB for the rest of the merge: a decompressed block per source and the writer of the new part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for t in t_sparse t_dense
do
    if [ "$t" = "t_sparse" ]
    then
        columns=300; rows=100; seeds="1 2 3"; extra=""
    else
        columns=128; rows=4096; seeds="1 2"
        extra=", index_granularity = 1024, index_granularity_bytes = '10M', compact_parts_max_granules_to_buffer = 1"
    fi

    structure=$($CLICKHOUSE_CLIENT -q "SELECT arrayStringConcat(arrayMap(i -> concat('c', toString(i), ' UInt64'), range($columns)), ', ')")

    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $t;
        CREATE TABLE $t ($structure)
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS disk = 's3_disk', min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
            compact_parts_merge_max_bytes_to_prefetch_part = 1, auto_statistics_types = '' $extra;
    "

    for seed in $seeds
    do
        $CLICKHOUSE_CLIENT -q "INSERT INTO $t SELECT * FROM generateRandom('$structure', $seed) LIMIT $rows"
    done

    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE $t FINAL"
done

$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS part_log;

    SELECT
        table,
        count() > 0,
        countIf(part_type != 'Compact' OR NOT ProfileEvents['RemoteFSBuffers'] BETWEEN length(merged_from) AND 5 * length(merged_from)),
        countIf(peak_memory_usage > if(table = 't_dense', 128, 300) * 1048576 * 2 * length(merged_from) + 64 * 1048576)
    FROM system.part_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND database = currentDatabase() AND table IN ('t_sparse', 't_dense') AND event_type = 'MergeParts'
    GROUP BY table
    ORDER BY table;

    DROP TABLE t_sparse;
    DROP TABLE t_dense;
"
