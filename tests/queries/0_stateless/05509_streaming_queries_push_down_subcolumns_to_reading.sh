#!/usr/bin/env bash
# Tags: no-parallel-replicas, no-shared-merge-tree
# A stream also reads the parts committed after its plan was built, so the subcolumn reads of a subquery are not pushed down
# to it: the compact part inserted below cannot read `s.null` without the whole column.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CURDIR"/streaming.lib

$STREAMING_CLIENT -q "
    DROP TABLE IF EXISTS t_stream_subcolumns;
    CREATE TABLE t_stream_subcolumns (id UInt64, s Nullable(String)) ENGINE = MergeTree ORDER BY id
    SETTINGS min_rows_for_wide_part = 100, min_bytes_for_wide_part = 10000, write_marks_for_substreams_in_compact_parts = 0,
        ratio_of_defaults_for_sparse_serialization = 0.1, nullable_serialization_version = 'allow_sparse', $STREAMING_TABLE_SETTINGS;
    SYSTEM STOP MERGES t_stream_subcolumns;
    INSERT INTO t_stream_subcolumns SELECT number, repeat('x', 200) FROM numbers(100);
"

query_id="${CLICKHOUSE_DATABASE}_stream_subcolumns"
$STREAMING_CLIENT --query_id "$query_id" -q "
    SELECT count(), countIf(s.null), countIf(isNull(s)) FROM (SELECT * FROM (SELECT * FROM t_stream_subcolumns STREAM) LIMIT 102)
    SETTINGS max_threads = 1, serialize_query_plan = 0, optimize_functions_to_subcolumns = 1, query_plan_push_down_volume_reducing_functions = 1" &
pid=$!

# Insert only once the stream has read the first part, i.e. after its plan was built.
started=0
for _ in {1..300}; do
    read_rows=$($CLICKHOUSE_CLIENT -q "SELECT read_rows FROM system.processes WHERE query_id = '$query_id'")
    [[ -n "$read_rows" && "$read_rows" -ge 100 ]] && started=1 && break
    sleep 0.1
done
echo "stream read the first part: $started"

$STREAMING_CLIENT -q "INSERT INTO t_stream_subcolumns VALUES (100, NULL), (101, 'x')"
wait $pid

$STREAMING_CLIENT -q "
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_stream_subcolumns' AND active ORDER BY name;
    DROP TABLE t_stream_subcolumns;
"
