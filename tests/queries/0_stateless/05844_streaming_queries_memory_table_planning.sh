#!/usr/bin/env bash
# Tags: atomic-database

# The planner sees a streaming read of a `Memory` table as a stream: it rejects the operations that need the end of
# an unbounded stream, and the row policy is applied inside the read, where no expression of the user can get ahead of it.
# The read looks at the data when it starts, so it gets the rows of a materialized CTE, and a bounded read that is
# stopped early still reports the cursor to an incremental refresh.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./streaming.lib
. "$CUR_DIR"/streaming.lib

user="user_${CLICKHOUSE_DATABASE}"
policy="policy_${CLICKHOUSE_DATABASE}"

$STREAMING_CLIENT -q "
    CREATE TABLE m (k UInt64, v String) ENGINE = Memory;
    INSERT INTO m VALUES (1, '0'), (2, 'secret');
"

echo "--- an unbounded stream is rejected where its end is needed"
$STREAMING_CLIENT -q "
    SELECT count() FROM m STREAM; -- { serverError ILLEGAL_STREAM }
    SELECT k FROM m STREAM GROUP BY k; -- { serverError ILLEGAL_STREAM }
    SELECT * FROM m STREAM ORDER BY k LIMIT 1; -- { serverError ILLEGAL_STREAM }
    SELECT k FROM m STREAM INTERSECT SELECT k FROM m; -- { serverError ILLEGAL_STREAM }
    SELECT k FROM m STREAM SETTINGS use_query_cache = 1; -- { serverError ILLEGAL_STREAM }
"

echo "--- the same over a bounded stream"
$STREAMING_CLIENT -q "
    SELECT count() FROM m STREAM BOUNDED;
    SELECT k FROM m STREAM BOUNDED GROUP BY k ORDER BY k;
    SELECT * FROM m STREAM BOUNDED ORDER BY k LIMIT 1;
    SELECT k FROM m STREAM BOUNDED INTERSECT SELECT k FROM m WHERE k = 2;
"

echo "--- an unbounded stream with WHERE and LIMIT"
$STREAMING_CLIENT -q "SELECT k FROM m STREAM WHERE v = 'secret' LIMIT 1"

echo "--- a throwing predicate is not evaluated on the rows hidden by a row policy"
$STREAMING_CLIENT -q "
    DROP USER IF EXISTS $user;
    CREATE USER $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.m TO $user;
    CREATE ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.m USING k = 1 TO $user;
"
# The stateful function keeps the conjunction of the filters from being split, and without short-circuit evaluation
# all its parts are evaluated on every row: if the policy were merged into it, `toUInt64('secret')` would throw.
restricted="$STREAMING_CLIENT --user $user --short_circuit_function_evaluation=disable"
$restricted -q "SELECT k FROM ${CLICKHOUSE_DATABASE}.m STREAM BOUNDED PREWHERE toUInt64(v) = rowNumberInAllBlocks()"
$restricted -q "SELECT k FROM ${CLICKHOUSE_DATABASE}.m STREAM BOUNDED WHERE toUInt64(v) = rowNumberInAllBlocks()"
$restricted -q "SELECT k FROM ${CLICKHOUSE_DATABASE}.m STREAM BOUNDED PREWHERE toUInt64(v) = 0 WHERE toUInt64(v) = rowNumberInAllBlocks()"
$restricted -q "SELECT k FROM ${CLICKHOUSE_DATABASE}.m STREAM PREWHERE toUInt64(v) = rowNumberInAllBlocks() LIMIT 1"
$restricted -q "SELECT k FROM ${CLICKHOUSE_DATABASE}.m STREAM WHERE toUInt64(v) = rowNumberInAllBlocks() LIMIT 1"
$STREAMING_CLIENT -q "DROP ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.m; DROP USER $user"

echo "--- a materialized CTE"
$STREAMING_CLIENT --enable_materialized_cte=1 -q "
    WITH c AS MATERIALIZED (SELECT 1 AS x)
    SELECT x FROM c STREAM BOUNDED
    UNION ALL
    SELECT x FROM c;
"

echo "--- an incremental refresh whose query stops the read early"
$STREAMING_CLIENT -q "
    CREATE TABLE src (k UInt64) ENGINE = Memory;
    CREATE TABLE tgt (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO src SELECT number FROM numbers(1000) SETTINGS max_block_size = 10, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;

    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY
        AS SELECT DISTINCT toUInt64(1) AS k FROM src SETTINGS max_threads = 1;
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;
    SELECT count() FROM tgt;
    -- No new rows in the source: nothing is appended.
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;
    SELECT count() FROM tgt;
    INSERT INTO src VALUES (1000);
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;
    SELECT count() FROM tgt;
    DROP TABLE mv;

    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY
        AS SELECT k FROM src LIMIT 1; -- { serverError NOT_IMPLEMENTED }
"
