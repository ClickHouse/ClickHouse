#!/usr/bin/env bash
# `EXPLAIN PLAN tree = 1` of a plan that is not distributed: the steps are drawn as boxes, without a frame, and without
# the `Exchanges` and `Levels` sections. The queries are only explained, never executed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The widths of the boxes are computed from the step texts, and the text of a read step carries the database name.
# The harness replaces the name of the test database with `default` after the run, which would break the widths, so
# the tables live in a database whose name has a fixed length and is printed as a placeholder of the same length.
DB="db_$(echo -n "$CLICKHOUSE_DATABASE" | md5sum | cut -c1-8)"
PLACEHOLDER="db_XXXXXXXX"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $DB"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $DB"

$CLICKHOUSE_CLIENT --multiquery <<EOF | sed "s/$DB/$PLACEHOLDER/g"
-- A read box shows its parts and granules: pin the granularity and the part format, which the test harness randomizes
-- (the format of a part changes its number of granules), and merge the inserted blocks into one part.
CREATE TABLE $DB.small(sid UInt64) ENGINE = MergeTree ORDER BY sid SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
CREATE TABLE $DB.big(bid UInt64) ENGINE = MergeTree ORDER BY bid SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
CREATE TABLE $DB.merge_a(x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
CREATE TABLE $DB.merge_b(x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;

INSERT INTO $DB.small SELECT number FROM numbers(0, 1000);
OPTIMIZE TABLE $DB.small FINAL;
INSERT INTO $DB.big SELECT number FROM numbers(0, 100000);
OPTIMIZE TABLE $DB.big FINAL;
-- \`ReadFromMerge\` builds a nested plan only for a table with data.
INSERT INTO $DB.merge_a SELECT number FROM numbers(0, 10);
INSERT INTO $DB.merge_b SELECT number FROM numbers(0, 10);

-- The test harness randomizes these, and they change the plan.
SET query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0, use_hash_table_stats_for_join_reordering = 0;
-- Without join reordering the join box shows no row estimates.
SET query_plan_optimize_join_order_limit = 10;
SET enable_join_runtime_filters = 0, optimize_aggregation_in_order = 0, use_statistics = 0;
SET make_distributed_plan = 0, enable_parallel_replicas = 0;

SELECT '-- aggregation';
EXPLAIN tree = 1
SELECT bid % 10 AS k, sum(bid)
FROM $DB.big
GROUP BY k;

SELECT '-- join';
EXPLAIN tree = 1
SELECT count()
FROM $DB.big AS big
INNER JOIN $DB.small AS small ON small.sid = big.bid;

SELECT '-- union all';
EXPLAIN tree = 1
SELECT bid FROM $DB.big
UNION ALL
SELECT sid FROM $DB.small;

-- Without \`actions\` the boxes have no details, and \`Expression\` steps are still not drawn.
SELECT '-- without actions';
EXPLAIN tree = 1, actions = 0
SELECT bid % 10 AS k, sum(bid)
FROM $DB.big
GROUP BY k;

SELECT '-- without descriptions';
EXPLAIN tree = 1, description = 0
SELECT bid % 10 AS k, sum(bid)
FROM $DB.big
GROUP BY k;

-- The per-table plans of \`ReadFromMerge\` are nested plans: the box refers to them, and they are drawn after the main drawing.
SELECT '-- nested plans';
EXPLAIN tree = 1
SELECT x
FROM merge('$DB', '^merge_');

-- \`make_distributed_plan\` does not support \`WITH TOTALS\`: the query falls back to local execution, and the reason
-- is printed under the drawing.
SELECT '-- fallback to local execution';
EXPLAIN tree = 1
SELECT bid % 10 AS k, sum(bid)
FROM $DB.big
GROUP BY k WITH TOTALS
SETTINGS make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 1, max_rows_to_group_by = 0;
EOF

$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB"
