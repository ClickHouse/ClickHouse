#!/usr/bin/env bash
# `EXPLAIN PLAN tree = 1` of a plan that is not distributed and has nested plans: the box of the step that owns them
# ends with their ids, and every nested plan is drawn after the main drawing under `N<id> · nested plan of <step>`,
# including the nested plans of a nested plan. The queries are only explained, never executed.

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
-- (the format of a part changes its number of granules). Every table gets one insert, so one part.
CREATE TABLE $DB.a(x UInt64, s String) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
CREATE TABLE $DB.b(x UInt64, s String) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
CREATE TABLE $DB.c(x UInt64, s String) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
-- \`outer_m\` reads \`inner_m\`, a \`Merge\` table itself: the nested plan of \`inner_m\` has nested plans too.
CREATE TABLE $DB.inner_m(x UInt64, s String) ENGINE = Merge($DB, '^[ab]\$');
CREATE TABLE $DB.outer_m(x UInt64, s String) ENGINE = Merge($DB, '^(inner_m|c)\$');

-- \`ReadFromMerge\` builds a nested plan only for a table with data.
INSERT INTO $DB.a SELECT number, toString(number) FROM numbers(0, 10);
INSERT INTO $DB.b SELECT number, toString(number) FROM numbers(0, 10);
INSERT INTO $DB.c SELECT number, toString(number) FROM numbers(0, 10);

-- The test harness randomizes these, and they change the plan.
SET optimize_move_to_prewhere = 1, optimize_aggregation_in_order = 0, use_statistics = 0, query_plan_remove_unused_columns = 1;
SET make_distributed_plan = 0, enable_parallel_replicas = 0;

-- The aggregation is drawn over \`ReadFromMerge\`, and the filter is pushed into the reads of the nested plans.
SELECT '-- aggregation over nested plans';
EXPLAIN tree = 1
SELECT s, count()
FROM merge('$DB', '^[ab]\$')
WHERE x > 5
GROUP BY s;

-- The nested plans of the nested plan \`N2\` get the next ids and are drawn after it.
SELECT '-- nested plans of a nested plan';
EXPLAIN tree = 1
SELECT x
FROM $DB.outer_m;
EOF

$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB"
