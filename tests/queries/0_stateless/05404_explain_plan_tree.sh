#!/usr/bin/env bash
# `EXPLAIN PLAN tree = 1`: the plan of a query distributed to stateless workers, drawn as boxes, one per step,
# framed by query fragment. The queries are only explained, never executed.

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
CREATE TABLE $DB.small(sid UInt64, s Array(Int64)) ENGINE = MergeTree ORDER BY sid;
CREATE TABLE $DB.big(bid UInt64, b Array(Int64)) ENGINE = MergeTree ORDER BY bid;

INSERT INTO $DB.small SELECT number, [number] FROM numbers(0, 1000);
INSERT INTO $DB.big SELECT number, [number] FROM numbers(0, 100000);

SET query_plan_join_swap_table = 0;
-- Distributed aggregation cannot enforce a global \`max_rows_to_group_by\`, so pin it to 0.
SET max_rows_to_group_by = 0;
-- The test harness randomizes these, and aggregation in order changes the plan.
SET distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;

SET
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    enable_join_runtime_filters = 0,
    use_statistics = 0,
    distributed_plan_optimize_exchanges = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_force_exchange_kind = 'Streaming';

SELECT '-- shuffle join with aggregation';
EXPLAIN tree = 1
SELECT small.sid, count()
FROM $DB.big AS big, $DB.small AS small
WHERE small.sid = (big.bid + 1) % 5000
GROUP BY small.sid
SETTINGS distributed_plan_max_rows_to_broadcast = 0;

SELECT '-- broadcast join';
EXPLAIN tree = 1
SELECT count()
FROM $DB.big AS big, $DB.small AS small
WHERE small.sid = (big.bid + 1) % 5000
SETTINGS distributed_plan_max_rows_to_broadcast = 1000000;

SELECT '-- aggregation';
EXPLAIN tree = 1
SELECT bid % 10 AS k, sum(bid)
FROM $DB.big
GROUP BY k;

SELECT '-- order by';
EXPLAIN tree = 1
SELECT bid
FROM $DB.big
ORDER BY bid DESC;

SELECT '-- persisted exchanges';
EXPLAIN tree = 1
SELECT bid % 10 AS k, sum(bid)
FROM $DB.big
GROUP BY k
SETTINGS distributed_plan_force_exchange_kind = 'Persisted';

SELECT '-- without pretty';
EXPLAIN tree = 1, pretty = 0
SELECT bid % 10 AS k, sum(bid)
FROM $DB.big
GROUP BY k;

-- Without descriptions the boxes are narrower than the frame titles: an edge must cross the top border of a frame
-- after its title, not through it.
SELECT '-- without descriptions';
EXPLAIN tree = 1, description = 0
SELECT bid % 10 AS k, sum(bid)
FROM $DB.big
GROUP BY k;

-- Wide characters take two columns: the boxes and the labels must fit them, and the cut at 40 columns must not split one.
SELECT '-- wide characters';
CREATE TABLE $DB.\`大表名字测试数据\`(\`键\` UInt64) ENGINE = MergeTree ORDER BY \`键\`;
INSERT INTO $DB.\`大表名字测试数据\` SELECT number FROM numbers(0, 100000);
EXPLAIN tree = 1
SELECT \`键\` % 10 AS k, sum(\`键\`)
FROM $DB.\`大表名字测试数据\`
GROUP BY k;

-- A plan that is not distributed is printed as without \`tree\`. In the legacy format, so that the granule counts,
-- which depend on the randomized MergeTree settings, are not printed.
SET explain_query_plan_default = 'legacy';

SELECT '-- make_distributed_plan = 0';
EXPLAIN tree = 1
SELECT bid % 10 AS k, sum(bid)
FROM $DB.big
GROUP BY k
SETTINGS make_distributed_plan = 0;

SELECT '-- fallback to local execution';
EXPLAIN tree = 1
SELECT bid % 10 AS k, sum(bid)
FROM $DB.big
GROUP BY k WITH TOTALS
SETTINGS distributed_plan_fallback_to_local_execution = 1;
EOF

echo "-- errors"
for options in "fragments = 1" "json = 1" "distributed = 1" "optimize = 0"; do
    $CLICKHOUSE_CLIENT --make_distributed_plan=1 -q "EXPLAIN tree = 1, $options SELECT count() FROM $DB.big" 2>&1 \
        | grep -o -m1 "NOT_IMPLEMENTED\|BAD_ARGUMENTS"
done

$CLICKHOUSE_CLIENT -q "DROP DATABASE $DB"
