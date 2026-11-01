#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the remote arm needs the stateless worker configuration.
# A hash join reads no probe rows until its build side is complete, so the coordinator must start the
# stage that feeds the build side before the stage that feeds the probe side. For every query the test
# prints the joins with their probe (left) and build (right) inputs, then the tables of the stages in
# the order the coordinator starts them, read from its `Executing stage` debug lines.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every key set is a subset of the previous one, so each join below matches all rows of its smaller side.
$CLICKHOUSE_CLIENT --query "
CREATE TABLE probe_t (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE build_t (bk UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE third_t (ck UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE fourth_t (dk UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE fifth_t (ek UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO probe_t SELECT number FROM numbers(100000);
INSERT INTO build_t SELECT number * 10 FROM numbers(1000);
INSERT INTO third_t SELECT number * 20 FROM numbers(500);
INSERT INTO fourth_t SELECT number * 40 FROM numbers(250);
INSERT INTO fifth_t SELECT number * 80 FROM numbers(125);
"

# Pinned: streaming exchanges, because persisted producers are started before their consumer; `hash`,
# because a sorting join has no build side; no join runtime filters, which add steps to the printed plan;
# two buckets per stage, because the coordinator sends the tasks of a stage one by one; `max_rows_to_group_by`,
# because the CI profile sets it and `make_distributed_plan` rejects an aggregation with a row limit.
SETTINGS="make_distributed_plan = 1, enable_parallel_replicas = 0,
    distributed_plan_force_exchange_kind = 'Streaming', join_algorithm = 'hash', query_plan_optimize_join_order_randomize = 0,
    enable_join_runtime_filters = 0, distributed_plan_default_reader_bucket_count = 2,
    distributed_plan_default_shuffle_join_bucket_count = 2, max_rows_to_group_by = 0, distributed_plan_fallback_to_local_execution = 0"
# No swap and no reordering: each join builds its right input as written. The queries put the smaller side
# on the right, as the planner would.
AS_WRITTEN="query_plan_join_swap_table = 'false', query_plan_optimize_join_order_limit = 0"

function show()
{
    local label="$1"
    local query="$2"
    local case_settings="$3"
    local executors="${4:-1}"
    echo "$label"
    # The joins, exchanges and reads of the plan: under a join, `├──` leads to its probe input and `└──` to its build input.
    $CLICKHOUSE_CLIENT --query "EXPLAIN pretty = 1, compact = 1 $query SETTINGS $SETTINGS, $case_settings" \
        | grep -E "JoinLogical|Exchange|ReadFromMergeTree" | sed "s/${CLICKHOUSE_DATABASE}\.//"
    for execute_locally in $executors; do
        local logs
        logs=$($CLICKHOUSE_CLIENT --send_logs_level=debug --send_logs_source_regexp='^DistributedQueryPlanExecutor$' --query "
            $query SETTINGS $SETTINGS, $case_settings, distributed_plan_execute_locally = $execute_locally" 2>&1 >/dev/null) \
            || echo "  query failed, execute_locally = $execute_locally"
        echo "  start order, execute_locally = $execute_locally: $(grep -oE "\b${CLICKHOUSE_DATABASE}\.(probe_t|build_t|third_t|fourth_t|fifth_t)\b" <<< "$logs" \
            | sed "s/^${CLICKHOUSE_DATABASE}\.//" | xargs)"
    done
}

# probe_t (100000 rows) ⋈ build_t (1000 rows): build_t is smaller, so it is the build side, and probe_t the probe side.
# The two queries differ by one plan node above the join (`count()`); without a deliberate order the start order
# followed the parity of those nodes, so one of them started the probe side first.
# Only the first query also runs on the remote executor: it orders the stages with the same code as the local one, and
# sending every task over the network is slow in CI.
show "probe_t ⋈ build_t, count()" "SELECT count() FROM probe_t AS p INNER JOIN build_t AS b ON p.k = b.bk" \
    "$AS_WRITTEN, distributed_plan_max_rows_to_broadcast = 0" "1 0"
show "probe_t ⋈ build_t, p.k" "SELECT p.k FROM probe_t AS p INNER JOIN build_t AS b ON p.k = b.bk" \
    "$AS_WRITTEN, distributed_plan_max_rows_to_broadcast = 0"

# build_t (1000 rows) ⋈ probe_t (100000 rows), the small table written first: the planner swaps the inputs, so
# build_t is still the build side and starts first.
show "build_t ⋈ probe_t, swapped by the planner" "SELECT count() FROM build_t AS b INNER JOIN probe_t AS p ON b.bk = p.k" \
    "query_plan_join_swap_table = 'auto', query_plan_optimize_join_order_limit = 10, distributed_plan_max_rows_to_broadcast = 0"

# Left-deep ((((probe_t ⋈ build_t) ⋈ third_t) ⋈ fourth_t) ⋈ fifth_t), 100000, 1000, 500, 250 and 125 rows: each join
# gives as many rows as its right table, so the right table is always the smaller side and the build side. The outermost
# build side, fifth_t, starts first, and the probe_t at the bottom of the probe sides last.
show "five tables, left-deep" "SELECT count() FROM probe_t AS p INNER JOIN build_t AS b ON p.k = b.bk
    INNER JOIN third_t AS c ON b.bk = c.ck INNER JOIN fourth_t AS d ON c.ck = d.dk INNER JOIN fifth_t AS e ON d.dk = e.ek" \
    "$AS_WRITTEN, distributed_plan_max_rows_to_broadcast = 0"

# Bushy (probe_t ⋈ build_t) ⋈ ((third_t ⋈ fourth_t) ⋈ fifth_t): the right join gives 125 rows against 1000, so it is the
# build side of the outer join, and its whole subtree starts before probe_t ⋈ build_t, its own build side fifth_t first.
show "five tables, bushy" "SELECT count() FROM probe_t AS p INNER JOIN build_t AS b ON p.k = b.bk
    INNER JOIN (SELECT c.ck AS ck FROM third_t AS c INNER JOIN fourth_t AS d ON c.ck = d.dk INNER JOIN fifth_t AS e ON d.dk = e.ek) AS cde
    ON b.bk = cde.ck" \
    "$AS_WRITTEN, distributed_plan_max_rows_to_broadcast = 0"

# Star probe_t ⋈ build_t ⋈ third_t ⋈ fourth_t ⋈ fifth_t on probe_t's key: the four small tables are the build sides and
# are broadcast into the stage that reads probe_t. That stage starts first, as the consumer of all four, and then the
# build sides, from the outermost join inwards.
show "five tables, star of broadcast joins" "SELECT count() FROM probe_t AS p INNER JOIN build_t AS b ON p.k = b.bk
    INNER JOIN third_t AS c ON p.k = c.ck INNER JOIN fourth_t AS d ON p.k = d.dk INNER JOIN fifth_t AS e ON p.k = e.ek" \
    "$AS_WRITTEN, distributed_plan_max_rows_to_broadcast = 20000"
