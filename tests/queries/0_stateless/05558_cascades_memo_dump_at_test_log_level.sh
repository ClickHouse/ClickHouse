#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `Memo::dump` prints one block per group of the Cascades memo: the estimated row count, a
# `Logical:` section, a `Physical:` section with the cost of every costed alternative, and one
# `Best for <required properties>` summary per winning bucket whose inputs name the child groups
# the winner was built from. The structure is documented in
# src/Processors/QueryPlan/Optimizations/Cascades/ARCHITECTURE.md, section "The Memo".
#
# The optimizer prints the memo only through LOG_TEST, so the dump reaches a client only when the
# client asks for the `test` log level, and never at the `trace` level the server itself logs at.
# The test asks for it and keeps the log out of its own stderr.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_memo_dump (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_memo_dump SELECT number, number * 2 FROM numbers(1000);
"

DUMP="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_memo.log"

echo "-- value read through the Cascades planner"
$CLICKHOUSE_CLIENT --send_logs_level=test -q "
    SET param__internal_cascades_cluster_node_count = 4;
    SELECT v FROM t_memo_dump WHERE k = 7
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
             distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0,
             enable_parallel_replicas = 0, enable_join_runtime_filters = 0;
" 2> "$DUMP"

# Which alternatives the cost-based search keeps, and the estimates and costs it prints, depend on
# the data layout, so only the presence of each section is asserted, never a count or a number.
function present()
{
    grep -c -m1 -e "$1" "$DUMP" || true
}

echo "-- sections of the memo dump"
groups=$(grep -c '^Group #' "$DUMP")
logical=$(grep -c '^ *Logical:$' "$DUMP")
physical=$(grep -c '^ *Physical:$' "$DUMP")
echo "memo groups printed: $(( groups > 0 ))"
echo "one Logical section per group: $(( groups == logical ))"
echo "one Physical section per group: $(( groups == physical ))"
echo "estimated row count: $(present '^ *Statistics: rows: [0-9]')"
echo "costed physical alternative: $(present "^ *{.*} '[A-Za-z]*'.* cost: [0-9]")"
echo "best implementation per required properties: $(present '^ *Best for {.*}:$')"
echo "local and subtree cost of the best implementation: $(present '^ *Cost: local(.*) subtree(.*) : ')"
echo "winning chain into a child group: $(present ' <- #[0-9]')"

# Every child group a winner was built from must be a group the dump printed.
echo "-- inputs of the winners resolve inside the memo"
last_group=$(grep -o '^Group #[0-9][0-9]*' "$DUMP" | grep -o '[0-9][0-9]*$' | sort -n | tail -1)
echo "inputs pointing outside the memo: $(grep -o ' <- #[0-9][0-9]*' "$DUMP" \
    | grep -o '[0-9][0-9]*$' | awk -v last="${last_group:--1}" '$1 + 0 > last + 0' | wc -l)"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_memo_dump"
