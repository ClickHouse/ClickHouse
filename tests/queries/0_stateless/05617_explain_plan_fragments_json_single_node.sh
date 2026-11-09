#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `EXPLAIN PLAN fragments = 1, json = 1` of a plan that is not cut is the document of `json = 1`, plus the reason when the
# query fell back to local execution. Each document is taken in its own query: the step ids (`Node Id`) are numbered per
# query, so two `EXPLAIN` in one query would number their steps differently.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_fragments_json;
    CREATE TABLE t_fragments_json (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_fragments_json SELECT number, number FROM numbers(1000);
"

# In ClickHouse Cloud a scan on shared storage is replaced by `ReadFromMergeTreeAtWorker`, which prints no table name.
SETTINGS="explain_query_plan_default = 'legacy', enable_parallel_replicas = 0, max_rows_to_group_by = 0, optimize_aggregation_in_order = 0, distributed_plan_prefer_replicas_over_workers = 1"
QUERY="SELECT k % 10 AS g, sum(v) FROM t_fragments_json GROUP BY g"

explain()
{
    $CLICKHOUSE_CLIENT -q "EXPLAIN $1 $QUERY $2 SETTINGS $SETTINGS, $3 FORMAT TSVRaw"
}

compare()
{
    local label=$1
    shift
    echo "$label"
    # Only the lines that differ, without the line numbers, which follow the length of the plan.
    local changed
    changed=$(diff --unchanged-line-format= --old-line-format='< %L' --new-line-format='> %L' \
        <(explain "$1" "$2" "$3") <(explain "$4" "$5" "$6"))
    if [ -z "$changed" ]; then echo "same document"; else echo "$changed"; fi
}

compare "-- make_distributed_plan = 0" \
    "json = 1" "" "make_distributed_plan = 0" \
    "fragments = 1, json = 1" "" "make_distributed_plan = 0"

compare "-- make_distributed_plan = 0, pretty = 1" \
    "fragments = 1, json = 1, pretty = 0" "" "make_distributed_plan = 0" \
    "fragments = 1, json = 1, pretty = 1" "" "make_distributed_plan = 0"

# The query falls back to local execution: the only difference is the reason after the plan.
compare "-- fallback to local execution" \
    "json = 1" "WITH TOTALS" "make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 1" \
    "fragments = 1, json = 1" "WITH TOTALS" "make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 1"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_fragments_json"
