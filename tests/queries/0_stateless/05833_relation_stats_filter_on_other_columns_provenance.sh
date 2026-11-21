#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The estimate of a MergeTree read records `row-subset` on the columns that its predicates read and
# `filter-on-other-columns` on the others, as long as the predicates are the only cause of dropped rows and are
# attributed to the stored columns they read. The provenance is only visible in the trace log of the estimate, hence a
# shell test. Each query estimates `t_r` once, for the choice of the IEJoin key conditions.

# Prints the provenance of every column of `t_r` in the estimate of the join with the input `$1` under the settings `$2`,
# one column per line.
function print_provenance() {
    local query="$1"
    local settings="${2:+, $2}"
    $CLICKHOUSE_LOCAL --multiquery --query "
        CREATE TABLE t_l (a1 UInt32, a2 UInt32, a3 UInt32)
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS auto_statistics_types = 'basic, uniq_v2';

        CREATE TABLE t_r (f UInt32, b1 UInt32, b2 UInt32, b3 UInt32)
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS auto_statistics_types = 'basic, uniq_v2';

        SET materialize_statistics_on_insert = 1;
        INSERT INTO t_l SELECT number % 1000, number % 1000, (number * 97) % 100000 FROM numbers(1000);
        INSERT INTO t_r SELECT number % 10, number % 1000, 1000 + number % 1000, number % 1000 FROM numbers(1000);

        EXPLAIN actions = 1
        SELECT count() FROM t_l AS l JOIN ($query) AS r
        ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
        SETTINGS
            join_algorithm = 'ie_join',
            join_use_nulls = 0,
            use_statistics = 1,
            query_plan_optimize_join_order_limit = 0,
            enable_parallel_replicas = 0,
            send_logs_level = 'trace',
            send_logs_source_regexp = 'optimizeJoin'$settings
        FORMAT Null;
    " 2>&1 |
        grep -F 'estimate statistics t_r:' |
        grep -oE '(f|b[0-9]): [0-9]+ \(ndv: [^ ]+ range: [^)]+\)' |
        sed -E 's/: [0-9]+ \(ndv: [^ ]+ range: / range: /; s/\)$//' |
        sort
}

echo '-- WHERE on a column that is not an operand, PREWHERE optimization disabled'
print_provenance "SELECT * FROM t_r WHERE f < 5" "optimize_move_to_prewhere = 0"

echo '-- the same, PREWHERE optimization enabled'
print_provenance "SELECT * FROM t_r WHERE f < 5" "optimize_move_to_prewhere = 1"

echo '-- WHERE on an operand'
print_provenance "SELECT * FROM t_r WHERE b3 < 500"

echo '-- a filter on a column computed from an operand, not pushed down: no attribution'
print_provenance "SELECT * FROM (SELECT f, b1, b2, b3, b3 + 1 AS x FROM t_r) WHERE x < 500" \
    "query_plan_filter_push_down = 0, query_plan_merge_expressions = 0, optimize_move_to_prewhere = 0"

echo '-- the same filter pushed into the read: attributed to the operand'
print_provenance "SELECT * FROM (SELECT f, b1, b2, b3, b3 + 1 AS x FROM t_r) WHERE x < 500"

echo '-- a non-throwing read limit: no attribution'
print_provenance "SELECT * FROM t_r WHERE f < 5" "max_rows_to_read = 1000000, read_overflow_mode = 'break'"
