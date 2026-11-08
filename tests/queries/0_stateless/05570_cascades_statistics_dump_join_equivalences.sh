#!/usr/bin/env bash

# `ExpressionStatistics::dump` (QueryPlan/Optimizations/Cascades/Statistics.cpp) has no EXPLAIN
# surface: the derived statistics of a Cascades memo group are formatted only by the `LOG_TEST`
# calls in `StatisticsDerivation`. `LOG_IMPL` (Common/logger_useful.h) also runs its body when the
# query's own client log level is high enough, so `send_logs_level = 'test'` makes the server
# format them without a server config change. The formatted text is therefore user-visible: the
# client log of this query is kept and the test asserts values decoded out of it.
#
# The memo groups of an INNER equi-join cover the whole formatter: a `ReadFromMergeTree` group of
# a non-empty table has physical read bytes and a per-column entry for every read column, and only
# `JoinKind::Inner` adds the equality keys as an equivalence class (`StatisticsDerivation`), so the
# two-member class is printed once per group and skipped on its second member.
#
# Asserted, all decoded out of the formatted statistics: the estimated row counts of the groups
# that read a table (must be the two table sizes), the names of the key columns that carry
# per-column statistics, the members of the printed equivalence class, and the number of
# equivalence classes printed per group (a class must be printed once, not once per member).
# No byte size, no time and no log text is asserted. The join result itself is asserted too and
# must be the same as the result of the plan built without Cascades.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ORDER BY tuple()`: with no primary key nothing can prune granules, so the row count the index
# analysis reports, and with it the estimate the statistics carry, is the whole table. The part
# format and the serialization are pinned too: the formatter prints the physical read bytes only
# when the estimated row width is above zero, and that width is derived from the uncompressed
# column sizes of the parts, which a compact part does not track per column and a sparse column
# can leave empty.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_stats_dump_a;
    DROP TABLE IF EXISTS t_stats_dump_b;
    CREATE TABLE t_stats_dump_a (k UInt32, v String) ENGINE = MergeTree ORDER BY tuple()
      SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0,
        ratio_of_defaults_for_sparse_serialization = 1.0, index_granularity = 8192;
    CREATE TABLE t_stats_dump_b (k UInt32, w UInt64) ENGINE = MergeTree ORDER BY tuple()
      SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0,
        ratio_of_defaults_for_sparse_serialization = 1.0, index_granularity = 8192;
    SYSTEM STOP MERGES t_stats_dump_a;
    SYSTEM STOP MERGES t_stats_dump_b;
    INSERT INTO t_stats_dump_a SELECT number, 'v' || toString(number) FROM numbers(20);
    INSERT INTO t_stats_dump_b SELECT number, number * 3 FROM numbers(100);
"

LOG="${CLICKHOUSE_TMP:-.}/stats_dump_${CLICKHOUSE_DATABASE}.log"
rm -f "$LOG"
RESULT=

# The three sets the test asserts, each extracted out of the collected log. The loop below waits
# until every one of them is complete, so the break condition and the assertions cannot disagree:
# counting lines is not enough, because one table can contribute more than one read group, so two
# header lines can both belong to the same table.
reads_rows() { sed -nE 's/^estimated_rows: ([0-9]+) .*physical_read_bytes: [0-9].*/\1/p' "$LOG" | sort -n -u; }
key_cols() { grep -oE '`__table[12]\.k` NDV : [0-9]+ avg_bytes: [0-9]' "$LOG" | tr -d '`' | cut -d' ' -f1 | sort -u; }
eq_members() { grep -oE 'equal columns:( `[^`]+`)+' "$LOG" | sed 's/equal columns://' | tr -d '`' | tr ' ' '\n' | grep . | sort -u; }

# The server's logger is asynchronous (tests/config/config.d/logger_trace.xml), so a message
# formatted while the query is planned can still sit in the logger's own queue when the query
# ends, and is then never sent to the client. The query is repeated until the collected log holds
# two distinct read-group row counts, the per-column statistics of both join keys and both members
# of an equivalence class - the three kinds of line the formatter writes - and the lines of every
# attempt are collected in one file. The query is read only, so repeating it changes nothing.
#
# Session settings, so they also override the settings the test runner randomizes on the command
# line: Cascades needs both enable_cascades_optimizer and make_distributed_plan
# (Planner/PlannerJoinTree.cpp) plus a node count, which it cannot read from a cluster here, and
# enable_parallel_replicas or the join-order jitter would change which groups are derived.
# automatic_parallel_replicas_mode must be pinned next to enable_parallel_replicas: a nonzero
# automatic mode together with make_distributed_plan is rejected with SUPPORT_IS_DISABLED
# (Optimizations/QueryPlanOptimizationSettings.cpp), and clearing enable_parallel_replicas alone
# does not reset it.
for _ in {1..8}; do
    RESULT=$($CLICKHOUSE_CLIENT -q "
        SET send_logs_level = 'test';
        SET enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1;
        SET param__internal_cascades_cluster_node_count = 4;
        SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
        SET query_plan_optimize_join_order_randomize = 0, max_rows_to_group_by = 0;
        SELECT 'cascades', count(), sum(a.k), sum(b.w), sum(length(a.v))
        FROM t_stats_dump_a AS a INNER JOIN t_stats_dump_b AS b ON a.k = b.k;
    " 2>>"$LOG")
    [ "$(reads_rows | wc -l)" -ge 2 ] && [ "$(key_cols | wc -l)" -ge 2 ] \
        && [ "$(eq_members | wc -l)" -ge 2 ] && break
done

echo "$RESULT"
# The estimated row count of every group that reads a table: the two table sizes, 20 and 100.
echo "reads_estimated_rows $(reads_rows | paste -sd, -)"
# The join key columns that have per-column statistics.
echo "key_columns_with_stats $(key_cols | paste -sd, -)"
# The members of the equivalence class that the INNER join adds.
echo "equal_columns_members $(eq_members | paste -sd, -)"
# A class of two members must be printed once per group, not once per member.
echo "max_equal_columns_lines_per_group $(awk '
    /^estimated_rows: / { if (n > m) m = n; n = 0 }
    /^equal columns:/ { n++ }
    END { if (n > m) m = n; print m + 0 }' "$LOG")"

rm -f "$LOG"

$CLICKHOUSE_CLIENT -q "
    SET enable_cascades_optimizer = 0, make_distributed_plan = 0;
    SELECT 'plain', count(), sum(a.k), sum(b.w), sum(length(a.v))
    FROM t_stats_dump_a AS a INNER JOIN t_stats_dump_b AS b ON a.k = b.k;
    DROP TABLE t_stats_dump_a;
    DROP TABLE t_stats_dump_b;
"
