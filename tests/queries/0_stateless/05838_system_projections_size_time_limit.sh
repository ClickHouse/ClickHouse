#!/usr/bin/env bash
# Tags: no-parallel
# - no-parallel: the test switches the `slowdown_system_parts_enumeration` failpoint, which is a server-global switch.
#
# The size columns of `system.projections` walk the parts of every table, so the walk must stop on the time limit.
# The failpoint sleeps 500 ms per part, counted by the `SystemPartsEnumerationSlowdownSleeps` profile event:
# - for `t_slowdown_system_parts_snap*`, inside the parts snapshot of `MergeTree`, which polls the stop callback on every part;
# - for the other `t_slowdown_system_parts*` tables, in the loop that sums the projection parts.
# A query that ignores the time limit sleeps once per part, while one that honors it stops after a couple of sleeps.
# The sleep count is asserted instead of the elapsed time, because it has a deterministic upper bound.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

NUM_PARTS=8
MAX_SLEEPS=3
LOG_PREFIX="05838_${CLICKHOUSE_TEST_UNIQUE_NAME}_$$"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_slowdown_system_parts_snap_projections;
DROP TABLE IF EXISTS t_slowdown_system_parts_projections;

CREATE TABLE t_slowdown_system_parts_snap_projections (x UInt64, PROJECTION p (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x PARTITION BY x;
CREATE TABLE t_slowdown_system_parts_projections (x UInt64, PROJECTION p (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x PARTITION BY x;

INSERT INTO t_slowdown_system_parts_snap_projections SELECT number FROM numbers($NUM_PARTS) SETTINGS max_partitions_per_insert_block = 0;
INSERT INTO t_slowdown_system_parts_projections SELECT number FROM numbers($NUM_PARTS) SETTINGS max_partitions_per_insert_block = 0;

-- Without the failpoint and limits, the whole result is built.
SELECT 'full', table, total_rows FROM system.projections WHERE database = currentDatabase() ORDER BY table;
"

function disable_slowdown_failpoint()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT slowdown_system_parts_enumeration" > /dev/null 2>&1 ||:
}
trap disable_slowdown_failpoint EXIT

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT slowdown_system_parts_enumeration"

# $1 - a label, $2 - the table, $3 - the overflow mode.
function counted_query()
{
    $CLICKHOUSE_CLIENT --query "
    SELECT total_rows FROM system.projections WHERE database = currentDatabase() AND table = '$2'
    FORMAT Null
    SETTINGS max_execution_time = 0.5, timeout_overflow_mode = '$3', log_comment = '$LOG_PREFIX $1';
    " 2>&1 | grep -o -m1 "TIMEOUT_EXCEEDED"
}

counted_query snapshot_break t_slowdown_system_parts_snap_projections break
counted_query snapshot_throw t_slowdown_system_parts_snap_projections throw
counted_query sum_break t_slowdown_system_parts_projections break
counted_query sum_throw t_slowdown_system_parts_projections throw

# Only the metadata columns: the parts are not walked at all.
$CLICKHOUSE_CLIENT --query "
SELECT name FROM system.projections WHERE database = currentDatabase()
FORMAT Null
SETTINGS log_comment = '$LOG_PREFIX names';
"

disable_slowdown_failpoint

$CLICKHOUSE_CLIENT --query "
SYSTEM FLUSH LOGS query_log;

SELECT
    splitByChar(' ', log_comment)[2] AS label,
    type,
    ProfileEvents['SystemPartsEnumerationSlowdownSleeps'] <= if(label = 'names', 0, $MAX_SLEEPS)
FROM system.query_log
WHERE current_database = currentDatabase() AND type != 'QueryStart' AND startsWith(log_comment, '$LOG_PREFIX ')
ORDER BY label;

DROP TABLE t_slowdown_system_parts_snap_projections;
DROP TABLE t_slowdown_system_parts_projections;
"
