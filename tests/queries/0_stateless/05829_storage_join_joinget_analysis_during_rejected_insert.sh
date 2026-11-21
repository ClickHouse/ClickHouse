#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A persistent `INSERT` into a `Join` table that exceeds `max_rows_in_join` rolls the live state back
# by replacing the `HashJoin` under the write lock. The type analysis of `joinGet` reads the same
# `HashJoin`, so it must hold the read lock: otherwise the replacement could destroy the object while
# the analysis is using it (a use-after-free that sanitizers report).

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_analysis;
    CREATE TABLE join_analysis (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)
        SETTINGS max_rows_in_join = 2, join_overflow_mode = 'throw';
    INSERT INTO join_analysis VALUES (1, 'committed');
"

function failing_inserts()
{
    local end=$((SECONDS + 3))
    while [ $SECONDS -lt $end ]; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO join_analysis VALUES (2, 'a'), (3, 'b')" 2>&1 \
            | grep -q -F 'SET_SIZE_LIMIT_EXCEEDED' || echo "The INSERT did not fail with SET_SIZE_LIMIT_EXCEEDED"
    done
}

function analyses()
{
    local end=$((SECONDS + 3))
    while [ $SECONDS -lt $end ]; do
        $CLICKHOUSE_CLIENT --query "SELECT joinGet('join_analysis', 'v', toUInt64(number % 3)) FROM numbers(3) FORMAT Null"
    done
}

failing_inserts &
analyses &
analyses &
wait

$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_analysis ORDER BY k"
$CLICKHOUSE_CLIENT --query "SELECT joinGet('join_analysis', 'v', toUInt64(1))"
$CLICKHOUSE_CLIENT --query "DROP TABLE join_analysis"
