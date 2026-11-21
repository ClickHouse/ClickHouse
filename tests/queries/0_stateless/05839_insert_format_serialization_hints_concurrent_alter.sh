#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: pauses every INSERT with inline data on a server-wide failpoint.
# no-fasttest: tests that run alone are kept out of the fast test.

# An INSERT with inline data parses the columns with the types of its metadata snapshot.
# A concurrent ALTER may change the types before the INSERT reads the serialization hints of the table,
# and a hint of the new type (`String`) must not be applied to the old type (`Tuple`).
# https://github.com/ClickHouse/ClickHouse/issues/131943

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP=insert_pause_before_reading_serialization_hints

cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_hints_alter;
    CREATE TABLE t_hints_alter (k UInt64, s Tuple(a String, b String)) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_hints_alter VALUES (0, ('x', 'y'));
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

# Over HTTP the data is inlined into the query, so the input format gets the serialization hints.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_parsing_to_custom_serialization=1" \
    --data-binary @- <<< 'INSERT INTO t_hints_alter FORMAT JSONEachRow {"k": 1, "s": {"a": "p", "b": "q"}}' &
insert_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"

$CLICKHOUSE_CLIENT --query "ALTER TABLE t_hints_alter MODIFY COLUMN s String SETTINGS alter_sync = 0, mutations_sync = 0"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"

wait $insert_pid

$CLICKHOUSE_CLIENT --query "
    SELECT k, s, toTypeName(s) FROM t_hints_alter ORDER BY k;
    DROP TABLE t_hints_alter;
"
