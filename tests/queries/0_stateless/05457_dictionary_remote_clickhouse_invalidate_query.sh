#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 127.0.0.2 is not a local address for the dictionary source, so the invalidate query is sent through
# RemoteQueryExecutor, like for a source on another server. Its result structure is unknown in advance.

$CLICKHOUSE_CLIENT --query "
CREATE TABLE dict_src (id UInt64, val String, upd DateTime) ENGINE = Memory;
INSERT INTO dict_src VALUES (1, 'a', '2020-01-01 00:00:00');

CREATE DICTIONARY dict_remote (id UInt64, val String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST '127.0.0.2' PORT tcpPort() USER 'default' DB currentDatabase() TABLE 'dict_src' INVALIDATE_QUERY 'SELECT max(upd) FROM dict_src'))
LAYOUT(FLAT())
LIFETIME(1);

SELECT dictGet('dict_remote', 'val', toUInt64(1));
"

function wait_until()
{
    local deadline=$((SECONDS + 60))
    while [ "$($CLICKHOUSE_CLIENT --query "$1")" != "1" ] && [ $SECONDS -lt $deadline ]
    do
        sleep 1
    done
}

DICT_INFO="FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_remote'"
# Invalidate queries seen by the "remote" server.
CHECKS="FROM system.query_log WHERE current_database = currentDatabase() AND type != 'QueryStart' AND query LIKE 'SELECT max(upd)%'"

# The first periodic check always reloads: there is no previous invalidate query response to compare with.
t0=$($CLICKHOUSE_CLIENT --query "SELECT toUnixTimestamp(last_successful_update_time) $DICT_INFO")
wait_until "SELECT toUnixTimestamp(last_successful_update_time) > $t0 $DICT_INFO"

# A new row that does not change the invalidate query result.
$CLICKHOUSE_CLIENT --query "INSERT INTO dict_src VALUES (2, 'b', '2019-01-01 00:00:00')"
c0=$($CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log; SELECT count() $CHECKS")
wait_until "SYSTEM FLUSH LOGS query_log; SELECT count() >= $c0 + 2 $CHECKS"
# Two checks have passed since the insert, the dictionary must not have been reloaded.
$CLICKHOUSE_CLIENT --query "SELECT dictHas('dict_remote', toUInt64(2))"

# A new row that changes the invalidate query result.
$CLICKHOUSE_CLIENT --query "INSERT INTO dict_src VALUES (3, 'c', '2021-01-01 00:00:00')"
wait_until "SELECT dictHas('dict_remote', toUInt64(3))"
$CLICKHOUSE_CLIENT --query "SELECT dictGet('dict_remote', 'val', toUInt64(2)), dictGet('dict_remote', 'val', toUInt64(3))"

$CLICKHOUSE_CLIENT --query "SELECT last_exception $DICT_INFO"
