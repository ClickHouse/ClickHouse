#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# The `modification_hash` of a `SQL SECURITY DEFINER` materialized view also covers the tables read by
# the definer's `additional_table_filters` keyed by the view. In `system.tables` it must be `NULL` when
# such a filter applies: its dependencies are checked against the definer's grants, so a caller who
# can read the view and its whole target would otherwise observe the churn of a table it cannot read.

user="user_05409_${CLICKHOUSE_DATABASE}"
definer="definer_05409_${CLICKHOUSE_DATABASE}"
profile="profile_05409_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src_05409 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE target_05409 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE private_05409 (x UInt64, secret UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO private_05409 VALUES (1, 0);
    CREATE USER ${definer};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.src_05409 TO ${definer};
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.target_05409 TO ${definer};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.private_05409 TO ${definer};
    CREATE MATERIALIZED VIEW mv_05409 TO target_05409 (x UInt64)
        DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src_05409;
    CREATE USER ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.mv_05409 TO ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.target_05409 TO ${user};
"

function show_hash()
{
    $CLICKHOUSE_CLIENT --user "${user}" -q "
        SELECT '$1', isNotNull(modification_hash) FROM system.tables
        WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'mv_05409'"
}

show_hash 'no filter'

$CLICKHOUSE_CLIENT -q "
    CREATE SETTINGS PROFILE ${profile}
        SETTINGS additional_table_filters = '{\\'${CLICKHOUSE_DATABASE}.mv_05409\\': \\'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.private_05409)\\'}'
        TO ${definer}"
show_hash 'definer filter reading a private table'

$CLICKHOUSE_CLIENT -q "DROP TABLE mv_05409 SYNC; DROP SETTINGS PROFILE ${profile}; DROP USER ${user}, ${definer}"
