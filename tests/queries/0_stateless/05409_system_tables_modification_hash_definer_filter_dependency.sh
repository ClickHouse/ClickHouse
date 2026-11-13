#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the replicas of `cluster_for_parallel_replicas` in that configuration are not local, so the `MergeTree` modification hash is `NULL` by design.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# An `additional_table_filters` entry keyed by a `SQL SECURITY DEFINER` materialized view in the profile
# of its definer is never applied: the entries keyed by the view are taken from the settings of the query
# that reads it. So the `modification_hash` of the view does not cover the tables read by such a filter,
# and a caller who can read the view and its whole target must not observe, through `system.tables`, the
# churn of a table it cannot read: the hash stays the same when that table changes.

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

function get_hash()
{
    $CLICKHOUSE_CLIENT --user "${user}" -q "
        SELECT ifNull(toString(modification_hash), 'NULL') FROM system.tables
        WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'mv_05409'"
}

no_filter=$(get_hash)
[ "${no_filter}" != 'NULL' ] && echo "no filter: not NULL" || echo "no filter: NULL"

$CLICKHOUSE_CLIENT -q "
    CREATE SETTINGS PROFILE ${profile}
        SETTINGS additional_table_filters = '{\\'${CLICKHOUSE_DATABASE}.mv_05409\\': \\'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.private_05409)\\'}'
        TO ${definer}"
with_filter=$(get_hash)
[ "${with_filter}" = "${no_filter}" ] && echo "definer filter reading a private table keeps the hash: yes" || echo "definer filter reading a private table keeps the hash: no"

$CLICKHOUSE_CLIENT -q "INSERT INTO private_05409 VALUES (2, 0)"
after_private_insert=$(get_hash)
[ "${after_private_insert}" = "${no_filter}" ] && echo "a change of the private table keeps the hash: yes" || echo "a change of the private table keeps the hash: no"

$CLICKHOUSE_CLIENT -q "DROP TABLE mv_05409 SYNC; DROP SETTINGS PROFILE ${profile}; DROP USER ${user}, ${definer}"
