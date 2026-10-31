#!/usr/bin/env bash

# The `modification_hash` of a `SQL SECURITY DEFINER` materialized view in `system.tables` is `NULL`
# for a caller who cannot read every column of its target. When the target is itself a materialized
# view (`outer_mv TO inner_mv`), the same check for the inner view must still run against the caller,
# not against the outer view's definer: otherwise the outer hash would reveal changes to the inner
# target's columns the caller cannot read, while the inner hash is `NULL`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# Users are server-wide, so make the names unique per run.
user="user_05402_${CLICKHOUSE_DATABASE}"
definer="definer_05402_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src_05402 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE target_05402 (x UInt64, secret UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE USER ${definer};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.src_05402 TO ${definer};
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.target_05402 TO ${definer};
    CREATE MATERIALIZED VIEW inner_mv_05402 TO target_05402 (x UInt64)
        DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src_05402;
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.inner_mv_05402 TO ${definer};
    CREATE MATERIALIZED VIEW outer_mv_05402 TO inner_mv_05402 (x UInt64)
        DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src_05402;
    INSERT INTO src_05402 VALUES (1);
    CREATE USER ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.outer_mv_05402 TO ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.inner_mv_05402 TO ${user};
    GRANT SELECT(x) ON ${CLICKHOUSE_DATABASE}.target_05402 TO ${user};
"

function show_hash()
{
    $CLICKHOUSE_CLIENT --user "${user}" -q "
        SELECT '$1', name, isNotNull(modification_hash) FROM system.tables
        WHERE database = '${CLICKHOUSE_DATABASE}' AND name IN ('inner_mv_05402', 'outer_mv_05402')
        ORDER BY name"
}

show_hash 'exposed target column'

$CLICKHOUSE_CLIENT -q "GRANT SELECT(secret) ON ${CLICKHOUSE_DATABASE}.target_05402 TO ${user}"
show_hash 'every target column'

$CLICKHOUSE_CLIENT -q "DROP TABLE outer_mv_05402; DROP TABLE inner_mv_05402; DROP USER ${user}, ${definer}"
