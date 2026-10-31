#!/usr/bin/env bash

# The `Too many parts` checks of the tables behind a forwarding sink (`Buffer`, `Alias`) are prepared while
# the pipeline is being built, which resolves the `SQL SECURITY` definers of the materialized views behind it.
# A definer that cannot be resolved must not fail the INSERT earlier than the nested INSERT would: a `Buffer`
# that only keeps the rows in memory does not touch the view at all, and the nested INSERT behind an `Alias`
# skips the broken view under `materialized_views_ignore_errors`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05473_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS $user, ${user}_gone;
    CREATE USER $user IDENTIFIED WITH no_password;
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO $user;

    CREATE TABLE t_05473_dst (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE t_05473_hidden (n UInt64) ENGINE = Null;
    CREATE TABLE t_05473_buffer (n UInt64) ENGINE = Buffer(currentDatabase(), t_05473_hidden, 1, 1000, 1000, 1000000, 1000000, 1000000000, 1000000000);
    CREATE TABLE t_05473_alias ENGINE = Alias('t_05473_hidden');

    CREATE MATERIALIZED VIEW t_05473_mv TO t_05473_dst DEFINER = $user SQL SECURITY DEFINER
        AS SELECT n FROM t_05473_hidden;

    -- The stored definer of the view can no longer be resolved.
    ALTER USER $user RENAME TO ${user}_gone;
"

# The rows stay in the memory of the buffer: the view is not touched.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_05473_buffer VALUES (1)"
$CLICKHOUSE_CLIENT -q "SELECT 'buffered', count() FROM t_05473_buffer"

# The nested INSERT behind the alias skips the broken view.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "INSERT INTO t_05473_alias SETTINGS materialized_views_ignore_errors = 1 VALUES (2)"
$CLICKHOUSE_CLIENT -q "SELECT 'written by the view', count() FROM t_05473_dst"

# Without `materialized_views_ignore_errors` the INSERT still fails.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_05473_alias VALUES (3)" 2>&1 | grep -o -m1 'UNKNOWN_USER'

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_05473_mv SYNC;
    DROP TABLE t_05473_buffer;
    DROP TABLE t_05473_alias;
    DROP TABLE t_05473_hidden;
    DROP TABLE t_05473_dst;
    DROP USER IF EXISTS $user, ${user}_gone;
"
