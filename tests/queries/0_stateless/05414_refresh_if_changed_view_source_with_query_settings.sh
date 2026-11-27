#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# The sources of a `REFRESH ... IF CHANGED` refresh must be hashed under the settings the refresh reads
# them with, including the `SETTINGS` of a stored query: the refresh's own, and a source view's. Otherwise
# the hash of a source view moves between the check and the read, or with a profile value that the view's
# stored query overrides, and an `APPEND` view appends a duplicate copy of unchanged rows.
definer="definer_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer} SETTINGS max_rows_to_read_leaf = 1000;
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${definer};
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    CREATE VIEW v_invoker AS SELECT x FROM src;
    CREATE VIEW v_definer DEFINER = ${definer} SQL SECURITY DEFINER
        AS SELECT x FROM src SETTINGS max_rows_to_read_leaf = 100000;
    -- APPEND mode: every refresh that actually runs appends one row.
    CREATE MATERIALIZED VIEW mv_invoker REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt
        AS SELECT count() AS cnt FROM v_invoker SETTINGS max_rows_to_read_leaf = 100000;
    CREATE MATERIALIZED VIEW mv_definer REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt
        AS SELECT count() AS cnt FROM v_definer;
"

wait_for_rows()
{
    local table=$1
    local n=0
    for _ in {1..60}
    do
        n=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${table}")
        [ "$n" -ge "$2" ] && break
        sleep 0.5
    done
    echo "$n"
}

# The first refresh always runs: there is no previous state to compare to.
initial_invoker=$(wait_for_rows mv_invoker 1)
initial_definer=$(wait_for_rows mv_definer 1)

# A setting of the definer that its view's stored query overrides does not change a row.
$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS max_rows_to_read_leaf = 2000"

# Let several scheduled refreshes run against the unchanged source.
sleep 3

after_invoker=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv_invoker")
after_definer=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv_definer")
[ "$initial_invoker" = "1" ] && [ "$after_invoker" = "1" ] && echo "refresh query settings over an invoker view keep the watermark: yes" || echo "refresh query settings over an invoker view keep the watermark: no ($initial_invoker -> $after_invoker)"
[ "$initial_definer" = "1" ] && [ "$after_definer" = "1" ] && echo "overridden definer setting of a source view keeps the watermark: yes" || echo "overridden definer setting of a source view keeps the watermark: no ($initial_definer -> $after_definer)"

# A real change of the source still triggers both refreshes.
$CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (2)"
[ "$(wait_for_rows mv_invoker 2)" -ge 2 ] && echo "changed source triggers refresh through an invoker view: yes"
[ "$(wait_for_rows mv_definer 2)" -ge 2 ] && echo "changed source triggers refresh through a definer view: yes"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_invoker SYNC;
    DROP TABLE mv_definer SYNC;
    DROP TABLE v_invoker SYNC;
    DROP TABLE v_definer SYNC;
    DROP TABLE src SYNC;
    DROP USER ${definer};
"
