#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database, no-parallel-replicas
# no-replicated-database: refreshable MVs with non-replicated inner tables are refused on a Replicated database.
# no-parallel-replicas: the replicas of `cluster_for_parallel_replicas` in that configuration are not local, so the `MergeTree` modification hash is `NULL` by design.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# An `additional_table_filters` entry keyed by a view or a materialized view is applied by the planner of
# the query that reads it, from that query's own settings. When the refresh reads a `SQL SECURITY DEFINER`
# view or materialized view defined by another user, an entry keyed by that view in the profile of its
# definer is never applied: the read returns the same rows. Such an entry must not discard the
# `REFRESH ... IF CHANGED` watermark, otherwise these `APPEND` views would append unchanged rows again.

refresher="refresher_05498_${CLICKHOUSE_DATABASE}"
view_definer="view_definer_05498_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${refresher};
    CREATE USER ${view_definer};
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${refresher};
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${view_definer};

    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE mv_src_target (x UInt64) ENGINE = MergeTree ORDER BY x;
    -- A merge changes the active part set, which moves the modification hash and makes an extra
    -- refresh run. That is correct behavior, but it makes the row counts below unpredictable.
    SYSTEM STOP MERGES src;
    SYSTEM STOP MERGES mv_src_target;
    INSERT INTO src VALUES (1);
    INSERT INTO mv_src_target VALUES (1);

    CREATE VIEW v_definer DEFINER = ${view_definer} SQL SECURITY DEFINER AS SELECT x FROM src;
    CREATE MATERIALIZED VIEW mv_src TO mv_src_target DEFINER = ${view_definer} SQL SECURITY DEFINER AS SELECT x FROM src;

    -- APPEND mode: every refresh that actually runs appends one row.
    CREATE MATERIALIZED VIEW mv_through_definer_view REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt
        DEFINER = ${refresher} SQL SECURITY DEFINER AS SELECT count() AS cnt FROM v_definer;
    CREATE MATERIALIZED VIEW mv_through_mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt
        DEFINER = ${refresher} SQL SECURITY DEFINER AS SELECT count() AS cnt FROM mv_src;
"

counts()
{
    $CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM mv_through_definer_view) || ' ' || (SELECT count() FROM mv_through_mv)"
}

# The first refresh always runs: there is no previous state to compare to.
for _ in {1..60}
do
    read -r through_definer_view through_mv <<< "$(counts)"
    [ "$through_definer_view" -gt 0 ] && [ "$through_mv" -gt 0 ] && break
    sleep 0.5
done
initial=$(counts)

# Entries of the definer of both source views keyed by the views themselves, by bare and by qualified
# name. The refreshes read the views under the settings of the refreshing user, so none of them applies.
# (A user profile takes the map as a string.)
$CLICKHOUSE_CLIENT -q "ALTER USER ${view_definer} SETTINGS additional_table_filters = '{\\'v_definer\\': \\'x > 0\\', \\'${CLICKHOUSE_DATABASE}.v_definer\\': \\'x > 0\\', \\'mv_src\\': \\'x > 0\\', \\'${CLICKHOUSE_DATABASE}.mv_src\\': \\'x > 0\\'}'"

# Several refresh periods pass: a refresh that ran would have appended a row.
sleep 3
after=$(counts)
[ "$initial" = "1 1" ] && [ "$after" = "1 1" ] && echo "self-keyed filters of the view definer keep the watermark: yes" || echo "self-keyed filters of the view definer keep the watermark: no ($initial -> $after)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_through_definer_view SYNC;
    DROP TABLE mv_through_mv SYNC;
    DROP TABLE mv_src SYNC;
    DROP TABLE v_definer SYNC;
    DROP TABLE mv_src_target SYNC;
    DROP TABLE src SYNC;
    DROP USER ${refresher};
    DROP USER ${view_definer};
"
