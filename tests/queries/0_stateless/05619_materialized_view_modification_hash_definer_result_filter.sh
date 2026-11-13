#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the replicas of `cluster_for_parallel_replicas` in that configuration are not local, so the `MergeTree` modification hash is `NULL` by design.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# A materialized view pushes its read into the target table without a planner of its own, so the
# `additional_result_filter` of its definer is never applied to the read. A change of that filter must
# neither move `modification_hash` of the view (a `REFRESH ... IF CHANGED APPEND` reading the view would
# append the unchanged rows again) nor make it `NULL`.

# Users are server-wide, so make the name unique per run: concurrent runs (e.g. the flaky check) must
# not create, alter or drop each other's user.
definer="u_05619_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer};
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${definer};

    CREATE TABLE src_05619 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE tgt_05619 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW mv_05619 TO tgt_05619 DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src_05619;
    INSERT INTO tgt_05619 VALUES (1);
"

hash_of_view()
{
    $CLICKHOUSE_CLIENT -q "SELECT toString(modification_hash) FROM system.tables WHERE database = currentDatabase() AND name = 'mv_05619'"
}

before=$(hash_of_view)
[ -n "${before}" ] && echo 1

$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS additional_result_filter = 'x > 1'"

after=$(hash_of_view)
echo "$([ -n "${after}" ] && echo 1 || echo 0)	$([ "${before}" = "${after}" ] && echo 1 || echo 0)"

# The filter of the definer does not apply to the read.
$CLICKHOUSE_CLIENT -q "SELECT x FROM mv_05619"

# The view first: a user cannot be dropped while it is the definer of one.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_05619;
    DROP TABLE tgt_05619;
    DROP TABLE src_05619;
    DROP USER ${definer};
"
