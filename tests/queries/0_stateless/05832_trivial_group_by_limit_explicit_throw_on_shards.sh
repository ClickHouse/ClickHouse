#!/usr/bin/env bash
# Tags: shard
# Random settings limits: adaptive_aggregator_freeze_threshold=(16384, 16384); adaptive_aggregator_freeze_threshold_bytes=(4194304, 4194304); optimize_trivial_group_by_limit_query=(1, 1)
# A frozen adaptive aggregator counts only the groups its merge builds (see #130077), so `LIMIT` can end the query before `TOO_MANY_ROWS`.
# The user below pins `optimize_trivial_group_by_limit_query = 1 CONST`, which a randomized 0 from the client violates.
# An explicit `group_by_overflow_mode = 'throw'` disables the limit on groups of a trivial `GROUP BY ... LIMIT`
# on shards too, even if the shard enables `optimize_trivial_group_by_limit_query` itself.
# `throw` is the default value, so the shard must not lose the mark that the mode is explicit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"

function check()
{
    echo -n "$1: "
    $CLICKHOUSE_CLIENT "${@:3}" -q "$2 SETTINGS max_rows_to_group_by = 50, group_by_overflow_mode = 'throw', max_block_size = 10, max_threads = 1, enable_parallel_replicas = 0" 2>&1 \
        | awk '/TOO_MANY_ROWS/ { error = 1 } END { print error ? "TOO_MANY_ROWS" : NR }'
}

nested="view(SELECT number FROM numbers(1000) GROUP BY number LIMIT 5 SETTINGS optimize_trivial_group_by_limit_query = 1)"
check "nested override, local" "SELECT count() FROM $nested"
check "nested override, shard" "SELECT count() FROM remote('127.0.0.2', $nested)"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 10;
    INSERT INTO t SELECT number FROM numbers(1000);
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password SETTINGS optimize_trivial_group_by_limit_query = 1 CONST;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO $user;
"

check "pinned in the profile, local" "SELECT x FROM t GROUP BY x LIMIT 5" --user "$user"
check "pinned in the profile of the shard" "SELECT x FROM remote('127.0.0.2', currentDatabase(), t, '$user', '') GROUP BY x LIMIT 5"

$CLICKHOUSE_CLIENT -q "DROP USER $user; DROP TABLE t;"
