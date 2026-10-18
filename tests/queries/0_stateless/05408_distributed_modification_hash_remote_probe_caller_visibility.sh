#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

# `system.tables.modification_hash` of a `Distributed` table whose shard is remote is probed with the
# cluster connection's credentials. A caller that may read only the wrapper must not observe the churn
# of the remote table's other columns through it, so the probe fails closed unless the shard runs it as
# the caller.

user="u_05408_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${user};
    DROP TABLE IF EXISTS d_05408;
    DROP TABLE IF EXISTS t_05408;
    CREATE TABLE t_05408 (x UInt64, secret UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_05408 VALUES (1, 0);
    CREATE TABLE d_05408 (x UInt64) ENGINE = Distributed(test_cluster_one_shard_remote, currentDatabase(), t_05408);
    CREATE USER ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.d_05408 TO ${user};
"

query="SELECT modification_hash IS NULL FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'd_05408'"

# The cluster's configured user is the caller: the shard checks the caller's own visibility.
echo "same user: $($CLICKHOUSE_CLIENT -q "${query}")"
# Another user with access to the wrapper only: fail closed.
echo "restricted user: $($CLICKHOUSE_CLIENT --user "${user}" -q "${query}")"

$CLICKHOUSE_CLIENT -q "
    DROP USER ${user};
    DROP TABLE d_05408;
    DROP TABLE t_05408;
"
