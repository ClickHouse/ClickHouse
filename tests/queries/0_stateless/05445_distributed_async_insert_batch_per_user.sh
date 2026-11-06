#!/usr/bin/env bash
# Tags: distributed, no-fasttest
# Tag no-fasttest: the interserver `<secret>` requires SSL, which the fast test build does not have.

# Queued `Distributed` inserts of different users must not be sent in one batch: the batch is sent with
# the `ClientInfo` of its first file, and over a cluster with an interserver `<secret>` the shard
# authenticates the whole batch as that file's `initial_user`. Before the fix, rows of a user without
# the remote `INSERT` grant were delivered under the identity of a user that had it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Flushing the rejected file (also when the table is dropped) logs the error on the server.
CLICKHOUSE_CLIENT=$(echo "$CLICKHOUSE_CLIENT" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL:-none}/--send_logs_level=none/g")

WRITER="writer_${CLICKHOUSE_DATABASE}"
TENANT="tenant_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
    DROP USER IF EXISTS ${WRITER}, ${TENANT};
    CREATE TABLE target (key Int) ENGINE = MergeTree ORDER BY key;
    CREATE TABLE dist (key Int) ENGINE = Distributed(test_cluster_interserver_secret, currentDatabase(), target, 0)
        SETTINGS background_insert_batch = 1;
    SYSTEM STOP DISTRIBUTED SENDS dist;
    CREATE USER ${WRITER} IDENTIFIED WITH no_password;
    CREATE USER ${TENANT} IDENTIFIED WITH no_password;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.dist TO ${WRITER}, ${TENANT};
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.target TO ${WRITER};
"

${CLICKHOUSE_CLIENT} --user "${WRITER}" --distributed_foreground_insert 0 --prefer_localhost_replica 0 -q "INSERT INTO dist VALUES (1)"
${CLICKHOUSE_CLIENT} --user "${TENANT}" --distributed_foreground_insert 0 --prefer_localhost_replica 0 -q "INSERT INTO dist VALUES (2)"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH DISTRIBUTED dist" 2>&1 | grep -o -m1 "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} -q "SELECT key FROM target WHERE key = 2"

${CLICKHOUSE_CLIENT} -m -q "
    DROP TABLE dist;
    DROP USER ${WRITER}, ${TENANT};
"
