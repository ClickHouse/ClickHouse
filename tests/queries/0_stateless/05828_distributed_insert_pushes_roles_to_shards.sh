#!/usr/bin/env bash
# Tags: distributed, no-fasttest
# Tag no-fasttest: the interserver `<secret>` requires SSL, which the fast test build does not have.

# An INSERT into a Distributed table over a cluster with an interserver `<secret>` passes the roles granted to the
# user to the shards, so a shard authorizes it with a role that is not a default role there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Flushing a rejected file (also when the table is dropped) logs the error on the server.
CLICKHOUSE_CLIENT=$(echo "$CLICKHOUSE_CLIENT" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL:-none}/--send_logs_level=none/g")

USER_NAME="u_${CLICKHOUSE_DATABASE}"
ROLE_NAME="r_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
    DROP USER IF EXISTS ${USER_NAME};
    DROP ROLE IF EXISTS ${ROLE_NAME};
    CREATE TABLE target (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE dist (x UInt32) ENGINE = Distributed(test_cluster_interserver_secret, currentDatabase(), target, x);
    CREATE TABLE dist_batch (x UInt32) ENGINE = Distributed(test_cluster_interserver_secret, currentDatabase(), target, x)
        SETTINGS background_insert_batch = 1;
    SYSTEM STOP DISTRIBUTED SENDS dist;
    SYSTEM STOP DISTRIBUTED SENDS dist_batch;
    CREATE ROLE ${ROLE_NAME};
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.dist TO ${ROLE_NAME};
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.dist_batch TO ${ROLE_NAME};
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.target TO ${ROLE_NAME};
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    GRANT ${ROLE_NAME} TO ${USER_NAME};
    ALTER USER ${USER_NAME} DEFAULT ROLE NONE;
"

function insert_as_user()
{
    ${CLICKHOUSE_CLIENT} --user "${USER_NAME}" -m -q "SET ROLE ${ROLE_NAME}; INSERT INTO $1 SETTINGS prefer_localhost_replica = 0, $2"
}

echo "-- sync"
insert_as_user dist "distributed_foreground_insert = 1 VALUES (1), (2)"

echo "-- sync, push_external_roles_in_interserver_queries = 0"
insert_as_user dist "distributed_foreground_insert = 1, push_external_roles_in_interserver_queries = 0 VALUES (100)" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "-- async"
insert_as_user dist "distributed_foreground_insert = 0 VALUES (3), (4)"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH DISTRIBUTED dist"

echo "-- async, batch"
insert_as_user dist_batch "distributed_foreground_insert = 0 VALUES (5)"
insert_as_user dist_batch "distributed_foreground_insert = 0 VALUES (7)"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH DISTRIBUTED dist_batch"

echo "-- async, push_external_roles_in_interserver_queries = 0"
insert_as_user dist "distributed_foreground_insert = 0, push_external_roles_in_interserver_queries = 0 VALUES (200)"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH DISTRIBUTED dist" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "-- target"
${CLICKHOUSE_CLIENT} -q "SELECT x FROM target ORDER BY x"

${CLICKHOUSE_CLIENT} -m -q "
    DROP TABLE dist;
    DROP TABLE dist_batch;
    DROP USER ${USER_NAME};
    DROP ROLE ${ROLE_NAME};
"
