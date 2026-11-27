#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-parallel, no-fasttest
# Tag zookeeper: the page under test reports on replicated tables.
# Tag no-shared-merge-tree: the page walks ReplicatedMergeTree tables only; the private CI rewrites the engine.
# Tag no-parallel: the failpoint is server-wide and changes the page for every concurrent request.
# Tag no-fasttest: tests that run alone are kept out of the fast test.

# `/replicas_status` checks that a replica is not read-only and then asks it for its delays, which
# checks the same again and throws. A replica shut down in between (a concurrent `DROP TABLE`,
# `DETACH TABLE`, `SYSTEM RESTART REPLICA`) used to answer HTTP 500 for the whole server. The
# failpoint stands for that shutdown: the replica has to be reported as read-only.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

STATUS_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/replicas_status"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_live (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/live', 'r1') ORDER BY a"

# In range: the replica is not read-only, so the page does ask it for its delays.
${CLICKHOUSE_CLIENT} -q "SELECT table, is_readonly FROM system.replicas WHERE database = currentDatabase()"

# Only the line of this test's own table and the status of that same response are looked at.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT replicas_status_replica_turns_readonly"
response=$(${CLICKHOUSE_CURL} -sS -w '\n%{http_code}' "${STATUS_URL}?verbose=1")
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT replicas_status_replica_turns_readonly"

status=''
if echo "$response" | grep -q "^${CLICKHOUSE_DATABASE}\.t_live:.*is readonly"
then
    status=$(echo "$response" | tail -n 1)
fi

echo "status of the page reporting the replica as read-only: ${status}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_live SYNC"
