#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: the failpoint is server-wide.
# no-fasttest: failpoints and the interserver secret are not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="u_05631_${CLICKHOUSE_DATABASE}"

# A row of unknown origin (an asynchronous insert by a user the local policy allows from any host) joining a row from
# a known host makes the batch's origin unknown. If that row fails to be appended, the earlier row must still be
# flushed under its own host, which the shard of a cluster-secret `Distributed` checks. The shard's stricter policy is
# simulated by restricting the user after the rows are buffered.
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP USER IF EXISTS ${user};
CREATE TABLE ${db}.dst (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE ${db}.dist (s String) ENGINE = Distributed(test_cluster_interserver_secret, ${db}, dst, 0);
CREATE TABLE ${db}.buf (s String) ENGINE = Buffer(${db}, dist, 1, 3600, 7200, 1000000, 2000000, 100000000, 200000000);
CREATE USER ${user} SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 1;
GRANT INSERT ON ${db}.* TO ${user};
EOF
${CLICKHOUSE_CLIENT} --user "${user}" --query "INSERT INTO ${db}.buf SETTINGS async_insert = 0 VALUES ('known')"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT storage_buffer_fail_appending_block"
echo -n "append rejected: "
${CLICKHOUSE_CLIENT} --user "${user}" --query "
    INSERT INTO ${db}.buf SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES ('unknown')" 2>&1 | grep -o -m1 FAULT_INJECTED
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT storage_buffer_fail_appending_block"

${CLICKHOUSE_CLIENT} --query "ALTER USER ${user} HOST IP '127.0.0.1', IP '::1'"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.buf"
${CLICKHOUSE_CLIENT} --query "SELECT groupArray(s) FROM ${db}.dst"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
