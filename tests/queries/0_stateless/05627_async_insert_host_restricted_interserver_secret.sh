#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the interserver secret requires SSL

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="u_05627_${CLICKHOUSE_DATABASE}"

# A shard of a `Distributed` over an interserver secret re-authenticates the initial user against the initial address.
# The user may connect only from the loopback, so the insert must reach the shard with the client's address.
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP USER IF EXISTS ${user};
CREATE TABLE ${db}.dst (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE ${db}.dist (s String) ENGINE = Distributed(test_cluster_interserver_secret, ${db}, dst, 0);
CREATE TABLE ${db}.buf (s String) ENGINE = Buffer(${db}, dist, 1, 1000, 1000, 1000000, 1000000, 1000000000, 1000000000);
CREATE USER ${user} HOST IP '127.0.0.1', IP '::1' SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 1;
GRANT INSERT ON ${db}.* TO ${user};
EOF

# An asynchronous insert is flushed in a context of its own, which must keep the client's address.
${CLICKHOUSE_CLIENT} --user "${user}" --query "
    INSERT INTO ${db}.dist
    SETTINGS async_insert = 1, wait_for_async_insert = 1, prefer_localhost_replica = 0, distributed_foreground_insert = 1
    VALUES ('direct')"

# The same through a `Buffer`, whose rows are flushed later under the identity captured from that context.
${CLICKHOUSE_CLIENT} --user "${user}" --query "
    INSERT INTO ${db}.buf SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES ('buffered')"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.buf"

${CLICKHOUSE_CLIENT} --query "SELECT s FROM ${db}.dst ORDER BY s"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
