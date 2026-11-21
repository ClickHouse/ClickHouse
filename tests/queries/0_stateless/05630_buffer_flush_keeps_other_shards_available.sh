#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: the failpoint is server-wide.
# no-fasttest: failpoints are not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user_a="ua_05630_${CLICKHOUSE_DATABASE}"
user_b="ub_05630_${CLICKHOUSE_DATABASE}"
fp=distributed_sink_pause_before_push

# Rows of different identities are buffered in different shards. While an insert flushes its own shard, which can
# take a whole destination write, the other shard must stay available to its writers: here the flush is held in the
# write of a foreground `Distributed` destination, and an insert into the other shard must not wait for it.
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP USER IF EXISTS ${user_a};
DROP USER IF EXISTS ${user_b};
CREATE TABLE ${db}.dst (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE ${db}.dist (s String) ENGINE = Distributed(test_shard_localhost, ${db}, dst);
CREATE TABLE ${db}.buf (s String) ENGINE = Buffer(${db}, dist, 2, 3600, 7200, 1, 2, 100000000, 200000000);
CREATE USER ${user_a} SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 1;
CREATE USER ${user_b} SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 1;
GRANT INSERT ON ${db}.* TO ${user_a}, ${user_b};
EOF
${CLICKHOUSE_CLIENT} --user "${user_a}" --query "INSERT INTO ${db}.buf SETTINGS async_insert = 0 VALUES ('a1'), ('a2')"
${CLICKHOUSE_CLIENT} --user "${user_b}" --query "INSERT INTO ${db}.buf SETTINGS async_insert = 0 VALUES ('b1')"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${fp}"
# Exceeds max_rows of the shard of user_a, which is flushed first and held in the destination write.
${CLICKHOUSE_CLIENT} --user "${user_a}" --query "INSERT INTO ${db}.buf SETTINGS async_insert = 0 VALUES ('a3')" &
insert_a_pid=$!
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${fp} PAUSE"
# Fits into the shard of user_b without a flush.
echo -n "insert into the other shard does not wait for the flush: "
if timeout 60 ${CLICKHOUSE_CLIENT} --user "${user_b}" --query "INSERT INTO ${db}.buf SETTINGS async_insert = 0 VALUES ('b2')"; then
    echo "OK"
else
    echo "UNEXPECTED"
fi
${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT ${fp}"
wait "${insert_a_pid}"

${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.buf"
${CLICKHOUSE_CLIENT} --query "SELECT groupArray(s) FROM (SELECT s FROM ${db}.dst ORDER BY s)"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${fp}"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user_a}"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user_b}"
