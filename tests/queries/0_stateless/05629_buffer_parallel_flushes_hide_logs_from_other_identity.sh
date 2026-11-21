#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: the failpoint is server-wide.
# no-fasttest: failpoints and the interserver secret are not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=warning
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user1="u1_05629_${CLICKHOUSE_DATABASE}"
user2="u2_05629_${CLICKHOUSE_DATABASE}"
fp=storage_buffer_pause_flush_for_other_identity

# The flushes of two Buffer shards run in parallel in the thread group of the query that triggers them. A flush for
# another identity must not show that query's client anything, including from the threads the flush starts: here the
# workers of a foreground `Distributed` insert. One flush is held until the other has finished, so the end of one
# flush must not expose the other.
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP USER IF EXISTS ${user1};
DROP USER IF EXISTS ${user2};
CREATE TABLE ${db}.dst (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE ${db}.dist (s String) ENGINE = Distributed(test_cluster_interserver_secret, ${db}, dst, 0);
CREATE TABLE ${db}.buf (s String) ENGINE = Buffer(${db}, dist, 2, 3600, 7200, 1000000, 2000000, 100000000, 200000000);
CREATE USER ${user1} SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 1;
CREATE USER ${user2} SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 1;
GRANT INSERT ON ${db}.* TO ${user1}, ${user2};
EOF
# The rows of different identities are buffered in different shards.
${CLICKHOUSE_CLIENT} --user "${user1}" --query "INSERT INTO ${db}.buf SETTINGS async_insert = 0 VALUES ('one')"
${CLICKHOUSE_CLIENT} --user "${user2}" --query "INSERT INTO ${db}.buf SETTINGS async_insert = 0 VALUES ('two')"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${fp}"
trace_client="${CLICKHOUSE_CLIENT/--send_logs_level=warning/--send_logs_level=trace}"
optimize_log="${CLICKHOUSE_TMP}/05629_optimize_${CLICKHOUSE_DATABASE}.log"
${trace_client} --query "OPTIMIZE TABLE ${db}.buf" > "${optimize_log}" 2>&1 &
optimize_pid=$!
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${fp} PAUSE"
# Let the other flush finish while this one is held.
for _ in {1..600}; do
    [ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db}.dst")" -ge 1 ] && break
    sleep 0.1
done
${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT ${fp}"
wait "${optimize_pid}"

echo -n "flushes hidden from the triggering client: "
if ! grep -qE "Connection \(|${user1}|${user2}" "${optimize_log}"; then echo "OK"; else echo "UNEXPECTED"; fi
${CLICKHOUSE_CLIENT} --query "SELECT groupArray(s) FROM (SELECT s FROM ${db}.dst ORDER BY s)"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${fp}"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user1}"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user2}"
rm -f "${optimize_log}"
