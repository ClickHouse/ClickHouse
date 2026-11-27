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

# An asynchronous insert by a user that the local policy allows from any host keeps no client host, yet the shard
# checks its own policy, which may restrict the user. Rows of such an insert must not be authorized under the host of
# rows buffered with them, in either order. The shard's stricter policy is simulated by restricting the user after the
# rows are buffered; once it is relaxed again, the retained batch is delivered.
any_user="u_05627_any_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP USER IF EXISTS ${any_user};
CREATE TABLE ${db}.mixed_dst (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE ${db}.mixed_dist (s String) ENGINE = Distributed(test_cluster_interserver_secret, ${db}, mixed_dst, 0);
CREATE TABLE ${db}.mixed_buf (s String) ENGINE = Buffer(${db}, mixed_dist, 1, 1000, 1000, 1000000, 1000000, 1000000000, 1000000000);
CREATE USER ${any_user} SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 1;
GRANT INSERT ON ${db}.* TO ${any_user};
EOF

for order in "sync async" "async sync"; do
    for kind in ${order}; do
        if [ "${kind}" = async ]; then async=1; else async=0; fi
        ${CLICKHOUSE_CLIENT} --user "${any_user}" --query "
            INSERT INTO ${db}.mixed_buf SETTINGS async_insert = ${async}, wait_for_async_insert = 1 VALUES ('${kind}')"
    done
    ${CLICKHOUSE_CLIENT} --query "ALTER USER ${any_user} HOST IP '127.0.0.1', IP '::1'"
    echo -n "${order}: not authorized under the known host: "
    if ${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.mixed_buf" 2>&1 | grep -q "retained for retry"; then echo "OK"; else echo "UNEXPECTED"; fi
    ${CLICKHOUSE_CLIENT} --query "ALTER USER ${any_user} HOST ANY"
    ${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.mixed_buf"
    ${CLICKHOUSE_CLIENT} --query "SELECT '${order}: delivered once allowed', groupArray(s) FROM (SELECT s FROM ${db}.mixed_dst ORDER BY s)"
    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE ${db}.mixed_dst"
done

# A shard's denial reached through a materialized view says nothing of the privileges of the user here, so the batch is
# retained for retry, as for a `Distributed` destination, not dropped as if the user had lost a privilege.
mv_user="u_05627_mv_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP USER IF EXISTS ${mv_user};
CREATE USER ${mv_user} SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 1;
CREATE TABLE ${db}.mv_src (s String) ENGINE = Null;
CREATE TABLE ${db}.mv_dst (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE ${db}.mv_dist (s String) ENGINE = Distributed(test_cluster_interserver_secret, ${db}, mv_dst, 0);
CREATE MATERIALIZED VIEW ${db}.mv TO ${db}.mv_dist DEFINER = ${mv_user} SQL SECURITY DEFINER AS SELECT s FROM ${db}.mv_src;
CREATE TABLE ${db}.mv_buf (s String) ENGINE = Buffer(${db}, mv_src, 1, 1000, 1000, 1000000, 1000000, 1000000000, 1000000000);
GRANT INSERT ON ${db}.mv_buf TO ${mv_user};
GRANT INSERT, SELECT ON ${db}.mv_src TO ${mv_user};
GRANT INSERT ON ${db}.mv_dist TO ${mv_user};
EOF
${CLICKHOUSE_CLIENT} --user "${mv_user}" --query "INSERT INTO ${db}.mv_buf SETTINGS async_insert = 0 VALUES ('via_view')"
echo -n "denied by the shard, retained: "
if ${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.mv_buf" 2>&1 | grep -q "retained for retry"; then echo "OK"; else echo "UNEXPECTED"; fi
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON ${db}.mv_dst TO ${mv_user}"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.mv_buf"
${CLICKHOUSE_CLIENT} --query "SELECT 'delivered once allowed', groupArray(s) FROM ${db}.mv_dst"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${db}.mv"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
${CLICKHOUSE_CLIENT} --query "DROP USER ${any_user}"
${CLICKHOUSE_CLIENT} --query "DROP USER ${mv_user}"
