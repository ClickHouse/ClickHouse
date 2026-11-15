#!/usr/bin/env bash
# Tags: no-fasttest
# A query scoped with SET ROLE must have that scope honored on remote nodes of a cluster without an interserver secret.
# There the remote node runs the query as its own cluster user and adds the row policies of the initial user,
# which must be the policies of the roles the initiator switched to, not of all the default roles of the user.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="user_${CLICKHOUSE_DATABASE}"
ROLE_1="role_1_${CLICKHOUSE_DATABASE}"
ROLE_2="role_2_${CLICKHOUSE_DATABASE}"
ROLE_3="role_3_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS data_dist;
DROP TABLE IF EXISTS data_dist_dist;
DROP TABLE IF EXISTS data_dist_secret;
CREATE TABLE data (id UInt32, key UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO data VALUES (1, 1), (2, 2), (3, 1), (4, 3), (5, 2), (6, 1), (7, 4), (8, 2);
CREATE TABLE data_dist AS data ENGINE = Distributed(test_cluster_two_shards_localhost, ${CLICKHOUSE_DATABASE}, data);
CREATE TABLE data_dist_dist AS data ENGINE = Distributed(test_cluster_two_shards_localhost, ${CLICKHOUSE_DATABASE}, data_dist);
CREATE TABLE data_dist_secret AS data ENGINE = Distributed(test_cluster_interserver_secret, ${CLICKHOUSE_DATABASE}, data);

DROP ROLE IF EXISTS ${ROLE_1}, ${ROLE_2}, ${ROLE_3};
CREATE ROLE ${ROLE_1};
CREATE ROLE ${ROLE_2};
CREATE ROLE ${ROLE_3};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${ROLE_1}, ${ROLE_2};
CREATE ROW POLICY p_1 ON ${CLICKHOUSE_DATABASE}.data FOR SELECT USING key = 1 TO ${ROLE_1};
CREATE ROW POLICY p_2 ON ${CLICKHOUSE_DATABASE}.data FOR SELECT USING key = 2 TO ${ROLE_2};
CREATE ROW POLICY p_3 ON ${CLICKHOUSE_DATABASE}.data FOR SELECT USING key = 3 TO ${ROLE_3};
DROP USER IF EXISTS ${USER};
CREATE USER ${USER} IDENTIFIED WITH no_password;
GRANT ${ROLE_1}, ${ROLE_2} TO ${USER};
GRANT REMOTE, CREATE TEMPORARY TABLE ON *.* TO ${USER};
ALTER USER ${USER} DEFAULT ROLE ALL;
"

SETTINGS="SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0"

function query_with_role()
{
    local role=$1
    local table=$2
    $CLICKHOUSE_CLIENT --user "${USER}" -m -q "
    SET ROLE ${role};
    SELECT key, count() FROM ${table} GROUP BY key ORDER BY key ${SETTINGS};
    "
}

function role_name()
{
    echo "${1#role_}" | sed "s/_${CLICKHOUSE_DATABASE}//"
}

# Each shard is read remotely, so every row of the matching key is returned twice.
for role in "${ROLE_1}" "${ROLE_2}"
do
    echo "-- $(role_name "${role}")"
    query_with_role "${role}" data_dist
done

# Each shard of the outer table is a node that runs a query as the cluster user and reads both shards again.
for role in "${ROLE_1}" "${ROLE_2}"
do
    echo "-- two hops, $(role_name "${role}")"
    query_with_role "${role}" data_dist_dist
done

# The remote nodes authenticate as the user that runs the initial query.
for role in "${ROLE_1}" "${ROLE_2}"
do
    echo "-- same user, $(role_name "${role}")"
    query_with_role "${role}" "remote('127.0.0.{1,2}', ${CLICKHOUSE_DATABASE}, data, '${USER}', '')"
done

# The same, and the remote nodes read both shards again as the cluster user.
for role in "${ROLE_1}" "${ROLE_2}"
do
    echo "-- same user, two hops, $(role_name "${role}")"
    query_with_role "${role}" "remote('127.0.0.{1,2}', ${CLICKHOUSE_DATABASE}, data_dist, '${USER}', '')"
done

# The same, and the remote nodes read both shards again over a connection with an interserver secret,
# where the next node enables the forwarded roles.
for role in "${ROLE_1}" "${ROLE_2}"
do
    echo "-- same user, two hops with a secret, $(role_name "${role}")"
    query_with_role "${role}" "remote('127.0.0.{1,2}', ${CLICKHOUSE_DATABASE}, data_dist_secret, '${USER}', '')"
done

# The row policies of the roles change when another role is granted to a role.
echo "-- role granted to a role"
query_with_role "${ROLE_1}" data_dist
$CLICKHOUSE_CLIENT -q "GRANT ${ROLE_3} TO ${ROLE_1}"
query_with_role "${ROLE_1}" data_dist
$CLICKHOUSE_CLIENT -q "REVOKE ${ROLE_3} FROM ${ROLE_1}"
query_with_role "${ROLE_1}" data_dist

$CLICKHOUSE_CLIENT -m -q "
DROP ROW POLICY IF EXISTS p_1 ON ${CLICKHOUSE_DATABASE}.data;
DROP ROW POLICY IF EXISTS p_2 ON ${CLICKHOUSE_DATABASE}.data;
DROP ROW POLICY IF EXISTS p_3 ON ${CLICKHOUSE_DATABASE}.data;
DROP TABLE IF EXISTS data_dist_dist;
DROP TABLE IF EXISTS data_dist_secret;
DROP TABLE IF EXISTS data_dist;
DROP TABLE IF EXISTS data;
DROP USER IF EXISTS ${USER};
DROP ROLE IF EXISTS ${ROLE_1}, ${ROLE_2}, ${ROLE_3};
"
