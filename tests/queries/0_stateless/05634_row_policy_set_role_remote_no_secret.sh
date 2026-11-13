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

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS data_dist;
CREATE TABLE data (id UInt32, key UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO data VALUES (1, 1), (2, 2), (3, 1), (4, 3), (5, 2), (6, 1), (7, 4), (8, 2);
CREATE TABLE data_dist AS data ENGINE = Distributed(test_cluster_two_shards_localhost, ${CLICKHOUSE_DATABASE}, data);

DROP ROLE IF EXISTS ${ROLE_1}, ${ROLE_2};
CREATE ROLE ${ROLE_1};
CREATE ROLE ${ROLE_2};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.data TO ${ROLE_1}, ${ROLE_2};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.data_dist TO ${ROLE_1}, ${ROLE_2};
CREATE ROW POLICY p_1 ON ${CLICKHOUSE_DATABASE}.data FOR SELECT USING key = 1 TO ${ROLE_1};
CREATE ROW POLICY p_2 ON ${CLICKHOUSE_DATABASE}.data FOR SELECT USING key = 2 TO ${ROLE_2};
DROP USER IF EXISTS ${USER};
CREATE USER ${USER} IDENTIFIED WITH no_password;
GRANT ${ROLE_1}, ${ROLE_2} TO ${USER};
ALTER USER ${USER} DEFAULT ROLE ALL;
"

# Each shard is read remotely, so every row of the matching key is returned twice.
for role in "${ROLE_1}" "${ROLE_2}"
do
    echo "-- ${role#role_}" | sed "s/_${CLICKHOUSE_DATABASE}//"
    $CLICKHOUSE_CLIENT --user "${USER}" -m -q "
    SET ROLE ${role};
    SELECT key, count() FROM data_dist GROUP BY key ORDER BY key SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;
    "
done

$CLICKHOUSE_CLIENT -m -q "
DROP ROW POLICY IF EXISTS p_1 ON ${CLICKHOUSE_DATABASE}.data;
DROP ROW POLICY IF EXISTS p_2 ON ${CLICKHOUSE_DATABASE}.data;
DROP TABLE IF EXISTS data_dist;
DROP TABLE IF EXISTS data;
DROP USER IF EXISTS ${USER};
DROP ROLE IF EXISTS ${ROLE_1}, ${ROLE_2};
"
