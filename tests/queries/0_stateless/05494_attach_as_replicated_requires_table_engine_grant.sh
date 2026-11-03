#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# no-replicated-database: `ATTACH TABLE ... AS [NOT] REPLICATED` is supported only for `Atomic` and `Ordinary` databases.
# no-shared-merge-tree: the tables must keep the engines they are created with.

# `ATTACH TABLE ... AS [NOT] REPLICATED` requires the `TABLE ENGINE` grant on the engine the table is converted to,
# and a refused conversion leaves the stored definition of the detached table as it was.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table converted to `ReplicatedMergeTree` has no metadata in ZooKeeper and logs a warning when it is attached.
CLIENT="${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null"
user="user_05494_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLIENT} --query "DROP USER IF EXISTS ${user}"
}

trap cleanup EXIT
cleanup

${CLIENT} <<EOF
CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mt VALUES (1);
CREATE TABLE rpl (x UInt64) ENGINE = ReplacingMergeTree ORDER BY x;
CREATE TABLE rmt (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt', 'r1') ORDER BY x;
DETACH TABLE mt;
DETACH TABLE rpl;
DETACH TABLE rmt;

CREATE USER ${user};
GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${user};
EOF

# Prints the grant that the refused query asks for.
function attach_as_user_denied()
{
    ${CLIENT} --user "${user}" --query "$1" 2>&1 | grep -o -m1 "TABLE ENGINE ON [A-Za-z]*"
}

echo "-- without TABLE ENGINE grants, both directions are refused"
attach_as_user_denied "ATTACH TABLE mt AS REPLICATED"
attach_as_user_denied "ATTACH TABLE rpl AS REPLICATED"
attach_as_user_denied "ATTACH TABLE rmt AS NOT REPLICATED"

echo "-- the refused queries keep the stored definitions"
${CLIENT} <<EOF
ATTACH TABLE mt;
ATTACH TABLE rpl;
ATTACH TABLE rmt;
SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name;
DETACH TABLE mt;
DETACH TABLE rpl;
DETACH TABLE rmt;
EOF

echo "-- TABLE ENGINE ON ReplicatedMergeTree allows only the conversion to ReplicatedMergeTree"
${CLIENT} --query "GRANT TABLE ENGINE ON ReplicatedMergeTree TO ${user}"
attach_as_user_denied "ATTACH TABLE rpl AS REPLICATED"
attach_as_user_denied "ATTACH TABLE rmt AS NOT REPLICATED"
${CLIENT} --user "${user}" --query "ATTACH TABLE mt AS REPLICATED"
${CLIENT} --query "SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name"
${CLIENT} --query "SELECT count() FROM mt"

echo "-- TABLE ENGINE ON MergeTree allows the conversion back"
${CLIENT} --query "DETACH TABLE mt"
${CLIENT} --query "GRANT TABLE ENGINE ON MergeTree TO ${user}"
${CLIENT} --user "${user}" --query "ATTACH TABLE mt AS NOT REPLICATED"
${CLIENT} --query "SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name"
${CLIENT} --query "SELECT count() FROM mt"

${CLIENT} <<EOF
ATTACH TABLE rpl;
ATTACH TABLE rmt;
DROP TABLE mt SYNC;
DROP TABLE rpl SYNC;
DROP TABLE rmt SYNC;
EOF
