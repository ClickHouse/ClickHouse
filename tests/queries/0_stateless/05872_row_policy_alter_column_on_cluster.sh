#!/usr/bin/env bash
# Tags: no-replicated-database, no-fasttest
# Tag no-replicated-database: a Replicated database refuses an ALTER that ON CLUSTER sends to its hosts
# Tag no-fasttest: needs ZooKeeper for ON CLUSTER
# the hosts of ON CLUSTER run the ALTER without the user, so they refuse to analyze a row policy without the grants of the user

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE
DDL_USER="u_${CLICKHOUSE_DATABASE}"
FILE="${CLICKHOUSE_DATABASE}_ids.tsv"
echo 1 > "$USER_FILES_PATH/$FILE"

refused() { $CLICKHOUSE_CLIENT "${@:3}" -q "$1" 2>&1 | grep -o "$2" | head -1; }

$CLICKHOUSE_CLIENT --multiquery -q "
DROP USER IF EXISTS $DDL_USER;
CREATE USER $DDL_USER;
CREATE TABLE tfn (id UInt32, extra UInt8, other UInt8) ENGINE = MergeTree ORDER BY id;
CREATE TABLE plain (id UInt32, extra UInt8) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pfn ON $DB.tfn FOR SELECT USING id > length(file('$FILE')) TO CURRENT_USER;
GRANT ALTER DROP COLUMN, ALTER ADD COLUMN ON $DB.* TO $DDL_USER;
GRANT CLUSTER ON *.* TO $DDL_USER;
"

echo "-- a host of ON CLUSTER does not analyze a policy with the full access of the server"
refused "ALTER TABLE $DB.tfn ON CLUSTER test_shard_localhost DROP COLUMN extra" ACCESS_DENIED --user "$DDL_USER"

echo "-- also not for a user that has the grants"
$CLICKHOUSE_CLIENT -q "GRANT READ ON FILE TO $DDL_USER"
refused "ALTER TABLE $DB.tfn ON CLUSTER test_shard_localhost DROP COLUMN extra" ACCESS_DENIED --user "$DDL_USER"
$CLICKHOUSE_CLIENT --user "$DDL_USER" -q "ALTER TABLE $DB.tfn DROP COLUMN extra"

echo "-- ON CLUSTER of a table without a policy, or of a command that does not drop, rename or modify a column"
$CLICKHOUSE_CLIENT --user "$DDL_USER" -q "ALTER TABLE $DB.plain ON CLUSTER test_shard_localhost DROP COLUMN extra" > /dev/null
$CLICKHOUSE_CLIENT --user "$DDL_USER" -q "ALTER TABLE $DB.tfn ON CLUSTER test_shard_localhost ADD COLUMN added UInt8" > /dev/null
$CLICKHOUSE_CLIENT -q "SELECT table, name FROM system.columns WHERE database = '$DB' ORDER BY table, position"

$CLICKHOUSE_CLIENT --multiquery -q "
DROP ROW POLICY pfn ON $DB.tfn;
DROP USER $DDL_USER;
"
rm -f "${USER_FILES_PATH:?}/$FILE"
