#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# no-fasttest: S3Queue is not built in the fast test.
# zookeeper: the queue keeps its metadata in Keeper.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

engine="ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV) SETTINGS mode = 'unordered'"
attached_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
with_uuid_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

# A full-definition ATTACH unfolds the macros of `keeper_path` like CREATE does, so a later rename cannot move
# the queue to another Keeper path.
${CLICKHOUSE_CLIENT} --send_logs_level=error -m -q "
CREATE TABLE created (a UInt32) ${engine}, keeper_path = '/clickhouse/{database}/created';
ATTACH TABLE attached UUID '${attached_uuid}' (a UInt32) ${engine}, keeper_path = '/clickhouse/{database}/attached';
SELECT name, position(create_table_query, '{database}') = 0, position(create_table_query, currentDatabase()) > 0
FROM system.tables WHERE database = currentDatabase() ORDER BY name;
"

# RESTORE assigns the UUID of the table itself, so a {uuid} in keeper_path stays allowed.
${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE with_uuid UUID '${with_uuid_uuid}' (a UInt32) ${engine}, keeper_path = '/clickhouse/${CLICKHOUSE_TEST_UNIQUE_NAME}/{uuid}';
BACKUP TABLE with_uuid TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}') FORMAT Null;
RESTORE TABLE with_uuid AS restored FROM Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}') FORMAT Null;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'restored';
DROP TABLE created;
DROP TABLE attached;
DROP TABLE with_uuid;
DROP TABLE restored;
"
