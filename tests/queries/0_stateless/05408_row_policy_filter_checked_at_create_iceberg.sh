#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
#
# CREATE ROW POLICY analyzes its filter against the current schema of a data lake table, as a read does,
# not against the columns this table object saw at its last read. Row policies are dropped explicitly
# because they outlive the test database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_rp"
rm -rf "${ICEBERG_PATH}"

$CLICKHOUSE_CLIENT <<EOF
SET allow_experimental_insert_into_iceberg = 1;
CREATE TABLE ice_w (id Int64) ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');
INSERT INTO ice_w VALUES (1), (2);
CREATE TABLE ice_r ENGINE = IcebergLocal('${ICEBERG_PATH}');
SELECT 'before', count() FROM ice_r;
EOF

# The schema gains a column through the other table object.
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 -q "ALTER TABLE ice_w ADD COLUMN tenant Nullable(String)"

$CLICKHOUSE_CLIENT <<EOF
CREATE ROW POLICY p ON ice_r USING tenant IS NULL TO ALL;
SELECT 'after', count() FROM ice_r;
DROP ROW POLICY p ON ice_r;
CREATE ROW POLICY p ON ice_r USING nosuch = 1 TO ALL; -- { serverError UNKNOWN_IDENTIFIER }
SELECT 'left', count() FROM system.row_policies WHERE database = currentDatabase();
DROP TABLE ice_r;
DROP TABLE ice_w;
EOF

rm -rf "${ICEBERG_PATH}"
