#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: `DataLakeCatalog` requires the USE_AVRO and USE_PARQUET build options.

# A non-Delta table of an open-source Unity catalog, which sends no `securable_kind`, is described with
# engine `Other`, and reading it names its format. Both Unity implementations.

# The failing queries are logged by the server, keep it off the client's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./unity_catalog_mock.lib
. "$CUR_DIR"/unity_catalog_mock.lib

start_unity_catalog_mock "
routes = {
    '/tables/wh.s.t': {'name': 't', 'table_type': 'EXTERNAL', 'data_source_format': 'CSV',
                       'storage_location': 's3://bucket/t', 'columns': [integer_column]},
}
"

for v2 in 0 1; do
    DB="${CLICKHOUSE_DATABASE}_unity_${v2}"
    create_unity_database "$DB" "$v2"
    ${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${DB}.\`s.t\` FORMAT TSVRaw" 2>&1 | grep -o -E '`c` Nullable\(Int32\)|ENGINE = Other|POCO_EXCEPTION'
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${DB}.\`s.t\`" 2>&1 | grep -o -m1 -E "data_source_format 'CSV'|POCO_EXCEPTION"
done
