#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# - no-fasttest: `DataLakeCatalog` requires the USE_AVRO and USE_PARQUET build options.
# - no-msan: delta-kernel-rs is not built with MSan, so Unity catalog Delta tables are unreadable there.

# A Unity catalog column whose `type_json` is a single `"` is a malformed catalog response.

# The failing schema is logged by the server, keep it off the client's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./unity_catalog_mock.lib
. "$CUR_DIR"/unity_catalog_mock.lib

start_unity_catalog_mock "
routes = {
    '/tables/wh.s.t': {'name': 't', 'data_source_format': 'DELTA', 'storage_location': 's3://bucket/t',
                       'columns': [{'name': 'c', 'nullable': True, 'type_json': '\"'}]},
}
"

DB="${CLICKHOUSE_DATABASE}_unity"
create_unity_database "$DB"
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE ${DB}.\`s.t\`" 2>&1 | grep -o -m1 -E 'Unsupported DeltaLake type|STD_EXCEPTION|out_of_range'
