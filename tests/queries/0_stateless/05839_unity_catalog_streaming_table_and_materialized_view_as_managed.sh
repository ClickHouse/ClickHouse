#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# - no-fasttest: `DataLakeCatalog` requires the USE_AVRO and USE_PARQUET build options.
# - no-msan: delta-kernel-rs is not built with MSan, and a readable Delta table needs it.

# A streaming table or a materialized view of a Unity catalog gets its location from a second request
# with `read_streaming_table_as_managed` and `read_materialized_view_as_managed`. When Databricks answers
# HTTP 400 to that request, the table keeps its columns and is not readable. Both Unity implementations.

# The failing queries are logged by the server, keep it off the client's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./unity_catalog_mock.lib
. "$CUR_DIR"/unity_catalog_mock.lib

start_unity_catalog_mock "
# The default description of these table types has no location and no format.
streaming_table = {'name': 'st', 'table_id': 'id-st', 'table_type': 'STREAMING_TABLE', 'columns': [integer_column]}
materialized_view = {'name': 'mv', 'table_id': 'id-mv', 'table_type': 'MATERIALIZED_VIEW',
                     'securable_kind': 'TABLE_MATERIALIZED_VIEW', 'columns': [integer_column]}

# Only the streaming table has a managed description.
streaming_table_as_managed = {'name': 'st', 'table_id': 'id-st', 'table_type': 'MANAGED', 'data_source_format': 'DELTA',
                              'storage_location': 's3://bucket/st', 'columns': [integer_column]}
not_managed_error = {'error_code': 'INVALID_PARAMETER_VALUE', 'message': 'mock: no managed description'}

routes = {
    '/schemas': {'schemas': [{'name': 's', 'catalog_name': 'wh', 'full_name': 'wh.s'}]},
    '/tables': {'tables': [streaming_table, materialized_view]},
    '/tables/wh.s.st': streaming_table,
    ('/tables/wh.s.st', 'as_managed'): streaming_table_as_managed,
    '/tables/wh.s.mv': materialized_view,
    ('/tables/wh.s.mv', 'as_managed'): (400, not_managed_error),
}
"

for v2 in 0 1; do
    echo "use_unity_catalog_v2 = ${v2}"
    DB="${CLICKHOUSE_DATABASE}_unity_${v2}"
    create_unity_database "$DB" "$v2"

    # The managed description gives the location.
    ${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${DB}.\`s.st\` FORMAT TSVRaw" 2>&1 | grep -o -E "\`c\` Nullable\(Int32\)|ENGINE = DeltaLake\('s3://bucket/st'"

    # Without the managed description, the columns of the default one remain.
    ${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${DB}.\`s.mv\` FORMAT TSVRaw" 2>&1 | grep -o -E '`c` Nullable\(Int32\)|ENGINE = Other'
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${DB}.\`s.mv\`" 2>&1 | grep -o -m1 "table_type 'MATERIALIZED_VIEW' and Databricks does not describe it as a managed table"

    # Only the table with the managed description is listed.
    ${CLICKHOUSE_CLIENT} --show_data_lake_catalogs_in_system_tables=1 --query "SHOW TABLES FROM ${DB}" 2>&1
done
