#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FILE="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sql"
trap 'rm -f -- "$FILE"' EXIT
cat > "$FILE" <<'SQL'
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2);
SQL
# Keep the file older than the cache entry so modification-time checks allow reuse.
touch -d '2000-01-01 00:00:00' "$FILE"

${CLICKHOUSE_CLIENT} --multiquery --query "
SET input_format_mysql_dump_table_name='src', input_format_mysql_dump_map_column_names=1,
    input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1,
    schema_inference_hints='', schema_inference_use_cache_for_file=1,
    input_format_max_rows_to_read_for_schema_inference=100,
    input_format_max_bytes_to_read_for_schema_inference=1048576;
DESCRIBE TABLE file('${CLICKHOUSE_TEST_UNIQUE_NAME}.sql', MySQLDump);
SET input_format_mysql_dump_map_column_names=0;
DESCRIBE TABLE file('${CLICKHOUSE_TEST_UNIQUE_NAME}.sql', MySQLDump);
SELECT a,b FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}.sql', MySQLDump) ORDER BY a;
SET input_format_mysql_dump_map_column_names=1;
SELECT a,b FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}.sql', MySQLDump) ORDER BY a;
"
