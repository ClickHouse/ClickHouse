#!/usr/bin/env bash
# Tags: no-fasttest

# An INSERT planned before ADD COLUMN commits a data file without the added column under the widened
# schema. Iceberg reads a field missing from a data file as NULL, in every format and whatever the
# generic `input_format_*_allow_missing_*` settings say, so they are all disabled here.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TABLE_DIR}"

LOCAL="${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0 --allow_experimental_iceberg_compaction=1
    --input_format_avro_allow_missing_fields=0 --input_format_orc_allow_missing_columns=0 --input_format_parquet_allow_missing_columns=0"

for fmt in Avro ORC Parquet; do
    TABLE_PATH="${TABLE_DIR}/${fmt}/"
    mkdir -p "${TABLE_PATH}"

    ${LOCAL} --query "
        CREATE TABLE t (a Int32, c Int64) ENGINE = IcebergLocal('${TABLE_PATH}', '${fmt}');
        INSERT INTO t SELECT number, number FROM numbers(3);
    "

    PIN="metadata/$(basename "$(ls "${TABLE_PATH}metadata"/*.metadata.json | sort -V | tail -1)")"
    ${LOCAL} --query "
        CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', '${fmt}');
        ALTER TABLE t ADD COLUMN d Nullable(Int64);
    "

    # The structure comes from the version before ADD COLUMN, the commit goes to the widened table.
    ${LOCAL} --query "
        CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', '${fmt}') SETTINGS iceberg_metadata_file_path = '${PIN}';
        INSERT INTO t SELECT number + 10, number + 10 FROM numbers(3);
    "

    ${LOCAL} --query "
        CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', '${fmt}');
        SELECT '${fmt}', 'rows', arraySort(groupArray(a)), countIf(d IS NULL) FROM t;
        SELECT '${fmt}', 'where_null', count() FROM t WHERE d IS NULL;
        SELECT '${fmt}', 'where_eq', count() FROM t WHERE d = 1;
        SELECT '${fmt}', 'star', * FROM t ORDER BY a;
    "
done

# Compaction rewrites data files only when there are position deletes, and only Parquet tables accept DELETE.
TABLE_PATH="${TABLE_DIR}/Parquet/"
${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    ALTER TABLE t DELETE WHERE a = 10;
"
${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    OPTIMIZE TABLE t;
"
${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    SELECT 'after_optimize', count(), countIf(d IS NULL), sum(c) FROM t;
    SELECT 'files_after_optimize', countIf(content = 'DATA'), countIf(content != 'DATA')
    FROM system.iceberg_files WHERE database = currentDatabase() AND table = 't';
"

# With missing fields tolerated, a field stored under its escaped Avro name is read by its Iceberg name, not as a default.
${CLICKHOUSE_LOCAL} --input_format_avro_allow_missing_fields=1 --query "
    SELECT 'escaped', id, \`name.column\`
    FROM file('${CURDIR}/data_avro/iceberg_escaped_field_name.avro', 'Avro', 'id Int32, \`name.column\` String') ORDER BY id"

# A field resolved directly wins over an escaped-name alias, and one column is filled from one field only.
${CLICKHOUSE_LOCAL} --input_format_avro_allow_missing_fields=0 --query "
    SELECT 'collision', y, \`n.c\`, z
    FROM file('${CURDIR}/data_avro/iceberg_field_name_collision.avro', 'Avro', 'y String, \`n.c\` String, z String') ORDER BY y"

rm -rf "${TABLE_DIR}"
