#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Iceberg writes.

# Row lineage of format version 3 Iceberg tables with ORC and Avro data files that do not store it: `_row_id` is
# inherited from `first_row_id` + the position in the file, `_last_updated_sequence_number` from the data sequence number.
# https://github.com/ClickHouse/ClickHouse/issues/130032

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for format in ORC Avro; do
    TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_row_lineage_${format}"
    TABLE="t_row_lineage_${format}"
    rm -rf "${TABLE_PATH}"

    # One data file per INSERT, written in `id` order: `first_row_id` 0 and 1000, data sequence numbers 1 and 2.
    ${CLICKHOUSE_CLIENT} --query "
        SET allow_insert_into_iceberg = 1;
        CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', '${format}') SETTINGS iceberg_format_version = 3;
        INSERT INTO ${TABLE} SELECT number FROM numbers(1000) SETTINGS max_insert_threads = 1;
        INSERT INTO ${TABLE} SELECT number + 1000 FROM numbers(1000) SETTINGS max_insert_threads = 1;
    "
    echo "${format}" "$(ls "${TABLE_PATH}/data" | wc -l)"

    # Small batches, so that the rows of all chunks but the first are at an offset in the file.
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), countIf(_row_id = id), countIf(_row_number = id % 1000), min(_last_updated_sequence_number), max(_last_updated_sequence_number)
        FROM ${TABLE} SETTINGS max_block_size = 37, input_format_orc_row_batch_size = 37
    "
    # A filter on the inherited `_row_id` must not lose rows when the data files are pruned by the manifest.
    ${CLICKHOUSE_CLIENT} --query "SELECT id, _row_id FROM ${TABLE} WHERE _row_id IN (5, 1234) ORDER BY id SETTINGS use_iceberg_partition_pruning = 1"

    # Both data files now have an older schema than the table, so they are read through the schema transformation.
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} ADD COLUMN v Nullable(String)"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(_row_id = id), countIf(v IS NULL) FROM ${TABLE}"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
    rm -rf "${TABLE_PATH}"
done

TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_row_lineage_orc_row_groups"
TABLE="t_row_lineage_orc_row_groups"
rm -rf "${TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'ORC') SETTINGS iceberg_format_version = 3;
    INSERT INTO ${TABLE} SELECT number FROM numbers(30000) SETTINGS max_insert_threads = 1;
    INSERT INTO ${TABLE} SELECT number + 30000 FROM numbers(30000) SETTINGS max_insert_threads = 1;
"
echo "ORC files of 30000 rows" "$(ls "${TABLE_PATH}/data" | wc -l)"

# Rows of 10000-row row groups: the filter skips the first row group of the second file, so the rows read after it
# must keep their position in the file.
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), countIf(_row_id = id), countIf(_row_number = id % 30000), min(id), max(id)
    FROM ${TABLE} WHERE id BETWEEN 41000 AND 41010 OR id BETWEEN 55000 AND 55010
    SETTINGS input_format_orc_filter_push_down = 1, input_format_orc_row_batch_size = 37
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
