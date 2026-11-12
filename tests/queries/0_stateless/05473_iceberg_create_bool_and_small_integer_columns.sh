#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: IcebergLocal needs Avro and Parquet

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_iceberg_bool_types/"
PART_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_iceberg_bool_part/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_types"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_part"
    rm -rf "${TABLE_PATH}" "${PART_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_types (b Bool, nb Nullable(Bool), ab Array(Bool), mb Map(String, Bool), tb Tuple(x Bool),
        i8 Int8, u8 UInt8, i16 Int16, u16 UInt16, ni8 Nullable(Int8))
    ENGINE = IcebergLocal('${TABLE_PATH}')"

echo '--- Iceberg schema ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        JSONExtractInt(f, 'id') AS id,
        JSONExtractString(f, 'name') AS name,
        multiIf(
            JSONType(f, 'type') = 'String', JSONExtractString(f, 'type'),
            JSONExtractString(f, 'type', 'type') = 'list', concat('list<', JSONExtractString(f, 'type', 'element'), '>'),
            JSONExtractString(f, 'type', 'type') = 'map',
                concat('map<', JSONExtractString(f, 'type', 'key'), ', ', JSONExtractString(f, 'type', 'value'), '>'),
            JSONExtractString(f, 'type', 'type') = 'struct',
                concat('struct<', arrayStringConcat(arrayMap(x -> concat(JSONExtractString(x, 'name'), ': ', JSONExtractString(x, 'type')),
                    JSONExtractArrayRaw(f, 'type', 'fields')), ', '), '>'),
            '?') AS type,
        JSONExtractBool(f, 'required') AS required
    FROM (SELECT arrayJoin(JSONExtractArrayRaw(JSONExtractArrayRaw(json, 'schemas')[1], 'fields')) AS f
          FROM file('${TABLE_PATH}metadata/v1.metadata.json', 'RawBLOB', 'json String'))
    ORDER BY id
    FORMAT TSV"

echo '--- DESCRIBE ---'
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE t_types FORMAT TSV" | cut -f1,2

# One row, so that the manifest holds exactly one data file.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO t_types VALUES (true, NULL, [true, false], {'k': true}, tuple(false), -128, 255, -32768, 65535, NULL)"

echo '--- bounds: Bool is 1 byte, the widened integers are 4-byte little-endian int ---'
MANIFEST=$(find "${TABLE_PATH}metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f)
${CLICKHOUSE_CLIENT} --query "
    WITH
        tupleElement(data_file, 'lower_bounds') AS lower,
        tupleElement(data_file, 'upper_bounds') AS upper
    SELECT id, hex(arrayFirst(x -> x.1 = id, lower).2) AS lower_bound, hex(arrayFirst(x -> x.1 = id, upper).2) AS upper_bound
    FROM file('${MANIFEST}', Avro)
    ARRAY JOIN [1, 6, 7, 8, 9] AS id
    ORDER BY id
    FORMAT TSV"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO t_types VALUES (false, false, [], {}, tuple(true), 127, 0, 32767, 0, -1)"

echo '--- data ---'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_types ORDER BY i8 FORMAT TSV"

echo '--- Parquet physical types ---'
${CLICKHOUSE_LOCAL} --query "
    SELECT DISTINCT col.name, col.physical_type, col.logical_type
    FROM (SELECT arrayJoin(columns) AS col FROM file('${TABLE_PATH}data/*.parquet', ParquetMetadata))
    WHERE col.name IN ('b', 'i8', 'u8', 'i16', 'u16')
    ORDER BY 1
    FORMAT TSV"

echo '--- ADD COLUMN ---'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE t_types ADD COLUMN nb2 Nullable(Bool)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE t_types ADD COLUMN ni16 Nullable(Int16)"
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE t_types FORMAT TSV" | cut -f1,2 | grep -E '^(nb2|ni16)\b'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO t_types (b, i8, u8, i16, u16, nb2, ni16) VALUES (true, 1, 1, 1, 1, true, -32768)"
${CLICKHOUSE_CLIENT} --query "SELECT i8, nb2, ni16 FROM t_types ORDER BY i8 FORMAT TSV"

echo '--- Bool partitions ---'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_part (id Int32, b Bool, nb Nullable(Bool)) ENGINE = IcebergLocal('${PART_PATH}') PARTITION BY (b, nb)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO t_part VALUES (1, true, NULL), (2, false, true), (3, true, true)"
for manifest in $(find "${PART_PATH}metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f); do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT tupleElement(data_file, 'partition') AS p, toTypeName(p) FROM file('${manifest}', Avro) FORMAT TSV"
done | sort
${CLICKHOUSE_CLIENT} --query "
    SELECT
        (SELECT count() FROM t_part WHERE b),
        (SELECT count() FROM t_part WHERE NOT b),
        (SELECT count() FROM t_part WHERE nb),
        (SELECT count() FROM t_part WHERE nb IS NULL)
    FORMAT TSV"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE t_part DROP PARTITION (false, true)"
${CLICKHOUSE_CLIENT} --query "SELECT id FROM t_part ORDER BY id FORMAT TSV"
