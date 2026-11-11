#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.
# Tag no-replicated-database: the replica of the other shard applies each `ALTER` again to the same table directory.

# https://github.com/ClickHouse/ClickHouse/issues/118905
# A `truncate[W]` partition field of an Iceberg `int` column is an `int` (4-byte bounds), and values near -2147483648
# are stored as the 32-bit wraparound that Iceberg Java and iceberg-rust compute.
# Fixture `iceberg_truncate_int_legacy_long`: written by ClickHouse 26.10.2.103, `(id int, x int)` partitioned by
# `truncate[10](x)`: INSERT (1, -2147483648), INSERT (2, -2147483648), (3, -2147483648), DELETE WHERE id = 3, so the
# partition has two data files and a position delete file, all recorded as `long` -2147483650.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0"
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

manifests()
{
    find "$1/metadata" -maxdepth 1 -name '*.avro' ! -name 'snap-*' -printf '%f\n' | sort | paste -sd, -
}

# Declared partition field types, stored values with their record counts, and bound widths of the manifest lists.
describe()
{
    local table_path="${LAKE}/$1"
    ${CH} --query "
        SELECT 'types', groupUniqArray(toTypeName(tupleElement(data_file.partition, 1)))
        FROM file('${table_path}/metadata/{$(manifests "${table_path}")}', Avro) WHERE data_file.content = $2;
        SELECT 'values', groupArray((value, records)) FROM (
            SELECT tupleElement(data_file.partition, 1) AS value, sum(data_file.record_count) AS records
            FROM file('${table_path}/metadata/{$(manifests "${table_path}")}', Avro) WHERE data_file.content = $2 AND status != 2
            GROUP BY value ORDER BY value);
        SELECT 'bounds', arraySort(groupUniqArray((length(p.lower_bound), length(p.upper_bound))))
        FROM file('${table_path}/metadata/snap-*.avro', Avro) ARRAY JOIN partitions AS p WHERE p.lower_bound IS NOT NULL"
}

echo '--- truncate[10] of Int32'
${CH} --query "
    CREATE TABLE t_int (id Int32, x Int32) ENGINE = IcebergLocal('${LAKE}/t_int', 'Parquet') PARTITION BY icebergTruncate(10, x);
    INSERT INTO t_int VALUES (1, -11), (2, -1), (3, 0), (4, 5), (5, 15), (6, 2147483647), (7, -2147483648);"
describe t_int 0

echo '--- truncate[10] of Nullable(Int32)'
${CH} --query "
    CREATE TABLE t_nullable (id Int32, x Nullable(Int32))
    ENGINE = IcebergLocal('${LAKE}/t_nullable', 'Parquet') PARTITION BY icebergTruncate(10, x);
    INSERT INTO t_nullable VALUES (1, NULL), (2, -3);"
describe t_nullable 0

echo '--- truncate[10] of Int16, an Iceberg int'
${CH} --query "
    CREATE TABLE t_int16 (id Int32, x Int16) ENGINE = IcebergLocal('${LAKE}/t_int16', 'Parquet') PARTITION BY icebergTruncate(10, x);
    INSERT INTO t_int16 VALUES (1, -32768), (2, 7);"
describe t_int16 0

echo '--- truncate[2^30] of Int32'
${CH} --query "
    CREATE TABLE t_wide (id Int32, x Int32)
    ENGINE = IcebergLocal('${LAKE}/t_wide', 'Parquet') PARTITION BY icebergTruncate(1073741824, x);
    INSERT INTO t_wide VALUES (1, -2147483648), (2, 5), (3, 1073741824);"
describe t_wide 0

echo '--- truncate[2^30 + 1] of Int32 is refused, of Int64 is not'
${CH} --query "
    CREATE TABLE t_too_wide (id Int32, x Int32)
    ENGINE = IcebergLocal('${LAKE}/t_too_wide', 'Parquet') PARTITION BY icebergTruncate(1073741825, x);
    CREATE TABLE t_too_wide_long (id Int32, x Int64)
    ENGINE = IcebergLocal('${LAKE}/t_too_wide_long', 'Parquet') PARTITION BY icebergTruncate(1073741825, x);"
${CH} --query "INSERT INTO t_too_wide VALUES (1, 1073741824)" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
${CH} --query "SELECT count() FROM t_too_wide"
${CH} --query "INSERT INTO t_too_wide_long VALUES (1, 1073741824)"
${CH} --query "SELECT count() FROM t_too_wide_long"

echo '--- truncate[10] of Int64 and truncate[3] of String keep their types'
${CH} --query "
    CREATE TABLE t_long (id Int32, x Int64) ENGINE = IcebergLocal('${LAKE}/t_long', 'Parquet') PARTITION BY icebergTruncate(10, x);
    INSERT INTO t_long VALUES (1, -2147483648), (2, 5);
    CREATE TABLE t_string (id Int32, s String) ENGINE = IcebergLocal('${LAKE}/t_string', 'Parquet') PARTITION BY icebergTruncate(3, s);
    INSERT INTO t_string VALUES (1, 'abcdef'), (2, 'xy');"
describe t_long 0
describe t_string 0

echo '--- reads with partition pruning'
for predicate in '1' 'x = -1' 'x = 5' 'x IN (-11, 15)' 'x BETWEEN -5 AND 7' 'x = -2147483648' 'x IN (-2147483648, 5)' 'x <= -2147483641'; do
    ${CH} --use_iceberg_partition_pruning=1 --query "
        SELECT '${predicate}', groupArray(id) FROM (SELECT id FROM t_int WHERE ${predicate} ORDER BY id)"
done

echo '--- DELETE and UPDATE'
${CH} --query "
    ALTER TABLE t_int DELETE WHERE x = 5;
    ALTER TABLE t_int UPDATE id = 20 WHERE x = -1;"
describe t_int 1
${CH} --query "SELECT id, x FROM t_int ORDER BY id"
${CH} --use_iceberg_partition_pruning=1 --query "SELECT groupArray(id) FROM (SELECT id FROM t_int WHERE x IN (-1, 5) ORDER BY id)"

echo '--- legacy table with a long partition value outside the int range'
cp -r "${CUR_DIR}/data_minio/iceberg_truncate_int_legacy_long" "${LAKE}/legacy"
${CH} --query "CREATE TABLE legacy ENGINE = IcebergLocal('${LAKE}/legacy', 'Parquet')"
legacy_state()
{
    ${CH} --query "
        SELECT count(), groupArray(id) FROM (SELECT id FROM legacy ORDER BY id);
        SELECT count() FROM file('${LAKE}/legacy/metadata/*.metadata.json', 'JSONAsString')"
}
legacy_state
# The cloud build runs `OPTIMIZE` as a background compaction, which does not return its error.
if [[ "${IS_CLOUD}" = "1" ]]; then
    echo 'ICEBERG_SPECIFICATION_VIOLATION'
else
    ${CH} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE legacy" 2>&1 \
        | grep -o -m1 'ICEBERG_SPECIFICATION_VIOLATION'
fi
${CH} --allow_experimental_iceberg_compaction=1 \
    --query "OPTIMIZE TABLE legacy MANIFEST SETTINGS iceberg_manifest_min_count_to_compact = 1" 2>&1 \
    | grep -o -m1 'ICEBERG_SPECIFICATION_VIOLATION'
${CH} --query "ALTER TABLE legacy DELETE WHERE x = -2147483648" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
${CH} --query "ALTER TABLE legacy UPDATE id = 3 WHERE id = 1" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
legacy_state
${CH} --query "INSERT INTO legacy VALUES (4, 5)"
${CH} --use_iceberg_partition_pruning=1 --query "
    SELECT groupArray(id) FROM (SELECT id FROM legacy WHERE x = -2147483648 ORDER BY id);
    SELECT groupArray(id) FROM (SELECT id FROM legacy WHERE x = 5 ORDER BY id)"

echo '--- manifest-list pruning'
${CH} --query "
    CREATE TABLE t_ml (id Int32, x Int32) ENGINE = IcebergLocal('${LAKE}/t_ml', 'Parquet') PARTITION BY icebergTruncate(10, x);
    INSERT INTO t_ml VALUES (1, 5);
    INSERT INTO t_ml VALUES (2, 25);
    INSERT INTO t_ml VALUES (3, -2147483648);"
n=0
for predicate in 'x = 5' 'x = 25' 'x = -2147483648'; do
    n=$((n + 1))
    ${CH} --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_ml_${n}" --use_iceberg_partition_pruning=1 --query "
        SELECT '${predicate}', groupArray(id) FROM (SELECT id FROM t_ml WHERE ${predicate} ORDER BY id)"
done
${CH} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT replaceOne(query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', ''), ProfileEvents['IcebergPartitionPrunedManifestFiles']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND query_id IN ('${CLICKHOUSE_TEST_UNIQUE_NAME}_ml_1', '${CLICKHOUSE_TEST_UNIQUE_NAME}_ml_2', '${CLICKHOUSE_TEST_UNIQUE_NAME}_ml_3')
    ORDER BY query_id"

echo '--- DELETE and UPDATE after promoting the int column to long'
# 2147483646, the key of -2147483648, is not what truncate[10] of a long computes, so mutations are refused.
${CH} --query "
    CREATE TABLE t_promoted (id Int32, x Int32)
    ENGINE = IcebergLocal('${LAKE}/t_promoted', 'Parquet') PARTITION BY icebergTruncate(10, x);
    INSERT INTO t_promoted VALUES (1, -2147483648);
    INSERT INTO t_promoted VALUES (2, 5);
    ALTER TABLE t_promoted MODIFY COLUMN x Int64;"
${CH} --query "ALTER TABLE t_promoted DELETE WHERE id = 1" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
${CH} --query "ALTER TABLE t_promoted UPDATE id = 3 WHERE id = 1" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
${CH} --query "SELECT id, x FROM t_promoted ORDER BY id"
${CH} --query "
    CREATE TABLE t_promoted_ok (id Int32, x Int32)
    ENGINE = IcebergLocal('${LAKE}/t_promoted_ok', 'Parquet') PARTITION BY icebergTruncate(10, x);
    INSERT INTO t_promoted_ok VALUES (1, -2147483638), (2, 5);
    ALTER TABLE t_promoted_ok MODIFY COLUMN x Int64;
    ALTER TABLE t_promoted_ok DELETE WHERE id = 1;
    ALTER TABLE t_promoted_ok UPDATE id = 3 WHERE id = 2;
    SELECT id, x FROM t_promoted_ok ORDER BY id;"

echo '--- DELETE and UPDATE of tables written by PyIceberg and Spark'
# https://github.com/ClickHouse/ClickHouse/issues/130669: fixtures of 05568, one copy per mutation.
WRAP="${CUR_DIR}/data_minio/iceberg_truncate_int32_wraparound"
for name in py_del_control py_del_wrapped py_upd_wrapped; do
    cp -r "${WRAP}/t" "${LAKE}/${name}"
done
cp -r "${WRAP}/promoted" "${LAKE}/py_promoted"
cp -r "${WRAP}/java_wide" "${LAKE}/java_wide"
for name in py_del_control py_del_wrapped py_upd_wrapped py_promoted java_wide; do
    ${CH} --query "CREATE TABLE ${name} ENGINE = IcebergLocal('${LAKE}/${name}')"
done
${CH} --query "
    DELETE FROM py_del_control WHERE id = 3;
    DELETE FROM py_del_wrapped WHERE id = 0;
    ALTER TABLE py_upd_wrapped UPDATE id = 100 WHERE id = 1;"
for name in py_del_control py_del_wrapped py_upd_wrapped; do
    ${CH} --query "SELECT '${name}', groupArray(id) FROM (SELECT id FROM ${name} ORDER BY id)"
done
${CH} --use_iceberg_partition_pruning=1 --query "
    SELECT groupArray(id) FROM (SELECT id FROM py_upd_wrapped WHERE x = -2147483641 ORDER BY id)"
# A wrapped key kept after `int` -> `long` promotion, and Java's truncate[1073741825] key: refused, rows unchanged.
${CH} --query "DELETE FROM py_promoted WHERE id = 0" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
${CH} --query "DELETE FROM java_wide WHERE id = 1" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
${CH} --query "
    SELECT groupArray(id) FROM (SELECT id FROM py_promoted ORDER BY id);
    SELECT groupArray(id) FROM (SELECT id FROM java_wide ORDER BY id)"

${CH} --query "
    DROP TABLE t_int; DROP TABLE t_nullable; DROP TABLE t_int16; DROP TABLE t_wide; DROP TABLE t_too_wide;
    DROP TABLE t_too_wide_long; DROP TABLE t_long; DROP TABLE t_string; DROP TABLE legacy; DROP TABLE t_ml;
    DROP TABLE t_promoted; DROP TABLE t_promoted_ok; DROP TABLE py_del_control; DROP TABLE py_del_wrapped;
    DROP TABLE py_upd_wrapped; DROP TABLE py_promoted; DROP TABLE java_wide;"
rm -rf "${LAKE}"
