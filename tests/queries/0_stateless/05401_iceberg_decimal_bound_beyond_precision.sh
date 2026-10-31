#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_dec_beyond"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

# Decimal arithmetic checks the range of the storage type, not the precision, so a data file can hold
# a value beyond the precision of its column, and min/max pruning must not change what a filter returns.
# One `INSERT` per data file; background Iceberg compaction would rewrite the manifests.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --query "
CREATE TABLE d9 (d Decimal(9, 2)) ENGINE = IcebergLocal('${ROOT}/d9/', 'Parquet')
SETTINGS allow_experimental_iceberg_compaction = 0;
INSERT INTO d9 SELECT arrayJoin([toDecimal32('1.00', 2), toDecimal32('9000000.00', 2) * 2]);
INSERT INTO d9 SELECT arrayJoin([toDecimal32('-1.00', 2), toDecimal32('-9000000.00', 2) * 2]);
INSERT INTO d9 SELECT toDecimal32('5.00', 2);
CREATE TABLE d38 (d Decimal(38, 38)) ENGINE = IcebergLocal('${ROOT}/d38/', 'Parquet')
SETTINGS allow_experimental_iceberg_compaction = 0;
INSERT INTO d38 SELECT arrayJoin([toDecimal128('0.1', 38), toDecimal128('0.8', 38) + toDecimal128('0.8', 38)]);
INSERT INTO d38 SELECT toDecimal128('0.5', 38) + toDecimal128('0.5', 38);
CREATE TABLE w38 (d Decimal(38, 38)) ENGINE = IcebergLocal('${ROOT}/w38/', 'Parquet')
SETTINGS allow_experimental_iceberg_compaction = 0;
INSERT INTO w38 SELECT toDecimal128('0.99', 38);
INSERT INTO w38 SELECT toDecimal128('-0.99', 38);
INSERT INTO w38 SELECT toDecimal128('0.7', 38);
INSERT INTO w38 SELECT toDecimal128('-0.70141183460469231731687303715884105728', 38);
CREATE TABLE w9 (d Decimal(9, 9)) ENGINE = IcebergLocal('${ROOT}/w9/', 'Parquet')
SETTINGS allow_experimental_iceberg_compaction = 0;
INSERT INTO w9 SELECT toDecimal32('0.7', 9);
" < /dev/null

echo '--- the data files hold values beyond the precision ---'
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    SELECT min(d), max(d) FROM d9;
    SELECT max(d) FROM d38;"

echo '--- a filter returns every matching row ---'
# `d38`'s second file holds exactly `10^38` unscaled, the largest bound rounding can produce.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    SELECT count() FROM d9 WHERE d > CAST('9999999.99' AS Decimal(9, 2));
    SELECT count() FROM d9 WHERE d < CAST('-9999999.99' AS Decimal(9, 2));
    SELECT count() FROM d38 WHERE d = toDecimal128('0.8', 38) + toDecimal128('0.8', 38);
    SELECT count() FROM d38 WHERE d = toDecimal128('0.5', 38) + toDecimal128('0.5', 38);"

echo '--- a bound widened past the precision is no endpoint for a monotonic function ---'
# `negate` and `round` overflow past the precision. The outer bounds of the `0.99` files leave `Int128`,
# the last `w38` file's lower bound widens to exactly its minimum, and `0.7` widens to `1.7` in both types.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    SELECT count() FROM w38 WHERE negate(d) = toDecimal128('0.99', 38);
    SELECT count() FROM w38 WHERE negate(d) = toDecimal128('0.70141183460469231731687303715884105728', 38);
    SELECT count() FROM w38 WHERE round(d) = round(toDecimal128('0.99', 38));
    SELECT count() FROM w38 WHERE round(d) = round(toDecimal128('-0.99', 38));
    SELECT count() FROM w9 WHERE round(d) = round(toDecimal32('0.7', 9));"

echo '--- bounds inside the precision still prune ---'
# Only the file holding `5.00` cannot hold `0`, and only the two negative `w38` files cannot hold `0.5`.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    SELECT count() FROM d9 WHERE d = CAST('0.00' AS Decimal(9, 2)) SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_c1';
    SELECT count() FROM w38 WHERE d > toDecimal128('0.5', 38) SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_c2';
    SYSTEM FLUSH LOGS query_log;
    SELECT max(ProfileEvents['IcebergMinMaxIndexPrunedFiles']) FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('${CLICKHOUSE_DATABASE}_c1', '${CLICKHOUSE_DATABASE}_c2')
    GROUP BY log_comment ORDER BY log_comment
    SETTINGS enable_parallel_replicas = 0;"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS d9 SYNC;
    DROP TABLE IF EXISTS d38 SYNC;
    DROP TABLE IF EXISTS w38 SYNC;
    DROP TABLE IF EXISTS w9 SYNC;"
