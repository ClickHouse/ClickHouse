#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 130112: an UPDATE with a non-deterministic condition neither loses nor duplicates rows. An UPDATE of a partition
# column selects its rows twice, so it refuses such a condition.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}/t"
PARTITIONED_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}/tp"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t (id Int32, v Int32) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    INSERT INTO t SELECT number, 0 FROM numbers(1000);
    ALTER TABLE t UPDATE v = 1 WHERE rand() % 2 = 0;
    CREATE TABLE tp (p Int32, id Int32, v Int32) ENGINE = IcebergLocal('${PARTITIONED_PATH}', 'Parquet') PARTITION BY p;
    INSERT INTO tp SELECT number % 4, number, 0 FROM numbers(1000);
    ALTER TABLE tp UPDATE v = 1 WHERE rand() % 2 = 0;"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(id), countIf(v = 1) BETWEEN 1 AND 999 FROM t"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(id), countIf(v = 1) BETWEEN 1 AND 999, countIf(p != id % 4) FROM tp"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE t UPDATE v = 2 WHERE id IN (SELECT number FROM numbers(1000) WHERE rand() % 2 = 0)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(id), countIf(v = 2) BETWEEN 1 AND 999 FROM t"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE tp UPDATE p = p + 4 WHERE rand() % 2 = 0" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE tp UPDATE p = p + 4 WHERE id IN (SELECT number FROM numbers(1000) WHERE rand() % 2 = 0)" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE tp UPDATE p = p + 4 WHERE arrayReduce('groupArraySample(1)', [id, id + 1])[1] % 2 = 0" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE tp UPDATE p = p + 4 WHERE arrayReduceInRanges('groupArraySample(1)', [(1, 2)], [id, id + 1])[1][1] % 2 = 0" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(id), max(p), countIf(v = 1) BETWEEN 1 AND 999 FROM tp"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE tp UPDATE p = p + 4 WHERE arrayReduce('groupArraySample(1, 42)', [id, id + 1])[1] % 2 = 0"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(id), countIf(p >= 4), countIf(p % 4 != id % 4) FROM tp"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
${CLICKHOUSE_CLIENT} --query "DROP TABLE tp"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
